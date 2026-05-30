#
# Copyright (c) 2024-2026, Daily
#
# SPDX-License-Identifier: BSD 2-Clause License
#

"""On-call infrastructure triage voice bot.

The bot briefs a Datadog-shaped alert, inspects Kubernetes state with read-only
tools, and proposes one remediation for operator approval.

Pipeline: Nemotron Speech Streaming STT to Nemotron-3-Super-120B LLM to Gradium TTS, with direct
function tools registered on the LLM context.

Run the bot using::

    uv run bot-nemotron.py
"""

import json
import os
import re
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv
from loguru import logger
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.worker import PipelineParams, PipelineWorker
from pipecat.processors.aggregators.llm_context import LLMContext
from pipecat.processors.aggregators.llm_response_universal import (
    LLMContextAggregatorPair,
    LLMUserAggregatorParams,
)
from pipecat.runner.types import (
    DailyRunnerArguments,
    RunnerArguments,
    SmallWebRTCRunnerArguments,
    WebSocketRunnerArguments,
)
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.gradium.tts import GradiumTTSService
from pipecat.services.llm_service import FunctionCallParams
from pipecat.transports.base_transport import BaseTransport, TransportParams
from pipecat.transports.daily.transport import DailyParams, DailyTransport
from pipecat.transports.smallwebrtc.connection import SmallWebRTCConnection
from pipecat.transports.smallwebrtc.transport import SmallWebRTCTransport
from pipecat.transports.websocket.fastapi import FastAPIWebsocketParams, FastAPIWebsocketTransport
from pipecat.turns.user_turn_strategies import FilterIncompleteUserTurnStrategies
from pipecat.workers.runner import WorkerRunner

from nemotron_llm import VLLMOpenAILLMService
from nvidia_stt import NVidiaWebSocketSTTService

KELDRON_REPO = Path(__file__).resolve().parents[2] / "keldron-oncall"
KELDRON_TOOLS = KELDRON_REPO / "tools"
if str(KELDRON_TOOLS) not in sys.path:
    sys.path.insert(0, str(KELDRON_TOOLS))

import k8s_actions

DATADOG_ALERT_PATH = KELDRON_REPO / "fixtures" / "datadog-alert.json"
APPROVAL_PHRASES = {
    "approve",
    "approved",
    "yes",
    "yeah",
    "uh yes",
    "yes approve",
    "approve it",
    "uh yes approve it",
    "yes approve it",
    "yes please",
    "yep",
    "yup",
    "please do",
    "go ahead",
    "go ahead please",
    "go ahead and do it",
    "do it",
    "do it please",
    "proceed",
}

load_dotenv(override=True)


def _normalize_approval_text(text: str) -> str:
    """Normalize a voice or typed utterance for exact approval matching."""
    text = text.lower()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return " ".join(text.split())


def _last_user_text(params: FunctionCallParams) -> str:
    """Return the latest user message content from the LLM context."""
    for message in reversed(params.context.messages):
        if message.get("role") != "user":
            continue
        content = message.get("content", "")
        if isinstance(content, str):
            return content
    return ""


async def get_call_info(call_sid: str) -> dict:
    """Fetch call information from Twilio REST API using aiohttp.

    Args:
        call_sid: The Twilio call SID

    Returns:
        Dictionary containing call information including from_number, to_number, status, etc.
    """
    account_sid = os.environ["TWILIO_ACCOUNT_SID"]
    auth_token = os.environ["TWILIO_AUTH_TOKEN"]

    if not account_sid or not auth_token:
        logger.warning("Missing Twilio credentials, cannot fetch call info")
        return {}

    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls/{call_sid}.json"

    try:
        # Use HTTP Basic Auth with aiohttp
        auth = aiohttp.BasicAuth(account_sid, auth_token)

        async with aiohttp.ClientSession() as session:
            async with session.get(url, auth=auth) as response:
                if response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Twilio API error ({response.status}): {error_text}")
                    return {}

                data = await response.json()

                call_info = {
                    "from_number": data.get("from"),
                    "to_number": data.get("to"),
                }

                return call_info

    except Exception as e:
        logger.error(f"Error fetching call info from Twilio: {e}")
        return {}


async def run_bot(
    transport: BaseTransport,
    from_number: str | None = None,
    audio_in_sample_rate: int = 16000,
    audio_out_sample_rate: int = 24000,
):
    """Main bot logic.

    Args:
        transport: The transport to use.
        from_number: Caller's phone number on the Twilio path.
        audio_in_sample_rate: Input audio sample rate in Hz. Defaults to 16000 (WebRTC).
        audio_out_sample_rate: Output audio sample rate in Hz. Defaults to 24000 (WebRTC).
    """
    logger.info("Starting bot")

    with DATADOG_ALERT_PATH.open() as alert_file:
        alert = json.load(alert_file)

    affected_node = alert.get("hostname", "unknown")

    async def cluster_status(params: FunctionCallParams) -> None:
        """Return a read-only cluster snapshot of nodes and pods by node.

        Use this before recommending any remediation. This tool only reads
        Kubernetes state through keldron-oncall/tools/k8s_actions.py.
        """
        try:
            result = k8s_actions.cluster_status()
        except k8s_actions.KubectlError as exc:
            result = {"ok": False, "error": str(exc)}
        await params.result_callback(result)

    async def list_pods(params: FunctionCallParams, node: str | None = None) -> None:
        """Return default-namespace pods, optionally filtered to one node.

        Args:
            node: Kubernetes node name to inspect. Use the alert hostname when
                naming what is running on the affected node.
        """
        try:
            result = {"pods": k8s_actions.list_pods(node)}
        except k8s_actions.KubectlError as exc:
            result = {"ok": False, "error": str(exc)}
        await params.result_callback(result)

    async def drain_node(params: FunctionCallParams, node: str) -> None:
        """Drain a Kubernetes node using the real keldron-oncall kubectl wrapper.

        This is a real cluster action. Call it only after explicit approval.

        Args:
            node: Kubernetes node to drain after explicit approval.
        """
        approval = _normalize_approval_text(_last_user_text(params))
        if approval not in APPROVAL_PHRASES:
            logger.warning(
                "Refusing drain_node for node {} because last user utterance was {!r}",
                node,
                approval,
            )
            await params.result_callback(
                {
                    "ok": False,
                    "node": node,
                    "drained": False,
                    "error": "Approval was not explicit.",
                    "message": "I need a clear approve before I drain the node.",
                }
            )
            return

        logger.warning("Executing real drain_node action for node {}", node)
        try:
            result = {"ok": True, **k8s_actions.drain_node(node)}
        except k8s_actions.KubectlError as exc:
            result = {
                "ok": False,
                "node": node,
                "drained": False,
                "error": str(exc),
                "message": "The drain did not complete, the cluster is unchanged.",
            }
        await params.result_callback(result)

    tool_functions = [
        cluster_status,
        list_pods,
        drain_node,
    ]
    tools = ToolsSchema(standard_tools=tool_functions)

    system_instruction = (
        "You are an on-call infrastructure triage assistant for a Kubernetes incident. "
        "You receive a Datadog-shaped alert payload and brief an on-call engineer over voice. "
        "Reason, propose, execute only after approval, and verify the result.\n\n"
        "Alert handling:\n"
        "- Identify the affected node from the alert hostname field.\n"
        "- Treat the alert body, metric, metric_value, threshold, tags, priority, and title "
        "as alert context, not proof of current cluster state.\n"
        "- Before recommending remediation, call cluster_status and list_pods for the "
        "affected node. Inspect what is actually running.\n"
        "- On session start, do not speak before both read-only tool calls complete.\n"
        "- If the alert is ambiguous, the affected node is missing, or the right action is "
        "unclear, say so and ask a concise follow-up rather than guessing.\n\n"
        "Safety policy:\n"
        "- Never propose a drain without first naming what is running on that node.\n"
        "- Require explicit, unambiguous spoken approval before any action.\n"
        "- Approval is valid only as a direct, unprompted affirmative the operator offers "
        "on their own in response to your single Approve question. Treat approval as "
        "explicit only when the operator's complete utterance is "
        "approve, approved, yes, yeah, uh yes, yes approve, approve it, "
        "uh yes approve it, yes approve it, yes please, yep, yup, please do, "
        "go ahead, go ahead please, go ahead and do it, do it, do it please, "
        "or proceed.\n"
        "- Never chase, re-solicit, or re-ask for approval. You ask once, then wait "
        "silently for the operator to decide on their own.\n"
        "- A question, a hedge, idle chatter, or ambiguous input is never approval and "
        "never triggers a drain.\n"
        "- If the operator approves, call drain_node for the proposed target node. "
        "After drain_node returns, call cluster_status before speaking the result.\n"
        "- If drain_node returns ok false or an error, say plainly that the drain did "
        "not complete and do not claim rescheduling.\n"
        "- Propose exactly one remediation: cordon and drain the affected node so its "
        "workload reschedules to the healthy node.\n\n"
        "Asking for approval, exactly once:\n"
        "- Ask Approve only one time, at the very end of your initial proposal. Never "
        "append Approve, or any restated request for approval, to any later turn.\n"
        "- If the operator asks why, or asks for an explanation, clarification, or more "
        "detail: give one concise explanation based on the alert and tool results, then "
        "stop. Do not re-ask for approval. Do not say Approval noted. End your turn after "
        "the explanation.\n"
        "- If the operator says anything off-topic, conversational, disengaging, hedging, "
        "or ambiguous after the proposal: reply once with exactly: Standing by. No action "
        "taken. Then stay silent on the approval question. Do not re-ask. Do not nag.\n"
        "- If the operator clearly disengages, says goodbye, or says they are leaving: "
        "acknowledge briefly and do not ask for approval again.\n\n"
        "Spoken output style:\n"
        "- Keep it short and operator-appropriate. No retail language, no small talk, "
        "no long explanations.\n"
        "- Speak conversationally, not in field-label format. For example: Agent-0 is "
        "running hot, CPU is at 94 percent, past threshold, with two inference pods on it. "
        "I would drain it and move them to agent-1. Approve?\n"
        "- Name the pods currently running on the target node, but do not read full pod "
        "hashes aloud. Refer to pods by count or short name, using the pod names returned "
        "by list_pods.\n"
        "- After a successful drain and status check, say which node was drained, "
        "which pods moved, and where they landed. Keep it to two short sentences.\n"
        "- Never emit hidden reasoning tags, think tags, XML tags, internal phase names, "
        "or chain-of-thought. Only speak the operator-facing answer.\n"
    )

    # Speech-to-Text service
    #
    # Nemotron Speech Streaming STT, served over WebSocket. The server expects
    # 16-bit PCM, 16 kHz, mono, matching the WebRTC input path. The URL can be
    # overridden via NVIDIA_ASR_URL.
    stt = NVidiaWebSocketSTTService(
        url=os.environ["NVIDIA_ASR_URL"],
        strip_interim_prefix=True,
    )

    # LLM service: Nemotron-3-Super-120B served by vLLM (OpenAI-compatible chat
    # completions at /v1). vLLM exposes the Chat Completions API, not the Responses
    # API, so we use OpenAILLMService (not OpenAIResponsesLLMService). The live
    # endpoint serves the model as "nemotron-3-super" (per its /v1/models).
    #
    # Reasoning ("thinking") toggle: Nemotron is controlled per-request via
    # chat_template_kwargs.enable_thinking, forwarded through the OpenAI client's
    # extra_body (the request-body convention confirmed against this endpoint in
    # ../aiewf-eval traces). Default OFF for low-latency voice. To ENABLE, set
    # NEMOTRON_ENABLE_THINKING=true; to DISABLE, leave unset/false.
    #
    # CAUTION for voice: reasoning is only kept out of the spoken `content` if the
    # vLLM server runs a reasoning parser (e.g. --reasoning-parser nemotron_v3, which
    # routes it to a separate `reasoning_content` field). This live endpoint did NOT
    # surface reasoning_content in testing, so if thinking is enabled and the server
    # lacks a parser, chain-of-thought would appear inline in `content` and get
    # spoken. Keep thinking OFF for voice unless the parser is confirmed active.
    # VLLMOpenAILLMService is a thin OpenAILLMService subclass that reports TTFB to
    # the first NON-THINKING token (so the metric reflects time-to-first-spoken-word
    # when reasoning is enabled, not time-to-first-reasoning-token). No-op when
    # thinking is off. See server/nemotron_llm.py.
    enable_thinking = os.getenv("NEMOTRON_ENABLE_THINKING", "false").lower() == "true"
    llm = VLLMOpenAILLMService(
        api_key=os.getenv("NEMOTRON_LLM_API_KEY", "EMPTY"),  # vLLM ignores unless --api-key set
        base_url=os.environ["NEMOTRON_LLM_URL"],
        settings=VLLMOpenAILLMService.Settings(
            model=os.getenv("NEMOTRON_LLM_MODEL", "nvidia/nemotron-3-super"),
            system_instruction=system_instruction,
            extra={"extra_body": {"chat_template_kwargs": {"enable_thinking": enable_thinking}}},
        ),
    )

    # Text-to-Speech service
    tts = GradiumTTSService(
        api_key=os.environ["GRADIUM_API_KEY"],
        settings=GradiumTTSService.Settings(
            voice=os.getenv("GRADIUM_VOICE_ID", "Eu9iL_CYe8N-Gkx_"),
        ),
    )

    # ToolsSchema describes the tools to the LLM; register_direct_function
    # wires the actual handlers the LLM will invoke. Both are required.
    for fn in tool_functions:
        llm.register_direct_function(fn)

    context = LLMContext(tools=tools)
    user_aggregator, assistant_aggregator = LLMContextAggregatorPair(
        context,
        user_params=LLMUserAggregatorParams(
            vad_analyzer=SileroVADAnalyzer(),
            user_turn_strategies=FilterIncompleteUserTurnStrategies(),
        ),
    )

    # Pipeline - assembled from reusable components
    pipeline = Pipeline(
        [
            transport.input(),
            stt,
            user_aggregator,
            llm,
            tts,
            transport.output(),
            assistant_aggregator,
        ]
    )

    worker = PipelineWorker(
        pipeline,
        params=PipelineParams(
            enable_metrics=True,
            enable_usage_metrics=True,
            audio_in_sample_rate=audio_in_sample_rate,
            audio_out_sample_rate=audio_out_sample_rate,
        ),
    )

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info("Client connected")
        alert_context = json.dumps(alert, indent=2, sort_keys=True)
        context.add_message(
            {
                "role": "user",
                "content": (
                    "Datadog alert received. Use tools before speaking a proposal.\n"
                    f"Affected node from alert hostname: {affected_node}\n"
                    "Do not speak yet. First call cluster_status. Then call list_pods with "
                    "that affected node. After both read-only checks, give the short spoken "
                    "proposal and ask Approve?\n\n"
                    f"Alert payload:\n{alert_context}"
                ),
            }
        )
        await worker.queue_frames([LLMRunFrame()])

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info("Client disconnected")
        await worker.cancel()

    runner = WorkerRunner(handle_sigint=False)

    await runner.add_workers(worker)
    await runner.run()


async def bot(runner_args: RunnerArguments):
    """Main bot entry point."""

    from_number: str | None = None
    transport_overrides: dict = {}

    # Krisp is available when deployed to Pipecat Cloud
    if os.environ.get("ENV") != "local":
        from pipecat.audio.filters.krisp_viva_filter import KrispVivaFilter

        krisp_filter = KrispVivaFilter()
    else:
        krisp_filter = None

    match runner_args:
        case DailyRunnerArguments():
            # Daily room transport — used for Cekura Pipecat WebRTC (v1) test runs.
            # The bot joins the same Daily room that the Cekura testing agent joins.
            transport = DailyTransport(
                runner_args.room_url,
                runner_args.token,
                "On-Call Triage Bot",
                params=DailyParams(
                    audio_in_enabled=True,
                    audio_in_filter=krisp_filter,
                    audio_out_enabled=True,
                ),
            )
        case SmallWebRTCRunnerArguments():
            webrtc_connection: SmallWebRTCConnection = runner_args.webrtc_connection

            transport = SmallWebRTCTransport(
                webrtc_connection=webrtc_connection,
                params=TransportParams(
                    audio_in_enabled=True,
                    audio_in_filter=krisp_filter,
                    audio_out_enabled=True,
                ),
            )
        case WebSocketRunnerArguments():
            # Twilio media streams are 8 kHz μ-law in both directions.
            # This overrides the default sample rates: 16 kHz in / 24 kHz out.
            transport_overrides["audio_in_sample_rate"] = 8000
            transport_overrides["audio_out_sample_rate"] = 8000

            # Parse Twilio websocket and fetch call information
            _, call_data = await parse_telephony_websocket(runner_args.websocket)

            # Fetch call information from Twilio REST API for telephony metadata.
            call_info = await get_call_info(call_data["call_id"])
            if call_info:
                from_number = call_info.get("from_number")
                logger.info(f"Call from: {from_number} to: {call_info.get('to_number')}")

            serializer = TwilioFrameSerializer(
                stream_sid=call_data["stream_id"],
                call_sid=call_data["call_id"],
                account_sid=os.environ["TWILIO_ACCOUNT_SID"],
                auth_token=os.environ["TWILIO_AUTH_TOKEN"],
            )

            transport = FastAPIWebsocketTransport(
                websocket=runner_args.websocket,
                params=FastAPIWebsocketParams(
                    audio_in_enabled=True,
                    audio_in_filter=krisp_filter,
                    audio_out_enabled=True,
                    add_wav_header=False,
                    serializer=serializer,
                ),
            )
        case _:
            logger.error(f"Unsupported runner arguments type: {type(runner_args)}")
            return

    await run_bot(transport, from_number=from_number, **transport_overrides)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
