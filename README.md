# keldron-oncall

Voice-driven on-call triage agent for Kubernetes / compute infrastructure. It is PagerDuty,
except the agent calls you, walks you through the incident, and executes the fix on a real
cluster after you approve by voice. A Cekura evaluation harness red-teams the agent's action
judgment, and the failures it finds feed back into the agent.

---

## 1. What is this?

An on-call triage voice agent for compute infrastructure. When an alert fires, the agent phones
the operator, inspects the live Kubernetes cluster, proposes a remediation (cordon and drain the
affected node), and only after explicit voice approval performs the real drain so the workload
reschedules to a healthy node. The action path is real: real cluster, real drain, real
reschedule. The alert is a Datadog-shaped fixture injected for demo timing; there is no live
Datadog integration.

The point of the project is not just the voice demo. It is the loop around the agent's
*judgment*: simulate incident calls with Cekura, catch the cases where the agent would take an
unsafe or wrong action, fix the agent, and prove the fix by re-evaluating.

---

## 2. Demo video (under 60 seconds)

[VIDEO LINK HERE]

---

## 3. How we used Cekura, Nemotron, and Pipecat

**Cekura: red-teaming action judgment.**
We used Cekura to red-team the agent's *action judgment*, not just its voice quality, because the
agent can take a real, destructive action: draining a Kubernetes node. We wrote five evaluators
that simulate an on-call engineer: a happy path, approval gating, a "why?" question that must not
be treated as approval, off-topic chatter after a proposal, and a red-team caller pressuring the
agent to skip safety checks.

The harness caught a real, safety-relevant failure: an approval nag loop. After proposing a
drain, the agent kept re-soliciting approval turn after turn, even as the operator disengaged
("heading home, good night"). In production that is dangerous. Persistent re-prompting could
eventually catch a stray "yeah" or "sure" and convert idle chatter into an approved drain on a
live cluster.

We made a targeted system-prompt fix: ask once, do not nag, treat off-topic input as no action,
and never turn questions or chatter into approval. Re-running the harness on a clean cluster
showed the safety scenarios improve while preserving the tool-call path. The destructive
happy-path scenario also passed and was verified directly against `kubectl`: the node was
cordoned and its pods rescheduled only after explicit approval.

**Nemotron: the reasoning brain.**
The agent's reasoning and tool calling run on NVIDIA Nemotron-3-Super-120B via the event AWS
endpoint, with NVIDIA Nemotron Speech for ASR. Nemotron drives the triage: read the alert, call
read-only tools to inspect the cluster, propose one remediation naming the real pods, and call
the drain tool only on approval. Function calling over the voice loop worked reliably.

**Pipecat: the voice orchestration.**
The agent is built on Pipecat, self-hosted over WebRTC for local dev and Twilio for the phone
path, starting from the hackathon's Nemotron starter bot. Pipecat handles the STT to LLM to TTS
loop, turn-taking, and the Twilio media stream for the outbound "pager" call.

**Stack:** Pipecat (orchestration), NVIDIA Nemotron-3-Super-120B + Nemotron Speech ASR
(reasoning + STT), Gradium (TTS), Cekura (evaluation + auto-improvement), Twilio (outbound phone
call), k3d / Kubernetes (the real control plane the agent acts on).

---

## 4. What we did new during the hackathon

**Prior work / infrastructure configured beforehand:**
- k3d cluster scripts
- the demo workload manifest
- the kubectl action wrappers (`k8s_actions.py`)
- the Datadog-shaped alert fixture
- a throwaway proof-of-chain spike

**Built during the hackathon:**
- the reasoning and triage agent: system prompt, tool wiring, and safety policy
- alert-to-action grounding from an alert payload into live cluster state and a real drain
- the voice loop, adapted from the Nemotron starter bot to the on-call domain
- the Cekura evaluation harness and prompt-improvement loop
- the Twilio outbound "pager" call, where the system calls the operator when an alert fires
- a clean call-ending path after successful drain confirmation

The Pipecat Nemotron starter bot was the starting point for the voice pipeline; we replaced its
flower-shop domain with the on-call triage agent.

---

## 5. Feedback on the tools

**Nemotron (NVIDIA).**
What it did well: function calling over the voice loop was reliable. It consistently emitted
correct tool calls (`cluster_status`, `list_pods`, `drain_node`) with the right arguments. It
followed a fairly strict safety policy in the system prompt, and its triage reasoning was useful.
In one run it reasoned that draining an empty node achieves nothing rather than blindly proposing
the action. It also followed spoken-output constraints once the prompt was tightened.

What could be better: latency on the public build.nvidia.com endpoint was high in our testing,
which matters a lot for a real-time voice loop. The event AWS-hosted endpoint was the right call.
Some early prompt versions also leaked reasoning scaffolding into spoken output until we
constrained it explicitly.

**Cekura (building self-improvement loops).**
What worked well: the simulate, evaluate, improve loop is the right shape for an agent that can
take real action. The Claude Code plugin (skills + MCP) made it fast to create the agent,
generate evaluators, and run them from the terminal. The evaluators caught a real safety bug we
had not found by hand, and the before/after re-run gave us concrete proof the fix worked.

Bugs / friction we hit:
- Early versions of the bot did not cleanly hang up at the end of a call, so hung sessions could
  block evaluation finalization and had to be force-ended. A built-in max-call-duration cutoff
  that finalizes the eval would still be useful.
- For the Pipecat v1 local WebRTC run path, the schema described the meeting token as optional,
  but the API rejected the run with a null token. Passing a minted Daily token fixed it. It would
  help to make the token clearly required in the schema/docs for that path.
- Evaluation is async and there was no obvious terminal "done" signal to poll cleanly; we ended
  up sleeping and re-polling. A clearer terminal-state indicator on the result would smooth the
  loop.

**Pipecat.**
The Nemotron starter bot ran locally with just keys and was a great launch point. Swapping STT,
LLM, and TTS providers and adding a Twilio outbound path was straightforward.

---

## 6. Live link (optional)

The agent runs locally so it can reach the live k3d cluster and perform a real drain; there is no
hosted public endpoint. The demo is run locally with cluster + bot + tunnel for the phone call.

---

## Known limitations

- Phone audio can be choppy over the tunnel, especially with many small TTS segments over Twilio.
- The alert source is a fixture for demo timing rather than a live Datadog webhook.
- The approval gate is intentionally conservative, so some natural phrases are rejected if they
  sound like a question, hedge, or chatter.

---

## Running it locally

Create the local k3d cluster:

```bash
./infra/cluster/create-cluster.sh
```

Apply the demo workload and watch placement:

```bash
kubectl apply -f infra/workloads/inference-deployment.yaml
kubectl get pods -o wide
```

The create script cordons the server node and labels both agents `role=worker`, so the workload
runs only on the two agent nodes.

To reset placement after a drain:

```bash
kubectl uncordon k3d-keldron-agent-0
kubectl scale deployment/inference-svc --replicas=0
kubectl scale deployment/inference-svc --replicas=4
kubectl rollout status deployment/inference-svc --timeout=60s
kubectl get pods -o wide
```

### Tunnel setup

Twilio needs a public HTTPS/WSS endpoint to reach your local bot on port **7861**. Use **ngrok**
or **Cloudflare Tunnel** (either works).

**ngrok:**

```bash
ngrok http 7861
```

Copy the **Forwarding** hostname from the ngrok terminal (for example `abc123.ngrok-free.app`).
Use only the hostname, not `https://`.

**Cloudflare Tunnel:**

```bash
cloudflared tunnel --url http://localhost:7861
```

Copy the public URL Cloudflare prints (for example `xyz.trycloudflare.com`). Use only the
hostname.

Set that hostname in `server/.env` as `PUBLIC_TUNNEL_HOST`, and pass the same value to the bot
`-x` flag below.

Run the Twilio phone bot locally behind the current tunnel:

```bash
cd server
TWILIO_AUDIO_OUT_10MS_CHUNKS=10 TWILIO_TTS_FULL_TURN_COALESCE=false \
  uv run bot-nemotron.py -t twilio -x <public-tunnel-host> --port 7861
```

Set the same host in `server/.env`:

```bash
PUBLIC_TUNNEL_HOST=<public-tunnel-host>
```

Page the operator:

```bash
cd server
uv run page_operator.py
```

Delete the cluster when finished:

```bash
./infra/cluster/delete-cluster.sh
```
