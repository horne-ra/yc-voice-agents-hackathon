"""Outbound pager: Twilio calls the operator, streams to local bot /ws."""

import asyncio
import json
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

load_dotenv()

KELDRON_REPO = Path(__file__).resolve().parents[2] / "keldron-oncall"
ALERT_PATH = KELDRON_REPO / "fixtures" / "datadog-alert.json"

TWIML = """<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="wss://{host}/ws"></Stream>
  </Connect>
  <Pause length="40"/>
</Response>"""

TWILIO_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"Missing required environment variable: {name}")
    return value


def _clean_host(host: str) -> str:
    host = host.strip().rstrip("/")
    for prefix in ("https://", "http://", "wss://", "ws://"):
        if host.startswith(prefix):
            return host.split("://", 1)[1]
    return host


async def main() -> None:
    account_sid = os.environ["TWILIO_ACCOUNT_SID"]
    auth_token = os.environ["TWILIO_AUTH_TOKEN"]
    from_number = _require_env("TWILIO_PHONE_NUMBER")
    to_number = _require_env("OPERATOR_PHONE")
    tunnel_host = _clean_host(os.environ["PUBLIC_TUNNEL_HOST"])

    if ALERT_PATH.is_file():
        alert = json.loads(ALERT_PATH.read_text())
        title = alert.get("alert_title") or alert.get("alert_id", "unknown")
        print(f"PagerDuty fired: {title}")
    else:
        print(f"Warning: alert fixture missing at {ALERT_PATH}", file=sys.stderr)

    url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Calls.json"
    payload = {"To": to_number, "From": from_number, "Twiml": TWIML.format(host=tunnel_host)}
    auth = aiohttp.BasicAuth(account_sid, auth_token)

    try:
        async with aiohttp.ClientSession(timeout=TWILIO_TIMEOUT) as session, session.post(
            url, auth=auth, data=payload
        ) as resp:
            body = await resp.text()
            if resp.status not in (200, 201):
                print(f"Twilio error ({resp.status}): {body}", file=sys.stderr)
                sys.exit(1)
            data = json.loads(body)
            print(f"Paging {to_number} from {from_number} -> wss://{tunnel_host}/ws")
            print(f"Call SID: {data.get('sid')}  status: {data.get('status')}")
    except asyncio.TimeoutError:
        print("Twilio API request timed out", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
