#!/usr/bin/env python3
"""Local discovery and SESSION_START smoke client for the BYOVA WebSocket API."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import uuid
from datetime import datetime, timezone

from aiohttp import ClientSession


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _headers() -> dict[str, str]:
    token = os.environ.get("BYOVA_WEBSOCKET_TOKEN", "").strip()
    return {"Authorization": f"Bearer {token}"} if token else {}


async def run(args: argparse.Namespace) -> None:
    async with ClientSession(headers=_headers()) as session:
        path = "/v1/listVirtualAgents" if args.mode == "list" else "/v1/va"
        async with session.ws_connect(args.url.rstrip("/") + path) as socket:
            if args.mode == "list":
                await socket.send_json({"customer_org_id": args.org_id})
            else:
                conversation_id = args.conversation_id or str(uuid.uuid4())
                payload = {
                    "conversation_id": conversation_id,
                    "customer_org_id": args.org_id,
                    "voice_va_input_type": {
                        "event_input": {"event_type": "SESSION_START"}
                    },
                }
                if args.agent_id:
                    payload["virtual_agent_id"] = args.agent_id
                await socket.send_json(
                    {
                        "type": "VOICE_VA_REQUEST",
                        "seq": 1,
                        "ts": _timestamp(),
                        "conversation_id": conversation_id,
                        "payload": payload,
                    }
                )
            message = await socket.receive(timeout=args.timeout)
            if message.type.name not in {"TEXT", "CLOSE", "CLOSED"}:
                raise RuntimeError(f"unexpected WebSocket message: {message.type.name}")
            if message.data:
                value = json.loads(message.data)
                if not args.include_audio:
                    for prompt in value.get("payload", {}).get("prompts", []):
                        encoded_audio = prompt.get("audio_content_b64")
                        if encoded_audio:
                            prompt["audio_content_b64"] = (
                                f"<base64 omitted: {len(encoded_audio)} characters>"
                            )
                print(json.dumps(value, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="ws://127.0.0.1:8765")
    parser.add_argument("--mode", choices=("list", "session"), default="list")
    parser.add_argument("--org-id", default="local-test")
    parser.add_argument("--agent-id")
    parser.add_argument("--conversation-id")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument(
        "--include-audio",
        action="store_true",
        help="print base64 prompt audio instead of a length-only placeholder",
    )
    asyncio.run(run(parser.parse_args()))


if __name__ == "__main__":
    main()
