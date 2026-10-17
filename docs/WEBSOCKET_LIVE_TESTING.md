# Resume WebSocket live testing

The WebSocket transport and native CES connector are implemented. This handoff
targets **local-ready**: implementation, regression tests, and a testing plan.
Live acceptance remains pending until the checks below pass on the intended
deployed commit. This gateway remains functional sample code.

## 1. Establish the local baseline

From the repository root, activate a virtual environment with
`requirements-dev.txt` installed, then run:

```bash
source venv/bin/activate
python -m grpc_tools.protoc -I./proto \
  --python_out=src/generated --grpc_python_out=src/generated proto/*.proto
python -m pytest tests/
PYTHONPATH=tools/byova_e2e/src python -m pytest tools/byova_e2e/tests/
```

For a local socket smoke check, start the gateway in one terminal:

```bash
GATEWAY_CONFIG=config/websocket_local_example.yaml python main.py
```

In another terminal with the same virtual environment activated:

```bash
python tools/websocket_smoke.py --url ws://127.0.0.1:58765 --mode list
python tools/websocket_smoke.py --url ws://127.0.0.1:58765 --mode session \
  --agent-id 'Local Audio: Local Playback'
```

Discovery must list Local Audio, and session start must return a response. This
client reads only the first response frame; use the socket tests to validate
complete chunk streams and their empty-audio FINAL terminator. Stop the local
gateway with Control-C after the checks.

## 2. Prepare the live target when returning

- Record the selected source commit and verify the target runs that same commit.
  Build releases only with `scripts/build-runtime-release.sh --ref HEAD --output
  /tmp/byova-gateway-runtime.tar.gz` after committing the intended changes.
- Use the existing private deployment procedure and configuration. Keep hostnames,
  resource IDs, credentials, and routing outside this shared example repository.
- Verify WSS routing for both `/v1/va` and `/v1/listVirtualAgents`. Enable enforced
  JWT validation and the distinct WebSocket datasource/Service App profile; the
  loopback-only unauthenticated example is unsuitable for the live target.
- Select the intended provider hop explicitly. `GECXConnector` uses CES gRPC;
  `GECXWebSocketConnector` uses CES WebSocket. Give them distinct agent IDs and
  transport restrictions when configured together. See the
  [GECX setup guide](guides/byova-gecx-setup.md).
- Use a dedicated test caller and approved WxCC test flow/destination. Configure
  the E2E runner's private gateway-events access as described in its
  [JSON test and gateway-evidence guide](../tools/byova_e2e/README.md#define-tests-in-json).

## 3. Run the acceptance matrix

| Scenario | Pass evidence |
| --- | --- |
| Authenticated discovery | Intended WebSocket agents appear; disabled agents do not. Missing/invalid JWT is rejected before upgrade. |
| Local Audio menu | DTMF 5 plays the transfer prompt and emits TRANSFER_TO_AGENT; DTMF 6 plays goodbye and emits SESSION_END. |
| Native CES voice | Welcome, a normal request, a long response, and a second caller turn are audible in order; each stream completes. |
| Natural pause | The configured pause is handled as one caller turn according to the existing GECX regression plan. |
| Autonomous/no-input output | A provider-initiated prompt reaches the caller without another caller frame. |
| Barge-in | Behavior matches the connector configuration; test enabled and disabled behavior only where supported by the agent/flow. |
| Transfer and metadata | Announcement precedes the terminal event; expected handoff summary reaches the configured Flow Designer variable. |
| Peer disconnect | One provider cleanup, one gateway end event, and no remaining conversation lease/session. |
| Gateway shutdown | Active calls close with 1001, provider cleanup completes within the drain budget, and new upgrades are rejected during shutdown. |
| gRPC regression | Existing gRPC discovery, voice, transfer, and termination still work with their independent datasource. |

The existing E2E plans already cover the Local Audio terminal controls and GECX
voice/terminal scenarios. From `tools/byova_e2e`, using its configured environment:

```bash
byova-e2e validate --config config/local-audio-dtmf.spec.json --list
byova-e2e validate --config config/gecx-regression.spec.json --list
```

Run each Local Audio test against the test destination:

```bash
byova-e2e run --destination "$BYOVA_TEST_DESTINATION" \
  --config config/local-audio-dtmf.spec.json --test dtmf-transfer
byova-e2e run --destination "$BYOVA_TEST_DESTINATION" \
  --config config/local-audio-dtmf.spec.json --test dtmf-session-end
```

Run the applicable listed GECX test IDs with the same `--config`/`--test` pattern,
adding `--gateway-transport websocket --gateway-agent-id "$BYOVA_TEST_AGENT_ID"`
to bind evidence to the selected native CES WebSocket agent. Supply these values
locally; do not commit deployment-specific test plans. Provider prompts and
handoff expectations must match the selected agent's actual behavior.

## 4. Record acceptance

For every run, retain the deployed commit, non-secret configuration fingerprint,
test ID, UTC time window, conversation ID, gateway transport, provider agent ID,
caller result, gateway terminal/cleanup events, and provider session evidence.
Keep recordings, transcripts, raw logs, and private deployment details in the
approved private evidence location.

A caller recording alone does not prove provider cleanup or handoff metadata.
Mark each matrix row pass/fail/pending and link its evidence. A provider cleanup
that exceeds the drain budget is a failed shutdown check; Python cannot forcibly
stop a blocked provider thread. Do not label the feature live-verified until
every applicable row passes on the tested commit.

The feature's Spec Kit-compatible acceptance criteria and task status are under
[`specs/001-websocket-transport`](../specs/001-websocket-transport/spec.md).
