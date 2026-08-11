# Voice Agent Audio Lab

The lab connects a local browser directly to the voice-agent providers configured for the
gateway. It intentionally bypasses Webex Contact Center, BYODS, the gateway gRPC service,
the ALB, and the telephony media path.

Supported paths:

- Google CX Agent Studio through CES `BidiRunSession`, with continuous call-style audio;
- AWS Lex V2 through `RecognizeUtterance`, matching the gateway connector's buffered-turn
  behavior.

The server reads provider routing and authentication settings from the same main gateway
configuration used by `main.py`. Provider credentials stay in the Python process; the browser
receives only sanitized target metadata and returned audio.

## Install

From the repository root:

```bash
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements-dev.txt
```

`requirements-dev.txt` includes the gateway dependencies plus the lab's local-only HTTP and
WebSocket server dependency. Gateway containers and runtime archives continue to install only
`requirements.txt`.

## Run from the gateway configuration

```bash
python -m tools.voice_agent_lab --gateway-config config/config.yaml
```

`--gateway-config` defaults to `GATEWAY_CONFIG`, then `config/config.yaml`, so the short form
usually works:

```bash
python -m tools.voice_agent_lab
```

For GECX, the lab accepts the connector's existing `access_token`, `service_account_key`,
OAuth fields, or Application Default Credentials. For AWS Lex, it uses the same boto3 default
credential chain as `AWSLexConnector`; credentials are never added to YAML.

AWS bot and alias discovery also follows the connector behavior: bots are listed in the
configured region and the most recently updated alias is selected.

AWS Lex `StartConversation` is not exposed by this Python lab because AWS supports that
bidirectional operation only in its C++, Java v2, and JavaScript v3 SDKs. The lab does not show
a mode that boto3 cannot run.

## Optional local targets

An optional gitignored overlay can add one-off GECX targets without changing the main gateway
configuration:

```bash
python -m tools.voice_agent_lab \
  --gateway-config config/config.yaml \
  --config tools/voice_agent_lab/config.local.yaml
```

The legacy `tools/gecx_audio_lab/config.local.yaml` shape remains accepted. Prefer adding
deployable targets to the main connector config so routing and credential references stay in
one place.

## Interpreting the modes

| Mode | Microphone behavior | Best comparison |
| --- | --- | --- |
| GECX bidirectional | Open once; mute/unmute while the session stays live | Raw CES versus WxCC call path |
| AWS connector parity | Start talking, then finish each turn | Current `AWSLexConnector` behavior |

The lab measures input commit to first returned audio, records transcripts and provider events,
flags unusually long low-energy frames, and can download the returned PCM as a WAV file.

For GECX output-quality comparisons, use the same text-only prompt with these profiles:

| Profile | CES output request | Audio played by the lab |
| --- | --- | --- |
| Native PCM | 24 kHz Linear16 | Original 24 kHz Linear16 |
| GECX direct mu-law (`wxcc`) | 8 kHz mu-law | GECX mu-law decoded to PCM for playback |
| Connector mu-law from GECX PCM (`connector_mulaw`) | 24 kHz Linear16 | Anti-aliased 8 kHz conversion, locally mu-law encoded once, then decoded for playback |

The third profile is a connector-path experiment. It preserves GECX's highest-quality output
until the final conversion required by the WxCC media path and makes the resulting transport
codec and byte count visible in the audio event metadata.

## Security

- Keep the server on its default loopback address. A non-loopback bind requires
  `--allow-remote`.
- Do not place private key JSON content or AWS access keys in lab configuration.
- Treat recorded audio and WAV downloads as test data; do not use customer conversations.
- The lab is development-only and must remain outside runtime release artifacts.
