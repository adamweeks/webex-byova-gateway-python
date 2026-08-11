"""Tests for the local provider-neutral voice-agent browser diagnostic tool."""

import base64
import io
import json
import math
import struct
from pathlib import Path
from types import SimpleNamespace

import pytest

from src.utils.telephony_audio import Linear16Resampler
from tools.gecx_audio_lab.audio import (
    AUDIO_PROFILES,
    OutputAudioConverter,
    browser_output_audio,
    ces_input_audio,
    linear16_to_mulaw,
    mulaw_to_linear16,
    pcm16_rms,
    silence_chunks,
)
from tools.gecx_audio_lab.server import (
    AWSLexRecognizeSession,
    AWSLexTarget,
    DirectGECXSession,
    LabSettings,
    SessionTarget,
    _safe_error_message,
)


def test_mulaw_silence_round_trip() -> None:
    pcm = b"\x00\x00" * 160

    encoded = linear16_to_mulaw(pcm)
    decoded = mulaw_to_linear16(encoded)

    assert encoded == b"\xff" * 160
    assert len(decoded) == len(pcm)
    assert pcm16_rms(decoded) == 0


def test_mulaw_round_trip_preserves_voice_shape() -> None:
    samples = [-24000, -12000, -3000, 0, 3000, 12000, 24000]
    pcm = b"".join(sample.to_bytes(2, "little", signed=True) for sample in samples)

    decoded = mulaw_to_linear16(linear16_to_mulaw(pcm))
    result = [
        int.from_bytes(decoded[index : index + 2], "little", signed=True)
        for index in range(0, len(decoded), 2)
    ]

    assert result[3] == 0
    assert result[:3] == sorted(result[:3])
    assert result[4:] == sorted(result[4:])
    assert result[0] < -20000
    assert result[-1] > 20000


def test_audio_profiles_convert_and_generate_expected_duration() -> None:
    native = AUDIO_PROFILES["native"]
    wxcc = AUDIO_PROFILES["wxcc"]
    connector_mulaw = AUDIO_PROFILES["connector_mulaw"]
    pcm_20ms_8khz = b"\x00\x00" * 160

    assert ces_input_audio(native, pcm_20ms_8khz) == pcm_20ms_8khz
    assert len(ces_input_audio(wxcc, pcm_20ms_8khz)) == 160
    assert len(browser_output_audio(wxcc, b"\xff" * 160)) == 320
    assert wxcc.public_dict()["label"] == "GECX direct mu-law"
    assert wxcc.public_dict()["outputEncoding"] == "MULAW"
    assert wxcc.public_dict()["outputSampleRateHertz"] == 8000
    assert wxcc.public_dict()["transportEncoding"] == "MULAW"
    assert wxcc.public_dict()["transportSampleRateHertz"] == 8000
    assert wxcc.public_dict()["transcoded"] is False
    assert connector_mulaw.public_dict()["label"] == (
        "Connector mu-law from GECX PCM"
    )
    assert connector_mulaw.public_dict()["outputEncoding"] == "LINEAR16"
    assert connector_mulaw.public_dict()["outputSampleRateHertz"] == 24000
    assert connector_mulaw.public_dict()["transportEncoding"] == "MULAW"
    assert connector_mulaw.public_dict()["transportSampleRateHertz"] == 8000
    assert connector_mulaw.public_dict()["transcoded"] is True
    assert sum(map(len, silence_chunks(native, 250))) == 8000
    assert sum(map(len, silence_chunks(wxcc, 250))) == 2000


def test_connector_mulaw_profile_downsamples_and_encodes_once() -> None:
    profile = AUDIO_PROFILES["connector_mulaw"]
    pcm_100ms_24khz = b"".join(
        struct.pack("<h", round(12000 * math.sin(2 * math.pi * 440 * index / 24000)))
        for index in range(2400)
    )

    converted = OutputAudioConverter(profile).process(pcm_100ms_24khz)

    assert converted.transport_encoding == "MULAW"
    assert converted.sample_rate_hertz == 8000
    assert len(converted.transport_audio) == 800
    assert len(converted.pcm16) == 1600
    assert pcm16_rms(converted.pcm16) > 7000


def test_linear16_downsampler_preserves_state_across_provider_frames() -> None:
    pcm = b"".join(
        struct.pack("<h", round(10000 * math.sin(2 * math.pi * 1000 * index / 24000)))
        for index in range(4800)
    )
    whole = Linear16Resampler(24000, 8000).process(pcm)
    streaming = Linear16Resampler(24000, 8000)
    split_at = 1554

    split = streaming.process(pcm[:split_at]) + streaming.process(pcm[split_at:])

    assert split == whole
    assert len(whole) == 3200


def test_linear16_downsampler_attenuates_out_of_band_audio() -> None:
    def tone(frequency_hertz: int) -> bytes:
        return b"".join(
            struct.pack(
                "<h",
                round(12000 * math.sin(2 * math.pi * frequency_hertz * index / 24000)),
            )
            for index in range(4800)
        )

    in_band = Linear16Resampler(24000, 8000).process(tone(1000))[400:]
    out_of_band = Linear16Resampler(24000, 8000).process(tone(4500))[400:]

    assert pcm16_rms(in_band) > 7000
    assert pcm16_rms(out_of_band) < 100


def test_session_target_builds_draft_and_published_paths() -> None:
    draft = SessionTarget.from_mapping(
        {
            "id": "draft",
            "project_id": "project-one",
            "application_id": "app-one",
        }
    )
    published = SessionTarget.from_mapping(
        {
            "id": "published",
            "project_id": "project-one",
            "location": "us",
            "application_id": "app-one",
            "deployment_id": "deployment-one",
        }
    )

    assert draft.deployment_path is None
    assert draft.public_dict()["deploymentMode"] == "draft"
    assert published.deployment_path == (
        "projects/project-one/locations/us/apps/app-one/deployments/deployment-one"
    )
    assert published.public_dict()["deploymentMode"] == "published"
    assert published.runtime_endpoint == "ces.us.rep.googleapis.com"


def test_gecx_target_exposes_both_mulaw_manual_test_profiles() -> None:
    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )

    assert target.public_dict()["supportedProfileIds"] == [
        "native",
        "wxcc",
        "connector_mulaw",
    ]


def test_settings_load_multiple_targets_without_exposing_credential_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TEST_GECX_CREDENTIALS", "/secret/key.json")
    config = tmp_path / "audio-lab.yaml"
    config.write_text(
        """
default_target: one
targets:
  - id: one
    label: First
    project_id: project-one
    application_id: app-one
    credentials_env: TEST_GECX_CREDENTIALS
  - id: two
    label: Second
    project_id: project-two
    application_id: app-two
    deployment_id: deployment-two
""".strip(),
        encoding="utf-8",
    )

    settings = LabSettings.from_yaml(config)
    public_json = str(settings.public_dict())

    assert settings.default_target_id == "one"
    assert list(settings.targets) == ["one", "two"]
    assert "/secret/key.json" not in public_json
    assert "TEST_GECX_CREDENTIALS" not in public_json


def test_direct_session_queues_live_audio_before_endpointing_silence() -> None:
    from google.cloud import ces_v1

    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    session = DirectGECXSession(
        target,
        AUDIO_PROFILES["native"],
        200,
        lambda _item: None,
        ces_module=ces_v1,
    )
    microphone_frame = b"\x01\x00" * 320
    session.begin_audio_turn()
    session.send_pcm16(microphone_frame)
    session.commit_audio_turn()

    requests = session._request_generator()
    config = next(requests)
    live_audio = next(requests)
    endpointing_one = next(requests)
    endpointing_two = next(requests)

    assert config.config.input_audio_config.sample_rate_hertz == 16000
    assert live_audio.realtime_input.audio == microphone_frame
    assert endpointing_one.realtime_input.audio == b"\x00" * 3200
    assert endpointing_two.realtime_input.audio == b"\x00" * 3200


def test_permission_error_is_actionable_and_does_not_include_rpc_metadata() -> None:
    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    permission_error = type("PermissionDenied", (Exception,), {})

    message = _safe_error_message(
        permission_error("very long RPC metadata and troubleshooting URL"), target
    )

    assert "roles/ces.client" in message
    assert "project project" in message
    assert "troubleshooting URL" not in message


def test_active_stream_permission_error_identifies_downstream_permission() -> None:
    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    permission_error = type("PermissionDenied", (Exception,), {})

    message = _safe_error_message(
        permission_error(
            "403 Permission 'aiplatform.ragCorpora.get' denied on resource "
            "projects/example [reason: IAM_PERMISSION_DENIED]"
        ),
        target,
        stream_was_active=True,
    )

    assert "downstream permission failure" in message
    assert "aiplatform.ragCorpora.get" in message
    assert "projects/example" not in message


def test_initial_text_reserves_turn_until_server_completes_it() -> None:
    from google.cloud import ces_v1

    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    session = DirectGECXSession(
        target,
        AUDIO_PROFILES["native"],
        200,
        lambda _item: None,
        ces_module=ces_v1,
    )
    session._initial_text = "Hello"
    session._turn_id = 1
    session._active_turn_id = 1
    session._active_turn_kind = "opening_text"

    with pytest.raises(RuntimeError, match="Finish the active turn"):
        session.begin_audio_turn()

    session._complete_turn()
    assert session.begin_audio_turn() == 2


def test_prior_completion_cannot_clear_new_streaming_microphone_turn() -> None:
    from google.cloud import ces_v1

    events = []
    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    session = DirectGECXSession(
        target,
        AUDIO_PROFILES["native"],
        2000,
        events.append,
        ces_module=ces_v1,
    )

    assert session.begin_audio_turn() == 1
    session._complete_turn()
    session.send_pcm16(b"\x01\x00" * 320)

    assert session._active_turn_id == 1
    assert not any(
        isinstance(event, dict) and event.get("type") == "turn_completed"
        for event in events
    )

    session._handle_message(
        SimpleNamespace(
            recognition_result=SimpleNamespace(transcript="hello"),
            interruption_signal=None,
            session_output=None,
            end_session=None,
        )
    )
    session._complete_turn()

    assert session._active_turn_id is None
    assert events[-1]["type"] == "turn_completed"


def test_continuous_microphone_rearms_after_agent_response() -> None:
    from google.cloud import ces_v1

    events = []
    target = SessionTarget(
        id="test",
        label="Test",
        project_id="project",
        location="us",
        application_id="app",
    )
    session = DirectGECXSession(
        target,
        AUDIO_PROFILES["native"],
        2000,
        events.append,
        ces_module=ces_v1,
    )

    assert session.begin_audio_turn(continuous=True) == 1
    session._active_audio_acknowledged = True
    session._complete_turn()

    completion = events[-1]
    assert completion == {
        "type": "turn_completed",
        "turnId": 1,
        "continuous": True,
        "nextTurnId": 2,
    }
    assert session._active_turn_id == 2
    assert session._active_turn_kind == "audio"
    session.send_pcm16(b"\x01\x00" * 320)

    session.commit_audio_turn()
    session._complete_turn()
    assert session._active_turn_id is None
    assert events[-1]["continuous"] is False


@pytest.mark.parametrize("target_id", ["", "has space", "../escape"])
def test_session_target_rejects_invalid_ids(target_id: str) -> None:
    with pytest.raises(ValueError):
        SessionTarget.from_mapping(
            {
                "id": target_id,
                "project_id": "project",
                "application_id": "app",
            }
        )


def _aws_target(*, streaming: bool = False) -> AWSLexTarget:
    return AWSLexTarget(
        id="aws-test",
        label="AWS Test",
        connector_id="aws_lex_connector",
        region_name="us-east-1",
        locale_id="en_US",
        bot_id="BOT123",
        bot_alias_id="ALIAS123",
        bot_name="Test Bot",
        streaming=streaming,
    )


def test_gateway_config_reuses_connector_auth_without_exposing_secrets(
    tmp_path: Path,
) -> None:
    gateway_config = tmp_path / "config.yaml"
    gateway_config.write_text(
        """
connectors:
  gecx_connector:
    type: gecx_connector
    config:
      project_id: project-one
      application_id: app-one
      service_account_key: /private/gecx-key.json
      access_token: should-never-reach-browser
  aws_lex_connector:
    type: aws_lex_connector
    config:
      region_name: us-east-1
      initial_trigger_text: hello
""".strip(),
        encoding="utf-8",
    )

    def discover(connector_id, config):
        assert connector_id == "aws_lex_connector"
        assert config["region_name"] == "us-east-1"
        return [_aws_target()]

    settings = LabSettings.from_sources(gateway_config, aws_discoverer=discover)
    public_json = json.dumps(settings.public_dict())

    assert list(settings.targets) == ["gecx_connector", "aws-test"]
    assert settings.targets["gecx_connector"].auth_config["service_account_key"] == (
        "/private/gecx-key.json"
    )
    assert "/private/gecx-key.json" not in public_json
    assert "should-never-reach-browser" not in public_json
    assert "connector access token" in public_json
    assert "AWS default credential chain" in public_json


def test_aws_connector_parity_buffers_one_turn_and_emits_audio() -> None:
    class RuntimeClient:
        def __init__(self):
            self.calls = []

        def recognize_utterance(self, **kwargs):
            self.calls.append(kwargs)
            transcript = base64.b64encode(json.dumps("hello lex").encode()).decode()
            messages = base64.b64encode(
                json.dumps([{"content": "Hello from Lex"}]).encode()
            ).decode()
            return {
                "inputTranscript": transcript,
                "messages": messages,
                "audioStream": io.BytesIO(b"\x01\x00" * 320),
            }

    events = []
    runtime = RuntimeClient()
    session = AWSLexRecognizeSession(
        _aws_target(),
        AUDIO_PROFILES["lex_native"],
        0,
        events.append,
        runtime_client=runtime,
    )
    session.start()
    session.begin_audio_turn()
    session.send_pcm16(b"\x02\x00" * 320)
    session.commit_audio_turn()
    session.join()

    call = runtime.calls[0]
    assert call["botId"] == "BOT123"
    assert call["botAliasId"] == "ALIAS123"
    assert call["requestContentType"] == "audio/l16; rate=16000; channels=1"
    assert call["inputStream"] == b"\x02\x00" * 320
    assert any(event.get("type") == "transcript" for event in events if isinstance(event, dict))
    assert any(event.get("type") == "agent_text" for event in events if isinstance(event, dict))
    packets = [event for event in events if not isinstance(event, dict)]
    assert packets[0].pcm16 == b"\x01\x00" * 320
    assert events[-1]["type"] == "turn_completed"


def test_aws_connector_parity_cancels_empty_microphone_turn() -> None:
    events = []
    session = AWSLexRecognizeSession(
        _aws_target(),
        AUDIO_PROFILES["lex_native"],
        0,
        events.append,
        runtime_client=object(),
    )

    session.begin_audio_turn()
    session.commit_audio_turn()

    assert events[-1] == {
        "type": "turn_completed",
        "turnId": 1,
        "continuous": False,
        "cancelled": True,
    }
    assert session._active_turn_id is None
