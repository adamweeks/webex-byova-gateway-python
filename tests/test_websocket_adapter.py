"""Tests for WebSocket and shared-message translation."""

import base64
import io
import wave

import pytest

from src.generated.byova_common_pb2 import EventInput, OutputEvent
from src.generated.voicevirtualagent_pb2 import Prompt, VoiceVAResponse
from src.transports.websocket_adapter import (
    UnsupportedMediaError,
    frame_response_payloads,
    request_to_protobuf,
    response_to_payload,
)
from src.transports.websocket_models import VoiceVARequestEnvelope


def _request() -> VoiceVARequestEnvelope:
    return VoiceVARequestEnvelope.model_validate(
        {
            "type": "VOICE_VA_REQUEST",
            "seq": 1,
            "ts": "2026-09-08T12:00:00Z",
            "conversation_id": "call-1",
            "payload": {
                "conversation_id": "call-1",
                "customer_org_id": "org-1",
                "virtual_agent_id": "agent-1",
                "voice_va_input_type": {"event_input": {"event_type": "SESSION_START"}},
            },
        }
    )


def _wav(audio: bytes) -> bytes:
    output = io.BytesIO()
    with wave.open(output, "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(1)
        wav_file.setframerate(8000)
        wav_file.writeframes(audio)
    return output.getvalue()


def test_request_maps_to_existing_protobuf_contract():
    request = request_to_protobuf(_request())
    assert request.customer_org_id == "org-1"
    assert request.virtual_agent_id == "agent-1"
    assert request.event_input.event_type == EventInput.EventType.SESSION_START


def test_response_maps_audio_events_and_summary_to_json_names():
    response = VoiceVAResponse(
        prompts=[
            Prompt(
                text="Transferring",
                audio_content=b"audio",
                is_barge_in_enabled=False,
            )
        ],
        output_events=[
            OutputEvent(
                event_type=OutputEvent.EventType.TRANSFER_TO_AGENT,
                name="transfer_requested",
                metadata={"summary": "Customer needs help"},
            )
        ],
        response_type=VoiceVAResponse.ResponseType.FINAL,
    )
    response.session_summary.text = "Customer needs help"
    payload = response_to_payload(response)
    assert payload["response_type"] == "FINAL"
    assert base64.b64decode(payload["prompts"][0]["audio_content_b64"]) == b"audio"
    assert payload["output_events"][0]["event_type"] == "TRANSFER_TO_AGENT"
    assert payload["session_summary"]["text"] == "Customer needs help"


def test_raw_chunk_mode_strips_wav_and_adds_empty_final():
    audio = bytes(range(256)) * 30
    response = VoiceVAResponse(
        prompts=[
            Prompt(
                text="Hello",
                audio_content=_wav(audio),
                is_barge_in_enabled=True,
            )
        ],
        response_type=VoiceVAResponse.ResponseType.FINAL,
    )
    payloads = frame_response_payloads(
        response, output_mode="raw_chunk", chunk_size=3200
    )
    assert [value["response_type"] for value in payloads] == [
        "CHUNK",
        "CHUNK",
        "CHUNK",
        "FINAL",
    ]
    rebuilt = b"".join(
        base64.b64decode(value["prompts"][0]["audio_content_b64"])
        for value in payloads[:-1]
    )
    assert rebuilt == audio
    assert payloads[-1]["prompts"] == [
        {"audio_content_b64": "", "is_barge_in_enabled": True}
    ]


def test_raw_chunk_mode_uses_one_terminator_for_multiple_inline_prompts():
    response = VoiceVAResponse(
        prompts=[
            Prompt(audio_content=b"a" * 100),
            Prompt(audio_content=b"b" * 100),
        ],
        response_type=VoiceVAResponse.ResponseType.FINAL,
    )

    payloads = frame_response_payloads(
        response, output_mode="raw_chunk", chunk_size=100
    )

    assert [
        base64.b64decode(value["prompts"][0]["audio_content_b64"])
        for value in payloads[:-1]
    ] == [b"a" * 100, b"b" * 100]
    assert payloads[-1]["prompts"] == [
        {"audio_content_b64": "", "is_barge_in_enabled": False}
    ]


def test_raw_chunk_mode_rejects_mixed_inline_and_uri_audio():
    response = VoiceVAResponse(
        prompts=[Prompt(audio_content=b"a" * 100, audio_uri="https://example.test/a")],
        response_type=VoiceVAResponse.ResponseType.FINAL,
    )

    with pytest.raises(UnsupportedMediaError, match="cannot mix"):
        frame_response_payloads(response, output_mode="raw_chunk")


def test_wav_final_rejects_raw_audio_and_chunk_responses():
    raw_final = VoiceVAResponse(
        prompts=[Prompt(audio_content=b"raw audio" * 20)],
        response_type=VoiceVAResponse.ResponseType.FINAL,
    )
    with pytest.raises(UnsupportedMediaError, match="WAV"):
        frame_response_payloads(raw_final, output_mode="wav_final")

    chunk = VoiceVAResponse(
        prompts=[Prompt(audio_content=b"x" * 100)],
        response_type=VoiceVAResponse.ResponseType.CHUNK,
    )
    with pytest.raises(UnsupportedMediaError, match="CHUNK"):
        frame_response_payloads(chunk, output_mode="wav_final")
