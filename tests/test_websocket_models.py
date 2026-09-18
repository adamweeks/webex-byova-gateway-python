"""Contract tests for strict WebSocket request validation."""

import base64
from copy import deepcopy

import pytest
from pydantic import ValidationError

from src.transports.websocket_models import (
    ListVARequest,
    VoiceVARequestEnvelope,
    parse_incoming_envelope,
)


def session_start() -> dict:
    return {
        "type": "VOICE_VA_REQUEST",
        "seq": 1,
        "ts": "2026-09-08T12:00:00Z",
        "conversation_id": "call-1",
        "payload": {
            "conversation_id": "call-1",
            "customer_org_id": "org-1",
            "virtual_agent_id": "agent-1",
            "voice_va_input_type": {
                "event_input": {
                    "event_type": "SESSION_START",
                    "name": "entry",
                    "parameters": {"entry_point": "ivr"},
                }
            },
        },
    }


def test_official_session_start_shape_is_accepted():
    parsed = parse_incoming_envelope(session_start())
    assert isinstance(parsed, VoiceVARequestEnvelope)
    assert parsed.payload.customer_org_id == "org-1"


def test_discovery_ignores_forward_compatible_unknown_fields():
    parsed = ListVARequest.model_validate(
        {
            "customer_org_id": "org-1",
            "is_default_virtual_agent_enabled": False,
            "future_control_plane_field": {"version": 2},
        }
    )

    assert parsed.customer_org_id == "org-1"
    assert parsed.is_default_virtual_agent_enabled is False


def test_concrete_envelope_rejects_unknown_fields_despite_upstream_allof_defect():
    value = session_start()
    value["unexpected"] = True
    with pytest.raises(ValidationError):
        parse_incoming_envelope(value)


def test_envelope_and_payload_conversation_ids_must_match():
    value = session_start()
    value["payload"]["conversation_id"] = "other-call"
    with pytest.raises(ValidationError, match="conversation_id must match"):
        parse_incoming_envelope(value)


def test_audio_rejects_invalid_base64_and_oversized_decoded_content():
    value = session_start()
    value["payload"]["voice_va_input_type"] = {
        "audio_input": {
            "caller_audio_b64": "not-base64!",
            "encoding": "MULAW_FORMAT",
            "sample_rate_hertz": 8000,
        }
    }
    with pytest.raises(ValidationError, match="valid base64"):
        parse_incoming_envelope(value)

    oversized = deepcopy(value)
    oversized["payload"]["voice_va_input_type"]["audio_input"]["caller_audio_b64"] = (
        base64.b64encode(b"x" * 65_537).decode("ascii")
    )
    with pytest.raises(ValidationError, match="exceeds 65536"):
        parse_incoming_envelope(oversized)


def test_audio_content_is_optional_per_published_contract():
    value = session_start()
    value["payload"]["voice_va_input_type"] = {
        "audio_input": {
            "encoding": "MULAW_FORMAT",
            "sample_rate_hertz": 8000,
        }
    }

    parsed = parse_incoming_envelope(value)

    assert parsed.payload.voice_va_input_type.audio_input.caller_audio_b64 is None


def test_unknown_message_type_is_fatal_to_parsing():
    with pytest.raises(ValueError, match="unsupported message type"):
        parse_incoming_envelope(
            {
                "type": "VOICE_VA_RESPONSE",
                "seq": 1,
                "ts": "2026-09-08T12:00:00Z",
                "conversation_id": "call-1",
            }
        )
