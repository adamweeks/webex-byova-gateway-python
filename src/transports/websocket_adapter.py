"""Translate WebSocket JSON messages to and from the shared gRPC data model."""

from __future__ import annotations

import base64
import io
import wave
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Iterable

from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

from src.generated.byova_common_pb2 import DTMFDigits, EventInput, OutputEvent
from src.generated.voicevirtualagent_pb2 import (
    VoiceInput,
    VoiceVARequest,
    VoiceVAResponse,
)

from .websocket_models import VoiceVARequestEnvelope


class UnsupportedMediaError(ValueError):
    """Raised when a response cannot be framed without transcoding."""


def request_to_protobuf(envelope: VoiceVARequestEnvelope) -> VoiceVARequest:
    """Convert a validated WebSocket request into the shared request model."""

    payload = envelope.payload
    request = VoiceVARequest(
        conversation_id=payload.conversation_id,
        customer_org_id=payload.customer_org_id,
        virtual_agent_id=payload.virtual_agent_id or "",
        allow_partial_responses=payload.allow_partial_responses,
        vendor_specific_config=payload.vendor_specific_config or "",
        additional_info=payload.additional_info,
    )
    input_value = payload.voice_va_input_type
    if hasattr(input_value, "audio_input"):
        audio = input_value.audio_input
        timestamp = Timestamp()
        if audio.audio_timestamp is not None:
            timestamp.FromDatetime(audio.audio_timestamp)
        request.audio_input.CopyFrom(
            VoiceInput(
                caller_audio=(
                    base64.b64decode(audio.caller_audio_b64, validate=True)
                    if audio.caller_audio_b64 is not None
                    else b""
                ),
                encoding=VoiceInput.VoiceEncoding.Value(audio.encoding),
                sample_rate_hertz=audio.sample_rate_hertz,
                audio_timestamp=timestamp,
                language_code=audio.language_code or "",
                is_single_utterance=audio.is_single_utterance,
            )
        )
    elif hasattr(input_value, "dtmf_input"):
        request.dtmf_input.dtmf_events.extend(
            DTMFDigits.Value(value) for value in input_value.dtmf_input.dtmf_events
        )
    else:
        event = input_value.event_input
        request.event_input.event_type = EventInput.EventType.Value(event.event_type)
        request.event_input.name = event.name or ""
        request.event_input.parameters.update(event.parameters)
    return request


def is_session_start(envelope: VoiceVARequestEnvelope) -> bool:
    value = envelope.payload.voice_va_input_type
    return (
        hasattr(value, "event_input")
        and value.event_input.event_type == "SESSION_START"
    )


def _text_content(value: Any) -> dict[str, Any] | None:
    field = value.WhichOneof("input_content")
    if not field:
        return None
    result: dict[str, Any] = {field: getattr(value, field)}
    if value.language_code:
        result["language_code"] = value.language_code
    return result


def response_to_payload(response: VoiceVAResponse) -> dict[str, Any]:
    """Convert a shared response into the intended WebSocket JSON payload."""

    payload: dict[str, Any] = {
        "response_type": VoiceVAResponse.ResponseType.Name(response.response_type)
    }
    if response.prompts:
        prompts = []
        for prompt in response.prompts:
            value: dict[str, Any] = {"is_barge_in_enabled": prompt.is_barge_in_enabled}
            if prompt.text:
                value["text"] = prompt.text
            if prompt.audio_uri:
                value["audio_uri"] = prompt.audio_uri
            if prompt.audio_content:
                value["audio_content_b64"] = base64.b64encode(
                    prompt.audio_content
                ).decode("ascii")
            prompts.append(value)
        payload["prompts"] = prompts
    if response.output_events:
        payload["output_events"] = [
            {
                "event_type": OutputEvent.EventType.Name(event.event_type),
                **({"name": event.name} if event.name else {}),
                **(
                    {
                        "metadata": MessageToDict(
                            event.metadata, preserving_proto_field_name=True
                        )
                    }
                    if event.metadata
                    else {}
                ),
            }
            for event in response.output_events
        ]
    if response.input_sensitive:
        payload["input_sensitive"] = True
    if response.input_mode:
        payload["input_mode"] = (
            response.DESCRIPTOR.fields_by_name["input_mode"]
            .enum_type.values_by_number[response.input_mode]
            .name
        )
    if response.HasField("input_handling_config"):
        payload["input_handling_config"] = MessageToDict(
            response.input_handling_config,
            preserving_proto_field_name=True,
            use_integers_for_enums=False,
        )
    transcript = _text_content(response.session_transcript)
    if transcript:
        payload["session_transcript"] = transcript
    summary = _text_content(response.session_summary)
    if summary:
        payload["session_summary"] = summary
    return payload


def _wav_frames(audio: bytes) -> bytes:
    try:
        with wave.open(io.BytesIO(audio), "rb") as wav_file:
            return wav_file.readframes(wav_file.getnframes())
    except (wave.Error, EOFError) as error:
        raise UnsupportedMediaError("invalid WAV response audio") from error


def _chunks(audio: bytes, chunk_size: int) -> Iterable[bytes]:
    if not audio:
        return []
    if len(audio) < 100:
        raise UnsupportedMediaError("raw response audio is smaller than 100 bytes")
    parts = [
        audio[offset : offset + chunk_size]
        for offset in range(0, len(audio), chunk_size)
    ]
    if len(parts) > 1 and len(parts[-1]) < 100:
        parts[-2] += parts[-1]
        parts.pop()
    return parts


def frame_response_payloads(
    response: VoiceVAResponse,
    *,
    output_mode: str,
    chunk_size: int = 3_200,
) -> list[dict[str, Any]]:
    """Apply the connector's frozen WebSocket output framing policy."""

    if not 100 <= chunk_size <= 65_536:
        raise ValueError("chunk_size must be between 100 and 65536 bytes")
    payload = response_to_payload(response)
    prompts = payload.get("prompts", [])
    audio_prompts = [item for item in prompts if item.get("audio_content_b64")]
    if output_mode == "wav_final":
        if response.response_type == VoiceVAResponse.ResponseType.CHUNK:
            raise UnsupportedMediaError("wav_final connector returned CHUNK audio")
        for item in audio_prompts:
            audio = base64.b64decode(item["audio_content_b64"], validate=True)
            if not audio.startswith(b"RIFF"):
                raise UnsupportedMediaError("wav_final response must contain WAV audio")
        return [payload]
    if output_mode != "raw_chunk":
        raise UnsupportedMediaError(f"unsupported output mode: {output_mode}")

    if response.response_type == VoiceVAResponse.ResponseType.CHUNK:
        for item in audio_prompts:
            audio = base64.b64decode(item["audio_content_b64"], validate=True)
            if not 100 <= len(audio) <= 65_536:
                raise UnsupportedMediaError("CHUNK audio must be 100 to 65536 bytes")
        return [payload]
    if not audio_prompts:
        return [payload]

    framed: list[dict[str, Any]] = []
    for prompt_index, item in enumerate(audio_prompts):
        audio = base64.b64decode(item["audio_content_b64"], validate=True)
        if audio.startswith(b"RIFF"):
            audio = _wav_frames(audio)
        for index, chunk in enumerate(_chunks(audio, chunk_size)):
            chunk_prompt = {
                "audio_content_b64": base64.b64encode(chunk).decode("ascii"),
                "is_barge_in_enabled": item.get("is_barge_in_enabled", False),
            }
            if index == 0 and prompt_index == 0 and item.get("text"):
                chunk_prompt["text"] = item["text"]
            framed.append({"prompts": [chunk_prompt], "response_type": "CHUNK"})

    final_payload = deepcopy(payload)
    final_payload["response_type"] = payload["response_type"]
    final_prompts = []
    for item in final_payload.pop("prompts", []):
        item.pop("audio_content_b64", None)
        item.pop("text", None)
        if item.get("audio_uri"):
            final_prompts.append(item)
    if final_prompts:
        final_payload["prompts"] = final_prompts
    framed.append(final_payload)
    return framed


def envelope(
    *, message_type: str, seq: int, conversation_id: str, payload: dict[str, Any]
) -> dict[str, Any]:
    return {
        "type": message_type,
        "seq": seq,
        "ts": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "conversation_id": conversation_id,
        "payload": payload,
    }
