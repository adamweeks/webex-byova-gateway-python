# ruff: noqa: UP006, UP037, UP045
"""Local-only HTTP/WebSocket bridge from a browser to CES BidiRunSession."""

from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import logging
import os
import queue
import re
import threading
import time
import uuid
import webbrowser
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, Mapping, Optional, Protocol, Union

import yaml
from aiohttp import WSMsgType, web

from .audio import (
    AUDIO_PROFILES,
    AudioProfile,
    browser_output_audio,
    ces_input_audio,
    pcm16_rms,
    silence_chunks,
)

LOGGER = logging.getLogger("gecx_audio_lab")
STATIC_ROOT = Path(__file__).resolve().parent / "static"
_SESSION_ID_PATTERN = re.compile(r"^[a-zA-Z0-9][a-zA-Z0-9-_]{4,62}$")
_STOP = object()


@dataclass(frozen=True)
class SessionTarget:
    """Non-secret GECX routing plus a reference to local credentials."""

    id: str
    label: str
    project_id: str
    location: str
    application_id: str
    deployment_id: Optional[str] = None
    deployment: Optional[str] = None
    entry_agent: Optional[str] = None
    api_endpoint: Optional[str] = None
    credentials_env: Optional[str] = None
    connector_id: Optional[str] = None
    auth_config: Optional[Mapping[str, Any]] = None

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "SessionTarget":
        """Validate and construct a target from YAML or CLI values."""
        required = ("id", "project_id", "application_id")
        missing = [field for field in required if not str(value.get(field, "")).strip()]
        if missing:
            raise ValueError(f"GECX target is missing: {', '.join(missing)}")
        target_id = str(value["id"]).strip()
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}", target_id):
            raise ValueError(f"Invalid GECX target id: {target_id}")
        return cls(
            id=target_id,
            label=str(value.get("label") or target_id),
            project_id=str(value["project_id"]).strip(),
            location=str(value.get("location") or "us").strip(),
            application_id=str(value["application_id"]).strip(),
            deployment_id=_optional_string(value.get("deployment_id")),
            deployment=_optional_string(value.get("deployment")),
            entry_agent=_optional_string(value.get("entry_agent")),
            api_endpoint=_optional_string(value.get("api_endpoint")),
            credentials_env=_optional_string(value.get("credentials_env")),
            connector_id=_optional_string(value.get("connector_id")),
            auth_config=value.get("auth_config"),
        )

    @property
    def app_path(self) -> str:
        return (
            f"projects/{self.project_id}/locations/{self.location}/"
            f"apps/{self.application_id}"
        )

    @property
    def deployment_path(self) -> Optional[str]:
        if self.deployment:
            return self.deployment
        if self.deployment_id:
            return f"{self.app_path}/deployments/{self.deployment_id}"
        return None

    @property
    def runtime_endpoint(self) -> str:
        if self.api_endpoint:
            return self.api_endpoint
        if self.location.lower() == "global":
            return "ces.googleapis.com"
        return f"ces.{self.location.lower()}.rep.googleapis.com"

    def public_dict(self) -> Dict[str, Any]:
        """Return target details safe to render in the browser."""
        return {
            "id": self.id,
            "label": self.label,
            "provider": "gecx",
            "providerLabel": "Google CX Agent Studio",
            "interactionMode": "continuous",
            "interactionLabel": "CES bidirectional stream",
            "supportedProfileIds": ["native", "wxcc"],
            "defaultProfileId": "native",
            "projectId": self.project_id,
            "location": self.location,
            "applicationId": self.application_id,
            "deploymentId": self.deployment_id,
            "deploymentMode": "published" if self.deployment_path else "draft",
            "entryAgent": self.entry_agent,
            "runtimeEndpoint": self.runtime_endpoint,
            "credentialMode": (
                _gecx_credential_mode(self.auth_config or {})
                if self.auth_config is not None
                else ("environment credential" if self.credentials_env else "ADC")
            ),
            "initialText": str((self.auth_config or {}).get("initial_message") or ""),
        }


@dataclass(frozen=True)
class AWSLexTarget:
    """One discovered AWS Lex bot and alias using gateway-owned auth."""

    id: str
    label: str
    connector_id: str
    region_name: str
    locale_id: str
    bot_id: str
    bot_alias_id: str
    bot_name: str
    initial_trigger_text: str = "hello"
    text_request_content_type: str = "text/plain; charset=utf-8"
    audio_request_content_type: str = "audio/l16; rate=16000; channels=1"
    response_content_type: str = "audio/pcm"
    streaming: bool = False

    def public_dict(self) -> Dict[str, Any]:
        mode = "streaming" if self.streaming else "connector_parity"
        return {
            "id": self.id,
            "label": self.label,
            "provider": "aws_lex",
            "providerLabel": "AWS Lex V2",
            "interactionMode": "continuous" if self.streaming else "manual_turns",
            "interactionLabel": (
                "StartConversation bidirectional stream"
                if self.streaming
                else "RecognizeUtterance connector parity"
            ),
            "supportedProfileIds": ["lex_native"],
            "defaultProfileId": "lex_native",
            "region": self.region_name,
            "locale": self.locale_id,
            "botName": self.bot_name,
            "mode": mode,
            "credentialMode": "AWS default credential chain",
            "initialText": self.initial_trigger_text,
        }


LabTarget = Union[SessionTarget, AWSLexTarget]


@dataclass(frozen=True)
class LabSettings:
    """Server configuration for one local Audio Lab process."""

    targets: Dict[str, LabTarget]
    default_target_id: str

    @classmethod
    def from_yaml(cls, path: Path) -> "LabSettings":
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        expanded = _expand_environment(raw)
        target_values = expanded.get("targets")
        if not isinstance(target_values, list) or not target_values:
            raise ValueError("Audio Lab config must contain at least one target")
        targets = {
            target.id: target
            for target in (SessionTarget.from_mapping(item) for item in target_values)
        }
        if len(targets) != len(target_values):
            raise ValueError("Audio Lab target ids must be unique")
        default_id = str(expanded.get("default_target") or next(iter(targets)))
        if default_id not in targets:
            raise ValueError(f"Unknown default_target: {default_id}")
        return cls(targets=targets, default_target_id=default_id)

    @classmethod
    def from_sources(
        cls,
        gateway_config: Path,
        lab_config: Optional[Path] = None,
        *,
        aws_discoverer: Optional[Callable[[str, Mapping[str, Any]], list[AWSLexTarget]]] = None,
    ) -> "LabSettings":
        """Load supported connector targets, then merge optional local overrides."""
        raw = yaml.safe_load(gateway_config.read_text(encoding="utf-8")) or {}
        targets: Dict[str, LabTarget] = {}
        discoverer = aws_discoverer or _discover_aws_lex_targets
        connectors = raw.get("connectors") or {}
        if not isinstance(connectors, Mapping):
            raise ValueError("Gateway config connectors must be a mapping")
        for connector_id, definition in connectors.items():
            if not isinstance(definition, Mapping):
                continue
            connector_type = str(definition.get("type") or "")
            connector_config = definition.get("config") or {}
            if not isinstance(connector_config, Mapping):
                continue
            if connector_type == "gecx_connector":
                try:
                    target = SessionTarget.from_mapping(
                        {
                            **connector_config,
                            "id": str(connector_id),
                            "label": str(connector_config.get("label") or connector_id),
                            "connector_id": str(connector_id),
                            "auth_config": dict(connector_config),
                        }
                    )
                except ValueError as exc:
                    LOGGER.warning("Skipping invalid GECX connector %s: %s", connector_id, exc)
                else:
                    targets[target.id] = target
            elif connector_type == "aws_lex_connector":
                for target in discoverer(str(connector_id), connector_config):
                    targets[target.id] = target

        default_id = next(iter(targets), "")
        if lab_config:
            overlay = cls.from_yaml(lab_config)
            targets.update(overlay.targets)
            default_id = overlay.default_target_id
        if not targets:
            raise ValueError(
                "No GECX or AWS Lex targets were found in the gateway configuration"
            )
        return cls(targets=targets, default_target_id=default_id or next(iter(targets)))

    def public_dict(self) -> Dict[str, Any]:
        return {
            "targets": [target.public_dict() for target in self.targets.values()],
            "defaultTargetId": self.default_target_id,
            "profiles": [profile.public_dict() for profile in AUDIO_PROFILES.values()],
            "defaultProfileId": self.targets[self.default_target_id].public_dict()[
                "defaultProfileId"
            ],
            "defaultEndpointingSilenceMs": 2000,
        }


@dataclass(frozen=True)
class AudioPacket:
    """One browser-ready PCM response with diagnostic metadata."""

    pcm16: bytes
    metadata: Dict[str, Any]


OutboundItem = Union[Dict[str, Any], AudioPacket]
OutboundCallback = Callable[[OutboundItem], None]


class DirectGECXSession:
    """Own one direct CES BidiRunSession and its browser audio queues."""

    def __init__(
        self,
        target: SessionTarget,
        profile: AudioProfile,
        endpointing_silence_ms: int,
        outbound: OutboundCallback,
        *,
        client: Any = None,
        ces_module: Any = None,
    ) -> None:
        self.target = target
        self.profile = profile
        self.endpointing_silence_ms = min(5000, max(0, endpointing_silence_ms))
        self.outbound = outbound
        self.session_id = uuid.uuid4().hex
        if not _SESSION_ID_PATTERN.match(self.session_id):
            self.session_id = f"s{self.session_id}"[:63]
        self.session_path = f"{target.app_path}/sessions/{self.session_id}"
        self._client = client
        self._ces = ces_module
        self._requests: queue.Queue[Any] = queue.Queue(maxsize=512)
        self._thread = threading.Thread(
            target=self._run,
            name=f"gecx-audio-lab-{self.session_id[:8]}",
            daemon=True,
        )
        self._lock = threading.Lock()
        self._call: Any = None
        self._stopped = threading.Event()
        self._initial_text = ""
        self._turn_id = 0
        self._active_turn_id: Optional[int] = None
        self._active_turn_kind: Optional[str] = None
        self._continuous_audio = False
        self._active_audio_acknowledged = False
        self._commit_started_at: Optional[float] = None
        self._first_audio_emitted = False
        self._audio_frame_index = 0
        self._received_server_message = False

    def start(self, initial_text: str = "") -> None:
        """Start the CES worker; initial text is sent after session config."""
        self._initial_text = initial_text.strip()
        if self._initial_text:
            with self._lock:
                self._turn_id += 1
                self._active_turn_id = self._turn_id
                self._active_turn_kind = "opening_text"
                self._commit_started_at = time.monotonic()
                self._first_audio_emitted = False
        self._thread.start()

    @property
    def is_closed(self) -> bool:
        """Return whether the CES worker can no longer accept input."""
        return self._stopped.is_set()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout=timeout)

    def begin_audio_turn(self, *, continuous: bool = False) -> int:
        """Open a browser microphone turn before binary audio arrives."""
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before starting another")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._active_turn_kind = "audio"
            self._active_audio_acknowledged = False
            self._continuous_audio = continuous
            self._commit_started_at = None
            self._first_audio_emitted = False
            turn_id = self._turn_id
        self._emit(
            "turn_started",
            turnId=turn_id,
            input="audio",
            continuous=continuous,
        )
        return turn_id

    def send_pcm16(self, pcm16: bytes) -> None:
        """Queue one real-time browser microphone frame for CES."""
        if not pcm16:
            return
        with self._lock:
            active = self._active_turn_id
            kind = self._active_turn_kind
        if active is None or kind != "audio":
            raise RuntimeError("Start a microphone turn before sending audio")
        self._put_request(("audio", ces_input_audio(self.profile, pcm16)))

    def commit_audio_turn(self) -> int:
        """Append endpointing silence after the browser releases push-to-talk."""
        with self._lock:
            if self._active_turn_id is None or self._active_turn_kind != "audio":
                raise RuntimeError("There is no microphone turn to commit")
            turn_id = self._active_turn_id
            self._continuous_audio = False
            self._commit_started_at = time.monotonic()
        self._put_request(("commit", self.endpointing_silence_ms))
        self._emit(
            "turn_committed",
            turnId=turn_id,
            endpointingSilenceMs=self.endpointing_silence_ms,
        )
        return turn_id

    def send_text(self, text: str) -> int:
        """Send a text turn to isolate CES TTS from microphone/STT behavior."""
        text = text.strip()
        if not text:
            raise ValueError("Text input cannot be empty")
        if len(text) > 4000:
            raise ValueError("Text input is limited to 4000 characters")
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before sending text")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._active_turn_kind = "text"
            self._active_audio_acknowledged = False
            self._continuous_audio = False
            self._commit_started_at = time.monotonic()
            self._first_audio_emitted = False
            turn_id = self._turn_id
        self._put_request(("text", text))
        self._emit("turn_committed", turnId=turn_id, input="text")
        return turn_id

    def stop(self) -> None:
        """Half-close requests and cancel a still-open response iterator."""
        if self._stopped.is_set():
            return
        self._stopped.set()
        try:
            self._requests.put_nowait(_STOP)
        except queue.Full:
            pass
        with self._lock:
            call = self._call
        if call is not None and hasattr(call, "cancel"):
            try:
                call.cancel()
            except Exception:  # pragma: no cover - transport-specific cleanup
                LOGGER.debug("CES call cancellation failed", exc_info=True)

    def _put_request(self, item: Any) -> None:
        if self._stopped.is_set():
            raise RuntimeError("The GECX session is closed")
        try:
            self._requests.put(item, timeout=1.0)
        except queue.Full as exc:
            raise RuntimeError("Audio input queue is full") from exc

    def _load_ces(self) -> Any:
        if self._ces is None:
            from google.cloud import ces_v1

            self._ces = ces_v1
        return self._ces

    def _create_client(self) -> Any:
        ces = self._load_ces()
        from google.api_core import client_options as client_options_lib

        credentials = None
        if self.target.auth_config is not None:
            credentials = _load_gecx_credentials(self.target.auth_config)
        elif self.target.credentials_env:
            credential_path = os.getenv(self.target.credentials_env)
            if not credential_path:
                raise RuntimeError(
                    f"Credential environment variable is not set: "
                    f"{self.target.credentials_env}"
                )
            path = Path(credential_path).expanduser()
            if not path.is_file():
                raise RuntimeError(
                    f"Credential file from {self.target.credentials_env} does not exist"
                )
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(path)

        options = client_options_lib.ClientOptions(
            api_endpoint=self.target.runtime_endpoint,
            quota_project_id=self.target.project_id,
        )
        if credentials is not None:
            return ces.SessionServiceClient(
                credentials=credentials,
                client_options=options,
            )
        return ces.SessionServiceClient(client_options=options)

    def _request_generator(self) -> Iterator[Any]:
        ces = self._load_ces()
        input_config = ces.InputAudioConfig(
            audio_encoding=getattr(ces.AudioEncoding, self.profile.input_encoding),
            sample_rate_hertz=self.profile.input_sample_rate_hertz,
        )
        output_config = ces.OutputAudioConfig(
            audio_encoding=getattr(ces.AudioEncoding, self.profile.output_encoding),
            sample_rate_hertz=self.profile.output_sample_rate_hertz,
        )
        session_kwargs: Dict[str, Any] = {
            "session": self.session_path,
            "input_audio_config": input_config,
            "output_audio_config": output_config,
            "enable_text_streaming": True,
        }
        if self.target.deployment_path:
            session_kwargs["deployment"] = self.target.deployment_path
        if self.target.entry_agent:
            session_kwargs["entry_agent"] = self.target.entry_agent
        yield ces.BidiSessionClientMessage(config=ces.SessionConfig(**session_kwargs))
        if self._initial_text and not self._stopped.is_set():
            yield ces.BidiSessionClientMessage(
                realtime_input=ces.SessionInput(text=self._initial_text)
            )

        while not self._stopped.is_set():
            try:
                item = self._requests.get(timeout=0.25)
            except queue.Empty:
                continue
            if item is _STOP:
                break
            kind, payload = item
            if kind == "audio":
                yield ces.BidiSessionClientMessage(
                    realtime_input=ces.SessionInput(audio=payload)
                )
            elif kind == "text":
                yield ces.BidiSessionClientMessage(
                    realtime_input=ces.SessionInput(text=payload)
                )
            elif kind == "commit":
                for chunk in silence_chunks(self.profile, int(payload)):
                    if self._stopped.is_set():
                        break
                    yield ces.BidiSessionClientMessage(
                        realtime_input=ces.SessionInput(audio=chunk)
                    )

    def _run(self) -> None:
        try:
            if self._client is None:
                self._client = self._create_client()
            # The synchronous Google wrapper waits for the first server message
            # before returning its iterator. Announce the local session first so
            # a browser can stream microphone input even when initial text is blank.
            self._emit(
                "session_started",
                sessionId=self.session_id,
                sessionPath=self.session_path,
                targetId=self.target.id,
                profileId=self.profile.id,
                initialTurnPending=bool(self._initial_text),
            )
            if self._initial_text:
                self._emit(
                    "turn_committed",
                    turnId=self._active_turn_id,
                    input="opening_text",
                )
            call = self._client.bidi_run_session(requests=self._request_generator())
            with self._lock:
                self._call = call
            for message in call:
                if self._stopped.is_set():
                    break
                self._received_server_message = True
                self._handle_message(message)
            if not self._stopped.is_set():
                message = (
                    "The GECX response stream closed unexpectedly. Start a new "
                    "direct session and check the AZ agent logs."
                )
                LOGGER.error(message)
                self._emit("error", message=message, source="gecx")
        except Exception as exc:
            if not self._stopped.is_set():
                safe_message = _safe_error_message(
                    exc,
                    self.target,
                    stream_was_active=self._received_server_message,
                )
                LOGGER.error("Direct GECX session failed: %s", safe_message)
                LOGGER.debug("Direct GECX session traceback", exc_info=True)
                self._emit("error", message=safe_message, source="gecx")
        finally:
            self._stopped.set()
            with self._lock:
                self._call = None
            self._emit("session_closed", sessionId=self.session_id)

    def _handle_message(self, message: Any) -> None:
        recognition = getattr(message, "recognition_result", None)
        if recognition:
            transcript = str(getattr(recognition, "transcript", "")).strip()
            with self._lock:
                if self._active_turn_kind == "audio":
                    self._active_audio_acknowledged = True
            if transcript:
                self._emit("transcript", text=transcript)

        if getattr(message, "interruption_signal", None):
            self._emit("interruption")

        output = getattr(message, "session_output", None)
        if output:
            text = str(getattr(output, "text", "") or "")
            if text:
                self._emit("agent_text", text=text)
            raw_audio = getattr(output, "audio", b"") or b""
            if isinstance(raw_audio, str):
                try:
                    raw_audio = base64.b64decode(raw_audio)
                except ValueError:
                    raw_audio = b""
            if raw_audio:
                self._emit_audio(bytes(raw_audio))
            if bool(getattr(output, "turn_completed", False)):
                self._complete_turn()
            if getattr(output, "end_session", None):
                self._emit("end_session", source="session_output")

        if getattr(message, "end_session", None):
            self._emit("end_session", source="server_message")

    def _emit_audio(self, raw_audio: bytes) -> None:
        pcm16 = browser_output_audio(self.profile, raw_audio)
        bytes_per_sample = 2 if self.profile.output_encoding == "LINEAR16" else 1
        encoded_duration_ms = (
            len(raw_audio)
            * 1000.0
            / (self.profile.output_sample_rate_hertz * bytes_per_sample)
        )
        with self._lock:
            self._audio_frame_index += 1
            frame_index = self._audio_frame_index
            turn_id = self._active_turn_id
            first_for_turn = bool(turn_id is not None and not self._first_audio_emitted)
            latency_ms = None
            if first_for_turn:
                self._first_audio_emitted = True
                if self._commit_started_at is not None:
                    latency_ms = (time.monotonic() - self._commit_started_at) * 1000
        metadata = {
            "type": "audio",
            "frameIndex": frame_index,
            "turnId": turn_id,
            "firstForTurn": first_for_turn,
            "commitToFirstAudioMs": round(latency_ms, 1) if latency_ms else None,
            "sampleRateHertz": self.profile.output_sample_rate_hertz,
            "encoding": "LINEAR16",
            "rawEncoding": self.profile.output_encoding,
            "rawBytes": len(raw_audio),
            "pcmBytes": len(pcm16),
            "encodedDurationMs": round(encoded_duration_ms, 1),
            "rms": round(pcm16_rms(pcm16), 1),
            "anomalouslyLong": encoded_duration_ms >= 5000,
        }
        self.outbound(AudioPacket(pcm16=pcm16, metadata=metadata))

    def _complete_turn(self) -> None:
        with self._lock:
            if (
                self._active_turn_kind == "audio"
                and not self._active_audio_acknowledged
                and self._commit_started_at is None
            ):
                LOGGER.info(
                    "Ignoring prior CES turn completion while microphone turn %s "
                    "is still streaming",
                    self._active_turn_id,
                )
                return
            turn_id = self._active_turn_id
            continuous = bool(
                turn_id is not None
                and self._active_turn_kind == "audio"
                and self._continuous_audio
            )
            next_turn_id = None
            if continuous:
                self._turn_id += 1
                next_turn_id = self._turn_id
                self._active_turn_id = next_turn_id
                self._active_turn_kind = "audio"
            else:
                self._active_turn_id = None
                self._active_turn_kind = None
                self._continuous_audio = False
            self._active_audio_acknowledged = False
            self._commit_started_at = None
            self._first_audio_emitted = False
        self._emit(
            "turn_completed",
            turnId=turn_id,
            continuous=continuous,
            nextTurnId=next_turn_id,
        )

    def _emit(self, event_type: str, **fields: Any) -> None:
        self.outbound({"type": event_type, **fields})


class LabSession(Protocol):
    """Provider-neutral session contract consumed by the browser bridge."""

    @property
    def is_closed(self) -> bool: ...

    def start(self, initial_text: str = "") -> None: ...

    def stop(self) -> None: ...

    def join(self, timeout: float = 5.0) -> None: ...

    def begin_audio_turn(self, *, continuous: bool = False) -> int: ...

    def send_pcm16(self, pcm16: bytes) -> None: ...

    def commit_audio_turn(self) -> int: ...

    def send_text(self, text: str) -> int: ...


class AWSLexRecognizeSession:
    """Connector-parity AWS Lex session using buffered RecognizeUtterance calls."""

    def __init__(
        self,
        target: AWSLexTarget,
        profile: AudioProfile,
        endpointing_silence_ms: int,
        outbound: OutboundCallback,
        *,
        runtime_client: Any = None,
    ) -> None:
        del endpointing_silence_ms
        self.target = target
        self.profile = profile
        self.outbound = outbound
        self.session_id = f"lab-{uuid.uuid4().hex}"[:100]
        self._runtime_client = runtime_client
        self._stopped = threading.Event()
        self._lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._turn_id = 0
        self._active_turn_id: Optional[int] = None
        self._audio_buffer = bytearray()
        self._commit_started_at: Optional[float] = None
        self._audio_frame_index = 0

    @property
    def is_closed(self) -> bool:
        return self._stopped.is_set()

    def start(self, initial_text: str = "") -> None:
        if self._runtime_client is None:
            import boto3

            self._runtime_client = boto3.Session(
                region_name=self.target.region_name
            ).client("lexv2-runtime")
        self._emit(
            "session_started",
            sessionId=self.session_id,
            targetId=self.target.id,
            profileId=self.profile.id,
            provider="aws_lex",
            initialTurnPending=bool(initial_text.strip()),
        )
        opening = initial_text.strip()
        if opening:
            self.send_text(opening, input_kind="opening_text")

    def join(self, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        for worker in list(self._workers):
            if worker is threading.current_thread():
                continue
            worker.join(max(0.0, deadline - time.monotonic()))

    def begin_audio_turn(self, *, continuous: bool = False) -> int:
        if continuous:
            raise RuntimeError(
                "AWS connector-parity mode uses explicit Start talking / Finish turn input"
            )
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before starting another")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._audio_buffer.clear()
            turn_id = self._turn_id
        self._emit("turn_started", turnId=turn_id, input="audio", continuous=False)
        return turn_id

    def send_pcm16(self, pcm16: bytes) -> None:
        if self._stopped.is_set():
            raise RuntimeError("The AWS Lex session is closed")
        with self._lock:
            if self._active_turn_id is None:
                raise RuntimeError("Start a microphone turn before sending audio")
            self._audio_buffer.extend(pcm16[: len(pcm16) // 2 * 2])

    def commit_audio_turn(self) -> int:
        with self._lock:
            if self._active_turn_id is None:
                raise RuntimeError("There is no microphone turn to commit")
            turn_id = self._active_turn_id
            audio = bytes(self._audio_buffer)
            self._audio_buffer.clear()
            self._commit_started_at = time.monotonic()
        if not audio:
            with self._lock:
                self._active_turn_id = None
                self._commit_started_at = None
            self._emit("turn_completed", turnId=turn_id, continuous=False, cancelled=True)
            return turn_id
        self._emit("turn_committed", turnId=turn_id, input="audio")
        self._start_worker(
            self._recognize,
            turn_id,
            self.target.audio_request_content_type,
            audio,
        )
        return turn_id

    def send_text(self, text: str, *, input_kind: str = "text") -> int:
        text = text.strip()
        if not text:
            raise ValueError("Text input cannot be empty")
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before sending text")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._commit_started_at = time.monotonic()
            turn_id = self._turn_id
        self._emit("turn_committed", turnId=turn_id, input=input_kind)
        self._start_worker(
            self._recognize,
            turn_id,
            self.target.text_request_content_type,
            text.encode("utf-8"),
        )
        return turn_id

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        self._emit("session_closed", sessionId=self.session_id)

    def _start_worker(self, function: Callable[..., None], *args: Any) -> None:
        worker = threading.Thread(
            target=function,
            args=args,
            name=f"lex-recognize-{self.session_id[-8:]}",
            daemon=True,
        )
        self._workers.append(worker)
        worker.start()

    def _recognize(self, turn_id: int, content_type: str, content: bytes) -> None:
        try:
            response = self._runtime_client.recognize_utterance(
                botId=self.target.bot_id,
                botAliasId=self.target.bot_alias_id,
                localeId=self.target.locale_id,
                sessionId=self.session_id,
                requestContentType=content_type,
                responseContentType=self.target.response_content_type,
                inputStream=content,
            )
            transcript = _decode_lex_header(response.get("inputTranscript"))
            if isinstance(transcript, str) and transcript.strip():
                self._emit("transcript", text=transcript.strip())
            messages = _decode_lex_header(response.get("messages")) or []
            for message in messages if isinstance(messages, list) else []:
                text = str(message.get("content") or "").strip()
                if text:
                    self._emit("agent_text", text=text)
            stream = response.get("audioStream")
            if stream is not None:
                raw_audio = stream.read()
                if hasattr(stream, "close"):
                    stream.close()
                if raw_audio:
                    self._emit_audio(bytes(raw_audio), turn_id)
        except Exception as exc:
            if not self._stopped.is_set():
                LOGGER.error("AWS Lex RecognizeUtterance failed", exc_info=True)
                self._emit("error", message=_safe_aws_error(exc), source="aws_lex")
        finally:
            with self._lock:
                if self._active_turn_id == turn_id:
                    self._active_turn_id = None
                    self._commit_started_at = None
            if not self._stopped.is_set():
                self._emit("turn_completed", turnId=turn_id, continuous=False)

    def _emit_audio(self, raw_audio: bytes, turn_id: int) -> None:
        with self._lock:
            self._audio_frame_index += 1
            frame_index = self._audio_frame_index
            latency_ms = (
                (time.monotonic() - self._commit_started_at) * 1000
                if self._commit_started_at is not None
                else None
            )
        duration_ms = len(raw_audio) * 1000.0 / (16000 * 2)
        self.outbound(
            AudioPacket(
                pcm16=raw_audio,
                metadata={
                    "type": "audio",
                    "frameIndex": frame_index,
                    "turnId": turn_id,
                    "firstForTurn": True,
                    "commitToFirstAudioMs": round(latency_ms, 1) if latency_ms else None,
                    "sampleRateHertz": 16000,
                    "encoding": "LINEAR16",
                    "rawEncoding": "LINEAR16",
                    "rawBytes": len(raw_audio),
                    "pcmBytes": len(raw_audio),
                    "encodedDurationMs": round(duration_ms, 1),
                    "rms": round(pcm16_rms(raw_audio), 1),
                    "anomalouslyLong": duration_ms >= 5000,
                },
            )
        )

    def _emit(self, event_type: str, **fields: Any) -> None:
        self.outbound({"type": event_type, **fields})


class AWSLexStreamingSession:
    """Direct AWS Lex StartConversation session with a live request event stream."""

    def __init__(
        self,
        target: AWSLexTarget,
        profile: AudioProfile,
        endpointing_silence_ms: int,
        outbound: OutboundCallback,
        *,
        runtime_client: Any = None,
    ) -> None:
        del endpointing_silence_ms
        self.target = target
        self.profile = profile
        self.outbound = outbound
        self.session_id = f"lab-{uuid.uuid4().hex}"[:100]
        self._runtime_client = runtime_client
        self._requests: queue.Queue[Any] = queue.Queue(maxsize=512)
        self._stopped = threading.Event()
        self._thread = threading.Thread(
            target=self._run,
            name=f"lex-stream-{self.session_id[-8:]}",
            daemon=True,
        )
        self._lock = threading.Lock()
        self._response_stream: Any = None
        self._turn_id = 0
        self._active_turn_id: Optional[int] = None
        self._active_turn_kind: Optional[str] = None
        self._continuous_audio = False
        self._commit_started_at: Optional[float] = None
        self._first_audio_emitted = False
        self._audio_frame_index = 0
        self._initial_text = ""

    @property
    def is_closed(self) -> bool:
        return self._stopped.is_set()

    def start(self, initial_text: str = "") -> None:
        self._initial_text = initial_text.strip()
        if self._initial_text:
            self._turn_id = 1
            self._active_turn_id = 1
            self._active_turn_kind = "text"
            self._commit_started_at = time.monotonic()
        self._thread.start()

    def join(self, timeout: float = 5.0) -> None:
        if self._thread.is_alive() and self._thread is not threading.current_thread():
            self._thread.join(timeout)

    def begin_audio_turn(self, *, continuous: bool = False) -> int:
        if not continuous:
            raise RuntimeError("AWS streaming mode keeps call audio open continuously")
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before starting another")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._active_turn_kind = "audio"
            self._continuous_audio = True
            self._commit_started_at = None
            self._first_audio_emitted = False
            turn_id = self._turn_id
        self._emit("turn_started", turnId=turn_id, input="audio", continuous=True)
        return turn_id

    def send_pcm16(self, pcm16: bytes) -> None:
        with self._lock:
            if self._active_turn_id is None:
                raise RuntimeError("Start a microphone turn before sending audio")
        self._put_request(("audio", bytes(pcm16[: len(pcm16) // 2 * 2])))

    def commit_audio_turn(self) -> int:
        with self._lock:
            if self._active_turn_id is None:
                raise RuntimeError("There is no microphone turn to commit")
            turn_id = self._active_turn_id
            self._commit_started_at = time.monotonic()
        self._emit("turn_committed", turnId=turn_id, input="audio")
        return turn_id

    def send_text(self, text: str) -> int:
        text = text.strip()
        if not text:
            raise ValueError("Text input cannot be empty")
        with self._lock:
            if self._active_turn_id is not None:
                raise RuntimeError("Finish the active turn before sending text")
            self._turn_id += 1
            self._active_turn_id = self._turn_id
            self._active_turn_kind = "text"
            self._continuous_audio = False
            self._commit_started_at = time.monotonic()
            self._first_audio_emitted = False
            turn_id = self._turn_id
        self._put_request(("text", text))
        self._emit("turn_committed", turnId=turn_id, input="text")
        return turn_id

    def stop(self) -> None:
        if self._stopped.is_set():
            return
        self._stopped.set()
        try:
            self._requests.put_nowait(("disconnect", None))
            self._requests.put_nowait(_STOP)
        except queue.Full:
            pass
        stream = self._response_stream
        if stream is not None and hasattr(stream, "close"):
            try:
                stream.close()
            except Exception:
                LOGGER.debug("AWS response stream close failed", exc_info=True)

    def _put_request(self, item: Any) -> None:
        if self._stopped.is_set():
            raise RuntimeError("The AWS Lex stream is closed")
        try:
            self._requests.put(item, timeout=1.0)
        except queue.Full as exc:
            raise RuntimeError("Audio input queue is full") from exc

    def _request_generator(self) -> Iterator[Dict[str, Any]]:
        yield {
            "ConfigurationEvent": {
                "responseContentType": self.target.response_content_type,
                "eventId": uuid.uuid4().hex,
                "clientTimestampMillis": int(time.time() * 1000),
            }
        }
        if self._initial_text:
            yield _lex_text_event(self._initial_text)
        while True:
            item = self._requests.get()
            if item is _STOP:
                break
            kind, payload = item
            if kind == "audio":
                yield {
                    "AudioInputEvent": {
                        "audioChunk": payload,
                        "contentType": self.target.audio_request_content_type,
                        "eventId": uuid.uuid4().hex,
                        "clientTimestampMillis": int(time.time() * 1000),
                    }
                }
            elif kind == "text":
                yield _lex_text_event(payload)
            elif kind == "disconnect":
                yield {
                    "DisconnectionEvent": {
                        "eventId": uuid.uuid4().hex,
                        "clientTimestampMillis": int(time.time() * 1000),
                    }
                }

    def _run(self) -> None:
        try:
            if self._runtime_client is None:
                import boto3

                self._runtime_client = boto3.Session(
                    region_name=self.target.region_name
                ).client("lexv2-runtime")
            self._emit(
                "session_started",
                sessionId=self.session_id,
                targetId=self.target.id,
                profileId=self.profile.id,
                provider="aws_lex",
                initialTurnPending=bool(self._initial_text),
            )
            if self._initial_text:
                self._emit("turn_committed", turnId=1, input="opening_text")
            response = self._runtime_client.start_conversation(
                botId=self.target.bot_id,
                botAliasId=self.target.bot_alias_id,
                localeId=self.target.locale_id,
                sessionId=self.session_id,
                conversationMode="AUDIO",
                requestEventStream=self._request_generator(),
            )
            stream = response["responseEventStream"]
            self._response_stream = stream
            for event in stream:
                if self._stopped.is_set():
                    break
                self._handle_event(event)
        except Exception as exc:
            if not self._stopped.is_set():
                LOGGER.error("AWS Lex StartConversation failed", exc_info=True)
                self._emit("error", message=_safe_aws_error(exc), source="aws_lex")
        finally:
            self._stopped.set()
            self._emit("session_closed", sessionId=self.session_id)

    def _handle_event(self, event: Mapping[str, Any]) -> None:
        if "TranscriptEvent" in event:
            text = str(event["TranscriptEvent"].get("transcript") or "").strip()
            if text:
                self._emit("transcript", text=text)
        elif "TextResponseEvent" in event:
            for message in event["TextResponseEvent"].get("messages") or []:
                text = str(message.get("content") or "").strip()
                if text:
                    self._emit("agent_text", text=text)
        elif "AudioResponseEvent" in event:
            audio = bytes(event["AudioResponseEvent"].get("audioChunk") or b"")
            if audio:
                self._emit_audio(audio)
        elif "IntentResultEvent" in event:
            self._complete_turn()
        elif "PlaybackInterruptionEvent" in event:
            self._emit("interruption")
        else:
            exception_name = next((key for key in event if key.endswith("Exception")), None)
            if exception_name:
                detail = event[exception_name]
                raise RuntimeError(str(detail.get("message") or exception_name))

    def _emit_audio(self, raw_audio: bytes) -> None:
        with self._lock:
            self._audio_frame_index += 1
            frame_index = self._audio_frame_index
            turn_id = self._active_turn_id
            first = turn_id is not None and not self._first_audio_emitted
            latency_ms = None
            if first:
                self._first_audio_emitted = True
                if self._commit_started_at is not None:
                    latency_ms = (time.monotonic() - self._commit_started_at) * 1000
        duration_ms = len(raw_audio) * 1000.0 / (16000 * 2)
        self.outbound(
            AudioPacket(
                pcm16=raw_audio,
                metadata={
                    "type": "audio",
                    "frameIndex": frame_index,
                    "turnId": turn_id,
                    "firstForTurn": first,
                    "commitToFirstAudioMs": round(latency_ms, 1) if latency_ms else None,
                    "sampleRateHertz": 16000,
                    "encoding": "LINEAR16",
                    "rawEncoding": "LINEAR16",
                    "rawBytes": len(raw_audio),
                    "pcmBytes": len(raw_audio),
                    "encodedDurationMs": round(duration_ms, 1),
                    "rms": round(pcm16_rms(raw_audio), 1),
                    "anomalouslyLong": duration_ms >= 5000,
                },
            )
        )

    def _complete_turn(self) -> None:
        with self._lock:
            turn_id = self._active_turn_id
            continuous = bool(
                turn_id is not None
                and self._active_turn_kind == "audio"
                and self._continuous_audio
            )
            next_turn_id = None
            if continuous:
                self._turn_id += 1
                next_turn_id = self._turn_id
                self._active_turn_id = next_turn_id
                self._active_turn_kind = "audio"
            else:
                self._active_turn_id = None
                self._active_turn_kind = None
                self._continuous_audio = False
            self._commit_started_at = None
            self._first_audio_emitted = False
        self._emit(
            "turn_completed",
            turnId=turn_id,
            continuous=continuous,
            nextTurnId=next_turn_id,
        )

    def _emit(self, event_type: str, **fields: Any) -> None:
        self.outbound({"type": event_type, **fields})


def create_app(
    settings: LabSettings,
    *,
    session_factory: Optional[Callable[..., LabSession]] = None,
) -> web.Application:
    """Create the local-only aiohttp application."""
    app = web.Application(client_max_size=2 * 1024 * 1024)
    app["settings"] = settings
    app["session_factory"] = session_factory or _create_provider_session

    async def index(_request: web.Request) -> web.FileResponse:
        return web.FileResponse(STATIC_ROOT / "index.html")

    async def config(_request: web.Request) -> web.Response:
        return web.json_response(settings.public_dict())

    async def health(_request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "service": "voice-agent-audio-lab"})

    async def websocket(request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20, max_msg_size=2 * 1024 * 1024)
        await ws.prepare(request)
        loop = asyncio.get_running_loop()
        outbound_queue: asyncio.Queue[OutboundItem] = asyncio.Queue(maxsize=512)
        active_session: Optional[LabSession] = None

        def enqueue_outbound(item: OutboundItem) -> None:
            def put() -> None:
                try:
                    outbound_queue.put_nowait(item)
                except asyncio.QueueFull:
                    LOGGER.error("Browser output queue is full")

            loop.call_soon_threadsafe(put)

        async def sender() -> None:
            try:
                while not ws.closed:
                    item = await outbound_queue.get()
                    if isinstance(item, AudioPacket):
                        await ws.send_json(item.metadata)
                        await ws.send_bytes(item.pcm16)
                    else:
                        await ws.send_json(item)
            except asyncio.CancelledError:
                raise
            except Exception:
                if not ws.closed:
                    LOGGER.warning("Browser output stream closed", exc_info=True)

        sender_task = asyncio.create_task(sender())
        await ws.send_json({"type": "socket_ready"})
        try:
            async for message in ws:
                if message.type == WSMsgType.BINARY:
                    if active_session is None:
                        await _send_error(
                            ws, "Start a voice-agent session before sending audio"
                        )
                        continue
                    try:
                        active_session.send_pcm16(bytes(message.data))
                    except (RuntimeError, ValueError) as exc:
                        await _send_error(ws, str(exc))
                    continue
                if message.type == WSMsgType.ERROR:
                    break
                if message.type != WSMsgType.TEXT:
                    continue
                try:
                    command = json.loads(message.data)
                    command_type = str(command.get("type", ""))
                except (TypeError, json.JSONDecodeError):
                    await _send_error(ws, "Invalid JSON command")
                    continue

                try:
                    if command_type == "start":
                        if active_session is not None:
                            active_session.stop()
                            await asyncio.to_thread(active_session.join, 2.0)
                        target_id = str(
                            command.get("targetId") or settings.default_target_id
                        )
                        profile_id = str(command.get("profileId") or "native")
                        target = settings.targets.get(target_id)
                        profile = AUDIO_PROFILES.get(profile_id)
                        if target is None:
                            raise ValueError(f"Unknown voice-agent target: {target_id}")
                        if profile is None:
                            raise ValueError(f"Unknown audio profile: {profile_id}")
                        supported_profiles = target.public_dict()["supportedProfileIds"]
                        if profile_id not in supported_profiles:
                            raise ValueError(
                                f"Audio profile {profile_id} is not supported by {target.label}"
                            )
                        endpointing_ms = int(command.get("endpointingSilenceMs", 2000))
                        factory = app["session_factory"]
                        active_session = factory(
                            target,
                            profile,
                            endpointing_ms,
                            enqueue_outbound,
                        )
                        active_session.start(str(command.get("initialText") or ""))
                    elif command_type == "talk_start":
                        _require_session(active_session).begin_audio_turn(
                            continuous=bool(command.get("continuous", False))
                        )
                    elif command_type == "commit":
                        _require_session(active_session).commit_audio_turn()
                    elif command_type == "text":
                        _require_session(active_session).send_text(
                            str(command.get("text") or "")
                        )
                    elif command_type == "stop":
                        if active_session is not None:
                            active_session.stop()
                            await asyncio.to_thread(active_session.join, 2.0)
                            active_session = None
                    elif command_type == "ping":
                        await ws.send_json({"type": "pong"})
                    else:
                        raise ValueError(f"Unknown command: {command_type}")
                except (RuntimeError, ValueError) as exc:
                    await _send_error(ws, str(exc))
        finally:
            if active_session is not None:
                active_session.stop()
                await asyncio.to_thread(active_session.join, 2.0)
            sender_task.cancel()
            try:
                await sender_task
            except asyncio.CancelledError:
                pass
        return ws

    app.router.add_get("/", index)
    app.router.add_get("/api/config", config)
    app.router.add_get("/health", health)
    app.router.add_get("/ws", websocket)
    app.router.add_static("/static/", STATIC_ROOT, show_index=False)
    return app


def _require_session(
    session: Optional[LabSession],
) -> LabSession:
    if session is None:
        raise RuntimeError("Start a voice-agent session first")
    if session.is_closed:
        raise RuntimeError("The provider stream has closed. Start a new direct session")
    return session


async def _send_error(ws: web.WebSocketResponse, message: str) -> None:
    await ws.send_json({"type": "error", "message": message, "source": "browser"})


def _create_provider_session(
    target: LabTarget,
    profile: AudioProfile,
    endpointing_silence_ms: int,
    outbound: OutboundCallback,
) -> LabSession:
    if isinstance(target, AWSLexTarget):
        return AWSLexRecognizeSession(target, profile, endpointing_silence_ms, outbound)
    return DirectGECXSession(target, profile, endpointing_silence_ms, outbound)


def _discover_aws_lex_targets(
    connector_id: str,
    config: Mapping[str, Any],
) -> list[AWSLexTarget]:
    """Discover Lex bots exactly as the connector does, using the default chain."""
    import boto3

    from src.connectors.aws_lex_config import AWSLexConfig
    from src.connectors.aws_lex_session_manager import AWSLexSessionManager

    lex_config = AWSLexConfig(dict(config), LOGGER)
    session = boto3.Session(region_name=lex_config.get_region_name())
    client = session.client("lexv2-models")
    manager = AWSLexSessionManager(LOGGER)
    names = manager.get_available_agents(client)
    targets: list[AWSLexTarget] = []
    for display_name in names:
        bot_id = manager._bot_name_to_id_map[display_name]
        alias_id = manager._bot_alias_map[bot_id]
        bot_name = display_name.split(": ", 1)[-1]
        common = {
            "connector_id": connector_id,
            "region_name": lex_config.get_region_name(),
            "locale_id": lex_config.get_locale_id(),
            "bot_id": bot_id,
            "bot_alias_id": alias_id,
            "bot_name": bot_name,
            "initial_trigger_text": lex_config.get_initial_trigger_text(),
            "text_request_content_type": lex_config.get_text_request_content_type(),
            "audio_request_content_type": lex_config.get_audio_request_content_type(),
            "response_content_type": lex_config.get_response_content_type(),
        }
        safe_bot_id = re.sub(r"[^a-zA-Z0-9_-]", "-", bot_id)
        targets.append(
            AWSLexTarget(
                id=f"{connector_id}-{safe_bot_id}-parity",
                label=f"AWS / {bot_name} / connector parity",
                streaming=False,
                **common,
            )
        )
    return targets


def _gecx_credential_mode(config: Mapping[str, Any]) -> str:
    if config.get("access_token"):
        return "connector access token"
    if config.get("service_account_key"):
        return "connector service account"
    if config.get("oauth_client_id") and config.get("oauth_client_secret"):
        return "connector OAuth credentials"
    return "Application Default Credentials"


def _load_gecx_credentials(config: Mapping[str, Any]) -> Optional[Any]:
    """Resolve the same authentication fields accepted by GECXConnector."""
    access_token = _optional_string(config.get("access_token"))
    service_account_key = _optional_string(config.get("service_account_key"))
    oauth_client_id = _optional_string(config.get("oauth_client_id"))
    oauth_client_secret = _optional_string(config.get("oauth_client_secret"))
    oauth_token_file = Path(
        str(config.get("oauth_token_file") or "gecx_oauth_token.json")
    ).expanduser()
    if access_token:
        from google.oauth2.credentials import Credentials

        return Credentials(token=access_token)
    if service_account_key:
        path = Path(service_account_key).expanduser()
        if not path.is_file():
            raise RuntimeError(f"Configured GECX service account file does not exist: {path}")
        from google.oauth2 import service_account

        return service_account.Credentials.from_service_account_file(path)
    if oauth_client_id and oauth_client_secret:
        from google.auth.transport.requests import Request
        from google.oauth2.credentials import Credentials
        from google_auth_oauthlib.flow import InstalledAppFlow

        scopes = ["https://www.googleapis.com/auth/cloud-platform"]
        credentials = None
        if oauth_token_file.exists():
            credentials = Credentials.from_authorized_user_file(
                str(oauth_token_file), scopes
            )
        if not credentials or not credentials.valid:
            if credentials and credentials.expired and credentials.refresh_token:
                credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_config(
                    {
                        "installed": {
                            "client_id": oauth_client_id,
                            "client_secret": oauth_client_secret,
                            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                            "token_uri": "https://oauth2.googleapis.com/token",
                            "redirect_uris": ["http://localhost:8090"],
                        }
                    },
                    scopes,
                )
                credentials = flow.run_local_server(port=8090, open_browser=True)
            _save_google_oauth_credentials(credentials, oauth_token_file)
        return credentials
    return None


def _save_google_oauth_credentials(credentials: Any, token_path: Path) -> None:
    token_path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = token_path.with_name(f".{token_path.name}.{uuid.uuid4().hex}.tmp")
    temporary_path.write_text(credentials.to_json() + "\n", encoding="utf-8")
    temporary_path.chmod(0o600)
    os.replace(temporary_path, token_path)
    token_path.chmod(0o600)


def _decode_lex_header(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        decoded = base64.b64decode(value)
        try:
            decoded = gzip.decompress(decoded)
        except gzip.BadGzipFile:
            pass
        return json.loads(decoded.decode("utf-8"))
    except (ValueError, TypeError, UnicodeDecodeError, json.JSONDecodeError, OSError):
        return value


def _lex_text_event(text: str) -> Dict[str, Any]:
    return {
        "TextInputEvent": {
            "text": text,
            "eventId": uuid.uuid4().hex,
            "clientTimestampMillis": int(time.time() * 1000),
        }
    }


def _safe_aws_error(exc: Exception) -> str:
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        error = response.get("Error") or {}
        code = str(error.get("Code") or type(exc).__name__)
        message = str(error.get("Message") or "AWS Lex request failed")
        return f"{code}: {message}"[:700]
    return f"{type(exc).__name__}: {str(exc)}"[:700]


def _optional_string(value: Any) -> Optional[str]:
    text = str(value or "").strip()
    return text or None


def _safe_error_message(
    exc: Exception,
    target: SessionTarget,
    *,
    stream_was_active: bool = False,
) -> str:
    """Reduce verbose Google RPC errors to an actionable browser message."""
    error_name = type(exc).__name__
    if error_name == "PermissionDenied":
        summary = str(exc).split(" [reason:", 1)[0].strip()
        permission_match = re.search(
            r"Permission ['\"]?([A-Za-z0-9._/-]+)['\"]? denied",
            summary,
            flags=re.IGNORECASE,
        )
        permission = permission_match.group(1) if permission_match else None
        if stream_was_active:
            detail = f" ({permission})" if permission else ""
            return (
                f"The GECX stream hit a downstream permission failure{detail} "
                "after the direct session was authorized. Check the AZ agent's "
                "service identity and tool permissions."
            )
        return (
            "GECX permission denied. The selected credential needs "
            "ces.sessions.bidiRunSession (normally roles/ces.client) on "
            f"project {target.project_id}."
        )
    if error_name == "DefaultCredentialsError":
        return (
            "Google credentials are unavailable. Configure credentials_env or "
            "Application Default Credentials."
        )
    summary = str(exc).split(" [reason:", 1)[0].strip()
    if len(summary) > 700:
        summary = summary[:697] + "..."
    return f"{error_name}: {summary}" if summary else error_name


def _expand_environment(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [_expand_environment(item) for item in value]
    if isinstance(value, dict):
        return {key: _expand_environment(item) for key, item in value.items()}
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a local browser directly against configured voice-agent providers."
    )
    parser.add_argument(
        "--gateway-config",
        type=Path,
        default=Path(os.getenv("GATEWAY_CONFIG", "config/config.yaml")),
        help="Main gateway YAML whose connector auth/config should be reused",
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Optional local target overlay (legacy GECX target format)",
    )
    parser.add_argument("--project-id", help="Single-target Google Cloud project")
    parser.add_argument("--location", default="us", help="CES location (default: us)")
    parser.add_argument("--application-id", help="Single-target CES application id")
    parser.add_argument("--deployment-id", help="Optional published deployment id")
    parser.add_argument("--entry-agent", help="Optional CES entry agent resource")
    parser.add_argument("--api-endpoint", help="Optional CES runtime endpoint override")
    parser.add_argument(
        "--credentials-env",
        help="Environment variable containing a service-account JSON path",
    )
    parser.add_argument("--label", default="GECX target", help="Target label")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--allow-remote",
        action="store_true",
        help="Allow binding beyond localhost (credentials still remain server-side)",
    )
    parser.add_argument("--no-open", action="store_true", help="Do not open a browser")
    parser.add_argument("--debug", action="store_true")
    return parser


def _settings_from_args(args: argparse.Namespace) -> LabSettings:
    if args.project_id or args.application_id:
        if not args.project_id or not args.application_id:
            raise ValueError("Provide both --project-id and --application-id")
        target = SessionTarget.from_mapping(
            {
                "id": "default",
                "label": args.label,
                "project_id": args.project_id,
                "location": args.location,
                "application_id": args.application_id,
                "deployment_id": args.deployment_id,
                "entry_agent": args.entry_agent,
                "api_endpoint": args.api_endpoint,
                "credentials_env": args.credentials_env,
            }
        )
        return LabSettings(targets={target.id: target}, default_target_id=target.id)
    gateway_config = args.gateway_config.expanduser()
    if gateway_config.is_file():
        return LabSettings.from_sources(
            gateway_config,
            args.config.expanduser() if args.config else None,
        )
    if args.config:
        return LabSettings.from_yaml(args.config.expanduser())
    raise ValueError(f"Gateway config does not exist: {gateway_config}")


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    if args.host not in {"127.0.0.1", "localhost", "::1"} and not args.allow_remote:
        parser.error("Non-loopback binding requires --allow-remote")
    try:
        settings = _settings_from_args(args)
    except (OSError, ValueError, yaml.YAMLError) as exc:
        parser.error(str(exc))
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Botocore DEBUG output includes signed request headers and temporary tokens.
    # Keep third-party transport logs quiet even when lab diagnostics are enabled.
    for logger_name in ("botocore", "urllib3", "google.auth.transport"):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
    url = f"http://{args.host}:{args.port}"
    print(f"Voice Agent Audio Lab: {url}")
    print(
        "Connector credentials stay in the Python process and are never sent to the browser."
    )
    if not args.no_open:
        threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    web.run_app(
        create_app(settings),
        host=args.host,
        port=args.port,
        print=None,
    )


if __name__ == "__main__":
    main()
