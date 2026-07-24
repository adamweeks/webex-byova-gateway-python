"""Thread-safe caller turn state and structured observability."""

from __future__ import annotations

import logging
import threading
import time
from collections import deque
from enum import Enum
from typing import Any, Callable, Deque, Dict, List, Optional


class InputTurnState(str, Enum):
    """Gateway-owned caller input states."""

    WAITING = "waiting_for_speech"
    ACTIVE = "speech_active"
    END_PENDING = "speech_end_pending"
    TERMINAL = "terminal"


class SpeechStartDisposition(str, Enum):
    """How an observed speech start relates to the logical caller turn."""

    NEW_TURN = "new_turn"
    RESUMED = "resumed_during_end_grace"
    CONTINUED = "continued_while_response_pending"
    DUPLICATE = "duplicate_active_start"
    TERMINAL = "terminal_input"


class TurnTracker:
    """Track one conversation's logical caller turns and ordered evidence."""

    def __init__(
        self,
        conversation_id: str,
        *,
        logger: Optional[logging.Logger] = None,
        clock: Callable[[], float] = time.time,
        event_limit: int = 100,
    ) -> None:
        self.conversation_id = conversation_id
        self.logger = logger or logging.getLogger(__name__)
        self._clock = clock
        self._lock = threading.RLock()
        self._started_at = clock()
        self._events: Deque[Dict[str, Any]] = deque(maxlen=max(1, event_limit))

        self.input_state = InputTurnState.WAITING
        self.response_pending = False
        self.sequence = 0
        self.gateway_turn_index = 0
        self.gateway_turn_id = ""
        self.ces_turn_index = 0
        self.ces_turn_offset = 0
        self.vendor_session_id = ""
        self.vendor_session_path = ""

        self._turn_started_at: Optional[float] = None
        self._start_emitted = False
        self._end_emitted = False
        self._vendor_output_recorded = False
        self._gateway_output_recorded = False
        self._pending_audio_bytes = 0
        self._turn_audio_bytes = 0
        self._audio_buffer_event_key = ""

    def set_vendor_context(self, context: Dict[str, Any]) -> None:
        """Attach vendor correlation fields once the connector session exists."""
        with self._lock:
            self.vendor_session_id = str(
                context.get("vendor_session_id", self.vendor_session_id)
            )
            self.vendor_session_path = str(
                context.get("vendor_session_path", self.vendor_session_path)
            )
            self.ces_turn_offset = max(
                0, int(context.get("ces_turn_offset", self.ces_turn_offset))
            )
            self._emit_locked("vendor_context_attached")

    def record_audio_frame(self, byte_count: int) -> None:
        """Account for caller audio without logging every telephony frame."""
        if byte_count <= 0:
            return
        with self._lock:
            if self.input_state == InputTurnState.TERMINAL:
                event_key = f"{self.input_state.value}:late_audio"
                if event_key != self._audio_buffer_event_key:
                    self._audio_buffer_event_key = event_key
                    self._emit_locked(
                        "late_audio_suppressed",
                        details={"bytes": byte_count},
                    )
                return
            if self.input_state == InputTurnState.ACTIVE:
                self._turn_audio_bytes += byte_count
                return

            self._pending_audio_bytes += byte_count
            event_key = (
                f"{self.input_state.value}:{self.response_pending}:"
                f"{self.gateway_turn_index}"
            )
            if event_key != self._audio_buffer_event_key:
                self._audio_buffer_event_key = event_key
                self._emit_locked(
                    "audio_buffered_pre_roll",
                    details={"bytes": byte_count},
                )

    def start_speech(self) -> SpeechStartDisposition:
        """Apply a VAD start and classify it against current turn state."""
        with self._lock:
            if self.input_state == InputTurnState.TERMINAL:
                self._emit_locked("speech_start_suppressed_terminal")
                return SpeechStartDisposition.TERMINAL
            if self.input_state == InputTurnState.ACTIVE:
                self._emit_locked("speech_start_suppressed_duplicate")
                return SpeechStartDisposition.DUPLICATE
            if self.input_state == InputTurnState.END_PENDING:
                self.input_state = InputTurnState.ACTIVE
                self._turn_audio_bytes += self._pending_audio_bytes
                self._pending_audio_bytes = 0
                self._audio_buffer_event_key = ""
                self._emit_locked("vad_speech_resumed")
                return SpeechStartDisposition.RESUMED
            if self.response_pending:
                self.input_state = InputTurnState.ACTIVE
                self._turn_audio_bytes += self._pending_audio_bytes
                self._pending_audio_bytes = 0
                self._audio_buffer_event_key = ""
                self._emit_locked("vad_speech_continued")
                return SpeechStartDisposition.CONTINUED

            self.gateway_turn_index += 1
            self.gateway_turn_id = (
                f"{self.conversation_id}:turn:{self.gateway_turn_index}"
            )
            self.ces_turn_index = self.gateway_turn_index + self.ces_turn_offset
            self.input_state = InputTurnState.ACTIVE
            self._turn_started_at = self._clock()
            self._start_emitted = False
            self._end_emitted = False
            self._vendor_output_recorded = False
            self._gateway_output_recorded = False
            self._turn_audio_bytes = self._pending_audio_bytes
            self._pending_audio_bytes = 0
            self._audio_buffer_event_key = ""
            self._emit_locked("vad_speech_started")
            return SpeechStartDisposition.NEW_TURN

    def detect_speech_end(self) -> bool:
        """Move active caller input into the end-grace state once."""
        with self._lock:
            if self.input_state != InputTurnState.ACTIVE:
                self._emit_locked("speech_end_suppressed_duplicate")
                return False
            self.input_state = InputTurnState.END_PENDING
            self._audio_buffer_event_key = ""
            self._emit_locked("vad_speech_ended")
            return True

    def commit_speech_segment(self) -> bool:
        """Commit the current speech segment while preserving its logical turn."""
        with self._lock:
            if self.input_state != InputTurnState.END_PENDING:
                self._emit_locked("speech_commit_suppressed_invalid_state")
                return False
            self.input_state = InputTurnState.WAITING
            self._audio_buffer_event_key = ""
            self._emit_locked("speech_segment_committed")
            return True

    def begin_response_wait(self) -> bool:
        """Claim the single connector response waiter for this logical turn."""
        with self._lock:
            if self.response_pending:
                self._emit_locked("response_wait_extended")
                return False
            self.response_pending = True
            self._emit_locked("response_wait_started")
            return True

    def complete_response(self) -> bool:
        """Complete the logical turn after its connector response is delivered."""
        with self._lock:
            if not self.response_pending:
                self._emit_locked("response_completion_suppressed_duplicate")
                return False
            self.response_pending = False
            self._emit_locked(
                "turn_completed",
                details={"caller_audio_bytes": self._turn_audio_bytes},
            )
            return True

    def mark_boundary_emitted(self, boundary: str) -> bool:
        """Guard outbound WxCC START/END boundaries exactly once per turn."""
        normalized = boundary.upper()
        with self._lock:
            if normalized == "START_OF_INPUT":
                if self._start_emitted:
                    self._emit_locked("wxcc_start_suppressed_duplicate")
                    return False
                self._start_emitted = True
                self._emit_locked("wxcc_start_emitted")
                return True
            if normalized == "END_OF_INPUT":
                if self._end_emitted:
                    self._emit_locked("wxcc_end_suppressed_duplicate")
                    return False
                self._end_emitted = True
                self._emit_locked("wxcc_end_emitted")
                return True
            raise ValueError(f"Unsupported speech boundary: {boundary}")

    def record_vendor_output(self, timestamp: Optional[float] = None) -> bool:
        """Record the first CES/connector output for the active logical turn."""
        with self._lock:
            if not self.gateway_turn_id or self._vendor_output_recorded:
                return False
            self._vendor_output_recorded = True
            self._emit_locked("first_vendor_output", event_time=timestamp)
            return True

    def record_gateway_output(self) -> bool:
        """Record the first WxCC prompt/event output for the logical turn."""
        with self._lock:
            if not self.gateway_turn_id or self._gateway_output_recorded:
                return False
            self._gateway_output_recorded = True
            self._emit_locked("first_gateway_output")
            return True

    def record_interruption(
        self, source: str, timestamp: Optional[float] = None
    ) -> None:
        """Record an interruption without changing terminal lifecycle state."""
        with self._lock:
            self._emit_locked(
                "turn_interrupted",
                event_time=timestamp,
                details={"source": source},
            )

    def record_no_input(self) -> None:
        """Record a WxCC no-input timer without creating a caller turn."""
        with self._lock:
            self._emit_locked("no_input")

    def terminate(self, reason: str) -> None:
        """Record the terminal transition and reject subsequent caller audio."""
        with self._lock:
            if self.input_state == InputTurnState.TERMINAL:
                return
            self.input_state = InputTurnState.TERMINAL
            self.response_pending = False
            self._emit_locked("terminal_decision", details={"reason": reason})

    def context(self) -> Dict[str, Any]:
        """Return correlation fields to propagate to connector operations."""
        with self._lock:
            return {
                "gateway_turn_id": self.gateway_turn_id,
                "gateway_turn_index": self.gateway_turn_index,
                "ces_turn_index": self.ces_turn_index,
                "sequence": self.sequence,
                "vendor_session_id": self.vendor_session_id,
            }

    def is_response_pending(self) -> bool:
        """Return whether this logical turn already owns a response waiter."""
        with self._lock:
            return self.response_pending

    def snapshot(self) -> Dict[str, Any]:
        """Return current state and recent ordered evidence for monitoring."""
        with self._lock:
            return {
                **self.context(),
                "input_state": self.input_state.value,
                "response_pending": self.response_pending,
                "vendor_session_path": self.vendor_session_path,
                "events": [dict(event) for event in self._events],
            }

    def events(self) -> List[Dict[str, Any]]:
        """Return a copy of recent structured events."""
        with self._lock:
            return [dict(event) for event in self._events]

    def _emit_locked(
        self,
        event: str,
        *,
        event_time: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        timestamp = self._clock() if event_time is None else event_time
        self.sequence += 1
        turn_elapsed_ms = (
            None
            if self._turn_started_at is None
            else round((timestamp - self._turn_started_at) * 1000, 3)
        )
        record = {
            "event": event,
            "sequence": self.sequence,
            "timestamp": timestamp,
            "elapsed_ms": round((timestamp - self._started_at) * 1000, 3),
            "turn_elapsed_ms": turn_elapsed_ms,
            "conversation_id": self.conversation_id,
            "vendor_session_id": self.vendor_session_id,
            "gateway_turn_id": self.gateway_turn_id,
            "gateway_turn_index": self.gateway_turn_index,
            "ces_turn_index": self.ces_turn_index,
            "input_state": self.input_state.value,
            "response_pending": self.response_pending,
            **(details or {}),
        }
        self._events.append(record)
        self.logger.info(
            "turn_event event=%s sequence=%d timestamp=%.6f elapsed_ms=%.3f "
            "turn_elapsed_ms=%s conversation_id=%s vendor_session_id=%s "
            "gateway_turn_id=%s gateway_turn_index=%d ces_turn_index=%d "
            "input_state=%s response_pending=%s%s",
            event,
            self.sequence,
            timestamp,
            record["elapsed_ms"],
            "-" if turn_elapsed_ms is None else f"{turn_elapsed_ms:.3f}",
            self.conversation_id,
            self.vendor_session_id or "-",
            self.gateway_turn_id or "-",
            self.gateway_turn_index,
            self.ces_turn_index,
            self.input_state.value,
            str(self.response_pending).lower(),
            "".join(
                f" {key}={value}"
                for key, value in sorted((details or {}).items())
            ),
        )
