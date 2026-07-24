"""Deterministic tests for structured caller-turn state."""

from src.core.turn_tracker import (
    InputTurnState,
    SpeechStartDisposition,
    TurnTracker,
)


class FakeClock:
    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        value = self.value
        self.value += 0.1
        return value


def test_tracks_exactly_once_boundaries_with_monotonic_sequence():
    tracker = TurnTracker("conv-1", clock=FakeClock())
    tracker.set_vendor_context(
        {
            "vendor_session_id": "ces-1",
            "vendor_session_path": "projects/p/apps/a/sessions/ces-1",
            "ces_turn_offset": 1,
        }
    )

    assert tracker.start_speech() == SpeechStartDisposition.NEW_TURN
    assert tracker.start_speech() == SpeechStartDisposition.DUPLICATE
    assert tracker.mark_boundary_emitted("START_OF_INPUT") is True
    assert tracker.mark_boundary_emitted("START_OF_INPUT") is False
    assert tracker.detect_speech_end() is True
    assert tracker.detect_speech_end() is False
    assert tracker.commit_speech_segment() is True
    assert tracker.begin_response_wait() is True
    assert tracker.mark_boundary_emitted("END_OF_INPUT") is True
    assert tracker.mark_boundary_emitted("END_OF_INPUT") is False
    assert tracker.complete_response() is True

    snapshot = tracker.snapshot()
    sequences = [event["sequence"] for event in snapshot["events"]]
    assert sequences == list(range(1, len(sequences) + 1))
    assert snapshot["gateway_turn_id"] == "conv-1:turn:1"
    assert snapshot["gateway_turn_index"] == 1
    assert snapshot["ces_turn_index"] == 2
    assert snapshot["vendor_session_id"] == "ces-1"
    assert snapshot["input_state"] == InputTurnState.WAITING.value
    assert snapshot["response_pending"] is False


def test_resumed_segments_share_one_logical_turn_and_response_wait():
    tracker = TurnTracker("conv-2", clock=FakeClock())

    assert tracker.start_speech() == SpeechStartDisposition.NEW_TURN
    assert tracker.detect_speech_end() is True
    assert tracker.start_speech() == SpeechStartDisposition.RESUMED
    assert tracker.detect_speech_end() is True
    assert tracker.commit_speech_segment() is True
    assert tracker.begin_response_wait() is True

    assert tracker.start_speech() == SpeechStartDisposition.CONTINUED
    assert tracker.detect_speech_end() is True
    assert tracker.commit_speech_segment() is True
    assert tracker.begin_response_wait() is False
    assert tracker.complete_response() is True

    snapshot = tracker.snapshot()
    assert snapshot["gateway_turn_index"] == 1
    assert snapshot["gateway_turn_id"] == "conv-2:turn:1"
    assert [event["event"] for event in snapshot["events"]].count(
        "response_wait_started"
    ) == 1
    assert [event["event"] for event in snapshot["events"]].count(
        "response_wait_extended"
    ) == 1


def test_pre_roll_and_late_terminal_audio_are_accounted_without_frame_spam():
    tracker = TurnTracker("conv-3", clock=FakeClock())

    tracker.record_audio_frame(160)
    tracker.record_audio_frame(160)
    assert tracker.start_speech() == SpeechStartDisposition.NEW_TURN
    tracker.record_audio_frame(320)
    assert tracker.detect_speech_end() is True
    assert tracker.commit_speech_segment() is True
    assert tracker.begin_response_wait() is True
    assert tracker.complete_response() is True
    tracker.terminate("client_session_end")
    tracker.record_audio_frame(160)
    tracker.record_audio_frame(160)

    events = tracker.events()
    assert [event["event"] for event in events].count(
        "audio_buffered_pre_roll"
    ) == 1
    assert [event["event"] for event in events].count(
        "late_audio_suppressed"
    ) == 1
    completed = next(event for event in events if event["event"] == "turn_completed")
    assert completed["caller_audio_bytes"] == 640
    assert tracker.snapshot()["input_state"] == InputTurnState.TERMINAL.value


def test_records_first_vendor_and_gateway_output_timestamps_once():
    tracker = TurnTracker("conv-4", clock=FakeClock())
    tracker.start_speech()

    assert tracker.record_vendor_output(timestamp=1000.25) is True
    assert tracker.record_vendor_output(timestamp=1000.3) is False
    assert tracker.record_gateway_output() is True
    assert tracker.record_gateway_output() is False

    events = tracker.events()
    vendor = next(event for event in events if event["event"] == "first_vendor_output")
    gateway = next(
        event for event in events if event["event"] == "first_gateway_output"
    )
    assert vendor["timestamp"] == 1000.25
    assert gateway["timestamp"] > vendor["timestamp"]


def test_records_vendor_interruption_in_the_gateway_sequence():
    tracker = TurnTracker("conv-5", clock=FakeClock())
    tracker.start_speech()

    tracker.record_interruption("ces", timestamp=1000.25)

    event = tracker.events()[-1]
    assert event["event"] == "turn_interrupted"
    assert event["source"] == "ces"
    assert event["timestamp"] == 1000.25
    assert event["gateway_turn_id"] == "conv-5:turn:1"
