"""Socket-level tests for the BYOVA WebSocket transport."""

import asyncio
import base64
import logging
import wave
from typing import Any

import pytest
from aiohttp import WSServerHandshakeError
from aiohttp.test_utils import TestClient, TestServer

from src.auth.jwt_validator import AccessTokenException
from src.core.conversation_registry import ConversationRegistry
from src.core.virtual_agent_router import VirtualAgentRouter
from src.transports.websocket_server import WebSocketGatewayServer


class FakeRouter:
    def __init__(
        self,
        start_message_type: str = "session_start",
        websocket_agents: list[str] | None = None,
    ) -> None:
        self.agent_to_connector_name_map = {"Test Agent": "test_connector"}
        self.start_message_type = start_message_type
        self.websocket_agents = (
            ["Test Agent"] if websocket_agents is None else websocket_agents
        )
        self.end_calls = 0
        self.discovery_transports: list[str | None] = []

    def get_all_available_agents(self, transport=None):
        self.discovery_transports.append(transport)
        return self.websocket_agents if transport == "websocket" else ["Test Agent"]

    def get_connector_for_agent(self, agent_id, transport=None):
        if agent_id != "Test Agent":
            raise ValueError(agent_id)
        if transport == "websocket" and agent_id not in self.websocket_agents:
            raise ValueError(agent_id)
        return self

    def get_websocket_output_mode(self, agent_id):
        return "raw_chunk"

    def route_request(self, agent_id, operation, conversation_id, message_data):
        if operation == "start_conversation":
            return {
                "message_type": self.start_message_type,
                "text": "Connected",
                "audio_content": b"",
                "response_type": "final",
            }
        if operation == "end_conversation":
            self.end_calls += 1
            return None
        if operation in {"send_message", "handle_speech_boundary"}:
            return None
        raise AssertionError(operation)

    def set_async_response_sink(self, *args):
        return None

    def clear_async_response_sink(self, *args):
        return None

    def should_observe_speech_boundaries(self, *args):
        return False


def _start(
    seq: int = 1,
    conversation_id: str = "call-1",
    agent_id: str = "Test Agent",
) -> dict[str, Any]:
    return {
        "type": "VOICE_VA_REQUEST",
        "seq": seq,
        "ts": "2026-09-08T12:00:00Z",
        "conversation_id": conversation_id,
        "payload": {
            "conversation_id": conversation_id,
            "customer_org_id": "org-1",
            "virtual_agent_id": agent_id,
            "voice_va_input_type": {"event_input": {"event_type": "SESSION_START"}},
        },
    }


async def _client_for(server: WebSocketGatewayServer) -> TestClient:
    client = TestClient(TestServer(server.create_app()))
    await client.start_server()
    return client


async def _wait_for_cleanup(router: FakeRouter) -> None:
    for _ in range(50):
        if router.end_calls == 1:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("provider cleanup did not complete")


async def _receive_response_stream(socket) -> list[dict[str, Any]]:
    """Collect one ordered response stream through its FINAL frame."""
    frames = []
    while True:
        frame = await socket.receive_json()
        frames.append(frame)
        if frame["payload"]["response_type"] == "FINAL":
            return frames


def _write_wav(path, audio: bytes = bytes(range(256)) * 4) -> None:
    """Write deterministic PCM audio for Local Audio transport tests."""
    with wave.open(str(path), "wb") as wav_file:
        wav_file.setnchannels(1)
        wav_file.setsampwidth(1)
        wav_file.setframerate(8000)
        wav_file.writeframes(audio)


def test_health_is_available_without_websocket_authentication():
    async def scenario():
        class RejectingValidator:
            def validate_token(self, token):
                raise AssertionError("health must not invoke JWT validation")

        server = WebSocketGatewayServer(
            FakeRouter(), jwt_validator=RejectingValidator()
        )
        client = await _client_for(server)
        try:
            response = await client.get("/health")
            assert response.status == 200
            assert await response.json() == {
                "status": "healthy",
                "transport": "websocket",
            }
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_discovery_uses_shared_router_catalog():
    async def scenario():
        router = FakeRouter()
        server = WebSocketGatewayServer(
            router,
            allow_unauthenticated_local_dev=True,
            discovery_idle_timeout_seconds=0.25,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/listVirtualAgents")
            await socket.send_json({"customer_org_id": "org-1"})
            response = await socket.receive_json()
            assert response == {
                "virtual_agents": [
                    {
                        "id": "Test Agent",
                        "name": "Test Agent",
                        "description": "test_connector",
                    }
                ]
            }
            assert router.discovery_transports == ["websocket"]
            assert not socket.closed
            await socket.close()
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_local_audio_is_discoverable_and_streams_raw_audio_over_websocket(tmp_path):
    """Exercise Local Audio through the real router and WebSocket adapter."""
    welcome_path = tmp_path / "welcome.wav"
    _write_wav(welcome_path)

    router = VirtualAgentRouter()
    router.load_connectors(
        {
            "connectors": {
                "local_audio_connector": {
                    "class": "LocalAudioConnector",
                    "module": "connectors.local_audio_connector",
                    "config": {
                        "agent_id": "Local Playback",
                        "audio_base_path": str(tmp_path),
                        "audio_files": {"welcome": welcome_path.name},
                    },
                }
            }
        }
    )

    async def scenario():
        server = WebSocketGatewayServer(
            router,
            allow_unauthenticated_local_dev=True,
            discovery_idle_timeout_seconds=0.25,
        )
        client = await _client_for(server)
        try:
            discovery = await client.ws_connect("/v1/listVirtualAgents")
            await discovery.send_json({"customer_org_id": "org-1"})
            catalog = await discovery.receive_json()
            assert [agent["id"] for agent in catalog["virtual_agents"]] == [
                "Local Audio: Local Playback"
            ]
            await discovery.close()

            socket = await client.ws_connect("/v1/va")
            await socket.send_json(
                _start(
                    conversation_id="local-audio-call",
                    agent_id="Local Audio: Local Playback",
                )
            )
            responses = await _receive_response_stream(socket)
            assert all(item["type"] == "VOICE_VA_RESPONSE" for item in responses)
            assert len(responses) == 1
            assert responses[0]["payload"]["response_type"] == "FINAL"
            assert responses[0]["payload"]["prompts"][0][
                "is_barge_in_enabled"
            ] is True
            audio = base64.b64decode(
                responses[0]["payload"]["prompts"][0]["audio_content_b64"],
                validate=True,
            )
            assert audio.startswith(b"RIFF")
            assert audio[8:12] == b"WAVE"
            await socket.close()
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("wire_digit", "audio_name", "expected_event", "expected_text"),
    [
        (
            "DTMF_DIGIT_FIVE",
            "transfer.wav",
            "TRANSFER_TO_AGENT",
            "Transferring you to an agent. Please wait.",
        ),
        (
            "DTMF_DIGIT_SIX",
            "goodbye.wav",
            "SESSION_END",
            "Thank you for calling. Goodbye!",
        ),
    ],
)
def test_local_audio_dtmf_works_over_websocket(
    tmp_path,
    wire_digit,
    audio_name,
    expected_event,
    expected_text,
):
    """Route schema-valid DTMF through the socket into Local Audio."""
    welcome_path = tmp_path / "welcome.wav"
    action_path = tmp_path / audio_name
    _write_wav(welcome_path)
    _write_wav(action_path, bytes(reversed(range(256))) * 4)

    router = VirtualAgentRouter()
    router.load_connectors(
        {
            "connectors": {
                "local_audio_connector": {
                    "class": "LocalAudioConnector",
                    "module": "connectors.local_audio_connector",
                    "config": {
                        "agent_id": "Local Playback",
                        "audio_base_path": str(tmp_path),
                        "audio_files": {
                            "welcome": welcome_path.name,
                            "transfer": "transfer.wav",
                            "goodbye": "goodbye.wav",
                        },
                    },
                }
            }
        }
    )

    async def scenario():
        server = WebSocketGatewayServer(
            router,
            allow_unauthenticated_local_dev=True,
            terminal_peer_close_timeout_seconds=0.5,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(
                _start(
                    conversation_id="local-audio-dtmf",
                    agent_id="Local Audio: Local Playback",
                )
            )
            welcome_frames = await _receive_response_stream(socket)
            welcome = welcome_frames[-1]
            assert all(
                item["type"] == "VOICE_VA_RESPONSE" for item in welcome_frames
            )
            assert welcome["payload"]["input_mode"] == "INPUT_VOICE_DTMF"
            assert welcome["payload"]["input_handling_config"]["dtmf_config"] == {
                "inter_digit_timeout_msec": 5000,
                "termchar": "DTMF_DIGIT_POUND",
                "dtmf_input_length": 9,
            }
            assert welcome["payload"]["input_handling_config"][
                "speech_timers"
            ] == {"no_input_timeout_msec": 30000}

            await socket.send_json(
                {
                    "type": "VOICE_VA_REQUEST",
                    "seq": 2,
                    "ts": "2026-09-08T12:00:01Z",
                    "conversation_id": "local-audio-dtmf",
                    "payload": {
                        "conversation_id": "local-audio-dtmf",
                        "customer_org_id": "org-1",
                        "virtual_agent_id": "Local Audio: Local Playback",
                        "voice_va_input_type": {
                            "dtmf_input": {"dtmf_events": [wire_digit]}
                        },
                    },
                }
            )
            responses = await _receive_response_stream(socket)
            assert all(item["type"] == "VOICE_VA_RESPONSE" for item in responses)
            assert [item["seq"] for item in responses] == list(
                range(responses[0]["seq"], responses[0]["seq"] + len(responses))
            )
            assert responses[-1]["payload"]["response_type"] == "FINAL"
            assert responses[0]["payload"]["prompts"][0]["text"] == expected_text
            assert (
                responses[-1]["payload"]["output_events"][0]["event_type"]
                == expected_event
            )
            assert len(responses) == 1
            audio = base64.b64decode(
                responses[0]["payload"]["prompts"][0]["audio_content_b64"],
                validate=True,
            )
            assert audio.startswith(b"RIFF")
            assert audio[8:12] == b"WAVE"
            await socket.close()
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_discovery_accepts_unknown_fields_and_returns_only_websocket_agents():
    async def scenario():
        router = FakeRouter(websocket_agents=[])
        server = WebSocketGatewayServer(
            router,
            allow_unauthenticated_local_dev=True,
            discovery_idle_timeout_seconds=0.25,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/listVirtualAgents")
            await socket.send_json(
                {
                    "customer_org_id": "org-1",
                    "future_control_plane_field": True,
                }
            )
            assert await socket.receive_json() == {"virtual_agents": []}
            assert not socket.closed
            await socket.close()
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_websocket_runtime_rejects_agent_not_enabled_for_websocket():
    async def scenario():
        server = WebSocketGatewayServer(
            FakeRouter(websocket_agents=[]),
            allow_unauthenticated_local_dev=True,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert response["status"] == 404
            assert response["detail"] == "virtual agent was not found"
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_jwt_failure_is_rejected_before_websocket_upgrade():
    class RejectingValidator:
        def validate_token(self, token):
            raise AccessTokenException("invalid")

    async def scenario():
        server = WebSocketGatewayServer(
            FakeRouter(), jwt_validator=RejectingValidator()
        )
        client = await _client_for(server)
        try:
            try:
                await client.ws_connect(
                    "/v1/va", headers={"Authorization": "Bearer redacted"}
                )
                raise AssertionError("upgrade unexpectedly succeeded")
            except WSServerHandshakeError as error:
                assert error.status == 401
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("message_type", "event_type"),
    [
        ("session_end", "SESSION_END"),
        ("transfer", "TRANSFER_TO_AGENT"),
    ],
)
def test_terminal_response_waits_for_peer_close_and_cleans_provider_once(
    message_type, event_type
):
    async def scenario():
        router = FakeRouter(start_message_type=message_type)
        server = WebSocketGatewayServer(router, allow_unauthenticated_local_dev=True)
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            response = await socket.receive_json()
            assert response["type"] == "VOICE_VA_RESPONSE"
            assert response["payload"]["output_events"][0]["event_type"] == event_type
            await asyncio.sleep(0.05)
            assert not socket.closed
            await socket.close()
            await _wait_for_cleanup(router)
            assert socket.closed
            assert router.end_calls == 1
            terminal_events = [
                event
                for event in server.get_connection_events()
                if event["event_type"] == "terminal"
            ]
            assert len(terminal_events) == 1
            assert terminal_events[0]["outcome"] == event_type
            assert terminal_events[0]["transport"] == "websocket"
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_terminal_response_peer_close_wait_is_bounded():
    async def scenario():
        router = FakeRouter(start_message_type="session_end")
        server = WebSocketGatewayServer(
            router,
            allow_unauthenticated_local_dev=True,
            terminal_peer_close_timeout_seconds=0.05,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            response = await socket.receive_json()
            assert response["type"] == "VOICE_VA_RESPONSE"
            assert (
                response["payload"]["output_events"][0]["event_type"] == "SESSION_END"
            )
            await socket.receive()
            await _wait_for_cleanup(router)
            assert socket.closed
            assert router.end_calls == 1
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_first_application_message_must_be_session_start():
    async def scenario():
        server = WebSocketGatewayServer(
            FakeRouter(), allow_unauthenticated_local_dev=True
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            value = _start()
            value["payload"]["voice_va_input_type"]["event_input"]["event_type"] = (
                "CUSTOM_EVENT"
            )
            await socket.send_json(value)
            response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert response["status"] == 400
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_handshake_validation_logs_field_paths_without_peer_values(caplog):
    async def scenario():
        server = WebSocketGatewayServer(
            FakeRouter(), allow_unauthenticated_local_dev=True
        )
        client = await _client_for(server)
        try:
            with caplog.at_level(
                logging.INFO, logger="src.transports.websocket_server"
            ):
                socket = await client.ws_connect("/v1/va")
                value = _start()
                value["api_token\nforged"] = "do-not-log-this-peer-value"
                await socket.send_json(value)
                response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert response["status"] == 400
            messages = "\n".join(record.getMessage() for record in caplog.records)
            assert "websocket_voice_connection_opened" in messages
            assert "category=schema_validation_failed" in messages
            assert "api_token_forged:extra_forbidden" in messages
            assert "api_token\nforged" not in messages
            assert "do-not-log-this-peer-value" not in messages
            assert "org-1" not in messages
            assert "call-1" not in messages
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_post_handshake_validation_returns_error_and_logs_safely(caplog):
    async def scenario():
        router = FakeRouter()
        server = WebSocketGatewayServer(router, allow_unauthenticated_local_dev=True)
        client = await _client_for(server)
        try:
            with caplog.at_level(
                logging.INFO, logger="src.transports.websocket_server"
            ):
                socket = await client.ws_connect("/v1/va")
                await socket.send_json(_start())
                assert (await socket.receive_json())["type"] == "VOICE_VA_RESPONSE"
                invalid = _start(seq=2)
                invalid["payload"]["voice_va_input_type"] = {
                    "audio_input": {
                        "caller_audio_b64": "do-not-log-this-peer-value",
                        "encoding": "MULAW_FORMAT",
                    }
                }
                await socket.send_json(invalid)
                response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert response["status"] == 400
            messages = "\n".join(
                record.getMessage()
                for record in caplog.records
                if "websocket_voice_frame_rejected" in record.getMessage()
            )
            assert "websocket_voice_frame_rejected" in messages
            assert "message_index=2" in messages
            assert "category=schema_validation_failed" in messages
            assert "sample_rate_hertz:missing" in messages
            assert "do-not-log-this-peer-value" not in messages
            assert "org-1" not in messages
            assert "call-1" not in messages
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_duplicate_sequence_is_fatal_and_disconnect_cleans_provider_once():
    async def scenario():
        router = FakeRouter()
        server = WebSocketGatewayServer(router, allow_unauthenticated_local_dev=True)
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            assert (await socket.receive_json())["type"] == "VOICE_VA_RESPONSE"
            duplicate = _start()
            duplicate["payload"]["voice_va_input_type"] = {
                "event_input": {"event_type": "CUSTOM_EVENT"}
            }
            await socket.send_json(duplicate)
            response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert "seq must increase" in response["detail"]
            await socket.receive()
            await _wait_for_cleanup(router)
            assert socket.close_code == 1002
            assert router.end_calls == 1
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_second_session_start_is_fatal_even_with_increasing_sequence():
    async def scenario():
        router = FakeRouter()
        server = WebSocketGatewayServer(router, allow_unauthenticated_local_dev=True)
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            assert (await socket.receive_json())["type"] == "VOICE_VA_RESPONSE"
            await socket.send_json(_start(seq=2))
            response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert (
                "SESSION_START may only be sent as the first message"
                in response["detail"]
            )
            await socket.receive()
            await _wait_for_cleanup(router)
            assert router.end_calls == 1
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())


def test_existing_grpc_ownership_blocks_websocket_conversation():
    async def scenario():
        registry = ConversationRegistry()
        registry.acquire(
            customer_org_id="org-1",
            conversation_id="call-1",
            transport="grpc",
            agent_id="Test Agent",
            connection_id="grpc-1",
        )
        server = WebSocketGatewayServer(
            FakeRouter(),
            registry=registry,
            allow_unauthenticated_local_dev=True,
        )
        client = await _client_for(server)
        try:
            socket = await client.ws_connect("/v1/va")
            await socket.send_json(_start())
            response = await socket.receive_json()
            assert response["type"] == "ERROR"
            assert response["status"] == 409
        finally:
            await client.close()
            server.shutdown()

    asyncio.run(scenario())
