"""Webex BYOVA WebSocket transport backed by the shared conversation engine."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any

from aiohttp import WSMsgType, web
from pydantic import ValidationError

from src.auth.jwt_validator import JWTValidator
from src.core.conversation_registry import (
    ConversationConflictError,
    ConversationLease,
    ConversationRegistry,
)
from src.core.virtual_agent_router import VirtualAgentRouter
from src.core.wxcc_gateway_server import ConversationProcessor
from src.generated.byova_common_pb2 import OutputEvent
from src.generated.voicevirtualagent_pb2 import VoiceVAResponse

from .websocket_adapter import (
    UnsupportedMediaError,
    envelope,
    frame_response_payloads,
    is_session_start,
    request_to_protobuf,
)
from .websocket_models import (
    ListVARequest,
    PingEnvelope,
    VoiceVARequestEnvelope,
    parse_incoming_envelope,
)

WEBSOCKET_SCHEMA_UUID = "a38a10b7-43e4-4676-a076-a7d6dce9387d"


class WebSocketProtocolError(ValueError):
    """A fatal peer protocol error with a stable public code."""

    def __init__(
        self,
        detail: str,
        *,
        code: str = "bad_request",
        status: int = 400,
        close_code: int = 1002,
    ) -> None:
        super().__init__(detail)
        self.detail = detail
        self.code = code
        self.status = status
        self.close_code = close_code


@dataclass
class _Outbound:
    message_type: str
    payload: dict[str, Any] | None = None
    error: WebSocketProtocolError | None = None


class WebSocketGatewayServer:
    """aiohttp request handlers for the two BYOVA WebSocket endpoints."""

    def __init__(
        self,
        router: VirtualAgentRouter,
        *,
        registry: ConversationRegistry | None = None,
        jwt_validator: JWTValidator | None = None,
        allow_unauthenticated_local_dev: bool = False,
        vad_config: dict[str, Any] | None = None,
        first_message_timeout_seconds: float = 10.0,
        discovery_idle_timeout_seconds: float = 5.0,
        terminal_flush_timeout_seconds: float = 2.0,
        queue_maxsize: int = 100,
        queue_put_timeout_seconds: float = 1.0,
        max_message_bytes: int = 128 * 1024,
        connector_max_workers: int = 20,
        connector_max_pending: int = 20,
        output_chunk_bytes: int = 3_200,
    ) -> None:
        if queue_maxsize <= 0:
            raise ValueError("queue_maxsize must be greater than zero")
        if connector_max_workers <= 0 or connector_max_pending < 0:
            raise ValueError("connector worker limits are invalid")
        if not 100 <= output_chunk_bytes <= 65_536:
            raise ValueError("output_chunk_bytes must be between 100 and 65536")
        if jwt_validator is None and not allow_unauthenticated_local_dev:
            raise ValueError(
                "WebSocket JWT validation is required unless explicit local-dev "
                "authentication bypass is enabled"
            )

        self.router = router
        self.registry = registry or ConversationRegistry()
        self.jwt_validator = jwt_validator
        self.allow_unauthenticated_local_dev = allow_unauthenticated_local_dev
        self.vad_config = vad_config or {}
        self.first_message_timeout_seconds = max(
            0.1, float(first_message_timeout_seconds)
        )
        self.discovery_idle_timeout_seconds = max(
            0.1, float(discovery_idle_timeout_seconds)
        )
        self.terminal_flush_timeout_seconds = max(
            0.0, float(terminal_flush_timeout_seconds)
        )
        self.queue_maxsize = queue_maxsize
        self.queue_put_timeout_seconds = max(0.01, float(queue_put_timeout_seconds))
        self.max_message_bytes = max_message_bytes
        self.output_chunk_bytes = output_chunk_bytes
        self._executor = ThreadPoolExecutor(
            max_workers=connector_max_workers,
            thread_name_prefix="byova-websocket-connector",
        )
        self._worker_capacity = connector_max_workers + connector_max_pending
        self._worker_slots = threading.BoundedSemaphore(self._worker_capacity)
        self._processors: dict[tuple[str, str], ConversationProcessor] = {}
        self._processors_lock = threading.RLock()
        self._connection_events: list[dict[str, Any]] = []
        self.logger = logging.getLogger(__name__)

    def create_app(self) -> web.Application:
        app = web.Application(client_max_size=self.max_message_bytes)
        app.router.add_get("/health", self.handle_health)
        app.router.add_get("/v1/va", self.handle_voice_agent)
        app.router.add_get("/v1/listVirtualAgents", self.handle_list_virtual_agents)
        return app

    async def handle_health(self, request: web.Request) -> web.Response:
        """Return minimal process health for a private load-balancer probe."""
        return web.json_response({"status": "healthy", "transport": "websocket"})

    async def _authorize(self, request: web.Request) -> web.Response | None:
        if self.jwt_validator is None:
            if request.remote not in {"127.0.0.1", "::1"}:
                return web.json_response(
                    {"error": "local development authentication bypass is local-only"},
                    status=403,
                )
            return None

        authorization = request.headers.get("Authorization", "").strip()
        if not authorization:
            return web.json_response({"error": "unauthorized"}, status=401)
        if authorization.lower().startswith("bearer "):
            token = authorization[7:].strip()
        else:
            token = authorization
        if not token:
            return web.json_response({"error": "unauthorized"}, status=401)
        try:
            await self._run_bounded(self.jwt_validator.validate_token, token)
        except Exception:
            self.logger.warning(
                "WebSocket upgrade rejected because JWT validation failed"
            )
            return web.json_response({"error": "unauthorized"}, status=401)
        return None

    async def handle_list_virtual_agents(
        self, request: web.Request
    ) -> web.StreamResponse:
        rejection = await self._authorize(request)
        if rejection is not None:
            return rejection
        socket = web.WebSocketResponse(
            autoping=True,
            heartbeat=30.0,
            max_msg_size=self.max_message_bytes,
        )
        await socket.prepare(request)
        try:
            message = await asyncio.wait_for(
                socket.receive(), timeout=self.first_message_timeout_seconds
            )
            value = self._decode_text_json(message)
            ListVARequest.model_validate(value)
            agents = []
            for full_agent_id in self.router.get_all_available_agents(
                transport="websocket"
            ):
                name = full_agent_id.split(": ", 1)[-1]
                agents.append(
                    {
                        "id": full_agent_id,
                        "name": name,
                        "description": self.router.agent_to_connector_name_map.get(
                            full_agent_id, ""
                        ),
                    }
                )
            await socket.send_json({"virtual_agents": agents})
            self.logger.info(
                "WebSocket virtual-agent discovery response sent: agent_count=%d",
                len(agents),
            )
            try:
                await asyncio.wait_for(
                    socket.receive(), timeout=self.discovery_idle_timeout_seconds
                )
            except asyncio.TimeoutError:
                self.logger.info(
                    "WebSocket virtual-agent discovery peer remained open; "
                    "closing after idle timeout"
                )
            finally:
                if not socket.closed:
                    await socket.close(code=1000, message=b"discovery complete")
        except (
            asyncio.TimeoutError,
            ValidationError,
            WebSocketProtocolError,
            ValueError,
        ) as error:
            if isinstance(error, asyncio.TimeoutError):
                detail = "request timed out"
            elif isinstance(error, ValidationError):
                detail = "request does not match the WebSocket contract"
            else:
                detail = str(error)
            await self._send_error_direct(
                socket,
                conversation_id="listVirtualAgents",
                seq=1,
                error=WebSocketProtocolError(detail),
            )
        return socket

    async def handle_voice_agent(self, request: web.Request) -> web.StreamResponse:
        rejection = await self._authorize(request)
        if rejection is not None:
            return rejection
        socket = web.WebSocketResponse(
            autoping=True,
            heartbeat=30.0,
            max_msg_size=self.max_message_bytes,
        )
        await socket.prepare(request)

        lease: ConversationLease | None = None
        processor: ConversationProcessor | None = None
        response_sink = None
        termination_reason = "websocket_disconnect"
        close_code = 1001
        connection_id = uuid.uuid4().hex
        first_frame_type = "not_received"
        first_frame_bytes = 0
        self.logger.info(
            "websocket_voice_connection_opened connection_id=%s", connection_id
        )
        try:
            first_message = await asyncio.wait_for(
                socket.receive(), timeout=self.first_message_timeout_seconds
            )
            first_frame_type, first_frame_bytes = self._frame_diagnostics(
                first_message
            )
            first = self._parse_voice_message(first_message)
            if not isinstance(first, VoiceVARequestEnvelope) or not is_session_start(
                first
            ):
                raise WebSocketProtocolError(
                    "first application message must be a SESSION_START request"
                )

            agent_id = first.payload.virtual_agent_id
            if not agent_id:
                available_agents = self.router.get_all_available_agents(
                    transport="websocket"
                )
                if not available_agents:
                    raise WebSocketProtocolError(
                        "no virtual agents are available", status=404
                    )
                agent_id = available_agents[0]
            self.logger.info(
                "websocket_voice_session_start_accepted "
                "connection_id=%s frame_type=%s frame_bytes=%d agent_supplied=%s",
                connection_id,
                first_frame_type,
                first_frame_bytes,
                bool(first.payload.virtual_agent_id),
            )
            try:
                self.router.get_connector_for_agent(agent_id, transport="websocket")
            except ValueError as error:
                raise WebSocketProtocolError(
                    "virtual agent was not found", status=404
                ) from error

            lease = self.registry.acquire(
                customer_org_id=first.payload.customer_org_id,
                conversation_id=first.conversation_id,
                transport="websocket",
                agent_id=agent_id,
                connection_id=connection_id,
            )
            processor = ConversationProcessor(
                first.conversation_id,
                agent_id,
                self.router,
                self.vad_config,
                self.terminal_flush_timeout_seconds,
            )
            with self._processors_lock:
                self._processors[lease.key] = processor
            self._add_event("start", lease, agent_id)

            inbound: asyncio.Queue[VoiceVARequestEnvelope | None] = asyncio.Queue(
                maxsize=self.queue_maxsize
            )
            outbound: asyncio.Queue[_Outbound] = asyncio.Queue(
                maxsize=self.queue_maxsize
            )
            stop = asyncio.Event()
            terminal = asyncio.Event()
            loop = asyncio.get_running_loop()
            output_mode = self.router.get_websocket_output_mode(agent_id)

            async def enqueue_outbound(item: _Outbound) -> None:
                try:
                    await asyncio.wait_for(
                        outbound.put(item), timeout=self.queue_put_timeout_seconds
                    )
                except asyncio.TimeoutError as error:
                    raise WebSocketProtocolError(
                        "outbound queue remained full",
                        code="rate_limit",
                        status=429,
                        close_code=1013,
                    ) from error

            async def enqueue_response(response: VoiceVAResponse) -> None:
                payloads = frame_response_payloads(
                    response,
                    output_mode=output_mode,
                    chunk_size=self.output_chunk_bytes,
                )
                for payload in payloads:
                    await enqueue_outbound(_Outbound("VOICE_VA_RESPONSE", payload))

            def connector_response_sink(response: VoiceVAResponse) -> bool:
                future = asyncio.run_coroutine_threadsafe(
                    enqueue_response(response), loop
                )
                try:
                    future.result(timeout=self.queue_put_timeout_seconds + 0.25)
                    return True
                except Exception:
                    future.cancel()
                    return False

            response_sink = connector_response_sink
            processor.set_async_response_sink(response_sink)
            await inbound.put(first)

            async def reader() -> None:
                nonlocal close_code
                last_seq = first.seq
                try:
                    async for message in socket:
                        incoming = self._parse_voice_message(message)
                        if incoming.seq <= last_seq:
                            raise WebSocketProtocolError(
                                "seq must increase for every client message"
                            )
                        last_seq = incoming.seq
                        if incoming.conversation_id != lease.conversation_id:
                            raise WebSocketProtocolError(
                                "conversation_id cannot change on an open socket"
                            )
                        if isinstance(incoming, PingEnvelope):
                            await enqueue_outbound(_Outbound("PONG"))
                            continue
                        if is_session_start(incoming):
                            raise WebSocketProtocolError(
                                "SESSION_START may only be sent as the first message"
                            )
                        if incoming.payload.customer_org_id != lease.customer_org_id:
                            raise WebSocketProtocolError(
                                "customer_org_id cannot change on an open socket"
                            )
                        request_agent = incoming.payload.virtual_agent_id
                        if request_agent and request_agent != agent_id:
                            raise WebSocketProtocolError(
                                "virtual_agent_id cannot change on an open socket"
                            )
                        try:
                            await asyncio.wait_for(
                                inbound.put(incoming),
                                timeout=self.queue_put_timeout_seconds,
                            )
                        except asyncio.TimeoutError as error:
                            raise WebSocketProtocolError(
                                "inbound queue remained full",
                                code="rate_limit",
                                status=429,
                                close_code=1013,
                            ) from error
                except WebSocketProtocolError as error:
                    close_code = error.close_code
                    await enqueue_outbound(_Outbound("ERROR", error=error))
                finally:
                    stop.set()
                    try:
                        inbound.put_nowait(None)
                    except asyncio.QueueFull:
                        pass

            async def run_connector(
                envelope_value: VoiceVARequestEnvelope,
            ) -> list[VoiceVAResponse]:
                request_value = request_to_protobuf(envelope_value)
                return await self._run_bounded(
                    lambda: list(processor.process_request(request_value))
                )

            async def process() -> None:
                nonlocal close_code, termination_reason
                try:
                    while not stop.is_set():
                        item = await inbound.get()
                        try:
                            if item is None:
                                return
                            responses = await run_connector(item)
                            for response in responses:
                                await enqueue_response(response)
                                if self._is_terminal(response):
                                    termination_reason = "completed"
                                    terminal.set()
                                    stop.set()
                                    return
                        finally:
                            inbound.task_done()
                except UnsupportedMediaError as error:
                    close_code = 1003
                    await enqueue_outbound(
                        _Outbound(
                            "ERROR",
                            error=WebSocketProtocolError(
                                str(error),
                                code="unsupported_media",
                                status=415,
                                close_code=1003,
                            ),
                        )
                    )
                    stop.set()
                except Exception:
                    close_code = 1011
                    self.logger.exception(
                        "WebSocket connector processing failed for conversation %s",
                        lease.conversation_id,
                    )
                    await enqueue_outbound(
                        _Outbound(
                            "ERROR",
                            error=WebSocketProtocolError(
                                "virtual agent processing failed",
                                code="upstream_error",
                                status=502,
                                close_code=1011,
                            ),
                        )
                    )
                    stop.set()

            async def sender() -> None:
                server_seq = 0
                while not stop.is_set() or not outbound.empty():
                    try:
                        item = await asyncio.wait_for(outbound.get(), timeout=0.1)
                    except asyncio.TimeoutError:
                        continue
                    try:
                        server_seq += 1
                        if item.message_type == "ERROR" and item.error:
                            value = {
                                "type": "ERROR",
                                "seq": server_seq,
                                "ts": envelope(
                                    message_type="PONG",
                                    seq=server_seq,
                                    conversation_id=lease.conversation_id,
                                    payload={},
                                )["ts"],
                                "conversation_id": lease.conversation_id,
                                "code": item.error.code,
                                "status": item.error.status,
                                "detail": item.error.detail,
                            }
                        elif item.message_type == "PONG":
                            value = envelope(
                                message_type="PONG",
                                seq=server_seq,
                                conversation_id=lease.conversation_id,
                                payload={},
                            )
                            value.pop("payload")
                        else:
                            value = envelope(
                                message_type=item.message_type,
                                seq=server_seq,
                                conversation_id=lease.conversation_id,
                                payload=item.payload or {},
                            )
                        await socket.send_json(value)
                    finally:
                        outbound.task_done()

            reader_task = asyncio.create_task(reader(), name="byova-ws-reader")
            process_task = asyncio.create_task(process(), name="byova-ws-processor")
            sender_task = asyncio.create_task(sender(), name="byova-ws-sender")
            await stop.wait()
            if terminal.is_set():
                try:
                    await asyncio.wait_for(
                        outbound.join(), timeout=self.terminal_flush_timeout_seconds
                    )
                except asyncio.TimeoutError:
                    termination_reason = "terminal_flush_timeout"
            else:
                try:
                    await asyncio.wait_for(outbound.join(), timeout=0.5)
                except asyncio.TimeoutError:
                    pass
            for task in (reader_task, process_task):
                task.cancel()
            stop.set()
            try:
                await asyncio.wait_for(sender_task, timeout=0.5)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                sender_task.cancel()
            await socket.close(code=1000 if terminal.is_set() else close_code)

        except ConversationConflictError as error:
            await self._send_error_direct(
                socket,
                conversation_id=(lease.conversation_id if lease else "unknown"),
                seq=1,
                error=WebSocketProtocolError(str(error), status=409),
            )
        except asyncio.TimeoutError:
            self._log_handshake_failure(
                connection_id=connection_id,
                category="session_start_timeout",
                frame_type=first_frame_type,
                frame_bytes=first_frame_bytes,
            )
            await self._send_error_direct(
                socket,
                conversation_id="unknown",
                seq=1,
                error=WebSocketProtocolError(
                    "SESSION_START was not received before the deadline",
                    code="timeout",
                    status=408,
                    close_code=1008,
                ),
            )
        except (ValidationError, WebSocketProtocolError, ValueError) as error:
            self._log_handshake_failure(
                connection_id=connection_id,
                category=self._handshake_error_category(error),
                frame_type=first_frame_type,
                frame_bytes=first_frame_bytes,
                validation_fields=self._validation_fields(error),
            )
            if isinstance(error, WebSocketProtocolError):
                protocol_error = error
            elif isinstance(error, ValidationError):
                protocol_error = WebSocketProtocolError(
                    "message does not match the WebSocket contract"
                )
            else:
                protocol_error = WebSocketProtocolError(str(error))
            await self._send_error_direct(
                socket,
                conversation_id=(lease.conversation_id if lease else "unknown"),
                seq=1,
                error=protocol_error,
            )
        finally:
            if processor is not None and response_sink is not None:
                processor.clear_async_response_sink(response_sink)
            if processor is not None:
                try:
                    await self._run_bounded(
                        processor.cleanup,
                        termination_reason,
                        wait_seconds=5.0,
                    )
                except WebSocketProtocolError:
                    # Preserve exactly-once cleanup during sustained worker
                    # saturation without adding unbounded executor work.
                    processor.cleanup(termination_reason)
            if lease is not None:
                with self._processors_lock:
                    self._processors.pop(lease.key, None)
                self.registry.release(lease)
                self._add_event("end", lease, lease.agent_id, reason=termination_reason)
        return socket

    async def _run_bounded(
        self, callable_value, *args, wait_seconds: float | None = None
    ):
        """Run blocking work without allowing an unbounded executor backlog."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (
            self.queue_put_timeout_seconds
            if wait_seconds is None
            else max(0.0, wait_seconds)
        )
        while not self._worker_slots.acquire(blocking=False):
            if loop.time() >= deadline:
                raise WebSocketProtocolError(
                    "connector worker capacity is exhausted",
                    code="rate_limit",
                    status=429,
                    close_code=1013,
                )
            await asyncio.sleep(0.01)
        try:
            return await loop.run_in_executor(self._executor, callable_value, *args)
        finally:
            self._worker_slots.release()

    @staticmethod
    def _decode_text_json(message: Any) -> Any:
        if message.type == WSMsgType.TEXT:
            try:
                return json.loads(message.data)
            except json.JSONDecodeError as error:
                raise WebSocketProtocolError(
                    "message must contain valid JSON"
                ) from error
        if message.type == WSMsgType.BINARY:
            raise WebSocketProtocolError(
                "binary frames are not supported", close_code=1003
            )
        raise WebSocketProtocolError("WebSocket closed before a request was received")

    @staticmethod
    def _frame_diagnostics(message: Any) -> tuple[str, int]:
        """Return bounded, payload-free information about one inbound frame."""

        frame_type = getattr(message.type, "name", str(message.type))
        data = message.data
        if isinstance(data, str):
            frame_bytes = len(data.encode("utf-8", errors="replace"))
        elif isinstance(data, (bytes, bytearray, memoryview)):
            frame_bytes = len(data)
        else:
            frame_bytes = 0
        return frame_type, frame_bytes

    @staticmethod
    def _handshake_error_category(error: Exception) -> str:
        """Classify a handshake failure without logging peer-controlled values."""

        if isinstance(error, ValidationError):
            return "schema_validation_failed"
        if isinstance(error, WebSocketProtocolError):
            detail = error.detail
            if detail == "first application message must be a SESSION_START request":
                return "first_message_not_session_start"
            if detail == "message must contain valid JSON":
                return "invalid_json"
            if detail == "binary frames are not supported":
                return "binary_frame"
            if detail == "WebSocket closed before a request was received":
                return "closed_before_first_message"
            if detail.startswith("unsupported message type:"):
                return "unsupported_message_type"
            if detail == "no virtual agents are available":
                return "no_virtual_agents"
            if detail == "virtual agent was not found":
                return "virtual_agent_not_found"
            return "protocol_error"
        return "invalid_message"

    @staticmethod
    def _validation_fields(error: Exception) -> str:
        """Summarize validation locations and codes without input values."""

        if not isinstance(error, ValidationError):
            return "none"
        summaries = []
        for item in error.errors(
            include_url=False, include_context=False, include_input=False
        ):
            location = ".".join(
                WebSocketGatewayServer._safe_log_token(part)
                for part in item.get("loc", ())
            )
            error_type = WebSocketGatewayServer._safe_log_token(
                item.get("type", "validation_error")
            )
            summaries.append(f"{location}:{error_type}")
            if len(summaries) == 8:
                break
        return ",".join(summaries) or "unknown"

    @staticmethod
    def _safe_log_token(value: Any, *, max_length: int = 80) -> str:
        """Bound and neutralize a peer-influenced diagnostic token."""

        text = str(value)
        sanitized = "".join(
            character
            if character.isascii()
            and (character.isalnum() or character in "._-[]")
            else "_"
            for character in text
        )
        return sanitized[:max_length] or "unknown"

    def _log_handshake_failure(
        self,
        *,
        connection_id: str,
        category: str,
        frame_type: str,
        frame_bytes: int,
        validation_fields: str = "none",
    ) -> None:
        self.logger.warning(
            "websocket_voice_handshake_failed connection_id=%s category=%s "
            "frame_type=%s frame_bytes=%d validation_fields=%s",
            connection_id,
            category,
            frame_type,
            frame_bytes,
            validation_fields,
        )

    def _parse_voice_message(self, message: Any):
        return parse_incoming_envelope(self._decode_text_json(message))

    @staticmethod
    def _is_terminal(response: VoiceVAResponse) -> bool:
        return any(
            event.event_type
            in {
                OutputEvent.EventType.SESSION_END,
                OutputEvent.EventType.TRANSFER_TO_AGENT,
            }
            for event in response.output_events
        )

    async def _send_error_direct(
        self,
        socket: web.WebSocketResponse,
        *,
        conversation_id: str,
        seq: int,
        error: WebSocketProtocolError,
    ) -> None:
        if not socket.closed:
            value = envelope(
                message_type="PONG",
                seq=seq,
                conversation_id=conversation_id,
                payload={},
            )
            value.update(
                {
                    "type": "ERROR",
                    "code": error.code,
                    "status": error.status,
                    "detail": error.detail,
                }
            )
            value.pop("payload", None)
            await socket.send_json(value)
            await socket.close(
                code=error.close_code,
                message=error.code.encode("ascii", errors="ignore"),
            )

    def _add_event(
        self, event_type: str, lease: ConversationLease, agent_id: str, **values: Any
    ) -> None:
        event = {
            "event_type": event_type,
            "transport": "websocket",
            "customer_org_id": lease.customer_org_id,
            "conversation_id": lease.conversation_id,
            "agent_id": agent_id,
            **values,
        }
        self._connection_events.append(event)
        del self._connection_events[:-100]

    def get_active_conversations(self) -> dict[str, dict[str, Any]]:
        with self._processors_lock:
            return {
                f"{org_id}:{conversation_id}": {
                    "customer_org_id": org_id,
                    "conversation_id": conversation_id,
                    "agent_id": processor.virtual_agent_id,
                    "session_started": processor.session_started,
                    "transport": "websocket",
                }
                for (org_id, conversation_id), processor in self._processors.items()
            }

    def get_connection_events(self) -> list[dict[str, Any]]:
        return list(self._connection_events)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


class WebSocketGatewayRuntime:
    """Run the aiohttp listener in a dedicated event-loop thread."""

    def __init__(
        self,
        server: WebSocketGatewayServer,
        *,
        host: str = "0.0.0.0",
        port: int = 8765,
        startup_timeout_seconds: float = 10.0,
    ) -> None:
        self.server = server
        self.host = host
        self.port = port
        self.startup_timeout_seconds = startup_timeout_seconds
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._runner: web.AppRunner | None = None
        self._ready = threading.Event()
        self._startup_error: BaseException | None = None

    async def _start_async(self) -> None:
        self._runner = web.AppRunner(self.server.create_app())
        await self._runner.setup()
        site = web.TCPSite(self._runner, self.host, self.port)
        await site.start()

    def _run(self) -> None:
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._start_async())
        except BaseException as error:
            self._startup_error = error
            self._ready.set()
            self._loop.close()
            return
        self._ready.set()
        self._loop.run_forever()
        if self._runner is not None:
            self._loop.run_until_complete(self._runner.cleanup())
        self._loop.close()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            raise RuntimeError("WebSocket listener is already running")
        self._ready.clear()
        self._startup_error = None
        self._thread = threading.Thread(
            target=self._run,
            name="byova-websocket-listener",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(self.startup_timeout_seconds):
            raise TimeoutError("WebSocket listener startup timed out")
        if self._startup_error is not None:
            raise RuntimeError(
                "WebSocket listener failed to start"
            ) from self._startup_error

    def stop(self, timeout: float = 5.0) -> None:
        if self._loop and self._loop.is_running():
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=timeout)
        self.server.shutdown()
        self._thread = None
