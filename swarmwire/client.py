"""Thin asynchronous client for the C++ broker. Python 3.10+."""
import asyncio
import hashlib
import itertools
import json
import os
import socket
import struct
from dataclasses import dataclass
from typing import Any, Callable, Optional, TYPE_CHECKING
from .deadlines import deadline

if TYPE_CHECKING:
    from .transports import Endpoint

try:
    import orjson
except ImportError:
    orjson = None

HELLO, SEND, CREDIT, STORE, LOAD, STATS = range(1, 7)
SEND_STREAM = 7
ACK, DELIVERY, ERROR = 128, 129, 130
MAX_FRAME = 1024 * 1024 + 256


def encode(value: Any) -> bytes:
    return orjson.dumps(value) if orjson else json.dumps(value, separators=(",", ":"), allow_nan=False).encode()


def decode(data: bytes) -> Any:
    return orjson.loads(data) if orjson else json.loads(data)


class RelayError(RuntimeError):
    pass


class Backpressure(RelayError):
    """Nothing was accepted: retry later or reduce outstanding work."""


class RemoteError(RelayError):
    """The addressed worker failed this task and returned a correlated error."""


@dataclass(slots=True)
class Message:
    sender: str
    data: bytes
    decoded: Optional[dict] = None
    error: Optional[Exception] = None
    release: Optional[Callable[[], None]] = None


class Client:
    """Binary messages, pipelined acknowledgements and receiver credits.

    Send acknowledgement means queued by the broker, not processed by an agent.
    A timed-out send can have been accepted; never blindly retry side effects.
    """
    def __init__(self, reader, writer, name: str, capacity: int, acknowledged=True):
        self.reader, self.writer, self.name = reader, writer, name
        self.acknowledged = acknowledged
        self.inbox = asyncio.Queue(capacity)
        self._ready = asyncio.Event()
        self.on_close = []
        self._ids = itertools.count(1)
        self._pending = {}
        self._gate = asyncio.Semaphore(128)
        self._lock = asyncio.Lock()
        self._closed = False
        self._failure = None
        self._credit = 0
        self._credit_task = None
        self.interceptor: Optional[Callable[[Message], bool]] = None
        self._reader_task = asyncio.create_task(self._read(), name=f"swarmwire:{name}")

    @classmethod
    async def connect(cls, name: str, *, port=8765, unix=None, capacity=256, token=None, acknowledged=True):
        if not isinstance(capacity, int) or not 1 <= capacity <= 4096:
            raise ValueError("capacity must be 1..4096")
        if unix:
            reader, writer = await asyncio.open_unix_connection(unix)
        else:
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.get_extra_info("socket").setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        client = cls(reader, writer, name, capacity, acknowledged)
        try:
            await client._rpc(HELLO, name, struct.pack("!I", capacity) + (token if token is not None else os.getenv("SWARMWIRE_TOKEN", "")).encode())
        except BaseException:
            await client.close()
            raise
        return client

    async def _write(self, op: int, request_id: int, target: str, data: bytes):
        if self._closed or self._failure:
            raise RelayError("connection closed") from self._failure
        name = target.encode()
        length = 7 + len(name) + len(data)
        if len(name) > 128 or length > MAX_FRAME:
            raise ValueError("frame exceeds limit (payload about 1 MiB, target 128 bytes)")
        header = struct.pack("!IBIH", length, op, request_id, len(name)) + name
        async with self._lock:
            # Python 3.13.1's sendmsg transport can retain an empty iovec and
            # spin forever. Never give writelines an empty payload fragment.
            if data:
                self.writer.writelines((header, data))
            else:
                self.writer.write(header)
            await self.writer.drain()

    async def _rpc(self, op: int, target="", data=b"", timeout=10.0):
        async def exchange():
            async with self._gate:
                ident = next(self._ids)
                if ident >= 2**32:
                    raise RelayError("reconnect before request IDs wrap")
                future = asyncio.get_running_loop().create_future()
                self._pending[ident] = future
                try:
                    await self._write(op, ident, target, data)
                    return await future
                finally:
                    self._pending.pop(ident, None)
                    if future.done() and not future.cancelled():
                        future.exception()  # consume racing disconnect errors, too
        return await deadline(exchange(), timeout)

    async def _read(self):
        try:
            while True:
                size = struct.unpack("!I", await self.reader.readexactly(4))[0]
                if not 7 <= size <= MAX_FRAME:
                    raise RelayError("invalid frame length")
                packet = await self.reader.readexactly(size)
                op, ident, name_size = struct.unpack_from("!BIH", packet)
                if 7 + name_size > size:
                    raise RelayError("invalid frame name length")
                data = packet[7 + name_size:]
                if op == DELIVERY:
                    message = Message(packet[7:7 + name_size].decode(), data)
                    consumed = False
                    if self.interceptor:
                        try:
                            consumed = self.interceptor(message)
                        except Exception as exc:
                            message.error = exc
                    if consumed:
                        self._return_credit()
                    else:
                        self.inbox.put_nowait(message)  # broker credits guarantee available room
                        self._ready.set()
                elif op in (ACK, ERROR):
                    future = self._pending.get(ident)
                    if future and not future.done():
                        if op == ACK:
                            future.set_result(data)
                        else:
                            text = data.decode(errors="replace")
                            future.set_exception(Backpressure(text) if text.startswith("BACKPRESSURE:") else RelayError(text))
                    elif op == ERROR and not self.acknowledged:
                        # Streaming sends don't await positive broker receipts.
                        # A rejection must still be explicit; fail the connection
                        # and every pending application request, never ignore it.
                        raise RelayError("stream send rejected: " + data.decode(errors="replace"))
                else:
                    raise RelayError("unknown frame opcode")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._failure = exc
            for future in tuple(self._pending.values()):
                if not future.done():
                    future.set_exception(RelayError(f"connection failed: {type(exc).__name__}"))
        finally:
            # Wake a blocked receiver without competing with normal message capacity.
            self._closed = True
            self._ready.set()
            for callback in self.on_close:
                callback()
            self.writer.close()

    def _return_credit(self):
        self._credit += 1
        if not self._closed and (self._credit_task is None or self._credit_task.done()):
            self._credit_task = asyncio.create_task(self._flush_credit())

    async def _flush_credit(self):
        try:
            while self._credit and not self._closed:
                count, self._credit = self._credit, 0
                await self._write(CREDIT, 0, "", struct.pack("!I", count))
        except Exception:
            self.writer.close()

    async def send(self, to: str, data: bytes, *, timeout=10.0):
        if not isinstance(data, bytes):
            raise TypeError("binary Client.send expects bytes; use Agent for JSON")
        if len(data) > 1024 * 1024:
            raise ValueError("raw message exceeds 1 MiB")
        if self.acknowledged:
            await self._rpc(SEND, to, data, timeout)
        else:
            ident = next(self._ids)
            if ident >= 2**32:
                raise RelayError("reconnect before request IDs wrap")
            await deadline(self._write(SEND_STREAM, ident, to, data), timeout)

    async def receive(self, *, timeout=None) -> Message:
        expires_at = None if timeout is None else asyncio.get_running_loop().time() + timeout
        while self.inbox.empty():
            if self._closed:
                raise RelayError("connection closed") from self._failure
            if timeout is None:
                await self._ready.wait()
            else:
                remaining = expires_at - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                await deadline(self._ready.wait(), remaining)
        message = self.inbox.get_nowait()
        if self.inbox.empty():
            self._ready.clear()
        self._return_credit()
        return message

    async def store(self, data: bytes) -> str:
        key = hashlib.sha256(data).hexdigest()
        await self._rpc(STORE, key, data)
        return key

    async def load(self, key: str) -> bytes:
        data = await self._rpc(LOAD, key)
        if hashlib.sha256(data).hexdigest() != key:
            raise RelayError("blob integrity mismatch")
        return data

    async def stats(self) -> dict:
        return decode(await self._rpc(STATS))

    async def close(self):
        self._closed = True
        self._ready.set()
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(RelayError("connection closed"))
        tasks = [self._reader_task]
        if self._credit_task:
            tasks.append(self._credit_task)
        for task in tasks:
            task.cancel()
        self.writer.close()
        await asyncio.gather(*tasks, return_exceptions=True)
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


class Agent:
    """JSON adapter for arbitrary agents; the native router never parses JSON."""
    def __init__(self, client: "Endpoint"):
        if client.interceptor is not None:
            raise ValueError("one Agent adapter per client")
        self.client = client
        self._sequence = itertools.count(1)
        self._session = os.urandom(8).hex()
        self._requests = {}
        client.interceptor = self._intercept
        client.on_close.append(self._closed)
        if hasattr(client, "on_peer_close"):
            client.on_peer_close.append(self._peer_closed)

    def _peer_closed(self, name):
        for peer, future in tuple(self._requests.values()):
            if peer == name and not future.done():
                future.set_exception(RelayError("peer disconnected while waiting for reply"))

    def _closed(self):
        for _, future in tuple(self._requests.values()):
            if not future.done():
                future.set_exception(RelayError("connection closed while waiting for reply"))

    def _intercept(self, message: Message) -> bool:
        if len(message.data) > 65536:
            raise RelayError("Agent envelope exceeds 64 KiB; use blob references")
        value = decode(message.data)
        if not isinstance(value, dict) or not isinstance(value.get("payload"), dict) or not isinstance(value.get("id"), str) or value.get("kind") not in ("event", "task", "reply", "error"):
            raise RelayError("invalid Agent envelope")
        message.decoded = value
        # Always bind sender to the authenticated connection, never to body text.
        value["sender"] = message.sender
        if value.get("kind") in ("reply", "error"):
            entry = self._requests.get(value.get("correlation_id"))
            if entry and entry[0] == message.sender and not entry[1].done():
                if value["kind"] == "error":
                    entry[1].set_exception(RemoteError(f"{message.sender}: {value['payload'].get('type', 'error')}: {value['payload'].get('message', '')}"))
                else:
                    entry[1].set_result(value)
                return True
        return False

    async def send(self, to: str, payload: dict, *, kind="event", correlation_id=None, timeout=10.0, _id=None):
        if not isinstance(payload, dict) or kind not in ("event", "task", "reply", "error"):
            raise ValueError("payload must be an object and kind event/task/reply/error")
        ident = _id or f"{self._session}:{next(self._sequence)}"
        data = encode({"id": ident, "kind": kind, "correlation_id": correlation_id, "payload": payload})
        if len(data) > 65536:
            raise ValueError("Agent envelope exceeds 64 KiB; store large content and send its reference")
        await self.client.send(to, data, timeout=timeout)
        return ident

    async def receive(self, *, timeout=None) -> dict:
        message = await self.client.receive(timeout=timeout)
        if message.error:
            raise RelayError("received an invalid Agent envelope") from message.error
        return message.decoded

    async def reply(self, message: dict, payload: dict):
        return await self.send(message["sender"], payload, kind="reply", correlation_id=message["id"])

    async def request(self, to: str, payload: dict, *, timeout=30.0) -> dict:
        ident = f"{self._session}:{next(self._sequence)}"
        future = asyncio.get_running_loop().create_future()
        self._requests[ident] = (to, future)
        async def exchange():
            # The outer deadline covers send and reply; no nested send timer.
            await self.send(to, payload, kind="task", _id=ident, timeout=None)
            return await future
        try:
            return await deadline(exchange(), timeout)
        finally:
            self._requests.pop(ident, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def serve(self, handler, *, concurrency=1):
        """Wrap an async callable (payload, message) -> dict. No model SDK coupling.

        Task handler failures return correlated RemoteError replies. Transport
        failures and event-handler failures stop serve; supervise it externally.
        """
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency must be positive")
        async def worker():
            while True:
                message = await self.receive()
                try:
                    result = await handler(message["payload"], message)
                    if message.get("kind") == "task":
                        await self.reply(message, result)
                except Exception as exc:
                    if message.get("kind") == "task":
                        await self.send(message["sender"], {"type": type(exc).__name__, "message": str(exc)}, kind="error", correlation_id=message["id"])
                        continue
                    raise
        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
