"""Framework-independent endpoint contract and a direct, same-process fast path."""
import asyncio
import hashlib
import re
from typing import Callable, Optional, Protocol
from .client import Message, RelayError
from .deadlines import deadline


def validate_name(name):
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9_.-]{1,64}", name):
        raise ValueError("agent name must be 1..64 ASCII letters, digits, _, . or -")


class Endpoint(Protocol):
    name: str
    inbox: asyncio.Queue
    interceptor: Optional[Callable[[Message], bool]]
    on_close: list

    async def send(self, to: str, data: bytes, *, timeout=10.0): ...
    async def receive(self, *, timeout=None) -> Message: ...
    async def store(self, data: bytes) -> str: ...
    async def load(self, key: str) -> bytes: ...
    async def stats(self) -> dict: ...
    async def close(self): ...


class BufferedEndpoint:
    """Common hooks used by Agent, independent of networking or provider SDKs."""
    def __init__(self, name, capacity=256, artifacts=None):
        validate_name(name)
        if not isinstance(capacity, int) or not 1 <= capacity <= 4096:
            raise ValueError("capacity must be 1..4096")
        self.name = name
        self.inbox = asyncio.Queue(capacity)
        self.interceptor = None
        self.on_close = []
        self.artifacts = artifacts
        self._ready = asyncio.Event()
        self._closed = False
        self._failure = None

    def _fail(self, exc=None):
        if self._closed:
            return
        self._closed, self._failure = True, exc
        self._ready.set()
        for callback in tuple(self.on_close):
            callback()

    async def _deliver(self, message):
        if self._closed:
            raise RelayError("recipient closed")
        if self.interceptor:
            try:
                if self.interceptor(message):
                    return
            except Exception as exc:
                message.error = exc
        if self.inbox.full():
            # A closing receiver must also release a producer blocked on capacity.
            put = asyncio.create_task(self.inbox.put(message))
            closed = asyncio.create_task(self._wait_closed())
            try:
                done, _ = await asyncio.wait((put, closed), return_when=asyncio.FIRST_COMPLETED)
                if closed in done:
                    raise RelayError("recipient closed while sender waited")
            finally:
                for task in (put, closed):
                    if not task.done():
                        task.cancel()
                await asyncio.gather(put, closed, return_exceptions=True)
        else:
            self.inbox.put_nowait(message)
        self._ready.set()

    async def _wait_closed(self):
        # Closure has its own event, avoiding a busy loop on a non-empty inbox.
        event = asyncio.Event()
        self.on_close.append(event.set)
        try:
            if not self._closed:
                await event.wait()
        finally:
            self.on_close.remove(event.set)

    async def receive(self, *, timeout=None):
        loop = asyncio.get_running_loop()
        expires_at = None if timeout is None else loop.time() + timeout
        while self.inbox.empty():
            if self._closed:
                raise RelayError("endpoint closed") from self._failure
            if expires_at is None:
                await self._ready.wait()
            else:
                remaining = expires_at - loop.time()
                if remaining <= 0:
                    raise asyncio.TimeoutError()
                await deadline(self._ready.wait(), remaining)
        message = self.inbox.get_nowait()
        if self.inbox.empty():
            self._ready.clear()
        return message

    async def store(self, data):
        if self.artifacts is None:
            raise RelayError("configure a shared artifact provider for this backend")
        return await self.artifacts.store(data)

    async def load(self, key):
        if self.artifacts is None:
            raise RelayError("configure a shared artifact provider for this backend")
        return await self.artifacts.load(key)

    async def stats(self):
        return {"backend": type(self).__name__, "queued_messages": self.inbox.qsize()}

    async def close(self):
        self._fail()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


class MemoryArtifacts:
    """Bounded LRU shared by endpoints in one process; bytes are immutable."""
    def __init__(self, max_bytes=64 * 1024 * 1024):
        from collections import OrderedDict
        self.values = OrderedDict()
        self.size, self.max_bytes = 0, max_bytes

    async def store(self, data):
        if not isinstance(data, bytes) or len(data) > 1024 * 1024 or len(data) > self.max_bytes:
            raise ValueError("artifact must be bytes, at most 1 MiB and fit the store")
        key = hashlib.sha256(data).hexdigest()
        if key in self.values:
            self.values.move_to_end(key)
            return key
        while self.size + len(data) > self.max_bytes:
            _, old = self.values.popitem(last=False)
            self.size -= len(old)
        self.values[key] = data
        self.size += len(data)
        return key

    async def load(self, key):
        try:
            value = self.values[key]
        except KeyError as exc:
            raise RelayError("artifact missing or evicted") from exc
        self.values.move_to_end(key)
        return value


class MemoryBus:
    """Direct same-event-loop routing; no socket or serialized transport header."""
    def __init__(self):
        self.endpoints = {}
        self.artifacts = MemoryArtifacts()

    async def connect(self, name, *, capacity=256):
        if name in self.endpoints:
            raise RelayError("duplicate agent identity")
        endpoint = MemoryEndpoint(self, name, capacity)
        self.endpoints[name] = endpoint
        return endpoint

    async def close(self):
        await asyncio.gather(*(endpoint.close() for endpoint in tuple(self.endpoints.values())))


class MemoryEndpoint(BufferedEndpoint):
    def __init__(self, bus, name, capacity):
        super().__init__(name, capacity, bus.artifacts)
        self.bus = bus

    async def send(self, to, data, *, timeout=10.0):
        if self._closed:
            raise RelayError("endpoint closed")
        if not isinstance(data, bytes) or len(data) > 1024 * 1024:
            raise ValueError("message must be bytes, at most 1 MiB")
        try:
            target = self.bus.endpoints[to]
        except KeyError as exc:
            raise RelayError("unknown recipient") from exc
        message = Message(self.name, data)
        if not target.inbox.full():
            await target._deliver(message)
        else:
            await deadline(target._deliver(message), timeout)

    async def close(self):
        self._fail()
        if self.bus.endpoints.get(self.name) is self:
            self.bus.endpoints.pop(self.name)
