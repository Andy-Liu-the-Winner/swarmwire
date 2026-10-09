"""Direct event-callback client: no StreamReader task or per-message credit task."""
import asyncio
import itertools
import os
import socket
import struct
from .client import Client as StreamClient, Message, RelayError, Backpressure
from .client import HELLO, SEND, SEND_STREAM, CREDIT, ACK, DELIVERY, ERROR, MAX_FRAME
from .deadlines import deadline

SEND_STREAM_CREDIT, SEND_CREDIT = 8, 9


class WireProtocol(asyncio.Protocol):
    def __init__(self, client):
        self.client = client
        self.buffer = bytearray()

    def connection_made(self, transport):
        self.client.transport = transport
        transport.set_write_buffer_limits(high=65536, low=16384)

    def data_received(self, data):
        client = self.client
        if client._closed:
            return
        self.buffer.extend(data)
        offset = 0
        try:
            while len(self.buffer) - offset >= 4:
                size = struct.unpack_from('!I', self.buffer, offset)[0]
                if not 7 <= size <= MAX_FRAME:
                    raise RelayError('invalid frame length')
                end = offset + 4 + size
                if len(self.buffer) < end:
                    break
                op, ident, name_size = struct.unpack_from('!BIH', self.buffer, offset + 4)
                if 7 + name_size > size:
                    raise RelayError('invalid frame name length')
                body_start = offset + 11 + name_size
                payload = bytes(self.buffer[body_start:end])
                if op == DELIVERY:
                    sender = self.buffer[offset + 11:body_start].decode()
                    message = Message(sender, payload)
                    consumed = False
                    if client.interceptor:
                        try:
                            consumed = client.interceptor(message)
                        except Exception as exc:
                            message.error = exc
                    if consumed:
                        client._return_credit()
                    else:
                        client.inbox.put_nowait(message)
                        client._ready.set()
                elif op in (ACK, ERROR):
                    future = client._pending.get(ident)
                    if future and not future.done():
                        if op == ACK:
                            future.set_result(payload)
                        else:
                            text = payload.decode(errors='replace')
                            future.set_exception(Backpressure(text) if text.startswith('BACKPRESSURE:') else RelayError(text))
                    elif op == ERROR and not client.acknowledged:
                        raise RelayError('stream send rejected: ' + payload.decode(errors='replace'))
                else:
                    raise RelayError('unknown frame opcode')
                offset = end
            if offset:
                del self.buffer[:offset]
        except Exception as exc:
            client._fail(exc)
            client.transport.abort()

    def pause_writing(self):
        self.client._paused = True

    def resume_writing(self):
        client = self.client
        client._paused = False
        if client._drain_waiter and not client._drain_waiter.done():
            client._drain_waiter.set_result(None)

    def connection_lost(self, exc):
        client = self.client
        client._fail(exc)
        if not client._reader_task.done():
            client._reader_task.set_result(None)


class FastClient(StreamClient):
    """Same public endpoint API, using a direct protocol and piggybacked credits.

    Capability negotiation preserves compatibility with older native brokers.
    _reader_task is a closure Future for compatibility; no reader task is spawned.
    """
    def __init__(self, name, capacity, acknowledged):
        self.name, self.acknowledged = name, acknowledged
        self.loop = asyncio.get_running_loop()
        self.inbox = asyncio.Queue(capacity)
        self._ready = asyncio.Event()
        self.on_close, self.interceptor = [], None
        self._ids, self._pending = itertools.count(1), {}
        self._gate = asyncio.Semaphore(128)
        self._closed, self._failure = False, None
        self._credit, self._credit_handle = 0, None
        self._piggyback, self._paused = False, False
        self._drain_waiter = None
        self._reader_task = self.loop.create_future()
        self.transport = None

    @classmethod
    async def connect(cls, name, *, port=8765, unix=None, capacity=256, token=None, acknowledged=True):
        if not isinstance(capacity, int) or not 1 <= capacity <= 4096:
            raise ValueError('capacity must be 1..4096')
        client = cls(name, capacity, acknowledged)
        try:
            if unix:
                await client.loop.create_unix_connection(lambda: WireProtocol(client), unix)
            else:
                await client.loop.create_connection(lambda: WireProtocol(client), '127.0.0.1', port)
                client.transport.get_extra_info('socket').setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            capabilities = await client._rpc(HELLO, name, struct.pack('!I', capacity) + (token if token is not None else os.getenv('SWARMWIRE_TOKEN', '')).encode())
            client._piggyback = bool(capabilities and capabilities[0] & 1)
            return client
        except BaseException:
            if client.transport is not None:
                await client.close()
            raise

    def _fail(self, exc=None):
        if self._closed:
            return
        self._closed, self._failure = True, exc
        self._ready.set()
        if self._credit_handle:
            self._credit_handle.cancel()
            self._credit_handle = None
        if self._drain_waiter and not self._drain_waiter.done():
            self._drain_waiter.set_result(None)
        for future in tuple(self._pending.values()):
            if not future.done():
                future.set_exception(RelayError('connection closed'))
        for callback in tuple(self.on_close):
            callback()

    def _frame(self, op, ident, target, data):
        name = target.encode()
        credit = self._credit if self._piggyback and op in (SEND, SEND_STREAM) else 0
        length = 7 + len(name) + len(data) + (4 if credit else 0)
        if len(name) > 128 or length > MAX_FRAME:
            raise ValueError('frame exceeds limit')
        if credit:
            op = SEND_CREDIT if op == SEND else SEND_STREAM_CREDIT
            self._credit = 0
            if self._credit_handle:
                self._credit_handle.cancel()
                self._credit_handle = None
            extra = struct.pack('!I', credit)
        else:
            extra = b''
        return struct.pack('!IBIH', length, op, ident, len(name)) + name + extra + data

    async def _write(self, op, ident, target, data):
        while self._paused and not self._closed:
            if self._drain_waiter is None or self._drain_waiter.done():
                self._drain_waiter = self.loop.create_future()
            await asyncio.shield(self._drain_waiter)
        if self._closed:
            raise RelayError('connection closed') from self._failure
        self.transport.write(self._frame(op, ident, target, data))
        if self._paused:
            if self._drain_waiter is None or self._drain_waiter.done():
                self._drain_waiter = self.loop.create_future()
            await asyncio.shield(self._drain_waiter)
            if self._closed:
                raise RelayError('connection closed') from self._failure

    async def send(self, to, data, *, timeout=10.0):
        if not isinstance(data, bytes):
            raise TypeError('binary Client.send expects bytes; use Agent for JSON')
        if len(data) > 1048576:
            raise ValueError('raw message exceeds 1 MiB')
        if self.acknowledged:
            await self._rpc(SEND, to, data, timeout)
            return
        # Stream sends have no positive receipt; zero keeps negative receipts
        # distinct from management RPC IDs and avoids a stream counter wrap.
        ident = 0
        if self._closed:
            raise RelayError('connection closed') from self._failure
        if not self._paused:
            self.transport.write(self._frame(SEND_STREAM, ident, to, data))
            if not self._paused:
                return
            async def drain():
                while self._paused and not self._closed:
                    if self._drain_waiter is None or self._drain_waiter.done():
                        self._drain_waiter = self.loop.create_future()
                    await asyncio.shield(self._drain_waiter)
                if self._closed:
                    raise RelayError('connection closed') from self._failure
            await deadline(drain(), timeout)
        else:
            await deadline(self._write(SEND_STREAM, ident, to, data), timeout)

    def _return_credit(self):
        self._credit += 1
        if not self._closed and self._credit_handle is None:
            self._credit_handle = self.loop.call_soon(self._flush_credit)

    def _flush_credit(self):
        self._credit_handle = None
        if self._credit and not self._closed:
            count, self._credit = self._credit, 0
            self.transport.write(self._frame(CREDIT, 0, '', struct.pack('!I', count)))

    async def close(self):
        self._fail()
        if self.transport is None:
            return
        self.transport.close()
        try:
            await deadline(asyncio.shield(self._reader_task), 5)
        except asyncio.TimeoutError:
            self.transport.abort()
            await asyncio.shield(self._reader_task)
