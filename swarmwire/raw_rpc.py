"""Correlated binary RPC, without JSON or receive-queue wakeups for replies."""
import asyncio
import itertools
import os
import struct
from .client import RelayError, RemoteError
from .deadlines import deadline

MAGIC = b'\x89SW\x03'
HEADER = struct.Struct('!4sBQ')
TASK, REPLY, FAILURE = 1, 2, 3


class RawRPC:
    def __init__(self, endpoint):
        if endpoint.interceptor is not None:
            raise ValueError('one Agent or RawRPC facade per endpoint')
        self.endpoint, self.pending = endpoint, {}
        self.session = int.from_bytes(os.urandom(4), 'big') << 32
        self.sequence = itertools.count(1)
        endpoint.interceptor = self._intercept
        endpoint.on_close.append(self._closed)
        if hasattr(endpoint, 'on_peer_close'):
            endpoint.on_peer_close.append(self._peer_closed)

    def _closed(self):
        for _, future in tuple(self.pending.values()):
            if not future.done():
                future.set_exception(RelayError('connection closed while waiting for binary reply'))

    def _peer_closed(self, name):
        for peer, future in tuple(self.pending.values()):
            if peer == name and not future.done():
                future.set_exception(RelayError('peer disconnected while waiting for binary reply'))

    def _intercept(self, message):
        if len(message.data) < HEADER.size:
            raise RelayError('invalid binary RPC envelope')
        magic, kind, ident = HEADER.unpack_from(message.data)
        if magic != MAGIC or kind not in (TASK, REPLY, FAILURE):
            raise RelayError('invalid binary RPC envelope')
        body = message.data[HEADER.size:]
        if kind in (REPLY, FAILURE):
            entry = self.pending.get(ident)
            if entry and entry[0] == message.sender and not entry[1].done():
                if kind == REPLY:
                    entry[1].set_result(body)
                else:
                    entry[1].set_exception(RemoteError(body.decode(errors='replace')))
            return True  # Late/duplicate or wrong-sender responses never become tasks.
        message.data = body
        message.decoded = {'kind': kind, 'request_id': ident}
        return False

    async def request(self, to, data, *, timeout=30.0):
        if not isinstance(data, bytes) or len(data) > 1048576 - HEADER.size:
            raise ValueError('binary RPC payload must be bytes and fit the 1 MiB wire limit')
        sequence = next(self.sequence)
        if sequence >= 2**32:
            raise RelayError('create a new endpoint before binary session IDs wrap')
        ident = self.session + sequence
        future = asyncio.get_running_loop().create_future()
        self.pending[ident] = (to, future)
        async def exchange():
            await self.endpoint.send(to, HEADER.pack(MAGIC, TASK, ident) + data, timeout=None)
            return await future
        try:
            return await deadline(exchange(), timeout)
        finally:
            self.pending.pop(ident, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                future.exception()

    async def receive(self):
        message = await self.endpoint.receive()
        if message.error:
            raise RelayError('invalid binary RPC message') from message.error
        return message

    async def reply(self, message, data, *, error=False):
        if not isinstance(data, bytes) or len(data) > 1048576 - HEADER.size:
            raise ValueError('binary reply must be bytes and fit the 1 MiB wire limit')
        await self.endpoint.send(message.sender, HEADER.pack(MAGIC, FAILURE if error else REPLY, message.decoded['request_id']) + data)

    async def serve(self, handler, *, concurrency=1):
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError('concurrency must be positive')
        async def worker():
            while True:
                message = await self.receive()
                if message.decoded['kind'] != TASK:
                    continue
                try:
                    result = await handler(message.data, message)
                    await self.reply(message, result)
                except Exception as exc:
                    await self.reply(message, str(exc).encode()[:2048], error=True)
        tasks = [asyncio.create_task(worker()) for _ in range(concurrency)]
        try:
            await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
