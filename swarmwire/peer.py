"""Native discovery and single-use route tickets; direct socket data plane."""
import asyncio
import collections
import contextlib
import struct
import tempfile
from pathlib import Path
from .client import Message, RelayError, Backpressure, HELLO, ACK, DELIVERY, ERROR, CREDIT, MAX_FRAME
from .fast_client import FastClient
from .transports import BufferedEndpoint, validate_name
from .deadlines import deadline

REGISTER_ROUTE, OPEN_ROUTE, ACCEPT_ROUTE, DELIVERY_CREDIT = 10, 11, 12, 131


def frame(op, body=b'', name=''):
    encoded = name.encode()
    size = 7 + len(encoded) + len(body)
    if len(encoded) > 64 or size > MAX_FRAME:
        raise ValueError('peer frame exceeds limit')
    return struct.pack('!IBIH', size, op, 0, len(encoded)) + encoded + body


class PeerLink(asyncio.Protocol):
    """One receive slot per link; peers share the endpoint's bounded inbox."""
    def __init__(self, endpoint, *, target=None, ticket=None):
        self.endpoint, self.peer, self.ticket = endpoint, target, ticket
        self.outgoing = target is not None
        self.loop = asyncio.get_running_loop()
        self.ready, self.closed = self.loop.create_future(), self.loop.create_future()
        self.ready.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
        self.transport, self.buffer = None, bytearray()
        self.authenticating, self.active, self.failed = False, False, False
        self.reserved_slot, self.incoming_available = False, True
        self.out_window, self.claimed = 0, False
        self.waiters, self.paused, self.drain = collections.deque(), False, None
        self.pending_credit, self.credit_handle = 0, None
        self.validation = None
        self.handshake_timeout = None

    def connection_made(self, transport):
        self.transport = transport
        transport.set_write_buffer_limits(high=65536, low=16384)
        if self.endpoint._closed or self.endpoint._reserved >= self.endpoint.inbox.maxsize:
            self._reject(Backpressure('peer connection limit equals available inbox slots'))
            return
        self.endpoint._reserved += 1
        self.reserved_slot = True
        self.endpoint._links.add(self)
        self.handshake_timeout = self.loop.call_later(5, self._handshake_expired)
        if self.outgoing:
            transport.write(frame(HELLO, self.ticket.encode(), self.endpoint.name))

    def _reject(self, exc):
        self.failed = True
        if not self.ready.done():
            self.ready.set_exception(exc)
        if self.transport:
            with contextlib.suppress(Exception):
                self.transport.write(frame(ERROR, str(exc).encode()[:2048]))
            self.transport.close()

    def data_received(self, data):
        if self.failed:
            return
        self.buffer.extend(data)
        self._parse()

    def _parse(self):
        offset = 0
        try:
            while not self.authenticating and len(self.buffer) - offset >= 4:
                size = struct.unpack_from('!I', self.buffer, offset)[0]
                if not 7 <= size <= MAX_FRAME:
                    raise RelayError('invalid peer frame size')
                end = offset + 4 + size
                if len(self.buffer) < end:
                    break
                op, _, length = struct.unpack_from('!BIH', self.buffer, offset + 4)
                if length > 64 or 7 + length > size:
                    raise RelayError('invalid peer name length')
                start = offset + 11 + length
                name = self.buffer[offset + 11:start].decode()
                body = bytes(self.buffer[start:end])
                offset = end
                if op == ERROR:
                    raise RelayError(body.decode(errors='replace'))
                if not self.active:
                    if not self.outgoing and op == HELLO and not self.authenticating:
                        validate_name(name)
                        if len(body) != 32 or any(c not in b'0123456789abcdef' for c in body):
                            raise RelayError('invalid route ticket')
                        self.authenticating = True
                        self.transport.pause_reading()
                        self.validation = self.endpoint._task(self._validate(name, body.decode()))
                    elif self.outgoing and op == ACK and name == self.peer and body == b'\x01':
                        self.active, self.out_window = True, 1
                        self.handshake_timeout.cancel()
                        self.endpoint._routes.setdefault(self.peer, self)
                        self.ready.set_result(self)
                    else:
                        raise RelayError('peer handshake required')
                    continue
                if name:
                    raise RelayError('data-plane sender must come from the verified connection')
                if op == CREDIT:
                    if len(body) != 4:
                        raise RelayError('invalid peer credit')
                    self._grant(struct.unpack('!I', body)[0])
                elif op in (DELIVERY, DELIVERY_CREDIT):
                    if op == DELIVERY_CREDIT:
                        if len(body) < 4:
                            raise RelayError('invalid piggyback credit')
                        self._grant(struct.unpack_from('!I', body)[0])
                        body = body[4:]
                    if not self.incoming_available or len(body) > 1048576:
                        raise RelayError('peer exceeded its receive window')
                    self.incoming_available = False
                    try:
                        self.endpoint._dispatch(Message(self.peer, body, release=self.release_slot))
                    except Exception:
                        if not self.incoming_available:
                            self.release_slot()
                        raise
                else:
                    raise RelayError('invalid peer operation')
            if offset:
                del self.buffer[:offset]
        except Exception as exc:
            self._reject(exc)

    async def _validate(self, name, ticket):
        try:
            verified = (await self.endpoint.control._rpc(ACCEPT_ROUTE, ticket)).decode()
            if verified != name or self.failed:
                raise RelayError('route ticket does not match the sender')
            self.peer, self.active, self.out_window = verified, True, 1
            self.handshake_timeout.cancel()
            self.endpoint._routes.setdefault(self.peer, self)
            self.transport.write(frame(ACK, b'\x01', self.endpoint.name))
            if not self.ready.done():
                self.ready.set_result(self)
            self.authenticating = False
            self.transport.resume_reading()
            self._parse()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._reject(exc)

    def _handshake_expired(self):
        if not self.active and not self.failed:
            self._reject(RelayError('peer handshake timed out'))

    def _wake(self):
        if not self.out_window or self.claimed or self.failed:
            return
        while self.waiters:
            waiter = self.waiters.popleft()
            if not waiter.done():
                self.claimed = True
                waiter.set_result(None)
                break

    def _grant(self, count):
        if count != 1 or self.out_window + count > 1:
            raise RelayError('invalid receive-window grant')
        self.out_window += count
        self._wake()

    async def _claim(self):
        if self.failed:
            raise RelayError('peer connection closed')
        if self.out_window and not self.claimed and not self.waiters:
            self.claimed = True
            return
        waiter = self.loop.create_future()
        self.waiters.append(waiter)
        try:
            await waiter
        except BaseException:
            if waiter.done() and not waiter.cancelled() and waiter.exception() is None:
                self.claimed = False
                self._wake()
            raise
        finally:
            with contextlib.suppress(ValueError):
                self.waiters.remove(waiter)
            if waiter.done() and not waiter.cancelled():
                waiter.exception()

    async def _unpaused(self):
        while self.paused and not self.failed:
            if self.drain is None or self.drain.done():
                self.drain = self.loop.create_future()
            await asyncio.shield(self.drain)
        if self.failed:
            raise RelayError('peer connection closed')

    async def send(self, data):
        await self._claim()
        sent = False
        try:
            await self._unpaused()
            self._write_data(data)
            self.claimed, sent = False, True
            await self._unpaused()
        finally:
            if not sent:
                self.claimed = False
                self._wake()

    def _write_data(self, data):
        if self.pending_credit:
            self.pending_credit = 0
            if self.credit_handle:
                self.credit_handle.cancel()
                self.credit_handle = None
            packet = frame(DELIVERY_CREDIT, struct.pack('!I', 1) + data)
        else:
            packet = frame(DELIVERY, data)
        self.transport.write(packet)
        self.out_window -= 1

    def try_send(self, data):
        if self.failed or not self.active or not self.out_window or self.claimed or self.waiters or self.paused:
            return 0
        self._write_data(data)
        return 2 if self.paused else 1

    def release_slot(self):
        if self.incoming_available:
            raise RelayError('receive slot released twice')
        self.incoming_available = True
        if self.failed:
            self._release_reservation()
            return
        self.pending_credit = 1
        if self.credit_handle is None:
            self.credit_handle = self.loop.call_soon(self._flush_credit)

    def _flush_credit(self):
        self.credit_handle = None
        if self.pending_credit and not self.failed:
            self.pending_credit = 0
            self.transport.write(frame(CREDIT, struct.pack('!I', 1)))

    def pause_writing(self):
        self.paused = True

    def resume_writing(self):
        self.paused = False
        if self.drain and not self.drain.done():
            self.drain.set_result(None)

    def _release_reservation(self):
        if self.reserved_slot:
            self.endpoint._reserved -= 1
            self.reserved_slot = False

    def connection_lost(self, exc):
        self.failed = True
        if self.handshake_timeout:
            self.handshake_timeout.cancel()
        if self.credit_handle:
            self.credit_handle.cancel()
        if self.validation and not self.validation.done():
            self.validation.cancel()
        if not self.ready.done():
            self.ready.set_exception(RelayError('peer handshake closed'))
        for waiter in self.waiters:
            if not waiter.done():
                waiter.set_exception(RelayError('peer connection closed'))
        self.waiters.clear()
        if self.drain and not self.drain.done():
            self.drain.set_result(None)
        if self.incoming_available:
            self._release_reservation()
        self.endpoint._links.discard(self)
        if self.endpoint._routes.get(self.peer) is self:
            self.endpoint._routes.pop(self.peer)
        for callback in tuple(self.endpoint.on_peer_close):
            callback(self.peer)
        if not self.closed.done():
            self.closed.set_result(None)


class PeerEndpoint(BufferedEndpoint):
    """Bounded local peer endpoint. Native broker provides discovery/artifacts."""
    @classmethod
    async def connect(cls, name, *, port=8765, unix=None, network='unix', capacity=256, token=None, route_policy='direct', multiplex_threshold=16):
        if network not in ('unix', 'tcp'):
            raise ValueError('peer network must be unix or tcp')
        if capacity < 2 or route_policy not in ('direct', 'broker', 'auto') or multiplex_threshold < 2:
            raise ValueError('peer capacity >= 2, valid route policy, and threshold >= 2 required')
        endpoint = cls(name, capacity)
        endpoint.control = None
        endpoint.server, endpoint.temp = None, None
        endpoint._routes, endpoint._opening, endpoint._links, endpoint._tasks = {}, {}, set(), set()
        endpoint.on_peer_close = []
        endpoint._bytes, endpoint._sent = 0, 0
        endpoint._self_slot = asyncio.Semaphore(1) if capacity > 2 else None
        bulk_slots = max(1, capacity // 4)
        endpoint._reserved = bulk_slots + (1 if endpoint._self_slot else 0)
        endpoint.route_policy, endpoint.multiplex_threshold = route_policy, multiplex_threshold
        endpoint._multiplex_allowed = bool(unix or network == 'tcp')
        endpoint._last_path, endpoint._broker_until = {}, {}
        try:
            endpoint.control = await FastClient.connect(name, port=port, unix=unix, token=token, capacity=bulk_slots, acknowledged=False)
            endpoint.control.on_close.append(endpoint._control_lost)
            early = endpoint.control.inbox
            endpoint.control.inbox, endpoint.control._ready = endpoint.inbox, endpoint._ready
            endpoint.control.interceptor = endpoint._from_broker
            while not early.empty():
                message = early.get_nowait()
                if endpoint._from_broker(message):
                    endpoint.control._return_credit()
                else:
                    endpoint.inbox.put_nowait(message)
                    endpoint._ready.set()
            loop = asyncio.get_running_loop()
            if network == 'unix':
                endpoint.temp = tempfile.TemporaryDirectory(prefix='sw-peer-', dir='/tmp')
                path = str(Path(endpoint.temp.name) / 'data.sock')
                endpoint.server = await loop.create_unix_server(lambda: PeerLink(endpoint), path, backlog=capacity)
                address = 'unix:' + path
            else:
                endpoint.server = await loop.create_server(lambda: PeerLink(endpoint), '127.0.0.1', 0, backlog=capacity)
                address = 'tcp:127.0.0.1:' + str(endpoint.server.sockets[0].getsockname()[1])
            await endpoint.control._rpc(REGISTER_ROUTE, data=address.encode())
            return endpoint
        except BaseException:
            await endpoint.close()
            raise

    def _task(self, coroutine):
        task = asyncio.create_task(coroutine)
        self._tasks.add(task)
        def done(future):
            self._tasks.discard(future)
            if not future.cancelled():
                future.exception()
        task.add_done_callback(done)
        return task

    def _control_lost(self):
        if self._closed:
            return
        self._fail(RelayError('native discovery connection closed'))
        if self.server:
            self.server.close()
        for link in tuple(self._links):
            link.transport.abort()

    def _dispatch(self, message):
        if self._closed:
            raise RelayError('peer endpoint closed')
        self._remember_path(message.sender, 'peer')
        consumed = False
        if self.interceptor:
            try:
                consumed = self.interceptor(message)
            except Exception as exc:
                message.error = exc
        if consumed:
            message.release()
            return
        if self._bytes + len(message.data) > 64 * 1024 * 1024 or self.inbox.full():
            raise Backpressure('peer endpoint receive budget exhausted')
        self._bytes += len(message.data)
        self.inbox.put_nowait(message)
        self._ready.set()

    def _from_broker(self, message):
        self._remember_path(message.sender, 'broker')
        self._broker_until[message.sender] = asyncio.get_running_loop().time() + .1
        message.release = self.control._return_credit
        if self.interceptor:
            try:
                if self.interceptor(message):
                    return True  # FastClient itself returns this credit.
            except Exception as exc:
                message.error = exc
        if self._bytes + len(message.data) > 64 * 1024 * 1024:
            self.control._fail(Backpressure('peer endpoint receive budget exhausted'))
            self.control.transport.abort()
            return True
        self._bytes += len(message.data)
        return False  # FastClient enqueues into the shared bounded inbox.

    def _remember_path(self, name, path):
        if name not in self._last_path and len(self._last_path) >= 4096:
            oldest = next(iter(self._last_path))
            self._last_path.pop(oldest)
            self._broker_until.pop(oldest, None)
        self._last_path[name] = path

    async def _open(self, to):
        link = None
        try:
            response = await self.control._rpc(OPEN_ROUTE, to)
            address, ticket = response.decode().split('\n', 1)
            link = PeerLink(self, target=to, ticket=ticket)
            loop = asyncio.get_running_loop()
            if address.startswith('unix:/'):
                await loop.create_unix_connection(lambda: link, address[5:])
            elif address.startswith('tcp:127.0.0.1:'):
                await loop.create_connection(lambda: link, '127.0.0.1', int(address.rsplit(':', 1)[1]))
            else:
                raise RelayError('registry returned an unsupported local address')
            await deadline(asyncio.shield(link.ready), 5)
            return link
        except BaseException:
            if link and link.transport:
                link.transport.abort()
            raise
        finally:
            self._opening.pop(to, None)

    async def _send(self, to, data):
        if to == self.name:
            if self._self_slot is None:
                raise Backpressure('self-send needs capacity greater than one')
            await self._self_slot.acquire()
            try:
                if self._closed:
                    raise RelayError('peer endpoint closed')
                self._dispatch(Message(self.name, data, release=self._self_slot.release))
            except BaseException:
                self._self_slot.release()
                raise
            return
        link = self._routes.get(to)
        if link is None or not link.active or link.failed:
            if to not in self._opening:
                self._opening[to] = self._task(self._open(to))
            try:
                link = await asyncio.shield(self._opening[to])
            except asyncio.CancelledError:
                if self._closed:
                    raise RelayError('peer endpoint closed') from None
                raise
        await link.send(data)

    async def send(self, to, data, *, timeout=10.0):
        if self._closed:
            raise RelayError('peer endpoint closed')
        validate_name(to)
        if not isinstance(data, bytes) or len(data) > 1048576:
            raise ValueError('peer message must be bytes, at most 1 MiB')
        broker_reply = self._last_path.get(to) == 'broker' and (self.route_policy == 'direct' or to not in self._routes)
        multiplex = self.route_policy == 'auto' and self._multiplex_allowed and (len(self._routes) >= self.multiplex_threshold or self._broker_until.get(to, 0) > asyncio.get_running_loop().time())
        if to != self.name and (self.route_policy == 'broker' or multiplex or broker_reply):
            await self.control.send(to, data, timeout=timeout)
            self._sent += 1
            return
        link = self._routes.get(to)
        status = link.try_send(data) if link else 0
        if status:
            if status == 2:
                await deadline(link._unpaused(), timeout)
            self._sent += 1
            return
        await deadline(self._send(to, data), timeout)
        self._sent += 1

    async def receive(self, *, timeout=None):
        message = await super().receive(timeout=timeout)
        self._bytes -= len(message.data)
        message.release()
        return message

    async def preconnect(self, *names):
        if self.route_policy == 'auto' and self._multiplex_allowed:
            names = names[:max(0, self.multiplex_threshold - len(self._routes))]
        async def one(name):
            if name == self.name or name in self._routes:
                return
            if name not in self._opening:
                self._opening[name] = self._task(self._open(name))
            await asyncio.shield(self._opening[name])
        await asyncio.gather(*(one(name) for name in names))

    async def store(self, data):
        return await self.control.store(data)

    async def load(self, key):
        return await self.control.load(key)

    async def stats(self):
        return {**await self.control.stats(), 'backend': 'peer', 'route_policy': self.route_policy, 'peer_links': len(self._links), 'reserved_slots': self._reserved, 'receive_bytes': self._bytes, 'local_sends': self._sent}

    async def close(self):
        self._fail()
        if self._self_slot:
            self._self_slot.release()
        if self.server:
            self.server.close()
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        links = tuple(self._links)
        for link in links:
            link.transport.close()
        try:
            await deadline(asyncio.gather(*(asyncio.shield(link.closed) for link in links)), 5)
        except asyncio.TimeoutError:
            for link in links:
                link.transport.abort()
            await asyncio.gather(*(asyncio.shield(link.closed) for link in links))
        # Python 3.13 waits for accepted connections as well as the listener.
        if self.server:
            await self.server.wait_closed()
        if self.control:
            await self.control.close()
        if self.temp:
            self.temp.cleanup()


class PeerTransport:
    """Persistent peer mesh: native broker on control plane, direct data sockets.

    All workers in this transport must use PeerEndpoint. A receiver reserves one
    inbox slot per peer link (and one self slot when capacity > 1). Preconnect to
    keep discovery/authentication outside a latency-sensitive request.
    """
    def __init__(self, *, port=8765, unix=None, network='unix', capacity=256, token=None, route_policy='direct', multiplex_threshold=16):
        self.options = dict(port=port, unix=unix, network=network, capacity=capacity, token=token, route_policy=route_policy, multiplex_threshold=multiplex_threshold)

    async def connect(self, name):
        return await PeerEndpoint.connect(name, **self.options)


class AdaptiveTransport(PeerTransport):
    """Direct warm unicast; multiplex dense fan-out/fan-in through native broker.

    The default threshold is 16 established peer routes. Native receiver credits
    reserve a quarter of the inbox; peer connections reserve one slot each.
    Incoming broker traffic is mirrored and biases the reply route for 100 ms.
    This is a configurable heuristic, not a guarantee of optimal routing.
    """
    def __init__(self, **options):
        options.setdefault('route_policy', 'auto')
        super().__init__(**options)
