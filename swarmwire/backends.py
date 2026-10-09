"""Optional existing-product backends. Imports remain lazy."""
import asyncio
from pathlib import Path
from .client import Message, RelayError
from .transports import BufferedEndpoint, validate_name
from .deadlines import deadline


class ZeroMQMesh:
    """Persistent brokerless PUSH/PULL mesh, using ipc or inproc addresses.

    Names and discovery are scoped to this registry. IPC supports local processes
    sharing its directory. This is ephemeral trusted-peer messaging, not auth.
    """
    def __init__(self, directory=None, *, inproc=False, tcp=False, artifacts=None):
        import zmq.asyncio
        import uuid
        self.context = zmq.asyncio.Context()
        self.directory = Path(directory) if directory else None
        if inproc and tcp:
            raise ValueError("choose inproc or TCP")
        if not inproc and not tcp and self.directory is None:
            raise ValueError("IPC mesh requires a dedicated directory")
        if self.directory:
            self.directory.mkdir(parents=True, exist_ok=True)
        self.inproc, self.tcp = inproc, tcp
        self.addresses = {}
        self.namespace = uuid.uuid4().hex
        self.endpoints = {}
        self.artifacts = artifacts

    def address(self, name):
        validate_name(name)
        if self.tcp:
            try:
                return self.addresses[name]
            except KeyError as exc:
                raise RelayError("unknown TCP mesh recipient") from exc
        if self.inproc:
            return f"inproc://{self.namespace}/{name}"
        path = str(self.directory / (name + '.sock'))
        if len(path.encode()) > 100:
            raise ValueError("Unix socket path is too long; choose a shorter mesh directory")
        return f"ipc://{path}"

    async def connect(self, name, *, capacity=256):
        if name in self.endpoints:
            raise RelayError("duplicate agent identity")
        endpoint = ZeroMQEndpoint(self, name, capacity)
        self.endpoints[name] = endpoint
        return endpoint

    async def close(self):
        await asyncio.gather(*(e.close() for e in tuple(self.endpoints.values())))
        self.context.destroy(linger=0)


class ZeroMQEndpoint(BufferedEndpoint):
    def __init__(self, mesh, name, capacity):
        import zmq
        super().__init__(name, capacity, mesh.artifacts)
        self.mesh, self.outputs = mesh, {}
        address = "tcp://127.0.0.1:*" if mesh.tcp else mesh.address(name)
        self.socket = mesh.context.socket(zmq.PULL)
        self.socket.setsockopt(zmq.RCVHWM, capacity)
        self.socket.setsockopt(zmq.LINGER, 0)
        try:
            self.socket.bind(address)
            if mesh.tcp:
                mesh.addresses[name] = self.socket.getsockopt(zmq.LAST_ENDPOINT).decode()
        except BaseException:
            self.socket.close(0)
            raise
        self._reader_task = asyncio.create_task(self._read())

    async def _read(self):
        try:
            while True:
                sender, data = await self.socket.recv_multipart()
                await self._deliver(Message(sender.decode(), data))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._fail(exc)

    async def send(self, to, data, *, timeout=10.0):
        import zmq
        if self._closed:
            raise RelayError("endpoint closed")
        if not isinstance(data, bytes) or len(data) > 1024 * 1024:
            raise ValueError("message must be bytes, at most 1 MiB")
        validate_name(to)
        if to not in self.outputs:
            sock = self.mesh.context.socket(zmq.PUSH)
            sock.setsockopt(zmq.SNDHWM, self.inbox.maxsize)
            sock.setsockopt(zmq.IMMEDIATE, 1)
            sock.setsockopt(zmq.LINGER, 0)
            sock.connect(self.mesh.address(to))
            self.outputs[to] = sock
        future = self.outputs[to].send_multipart([self.name.encode(), data])
        if future.done():
            await future
        else:
            await deadline(future, timeout)

    async def close(self):
        if self._closed and self._reader_task.done():
            return
        self._fail()
        self._reader_task.cancel()
        await asyncio.gather(self._reader_task, return_exceptions=True)
        self.socket.close(0)
        for sock in self.outputs.values():
            sock.close(0)
        if self.mesh.endpoints.get(self.name) is self:
            self.mesh.endpoints.pop(self.name)
            self.mesh.addresses.pop(self.name, None)


class NatsEndpoint(BufferedEndpoint):
    """Core NATS endpoint; TLS/auth/reconnect options pass to nats.connect.

    The application must choose unique agent IDs. Core NATS is ephemeral;
    these methods do not enable JetStream or claim durable delivery.
    """
    @classmethod
    async def connect(cls, name, *, servers="nats://127.0.0.1:4222", namespace="swarmwire", capacity=256, artifacts=None, **options):
        import nats
        validate_name(namespace)
        endpoint = cls(name, capacity, artifacts)
        endpoint.namespace = namespace
        async def error(exc):
            endpoint.errors.append(exc)
        async def closed():
            endpoint._fail()
        endpoint.errors = []
        endpoint.connection = await nats.connect(servers=servers, error_cb=error, closed_cb=closed, **options)
        async def receive(message):
            size = message.data[0]
            if 1 + size > len(message.data):
                raise RelayError("invalid NATS sender envelope")
            await endpoint._deliver(Message(message.data[1:1 + size].decode(), message.data[1 + size:]))
        endpoint.subscription = await endpoint.connection.subscribe(endpoint._subject(name), cb=receive, pending_msgs_limit=capacity, pending_bytes_limit=4 * 1024 * 1024)
        await endpoint.connection.flush()
        return endpoint

    def _subject(self, name):
        validate_name(name)
        # Agent dots remain literal portions of the subject, not wildcards.
        return f"{self.namespace}.agent.{name}"

    async def send(self, to, data, *, timeout=10.0):
        if self._closed:
            raise RelayError("endpoint closed")
        if not isinstance(data, bytes) or len(data) > 1024 * 1024:
            raise ValueError("message must be bytes, at most 1 MiB")
        sender = self.name.encode()
        await self.connection.publish(self._subject(to), bytes([len(sender)]) + sender + data)

    async def stats(self):
        return {**await super().stats(), "nats": self.connection.stats, "errors": [str(e) for e in self.errors]}

    async def close(self):
        self._fail()
        if not self.connection.is_closed:
            await self.connection.close()
