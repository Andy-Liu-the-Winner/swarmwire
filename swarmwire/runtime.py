"""One small runtime API above every transport and framework adapter."""
import asyncio
import uuid
from .client import Agent, RelayError
from .fast_client import FastClient as Client
from .frameworks import result_dict
from .transports import MemoryBus


class NativeTransport:
    """Connect to an existing native broker. Application replies confirm completion.

    acknowledged=False chooses the streaming path: local write acceptance only.
    An asynchronous broker rejection closes the connection and fails pending work.
    Set acknowledged=True when every individual send needs a broker receipt.
    """
    def __init__(self, *, unix=None, port=8765, acknowledged=False, capacity=256, token=None):
        self.options = {"unix": unix, "port": port, "acknowledged": acknowledged, "capacity": capacity, "token": token}

    async def connect(self, name):
        return await Client.connect(name, **self.options)


class NatsTransport:
    def __init__(self, **options):
        self.options = options

    async def connect(self, name):
        from .backends import NatsEndpoint
        return await NatsEndpoint.connect(name, **self.options)


class Swarm:
    """Register async workers and exchange verified, correlated task results.

    All participants must share the transport's event loop or deployment scope.
    Cancellation stops the caller's wait; it does not terminate remote model work.
    """
    def __init__(self, transport=None):
        self.transport = transport or MemoryBus()
        self._owns_transport = transport is None
        self._endpoints = []
        self._workers = {}
        self._closed = False
        self.caller = None

    async def __aenter__(self):
        if self._closed or self.caller is not None:
            raise RelayError("a Swarm context can only be entered once")
        endpoint = await self.transport.connect("caller-" + uuid.uuid4().hex[:16])
        self._endpoints.append(endpoint)
        self.caller = Agent(endpoint)
        return self

    async def register(self, name, handler, *, concurrency=1):
        if not isinstance(concurrency, int) or concurrency < 1:
            raise ValueError("concurrency must be positive")
        if self._closed or self.caller is None:
            raise RelayError("enter the Swarm context before registering workers")
        if name in self._workers:
            raise RelayError("duplicate worker name")
        endpoint = await self.transport.connect(name)
        self._endpoints.append(endpoint)
        agent = Agent(endpoint)
        async def normalized(payload, message):
            return result_dict(await handler(payload, message))
        self._workers[name] = asyncio.create_task(agent.serve(normalized, concurrency=concurrency), name="worker:" + name)
        prepare = getattr(self.caller.client, 'preconnect', None)
        if prepare:
            await prepare(name)
        return agent

    async def request(self, to, payload, *, timeout=30.0):
        if self._closed or self.caller is None:
            raise RelayError("Swarm is not open")
        worker = self._workers.get(to)
        if worker and worker.done():
            if worker.cancelled():
                raise RelayError("worker was cancelled")
            raise RelayError("worker stopped") from worker.exception()
        return (await self.caller.request(to, payload, timeout=timeout))["payload"]

    async def explore(self, jobs, *, timeout=30.0):
        """Yield (worker_name, result) in completion order; independent jobs overlap."""
        async def run(name, payload):
            return name, await self.request(name, payload, timeout=timeout)
        tasks = [asyncio.create_task(run(name, payload)) for name, payload in jobs.items()]
        try:
            for ready in asyncio.as_completed(tasks):
                yield await ready
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        for task in self._workers.values():
            task.cancel()
        await asyncio.gather(*self._workers.values(), return_exceptions=True)
        await asyncio.gather(*(endpoint.close() for endpoint in self._endpoints))
        if self._owns_transport:
            await self.transport.close()

    async def __aexit__(self, *_):
        await self.close()
