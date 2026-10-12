"""Opt-in local transport with owned process lifecycle and startup-only fallback."""
import atexit
import asyncio
import json
import logging
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import uuid

from ._build import NativeBuildError, ensure_binary
from .client import RelayError
from .fast_client import FastClient
from .peer import AdaptiveTransport
from .transports import MemoryBus, validate_name


logger = logging.getLogger('swarmwire')
_brokers = set()


def _exit_cleanup():
    for broker in tuple(_brokers):
        broker.stop_sync()


atexit.register(_exit_cleanup)


class LocalBroker:
    """Private Unix broker, never a global daemon or a downloaded executable."""
    def __init__(self, *, binary=None, cache_dir=None, startup_timeout=10):
        if startup_timeout <= 0:
            raise ValueError('startup_timeout must be positive')
        self.binary, self.cache_dir = binary, cache_dir
        self.startup_timeout = startup_timeout
        self.process = None
        self.directory = None
        self.unix = None
        self.token = secrets.token_hex(32)
        self._stderr = None
        self._lifetime = None
        self._closed = False

    async def start(self):
        if self._closed:
            raise RelayError('local broker is closed')
        if self.process is not None:
            if self.process.poll() is not None:
                raise RelayError('local broker exited; create a new session')
            return self
        try:
            executable = await asyncio.to_thread(ensure_binary, binary=self.binary, cache_dir=self.cache_dir)
            self.directory = tempfile.TemporaryDirectory(prefix='sw-local-', dir='/tmp')
            self.unix = str(Path(self.directory.name) / 'relay.sock')
            self._stderr = tempfile.TemporaryFile()
            read_fd, self._lifetime = os.pipe()
            try:
                env = {key: value for key, value in os.environ.items()
                       if key in ('PATH', 'LANG', 'LD_LIBRARY_PATH', 'DYLD_LIBRARY_PATH', 'ASAN_OPTIONS', 'UBSAN_OPTIONS') or key.startswith('LC_')}
                env['SWARMWIRE_TOKEN'] = self.token
                self.process = subprocess.Popen([str(executable), '--unix', self.unix,
                                                 '--lifetime-fd', str(read_fd)],
                                                env=env, stdin=subprocess.DEVNULL,
                                                stdout=subprocess.PIPE, stderr=self._stderr,
                                                pass_fds=(read_fd,))
            finally:
                os.close(read_fd)
            _brokers.add(self)
            line = await asyncio.wait_for(asyncio.to_thread(self.process.stdout.readline), self.startup_timeout)
            if not line or json.loads(line) != {'ready': True}:
                raise RelayError('local broker did not announce readiness')
            probe = None
            try:
                probe = await asyncio.wait_for(FastClient.connect('health-' + uuid.uuid4().hex[:12],
                                               unix=self.unix, token=self.token), self.startup_timeout)
                await asyncio.wait_for(probe.stats(), self.startup_timeout)
            finally:
                if probe is not None:
                    await probe.close()
            logger.info('local broker started', extra={'swarmwire_event': 'broker_started', 'broker_pid': self.process.pid})
            return self
        except BaseException:
            await self.close()
            raise

    def stop_sync(self):
        if self._lifetime is not None:
            os.close(self._lifetime)
            self._lifetime = None
        process = self.process
        if process is not None:
            if process.poll() is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
            try:
                process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            if process.stdout:
                process.stdout.close()
        if self._stderr is not None:
            self._stderr.close()
            self._stderr = None
        if self.directory is not None:
            self.directory.cleanup()
            self.directory = None
        _brokers.discard(self)
        self._closed = True

    async def close(self):
        await asyncio.to_thread(self.stop_sync)


class ManagedLocalTransport:
    """Auto-build/start one local broker and use the existing adaptive data path.

    fallback=True selects MemoryBus only if startup fails before any registration.
    Once selected, a backend never changes and tasks are never replayed.
    """
    def __init__(self, *, binary=None, cache_dir=None, capacity=256, startup_timeout=10, fallback=False):
        if not isinstance(capacity, int) or not 2 <= capacity <= 4096:
            raise ValueError('capacity must be 2..4096')
        self.broker = LocalBroker(binary=binary, cache_dir=cache_dir, startup_timeout=startup_timeout)
        self.capacity = capacity
        self.fallback = fallback
        self.backend = None
        self._transport = None
        self._endpoints = set()
        self._lock = asyncio.Lock()
        self._closed = False
        self._error = None

    async def connect(self, name):
        validate_name(name)
        async with self._lock:
            if self._closed:
                raise RelayError('managed transport is closed')
            if self._error is not None:
                raise RelayError('managed transport startup failed') from self._error
            if self._transport is None:
                try:
                    await self.broker.start()
                    self._transport = AdaptiveTransport(unix=self.broker.unix, token=self.broker.token, capacity=self.capacity)
                    self.backend = 'local'
                except (NativeBuildError, OSError, RelayError, asyncio.TimeoutError, ValueError) as exc:
                    if not self.fallback:
                        self._error = exc
                        raise
                    # No endpoints have been admitted yet. Changing the transport
                    # here cannot replay a remote/model request or lose its reply.
                    self._transport = MemoryBus()
                    self.backend = 'memory'
                    logger.warning('local startup unavailable; using in-process memory transport',
                                   extra={'swarmwire_event': 'startup_fallback', 'reason_type': type(exc).__name__})
            if self.backend == 'local' and self.broker.process.poll() is not None:
                raise RelayError('local broker exited; tasks are not replayed; create a new session')
            endpoint = await self._transport.connect(name)
            self._endpoints.add(endpoint)
            return endpoint

    def status(self):
        process = self.broker.process
        return {'backend': self.backend, 'managed': True, 'closed': self._closed,
                'broker_pid': process.pid if process else None,
                'broker_alive': bool(process and process.poll() is None)}

    async def close(self):
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                await asyncio.gather(*(endpoint.close() for endpoint in self._endpoints), return_exceptions=True)
                self._endpoints.clear()
                if isinstance(self._transport, MemoryBus):
                    await self._transport.close()
            finally:
                await self.broker.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


def configured_transport(backend=None):
    """Choose a transport once, before workers run; environment is optional."""
    selected = backend if backend is not None else os.environ.get('SWARMWIRE_BACKEND', 'memory')
    if selected == 'memory':
        return MemoryBus(), selected
    if selected in ('local', 'auto'):
        return ManagedLocalTransport(fallback=selected == 'auto'), selected
    if selected == 'nats':
        from .runtime import NatsTransport
        url = os.environ.get('SWARMWIRE_NATS_URL')
        if not url:
            raise ValueError('Set SWARMWIRE_NATS_URL or pass an explicitly configured NatsTransport.')
        return NatsTransport(servers=url), selected
    if selected == 'external':
        from .runtime import NativeTransport
        unix = os.environ.get('SWARMWIRE_SOCKET')
        port = int(os.environ.get('SWARMWIRE_PORT', '8765'))
        if not 0 <= port <= 65535:
            raise ValueError('SWARMWIRE_PORT must be 0..65535')
        return NativeTransport(unix=unix, port=port), selected
    raise ValueError('backend must be memory, local, auto, nats, or external')
