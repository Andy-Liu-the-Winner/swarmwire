import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from swarmwire import ManagedLocalTransport, MemoryBus, RelayError, RemoteError, Swarm
from swarmwire._build import NativeBuildError, ensure_binary
from swarmwire.managed import LocalBroker, configured_transport

ROOT = Path(__file__).resolve().parents[1]
BINARY = os.getenv('SWARMWIRE_BINARY', str(ROOT / 'bin' / 'swarmwired'))


async def echo(payload, message):
    if payload.get('fail'):
        raise ValueError('expected handler failure')
    return payload


class ManagedTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {'SWARMWIRE_BINARY': BINARY, 'SWARMWIRE_BACKEND': 'memory'})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    async def test_local_roundtrip_stats_and_cleanup(self):
        swarm = Swarm(backend='local')
        async with swarm:
            await swarm.register('worker', echo, concurrency=4)
            replies = await asyncio.gather(*(swarm.request('worker', {'n': n}) for n in range(50)))
            self.assertEqual(replies, [{'n': n} for n in range(50)])
            with self.assertRaises(RemoteError):
                await swarm.request('worker', {'fail': True})
            self.assertEqual(await swarm.request('worker', {'ok': True}), {'ok': True})
            status = await swarm.stats()
            self.assertEqual(status['backend'], 'local')
            self.assertTrue(status['broker_alive'])
            self.assertEqual(status['pending_requests'], 0)
            broker = swarm.transport.broker
            directory = broker.directory.name
            self.assertNotIn(broker.token, json.dumps(status))
        self.assertEqual(broker.process.poll(), 0)
        self.assertFalse(Path(directory).exists())
        await swarm.close()

    async def test_concurrent_registration_starts_one_broker(self):
        async with ManagedLocalTransport() as transport:
            endpoints = await asyncio.gather(*(transport.connect('worker-' + str(n)) for n in range(12)))
            self.assertEqual(len({endpoint.control.transport.get_extra_info('peername') for endpoint in endpoints}), 1)
            self.assertTrue(transport.status()['broker_alive'])
        self.assertEqual(transport.broker.process.poll(), 0)

    async def test_isolated_sessions_and_existing_socket_untouched(self):
        a, b = Swarm(backend='local'), Swarm(backend='local')
        async with a, b:
            await a.register('same-name', echo)
            await b.register('same-name', echo)
            self.assertNotEqual(a.transport.broker.unix, b.transport.broker.unix)
            self.assertEqual(await a.request('same-name', {'session': 'a'}), {'session': 'a'})
            self.assertEqual(await b.request('same-name', {'session': 'b'}), {'session': 'b'})

    async def test_environment_switch_and_explicit_override(self):
        with patch.dict(os.environ, {'SWARMWIRE_BACKEND': 'local'}):
            async with Swarm() as swarm:
                self.assertEqual(swarm.status()['backend'], 'local')
            async with Swarm(backend='memory') as swarm:
                self.assertIsInstance(swarm.transport, MemoryBus)

    async def test_startup_only_fallback_without_replay(self):
        calls = 0
        async def counted(payload, message):
            nonlocal calls
            calls += 1
            return payload
        with patch.dict(os.environ, {'SWARMWIRE_BINARY': '/missing-sw-binary'}):
            with self.assertLogs('swarmwire', level='WARNING'):
                async with Swarm(backend='auto') as swarm:
                    await swarm.register('worker', counted)
                    self.assertEqual(swarm.status()['backend'], 'memory')
                    self.assertEqual(await swarm.request('worker', {'ok': True}), {'ok': True})
            with self.assertRaises(NativeBuildError):
                async with Swarm(backend='local'):
                    pass
        self.assertEqual(calls, 1)

    async def test_exception_in_application_stops_owned_broker(self):
        swarm = Swarm(backend='local')
        with self.assertRaisesRegex(ValueError, 'application failed'):
            async with swarm:
                raise ValueError('application failed')
        self.assertEqual(swarm.transport.broker.process.poll(), 0)

    async def test_killed_broker_fails_pending_request_and_cleans_peers(self):
        entered, release = asyncio.Event(), asyncio.Event()
        async def blocked(payload, message):
            entered.set()
            await release.wait()
            return payload
        swarm = Swarm(backend='local')
        async with swarm:
            await swarm.register('worker', blocked)
            pending = asyncio.create_task(swarm.request('worker', {}))
            await asyncio.wait_for(entered.wait(), 2)
            swarm.transport.broker.process.kill()
            await asyncio.to_thread(swarm.transport.broker.process.wait)
            with self.assertRaises(RelayError):
                await asyncio.wait_for(pending, 2)
            with self.assertRaises(RelayError):
                await swarm.register('another', echo)
        self.assertTrue(all(endpoint._cleanup_task.done() for endpoint in swarm._endpoints))
        self.assertFalse(swarm.transport.broker.directory)

    async def test_malformed_readiness_and_timeout_leave_no_process(self):
        for output in ('print("not-json", flush=True)', 'pass'):
            with tempfile.TemporaryDirectory(dir='/tmp') as directory:
                stub = Path(directory) / 'stub'
                stub.write_text('#!' + sys.executable + '\nimport time\n' + output + '\ntime.sleep(20)\n')
                stub.chmod(0o700)
                broker = LocalBroker(binary=stub, startup_timeout=.05)
                with self.assertRaises((ValueError, asyncio.TimeoutError)):
                    await broker.start()
                self.assertIsNotNone(broker.process.poll())
                self.assertIsNone(broker.directory)

    async def test_cancelled_startup_cleans_child(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as directory:
            stub = Path(directory) / 'stub'
            stub.write_text('#!' + sys.executable + '\nimport time\ntime.sleep(20)\n')
            stub.chmod(0o700)
            broker = LocalBroker(binary=stub)
            starting = asyncio.create_task(broker.start())
            while broker.process is None:
                await asyncio.sleep(.001)
            starting.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await starting
            self.assertIsNotNone(broker.process.poll())
            self.assertIsNone(broker.directory)

    async def test_explicit_transport_ownership_preserved(self):
        async with ManagedLocalTransport() as transport:
            async with Swarm(transport) as first:
                await first.register('worker', echo)
                await first.request('worker', {})
            self.assertTrue(transport.status()['broker_alive'])
            async with Swarm(transport) as second:
                await second.register('worker', echo)
                self.assertEqual(await second.request('worker', {'ok': True}), {'ok': True})

    def test_invalid_configuration_does_not_start_anything(self):
        for backend in ('unknown', ''):
            with self.assertRaises(ValueError):
                Swarm(backend=backend)
        with self.assertRaises(ValueError):
            Swarm(MemoryBus(), backend='local')
        with patch.dict(os.environ, {'SWARMWIRE_NATS_URL': ''}):
            with self.assertRaises(ValueError):
                configured_transport('nats')


class BuildTests(unittest.TestCase):
    def test_cached_native_build_is_reused_and_runs(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as cache:
            with patch.dict(os.environ, {'SWARMWIRE_BINARY': ''}):
                first = ensure_binary(cache_dir=cache)
                stamp = first.stat().st_mtime_ns
                second = ensure_binary(cache_dir=cache)
            self.assertEqual(first, second)
            self.assertEqual(stamp, second.stat().st_mtime_ns)
            result = subprocess.run([str(first), '--invalid'], capture_output=True)
            self.assertEqual(result.returncode, 1)
            self.assertIn(b'usage:', result.stderr)

    def test_missing_compiler_and_unsafe_cache_are_clear_errors(self):
        with tempfile.TemporaryDirectory(dir='/tmp') as cache:
            with patch.dict(os.environ, {'SWARMWIRE_BINARY': '', 'CXX': '/missing-cxx'}):
                with self.assertRaisesRegex(NativeBuildError, 'compiler'):
                    ensure_binary(cache_dir=cache)
            Path(cache).chmod(0o755)
            with patch.dict(os.environ, {'SWARMWIRE_BINARY': ''}):
                with self.assertRaisesRegex(NativeBuildError, 'private'):
                    ensure_binary(cache_dir=cache)


class ProcessTests(unittest.IsolatedAsyncioTestCase):
    async def test_launcher_connects_workers_in_separate_processes(self):
        worker = '''import asyncio
from swarmwire import Swarm
async def echo(payload, message): return payload
async def main():
    async with Swarm() as swarm:
        await swarm.register('child-worker', echo)
        print('ready', flush=True)
        await asyncio.Event().wait()
asyncio.run(main())
'''
        coordinator = '''import asyncio, sys
from swarmwire import Swarm
async def main():
    child = await asyncio.create_subprocess_exec(sys.executable, '-c', WORKER, stdout=asyncio.subprocess.PIPE)
    try:
        assert await child.stdout.readline() == b'ready\\n'
        async with Swarm() as swarm:
            print(await swarm.request('child-worker', {'separate_process': True}), flush=True)
    finally:
        child.terminate()
        await child.wait()
asyncio.run(main())
'''.replace('WORKER', repr(worker))
        process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'swarmwire', 'run', '--',
                    sys.executable, '-c', coordinator, env={**os.environ, 'SWARMWIRE_BINARY': BINARY},
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(process.returncode, 0, stderr.decode())
        self.assertIn(b"'separate_process': True", stdout)

    async def test_parent_crash_stops_broker_and_unlinks_socket(self):
        script = ('import asyncio,json,os; from swarmwire.managed import LocalBroker; '
                  'b=LocalBroker(); asyncio.run(b.start()); '
                  'print(json.dumps({"socket":b.unix,"pid":b.process.pid}),flush=True); os._exit(0)')
        env = {**os.environ, 'SWARMWIRE_BINARY': BINARY}
        process = await asyncio.create_subprocess_exec(sys.executable, '-c', script, env=env,
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(process.returncode, 0, stderr.decode())
        info = json.loads(stdout)
        for _ in range(100):
            if not Path(info['socket']).exists():
                break
            await asyncio.sleep(.01)
        self.assertFalse(Path(info['socket']).exists())
        # Abrupt parent exit can leave its private, now-empty temporary directory.
        Path(info['socket']).parent.rmdir()

    async def test_launcher_shares_configuration_and_preserves_exit_code(self):
        script = ('import asyncio,json; from swarmwire import Swarm; '
                  'exec("async def main():\\n async with Swarm() as s:\\n  print(json.dumps(s.status()))"); '
                  'asyncio.run(main()); raise SystemExit(7)')
        process = await asyncio.create_subprocess_exec(sys.executable, '-m', 'swarmwire', 'run', '--',
                    sys.executable, '-c', script, env={**os.environ, 'SWARMWIRE_BINARY': BINARY},
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, stderr = await asyncio.wait_for(process.communicate(), 5)
        self.assertEqual(process.returncode, 7, stderr.decode())
        self.assertEqual(json.loads(stdout)['configured_backend'], 'external')


if __name__ == '__main__':
    unittest.main()
