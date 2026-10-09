import asyncio
import json
import os
import struct
import subprocess
import sys
import unittest
from pathlib import Path
from swarmwire import Agent, Client, PeerEndpoint, PeerTransport, RelayError, RemoteError, Swarm, RawRPC
from swarmwire.peer import REGISTER_ROUTE, OPEN_ROUTE, ACCEPT_ROUTE, frame
from swarmwire.client import HELLO

ROOT = Path(__file__).resolve().parents[1]


class PeerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        binary = os.getenv('SWARMWIRE_BINARY', str(ROOT / 'bin/swarmwired'))
        self.broker = subprocess.Popen([binary, '--port', '0'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.port = json.loads(await asyncio.to_thread(self.broker.stdout.readline))['port']
        self.endpoints = []

    async def connect(self, name, **kwargs):
        endpoint = await PeerEndpoint.connect(name, port=self.port, **kwargs)
        self.endpoints.append(endpoint)
        return endpoint

    async def asyncTearDown(self):
        await asyncio.gather(*(endpoint.close() for endpoint in self.endpoints))
        self.broker.terminate()
        await asyncio.wait_for(asyncio.to_thread(self.broker.wait), 5)
        stderr = self.broker.stderr.read().decode()
        self.broker.stdout.close()
        self.broker.stderr.close()
        self.assertEqual(self.broker.returncode, 0, stderr)
        self.assertNotIn('AddressSanitizer', stderr)

    async def test_unix_and_tcp_framework_swarm(self):
        async def handler(payload, message):
            if payload.get('bad'):
                raise ValueError('bad exploration')
            return payload
        for network in ('unix', 'tcp'):
            async with Swarm(PeerTransport(port=self.port, network=network)) as swarm:
                await swarm.register('worker', handler, concurrency=4)
                replies = await asyncio.gather(*(swarm.request('worker', {'n': i}) for i in range(50)))
                self.assertEqual([r['n'] for r in replies], list(range(50)))
                with self.assertRaises(RemoteError):
                    await swarm.request('worker', {'bad': True})
                self.assertEqual(await swarm.request('worker', {'ok': True}), {'ok': True})
                self.assertEqual((await swarm.caller.client.stats())['routed'], 0)

    async def test_receive_windows_order_empty_and_large_messages(self):
        a, b = await self.connect('a'), await self.connect('b')
        async def producer():
            for payload in (b'', b'\x00\xff', b'x' * 1048576):
                await a.send('b', payload)
        sending = asyncio.create_task(producer())
        received = [await b.receive(timeout=3) for _ in range(3)]
        await sending
        self.assertEqual([m.data for m in received], [b'', b'\x00\xff', b'x' * 1048576])
        self.assertEqual(b._bytes, 0)

    async def test_disconnect_fails_only_matching_agent_requests(self):
        a = Agent(await self.connect('a'))
        b = await self.connect('b')
        pending = asyncio.create_task(a.request('b', {}, timeout=5))
        await b.receive(timeout=2)
        await b.close()
        with self.assertRaises(RelayError):
            await asyncio.wait_for(pending, 1)
        self.assertFalse(a._requests)

    async def test_route_ticket_is_destination_bound_and_single_use(self):
        a, b, c = await self.connect('a'), await self.connect('b'), await self.connect('c')
        response = (await a.control._rpc(OPEN_ROUTE, 'b')).decode()
        _, ticket = response.split('\n')
        with self.assertRaises(RelayError):
            await c.control._rpc(ACCEPT_ROUTE, ticket)
        self.assertEqual(await b.control._rpc(ACCEPT_ROUTE, ticket), b'a')
        with self.assertRaises(RelayError):
            await b.control._rpc(ACCEPT_ROUTE, ticket)

    async def test_spoofed_handshake_cannot_claim_an_existing_sender(self):
        a, b = await self.connect('a'), await self.connect('b', network='tcp')
        address, ticket = (await a.control._rpc(OPEN_ROUTE, 'b')).decode().split('\n')
        reader, writer = await asyncio.open_connection('127.0.0.1', int(address.rsplit(':',1)[1]))
        writer.write(frame(HELLO, ticket.encode(), 'forged'))
        await writer.drain()
        await asyncio.wait_for(reader.read(), 2)
        writer.close()
        await writer.wait_closed()
        await a.send('b', b'real')
        message = await b.receive(timeout=2)
        self.assertEqual((message.sender, message.data), ('a', b'real'))

    async def test_self_request_and_cancellation_preserve_credit(self):
        endpoint = await self.connect('self')
        agent = Agent(endpoint)
        async def handler(payload, message):
            return payload
        task = asyncio.create_task(agent.serve(handler))
        try:
            self.assertEqual((await agent.request('self', {'ok': True}))['payload'], {'ok': True})
            a, b = await self.connect('a', capacity=2), await self.connect('b', capacity=2)
            await a.send('b', b'first')
            pending = asyncio.create_task(a.send('b', b'cancelled'))
            await asyncio.sleep(.01)
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
            self.assertEqual((await b.receive(timeout=1)).data, b'first')
            await a.send('b', b'after-cancel')
            self.assertEqual((await b.receive(timeout=1)).data, b'after-cancel')
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_artifact_storage_and_unknown_peer(self):
        a, b = await self.connect('a'), await self.connect('b')
        key = await a.store(b'artifact')
        self.assertEqual(await b.load(key), b'artifact')
        with self.assertRaises(RelayError):
            await a.send('missing', b'hello')

    async def test_adaptive_dense_routes_use_native_multiplexing(self):
        a = await self.connect('a', route_policy='auto', multiplex_threshold=2, network='tcp')
        b, c = await self.connect('b'), await self.connect('c')
        await a.preconnect('b', 'c')
        before = (await a.stats())['routed']
        await a.send('b', b'batch-b')
        await a.send('c', b'batch-c')
        self.assertEqual((await b.receive(timeout=2)).data, b'batch-b')
        self.assertEqual((await c.receive(timeout=2)).data, b'batch-c')
        self.assertEqual((await a.stats())['routed'] - before, 2)
        self.assertLessEqual(a._reserved, a.inbox.maxsize)

    async def test_peer_worker_interoperates_with_routed_client(self):
        worker = Agent(await self.connect('worker'))
        source = await Client.connect('legacy-source', port=self.port, acknowledged=False)
        async def echo(payload, message):
            return payload
        serving = asyncio.create_task(worker.serve(echo))
        try:
            response = await Agent(source).request('worker', {'mixed': True})
            self.assertEqual(response['payload'], {'mixed': True})
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)
            await source.close()

    async def test_failed_discovery_can_succeed_after_worker_joins(self):
        a = await self.connect('a')
        with self.assertRaises(RelayError):
            await a.send('joining', b'early')
        self.assertNotIn('joining', a._opening)
        joining = await self.connect('joining')
        await a.send('joining', b'later')
        self.assertEqual((await joining.receive(timeout=2)).data, b'later')

    async def test_binary_rpc_parallel_errors_and_timeout_cleanup(self):
        caller = RawRPC(await self.connect('caller', route_policy='auto'))
        worker = RawRPC(await self.connect('worker'))
        waiting = asyncio.Event()
        async def echo(data, message):
            if data == b'error':
                raise ValueError('bad binary task')
            if data == b'wait':
                await waiting.wait()
            return data
        serving = asyncio.create_task(worker.serve(echo, concurrency=4))
        try:
            replies = await asyncio.gather(*(caller.request('worker', struct.pack('!Q', i)) for i in range(100)))
            self.assertEqual(replies, [struct.pack('!Q', i) for i in range(100)])
            with self.assertRaises(RemoteError):
                await caller.request('worker', b'error')
            with self.assertRaises(asyncio.TimeoutError):
                await caller.request('worker', b'wait', timeout=.01)
            self.assertFalse(caller.pending)
            waiting.set()
            self.assertEqual(await caller.request('worker', b'after-timeout'), b'after-timeout')
            await asyncio.sleep(.01)
            self.assertTrue(caller.endpoint.inbox.empty())
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)

    async def test_binary_rpc_routed_frames_and_maximum_payload(self):
        source = await Client.connect('source', port=self.port, acknowledged=False)
        target = await Client.connect('target', port=self.port, acknowledged=False)
        caller, worker = RawRPC(source), RawRPC(target)
        async def echo(data, message):
            return data
        serving = asyncio.create_task(worker.serve(echo))
        try:
            self.assertEqual(await caller.request('target', b''), b'')
            data = b'x' * (1048576 - 13)
            self.assertEqual(await caller.request('target', data), data)
            with self.assertRaises(ValueError):
                await caller.request('target', data + b'x')
        finally:
            serving.cancel()
            await asyncio.gather(serving, return_exceptions=True)
            await asyncio.gather(source.close(), target.close())

    async def test_separate_process_peer_worker(self):
        caller = Agent(await self.connect('caller'))
        code = '''import asyncio,sys
from swarmwire import Agent,PeerEndpoint
async def main():
 endpoint=await PeerEndpoint.connect('process-worker',port=int(sys.argv[1]))
 agent=Agent(endpoint)
 print('ready',flush=True)
 message=await agent.receive()
 await agent.reply(message,{'answer':42})
 await endpoint.close()
asyncio.run(main())
'''
        worker = await asyncio.create_subprocess_exec(sys.executable, '-c', code, str(self.port), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            self.assertEqual(await asyncio.wait_for(worker.stdout.readline(), 5), b'ready\n')
            self.assertEqual((await caller.request('process-worker', {}))['payload'], {'answer': 42})
            _, errors = await asyncio.wait_for(worker.communicate(), 5)
            self.assertEqual(worker.returncode, 0, errors.decode())
            self.assertEqual((await caller.client.stats())['routed'], 0)
        finally:
            if worker.returncode is None:
                worker.kill()
                await worker.wait()
