import asyncio
import json
import os
import struct
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from swarmwire import Agent, Backpressure, Client, RelayError, Tools, RemoteError

ROOT = Path(__file__).resolve().parents[1]


class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        binary = os.getenv("SWARMWIRE_BINARY", str(ROOT / "bin" / "swarmwired"))
        self.broker = subprocess.Popen([binary, "--port", "0"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.port = json.loads(await asyncio.to_thread(self.broker.stdout.readline))["port"]
        self.clients = []

    async def connect(self, name, **kwargs):
        client = await Client.connect(name, port=self.port, **kwargs)
        self.clients.append(client)
        return client

    async def asyncTearDown(self):
        await asyncio.gather(*(client.close() for client in self.clients))
        self.broker.terminate()
        try:
            await asyncio.wait_for(asyncio.to_thread(self.broker.wait), 5)
        except asyncio.TimeoutError:
            self.broker.kill()
            await asyncio.to_thread(self.broker.wait)
        stderr = self.broker.stderr.read().decode()
        self.broker.stdout.close()
        self.broker.stderr.close()
        self.assertEqual(self.broker.returncode, 0, stderr)
        self.assertNotIn("ERROR: AddressSanitizer", stderr)

    async def test_binary_order_and_empty_payload(self):
        a, b = await self.connect("a"), await self.connect("b", capacity=2)
        payloads = [b"", b"\x00\xff\x01"] + [str(i).encode() for i in range(30)]
        for data in payloads:
            await a.send("b", data)
        for data in payloads:
            message = await b.receive(timeout=2)
            self.assertEqual((message.sender, message.data), ("a", data))

    async def test_flow_control_keeps_control_plane_alive(self):
        a, b = await self.connect("a"), await self.connect("b", capacity=1)
        for _ in range(10):
            await a.send("b", b"hello")
        # A full receiver inbox must not prevent acknowledgements or blob responses.
        self.assertEqual((await b.stats())["routed"], 10)
        for _ in range(10):
            self.assertEqual((await b.receive(timeout=2)).data, b"hello")

    async def test_backpressure_is_explicit(self):
        a, b = await self.connect("a"), await self.connect("b", capacity=1)
        rejected = False
        for _ in range(20):
            try:
                await a.send("b", b"x" * 1048576)
            except Backpressure:
                rejected = True
                break
        self.assertTrue(rejected)
        self.assertGreater((await a.stats())["rejected"], 0)

    async def test_unknown_and_duplicate_identity(self):
        a = await self.connect("a")
        with self.assertRaises(RelayError):
            await a.send("missing", b"hello")
        with self.assertRaises(RelayError):
            await self.connect("a")
        with self.assertRaises(RelayError):
            await self.connect("invalid/name")

    async def test_store_deduplication_and_integrity(self):
        a, b = await self.connect("a"), await self.connect("b")
        data = b"large source material" * 10000
        first = await a.store(data)
        self.assertEqual(first, await a.store(data))
        self.assertEqual(data, await b.load(first))
        self.assertEqual((await a.stats())["blob_bytes"], len(data))
        with self.assertRaises(RelayError):
            await b.load("0" * 64)

    async def test_request_reply_and_parallel_handlers(self):
        a, b = Agent(await self.connect("a")), Agent(await self.connect("b"))
        async def handler(payload, message):
            return {"value": payload["value"] * 2}
        server = asyncio.create_task(b.serve(handler, concurrency=4))
        try:
            replies = await asyncio.gather(*(a.request("b", {"value": i}, timeout=3) for i in range(50)))
            self.assertEqual([r["payload"]["value"] for r in replies], list(range(0, 100, 2)))
        finally:
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_streaming_requests_and_handler_errors(self):
        a = Agent(await self.connect("a", acknowledged=False))
        b = Agent(await self.connect("b", acknowledged=False))
        async def handler(payload, message):
            if payload.get("bad"):
                raise ValueError("bad input")
            return payload
        server = asyncio.create_task(b.serve(handler, concurrency=4))
        try:
            with self.assertRaisesRegex(RemoteError, "bad input"):
                await a.request("b", {"bad": True})
            replies = await asyncio.gather(*(a.request("b", {"sequence": i}) for i in range(60)))
            self.assertEqual([r["payload"]["sequence"] for r in replies], list(range(60)))
        finally:
            server.cancel()
            await asyncio.gather(server, return_exceptions=True)

    async def test_streaming_rejection_fails_pending_request(self):
        a = Agent(await self.connect("a", acknowledged=False))
        with self.assertRaises(RelayError):
            await asyncio.wait_for(a.request("missing", {}), 1)
        self.assertTrue(a.client._closed)
        self.assertFalse(a._requests)

    async def test_streaming_backpressure_is_not_silent(self):
        a = await self.connect("a", acknowledged=False)
        await self.connect("b", capacity=1)
        for _ in range(20):
            try:
                await a.send("b", b"x" * 1048576)
            except RelayError:
                break
        await asyncio.wait_for(a._reader_task, 2)
        self.assertTrue(a._closed)
        self.assertIn("BACKPRESSURE", str(a._failure))

    async def test_timeout_and_cancel_cleanup(self):
        a, b = Agent(await self.connect("a")), Agent(await self.connect("b"))
        with self.assertRaises(asyncio.TimeoutError):
            await a.request("b", {}, timeout=.03)
        self.assertFalse(a._requests)
        with self.assertRaises(asyncio.TimeoutError):
            await a.receive(timeout=.01)
        await b.send("a", {"after": "timeout"})
        self.assertEqual((await a.receive(timeout=1))["payload"], {"after": "timeout"})

    async def test_disconnect_wakes_waiters(self):
        a = await self.connect("a")
        waiting = asyncio.create_task(a.receive())
        await asyncio.sleep(0)
        await a.close()
        with self.assertRaises(RelayError):
            await asyncio.wait_for(waiting, 1)

    async def test_invalid_agent_data_does_not_break_connection(self):
        raw, target = await self.connect("raw"), Agent(await self.connect("target"))
        await raw.send("target", b"not-json")
        with self.assertRaises(RelayError):
            await target.receive(timeout=1)
        self.assertEqual((await target.client.stats())["agents"], 2)

    async def test_many_agents_fan_in(self):
        sink = await self.connect("sink", capacity=16)
        senders = await asyncio.gather(*(self.connect(f"worker-{i}") for i in range(64)))
        await asyncio.gather(*(c.send("sink", str(i).encode()) for i, c in enumerate(senders)))
        received = [await sink.receive(timeout=3) for _ in range(64)]
        self.assertEqual({int(m.data) for m in received}, set(range(64)))
        self.assertEqual(len({m.sender for m in received}), 64)

    async def test_provider_tool_shapes_and_execution(self):
        a, b = Tools(Agent(await self.connect("a"))), Tools(Agent(await self.connect("b")))
        self.assertTrue(all(t["type"] == "function" for t in a.openai()))
        self.assertTrue(all("input_schema" in t for t in a.anthropic()))
        item = SimpleNamespace(type="function_call", name="relay_send", arguments=json.dumps({"to": "b", "payload": {"provider": "openai"}}), call_id="call-1")
        result = await a.execute_openai([item])
        self.assertTrue(json.loads(result[0]["output"])["queued"])
        self.assertEqual((await b.call("relay_receive", {"timeout": 1}))["messages"][0]["payload"]["provider"], "openai")
        item = SimpleNamespace(type="tool_use", name="relay_send", input={"to": "a", "payload": {"provider": "anthropic"}}, id="tool-1")
        self.assertFalse((await b.execute_anthropic([item]))[0]["is_error"])
        self.assertEqual((await a.call("relay_receive", {"timeout": 1}))["messages"][0]["payload"]["provider"], "anthropic")

    async def test_batch_partial_failure(self):
        a, b = Tools(Agent(await self.connect("a"))), await self.connect("b")
        results = (await a.call("relay_send_batch", {"messages": [{"to": "b", "payload": {}}, {"to": "missing", "payload": {}}]}))["results"]
        self.assertEqual([r["queued"] for r in results], [True, False])
        self.assertIsNotNone(await b.receive(timeout=1))

    async def test_fragmented_and_invalid_wire_frames(self):
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        name = b"fragmented"
        frame = struct.pack("!IBIH", 11 + len(name), 1, 1, len(name)) + name + struct.pack("!I", 1)
        for byte in frame:
            writer.write(bytes([byte]))
            await writer.drain()
        size = struct.unpack("!I", await reader.readexactly(4))[0]
        self.assertEqual((await reader.readexactly(size))[0], 128)
        writer.write(struct.pack("!I", 2**32-1))
        await writer.drain()
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
        writer.close()
        await writer.wait_closed()
        self.assertEqual((await (await self.connect("healthy")).stats())["agents"], 1)

    async def test_separate_process_worker(self):
        a = Agent(await self.connect("a"))
        code = """import asyncio,sys
from swarmwire import Client,Agent
async def main():
 c=await Client.connect('process-worker',port=int(sys.argv[1])); a=Agent(c)
 print('ready',flush=True)
 m=await a.receive(); await a.reply(m,{'answer':42}); await c.close()
asyncio.run(main())
"""
        worker = await asyncio.create_subprocess_exec(sys.executable, "-c", code, str(self.port), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            self.assertEqual(await asyncio.wait_for(worker.stdout.readline(), 3), b"ready\n")
            self.assertEqual((await a.request("process-worker", {}, timeout=3))["payload"], {"answer": 42})
            _, stderr = await asyncio.wait_for(worker.communicate(), 3)
            self.assertEqual(worker.returncode, 0, stderr)
        finally:
            if worker.returncode is None:
                worker.kill()
                await worker.wait()


if __name__ == "__main__":
    unittest.main()
