import asyncio
import contextlib
import importlib.util
import os
import tempfile
import unittest
from types import SimpleNamespace
from swarmwire import Agent, CallableAdapter, CrewAIAdapter, LangGraphAdapter, MemoryBus, MemoryArtifacts, OpenAIAgentsAdapter, RelayError, RemoteError, Swarm


class RuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_completion_order_and_error_recovery(self):
        slow_started, release = asyncio.Event(), asyncio.Event()
        async def slow(payload, message):
            slow_started.set()
            await release.wait()
            return payload
        async def fast(payload, message):
            await slow_started.wait()
            if payload.get("bad"):
                raise ValueError("invalid exploration")
            return payload
        async with Swarm() as swarm:
            await swarm.register("slow", slow)
            await swarm.register("fast", fast)
            results = swarm.explore({"slow": {"n": 1}, "fast": {"n": 2}})
            self.assertEqual(await anext(results), ("fast", {"n": 2}))
            release.set()
            self.assertEqual(await anext(results), ("slow", {"n": 1}))
            await results.aclose()
            with self.assertRaisesRegex(RemoteError, "invalid exploration"):
                await swarm.request("fast", {"bad": True})
            self.assertEqual(await swarm.request("fast", {"n": 3}), {"n": 3})

    async def test_early_exploration_close_cleans_callers(self):
        waiting = asyncio.Event()
        async def slow(payload, message):
            await waiting.wait()
            return payload
        async def fast(payload, message):
            return payload
        async with Swarm() as swarm:
            await swarm.register("slow", slow)
            await swarm.register("fast", fast)
            results = swarm.explore({"slow": {}, "fast": {}})
            self.assertEqual((await anext(results))[0], "fast")
            await results.aclose()
            self.assertFalse(swarm.caller._requests)
        self.assertTrue(all(task.done() for task in swarm._workers.values()))

    async def test_timeout_then_reuse_and_bad_result(self):
        release = asyncio.Event()
        async def worker(payload, message):
            if payload.get("wait"):
                await release.wait()
            if payload.get("bad_result"):
                return {"unserializable": object()}
            return payload
        async with Swarm() as swarm:
            await swarm.register("worker", worker, concurrency=2)
            with self.assertRaises(asyncio.TimeoutError):
                await swarm.request("worker", {"wait": True}, timeout=.01)
            self.assertFalse(swarm.caller._requests)
            with self.assertRaises(RemoteError):
                await swarm.request("worker", {"bad_result": True})
            self.assertEqual(await swarm.request("worker", {"ok": 1}), {"ok": 1})
            release.set()

    async def test_closed_full_receiver_releases_blocked_sender(self):
        bus = MemoryBus()
        a, b = await bus.connect("a"), await bus.connect("b", capacity=1)
        try:
            await a.send("b", b"first")
            pending = asyncio.create_task(a.send("b", b"second", timeout=None))
            await asyncio.sleep(.01)
            self.assertFalse(pending.done())
            await b.close()
            with self.assertRaises(RelayError):
                await asyncio.wait_for(pending, 1)
        finally:
            await bus.close()

    async def test_receive_deadline_close_and_identity(self):
        bus = MemoryBus()
        a = await bus.connect("a")
        with self.assertRaises(RelayError):
            await bus.connect("a")
        with self.assertRaises(asyncio.TimeoutError):
            await a.receive(timeout=.01)
        pending = asyncio.create_task(a.receive())
        await asyncio.sleep(0)
        await bus.close()
        with self.assertRaises(RelayError):
            await pending

    async def test_artifacts_deduplicate_and_evict_lru(self):
        artifacts = MemoryArtifacts(max_bytes=6)
        first, second = await artifacts.store(b"one"), await artifacts.store(b"two")
        self.assertEqual(await artifacts.store(b"one"), first)
        self.assertEqual(artifacts.size, 6)
        third = await artifacts.store(b"new")
        with self.assertRaises(RelayError):
            await artifacts.load(second)
        self.assertEqual(await artifacts.load(first), b"one")
        self.assertEqual(await artifacts.load(third), b"new")

    async def test_contract_adapters_map_inputs_results_and_options(self):
        # These fixtures test adapter contracts; they are not live SDK execution.
        class Crew:
            async def akickoff(self, *, inputs):
                return SimpleNamespace(raw=inputs["question"].upper())
        async def runner(agent, data, **options):
            self.assertEqual(options, {"session": "isolated-session"})
            return SimpleNamespace(final_output={"answer": data}, interruptions=[])
        async def invoke(data):
            return {"answer": data["question"][::-1]}
        async with Swarm() as swarm:
            await swarm.register("crew", CrewAIAdapter(lambda payload, message: Crew()))
            await swarm.register("sdk", OpenAIAgentsAdapter(object(), runner=runner, options_factory=lambda p,m: {"session": "isolated-session"}))
            await swarm.register("generic", CallableAdapter(invoke))
            self.assertEqual(await swarm.request("crew", {"question": "hello"}), {"text": "HELLO"})
            self.assertEqual(await swarm.request("sdk", {"input": "hello"}), {"answer": "hello"})
            self.assertEqual(await swarm.request("generic", {"question": "hello"}), {"answer": "olleh"})

    @unittest.skipUnless(importlib.util.find_spec("langgraph"), "install langgraph extra")
    async def test_real_langgraph_checkpointed_invocations(self):
        from typing import TypedDict
        from langgraph.graph import StateGraph, START, END
        from langgraph.checkpoint.memory import InMemorySaver
        class State(TypedDict):
            n: int
        async def double(state):
            return {"n": state["n"] * 2}
        graph = StateGraph(State).add_node("double", double).add_edge(START, "double").add_edge("double", END).compile(checkpointer=InMemorySaver())
        adapter = LangGraphAdapter(graph, config_factory=lambda p,m: {"configurable": {"thread_id": p["session"]}}, input_mapper=lambda p,m: {"n": p["n"]})
        async with Swarm() as swarm:
            await swarm.register("graph", adapter, concurrency=2)
            replies = await asyncio.gather(*(swarm.request("graph", {"n": i, "session": str(i)}) for i in range(10)))
            self.assertEqual([r["n"] for r in replies], list(range(0, 20, 2)))


class ExistingTransportTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, transport):
        async def worker(payload, message):
            if payload.get("bad"):
                raise ValueError("expected")
            return payload
        async with Swarm(transport) as swarm:
            await swarm.register("worker", worker, concurrency=4)
            replies = await asyncio.gather(*(swarm.request("worker", {"n": i}) for i in range(40)))
            self.assertEqual([r["n"] for r in replies], list(range(40)))
            with self.assertRaises(RemoteError):
                await swarm.request("worker", {"bad": True})
            self.assertEqual(await swarm.request("worker", {"ok": True}), {"ok": True})

    @unittest.skipUnless(importlib.util.find_spec("zmq"), "install zeromq extra")
    async def test_zeromq_inproc_and_ipc(self):
        from swarmwire.backends import ZeroMQMesh
        for inproc in (False, True):
            with tempfile.TemporaryDirectory(dir="/tmp", prefix="sw-test-") as directory:
                mesh = ZeroMQMesh(directory, inproc=inproc)
                try:
                    await self.exercise(mesh)
                finally:
                    await mesh.close()

    @unittest.skipUnless(importlib.util.find_spec("nats") and os.getenv("NATS_SERVER_BINARY"), "install nats extra and set NATS_SERVER_BINARY")
    async def test_real_nats(self):
        from swarmwire import NatsTransport
        from compare import Environment
        env = Environment(os.environ["NATS_SERVER_BINARY"])
        try:
            await env.start()
            await self.exercise(NatsTransport(servers=env.nats_url))
        finally:
            await env.stop()
