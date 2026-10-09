"""Reproducible transport benchmarks. Never calls an LLM or invents a baseline."""
import argparse
import asyncio
import json
import platform
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from swarmwire import Agent, Client

ROOT = Path(__file__).resolve().parent


def summary(mode, samples, payload, timing):
    values = sorted(timing)
    return {"mode": mode, "samples": samples, "payload_bytes": payload,
            **{f"rtt_us_p{p}": round(values[int((len(values) - 1) * p / 100)], 2) for p in (50, 95, 99)}}


async def python_rtt(connection, samples, size, json_mode):
    clients = [await Client.connect(f"bench-{json_mode}-{i}", **connection) for i in range(2)]
    a, b = clients
    data = b"x" * size
    count = samples + 100
    timings = []
    task = None
    try:
        if json_mode:
            first, second = Agent(a), Agent(b)
            async def handler(payload, message):
                return payload
            task = asyncio.create_task(second.serve(handler))
            for i in range(count):
                start = time.perf_counter_ns()
                answer = await first.request(b.name, {"text": data.decode()})
                elapsed = (time.perf_counter_ns() - start) / 1000
                assert answer["payload"]["text"] == data.decode()
                if i >= 100:
                    timings.append(elapsed)
        else:
            async def echo():
                for _ in range(count):
                    message = await b.receive()
                    await b.send(a.name, message.data)
            task = asyncio.create_task(echo())
            for i in range(count):
                start = time.perf_counter_ns()
                await a.send(b.name, data)
                answer = await a.receive()
                elapsed = (time.perf_counter_ns() - start) / 1000
                assert answer.data == data
                if i >= 100:
                    timings.append(elapsed)
        return summary("python_json" if json_mode else "python_raw", samples, size, timings)
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await asyncio.gather(*(client.close() for client in clients))


async def throughput(connection, count, agents):
    sink = await Client.connect("load-sink", capacity=256, **connection)
    workers = await asyncio.gather(*(Client.connect(f"load-{i}", **connection) for i in range(agents)))
    async def producer(index, client):
        for i in range(index, count, agents):
            await client.send(sink.name, i.to_bytes(8, "big") + b"x" * 56)
    async def consumer():
        received = set()
        for _ in range(count):
            received.add(int.from_bytes((await sink.receive()).data[:8], "big"))
        assert received == set(range(count)), "missing or duplicate messages"
    try:
        start = time.perf_counter()
        await asyncio.gather(consumer(), *(producer(i, c) for i, c in enumerate(workers)))
        elapsed = time.perf_counter() - start
        return {"mode": "python_fan_in", "messages": count, "agents": agents, "payload_bytes": 64, "seconds": round(elapsed, 4), "messages_per_second": round(count / elapsed)}
    finally:
        await asyncio.gather(sink.close(), *(c.close() for c in workers))


async def mcp_rtt(connection, samples):
    from contextlib import AsyncExitStack
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client
    async with AsyncExitStack() as stack:
        sessions = []
        for name in ("mcp-a", "mcp-b"):
            args = ["-m", "swarmwire", "mcp", "--agent", name]
            args += ["--unix", connection["unix"]] if "unix" in connection else ["--port", str(connection["port"])]
            read, write = await stack.enter_async_context(stdio_client(StdioServerParameters(command=sys.executable, args=args, cwd=str(ROOT))))
            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
            assert len((await session.list_tools()).tools) == 6
            sessions.append(session)
        a, b = sessions
        timings = []
        for i in range(samples + 10):
            start = time.perf_counter_ns()
            queued = await a.call_tool("relay_send", {"to": "mcp-b", "payload": {"text": "x" * 64}})
            delivered = await b.call_tool("relay_receive", {"timeout": 1, "limit": 1})
            assert not queued.isError and not delivered.isError
            message = json.loads(delivered.content[0].text)["messages"][0]
            reply = await b.call_tool("relay_send", {"to": "mcp-a", "payload": message["payload"]})
            answer = await a.call_tool("relay_receive", {"timeout": 1, "limit": 1})
            elapsed = (time.perf_counter_ns() - start) / 1000
            assert not reply.isError and not answer.isError
            assert json.loads(answer.content[0].text)["messages"][0]["payload"] == {"text": "x" * 64}
            if i >= 10:
                timings.append(elapsed)
        # Four MCP tool invocations plus the broker round trip; no model calls.
        return summary("mcp_four_tool_roundtrip", samples, 64, timings)


async def main(args):
    if args.samples < 1 or args.agents < 1 or args.messages < 1:
        raise ValueError("samples, agents and messages must be positive")
    with tempfile.TemporaryDirectory(prefix="swarmwire-") as tmp:
        connection = {"unix": str(Path(tmp) / "relay.sock")} if args.unix else {}
        flags = ["--unix", connection["unix"]] if args.unix else ["--port", "0"]
        broker = subprocess.Popen([str(ROOT / "bin" / "swarmwired"), *flags], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            line = await asyncio.to_thread(broker.stdout.readline)
            if not line:
                raise RuntimeError(broker.stderr.read().decode())
            ready = json.loads(line)
            if not args.unix:
                connection = {"port": ready["port"]}
                flags = ["--port", str(ready["port"])]
            results = []
            for size in (64, 4096):
                native = await asyncio.create_subprocess_exec(str(ROOT / "bin" / "swarmwired"), "bench", *flags, "--samples", str(args.samples), "--payload", str(size), stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                output, error = await native.communicate()
                if native.returncode:
                    raise RuntimeError(error.decode())
                results.append(json.loads(output))
                results.append(await python_rtt(connection, args.samples, size, False))
                results.append(await python_rtt(connection, args.samples, size, True))
            results.append(await throughput(connection, args.messages, args.agents))
            if args.mcp:
                results.append(await mcp_rtt(connection, min(args.samples, 100)))
            report = {"platform": platform.platform(), "python": platform.python_version(), "transport": "unix" if args.unix else "tcp_loopback", "loop": type(asyncio.get_running_loop()).__name__, "notes": ["Native RTT uses two blocking client sockets in one native process and a separate broker process.", "Python RTT uses sender and echo worker in one Python process and a separate broker process.", "Native and Python raw payload sizes match; JSON envelopes add metadata and serialization.", "MCP RTT includes four real SDK tool calls to two separate stdio server processes.", "Fan-in includes Python producers/consumer and verifies every message exactly once.", "No LLM calls. These are transport measurements, not agent completion times."], "results": results}
            text = json.dumps(report, indent=2)
            print(text)
            if args.output:
                Path(args.output).write_text(text + "\n")
        finally:
            broker.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(broker.wait), 5)
            except asyncio.TimeoutError:
                broker.kill()
                await asyncio.to_thread(broker.wait)
            broker.stdout.close()
            broker.stderr.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--samples", type=int, default=2000)
    parser.add_argument("--messages", type=int, default=10000)
    parser.add_argument("--agents", type=int, default=32)
    parser.add_argument("--unix", action="store_true")
    parser.add_argument("--fast", action="store_true")
    parser.add_argument("--mcp", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()
    if args.fast:
        import uvloop
        uvloop.install()
    asyncio.run(main(args))
