"""Head-to-head agent API benchmarks; no model calls, loss tolerated, or best-run selection."""
import argparse
import asyncio
import contextlib
import hashlib
import importlib.metadata
import json
import platform
import random
import re
import shutil
import subprocess
import struct
import tempfile
import time
from pathlib import Path
from swarmwire import Agent, Client, MemoryBus, MemoryArtifacts, PeerEndpoint, RawRPC
from swarmwire.backends import NatsEndpoint, ZeroMQMesh
from swarmwire.deadlines import deadline

ROOT = Path(__file__).resolve().parent


async def native_connect(env, mode, name, *, acknowledged=False):
    if mode.startswith(("swarmwire_peer_", "swarmwire_auto_")):
        tcp = mode.endswith("tcp")
        options = {"port": env.native_port} if tcp and "auto" in mode else {"unix": env.native_unix}
        return await PeerEndpoint.connect(name, **options, network="tcp" if tcp else "unix", capacity=512, route_policy="auto" if "auto" in mode else "direct")
    options = {"unix": env.native_unix} if "unix" in mode else {"port": env.native_port}
    return await Client.connect(name, capacity=512, acknowledged=acknowledged, **options)


def percentiles(values):
    values = sorted(values)
    return {f"p{p}_us": round(values[int((len(values)-1)*p/100)], 3) for p in (50, 95, 99)}


class Environment:
    def __init__(self, nats_binary):
        self.nats_binary = nats_binary
        self.processes = []
        self.temp = tempfile.TemporaryDirectory(prefix="swarmwire-compare-", dir="/tmp")
        self.directory = Path(self.temp.name)

    async def start(self):
        self.native_unix = str(self.directory / "native.sock")
        for flags, field in ((["--port", "0"], "native_port"), (["--unix", self.native_unix], None)):
            proc = subprocess.Popen([str(ROOT / "bin" / "swarmwired"), *flags], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            self.processes.append(proc)
            line = await asyncio.wait_for(asyncio.to_thread(proc.stdout.readline), 5)
            if not line:
                raise RuntimeError(proc.stderr.read().decode())
            if field:
                setattr(self, field, json.loads(line)["port"])
        proc = subprocess.Popen([self.nats_binary, "-a", "127.0.0.1", "-p", "-1"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.processes.append(proc)
        while True:
            line = await asyncio.wait_for(asyncio.to_thread(proc.stderr.readline), 5)
            if not line:
                raise RuntimeError("NATS server failed to start")
            match = re.search(rb"Listening for client connections on 127\.0\.0\.1:(\d+)", line)
            if match:
                self.nats_url = "nats://127.0.0.1:" + match.group(1).decode()
                break

    async def stop(self):
        for proc in self.processes:
            if proc.poll() is None:
                proc.terminate()
            try:
                await asyncio.wait_for(asyncio.to_thread(proc.wait), 5)
            except asyncio.TimeoutError:
                proc.kill()
                await asyncio.to_thread(proc.wait)
            proc.stdout.close()
            proc.stderr.close()
        self.temp.cleanup()


class Session:
    def __init__(self, env, mode, case_id):
        self.env, self.mode, self.case_id = env, mode, case_id
        self.endpoints, self.tasks = [], []
        self.owner = None
        if mode == "swarmwire_memory":
            self.owner = MemoryBus()
        elif mode.startswith("zeromq"):
            self.owner = ZeroMQMesh(env.directory / case_id, inproc=mode.endswith("inproc"), tcp=mode.endswith("tcp"), artifacts=MemoryArtifacts())
        self.artifacts = MemoryArtifacts()

    async def connect(self, suffix):
        name = self.case_id + "-" + suffix
        if self.mode == "swarmwire_memory" or self.mode.startswith("zeromq"):
            endpoint = await self.owner.connect(name, capacity=512)
        elif self.mode.startswith(("swarmwire_peer_", "swarmwire_auto_")):
            endpoint = await native_connect(self.env, self.mode, name)
        elif self.mode in ("swarmwire_unix", "swarmwire_unix_stream"):
            endpoint = await Client.connect(name, unix=self.env.native_unix, capacity=512, acknowledged=not self.mode.endswith("stream"))
        elif self.mode in ("swarmwire_tcp", "swarmwire_tcp_stream"):
            endpoint = await Client.connect(name, port=self.env.native_port, capacity=512, acknowledged=not self.mode.endswith("stream"))
        elif self.mode == "nats_core":
            endpoint = await NatsEndpoint.connect(name, servers=self.env.nats_url, namespace="comparison", capacity=512, artifacts=self.artifacts)
        else:
            raise ValueError(self.mode)
        self.endpoints.append(endpoint)
        return Agent(endpoint)

    async def worker(self, name):
        agent = await self.connect(name)
        async def echo(payload, message):
            return payload
        self.tasks.append(asyncio.create_task(agent.serve(echo)))
        return agent

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        # Async backends must report errors, rather than silently improve throughput by dropping.
        errors = [str(e) for endpoint in self.endpoints for e in getattr(endpoint, "errors", [])]
        await asyncio.gather(*(endpoint.close() for endpoint in self.endpoints))
        if self.owner:
            await self.owner.close()
        if errors:
            raise RuntimeError("transport errors: " + str(errors))


async def rtt(session, samples, size):
    first, second = await session.connect("caller"), await session.worker("echo")
    data = {"text": "x" * size, "sequence": 0}
    values = []
    first_exchange = None
    for i in range(samples + 100):
        data["sequence"] = i
        start = time.perf_counter_ns()
        reply = await first.request(second.client.name, data, timeout=5)
        elapsed = (time.perf_counter_ns() - start) / 1000
        if i == 0:
            first_exchange = elapsed
        if reply["payload"] != data:
            raise AssertionError("mismatched reply")
        if i >= 100:
            values.append(elapsed)
    return {"workload": "agent_rtt", "payload_text_bytes": size, "samples": samples, "first_exchange_us": round(first_exchange,3), **percentiles(values)}


async def scatter(session, samples, count):
    caller = await session.connect("caller")
    workers = [await session.worker(f"worker-{i}") for i in range(count)]
    values = []
    for iteration in range(samples + 10):
        start = time.perf_counter_ns()
        replies = await asyncio.gather(*(caller.request(worker.client.name, {"worker": i, "sequence": iteration, "text": "x"*64}, timeout=5) for i, worker in enumerate(workers)))
        elapsed = (time.perf_counter_ns() - start) / 1000
        if {reply["payload"]["worker"] for reply in replies} != set(range(count)) or any(reply["payload"]["sequence"] != iteration for reply in replies):
            raise AssertionError("scatter reply mismatch")
        if iteration >= 10:
            values.append(elapsed)
    return {"workload": "scatter_gather", "workers": count, "samples": samples, **percentiles(values)}


async def completion_rate(session, count, producers):
    worker = await session.worker("sink")
    callers = [await session.connect(f"producer-{i}") for i in range(producers)]
    async def producer(index, caller):
        verified = 0
        for i in range(index, count, producers):
            reply = await caller.request(worker.client.name, {"sequence": i, "text": "x"*64}, timeout=10)
            if reply["payload"]["sequence"] != i:
                raise AssertionError("completion mismatch")
            verified += 1
        return verified
    # Warm every route before timing, including ZeroMQ's lazy connections.
    await asyncio.gather(*(caller.request(worker.client.name, {"warmup": True}) for caller in callers))
    start = time.perf_counter()
    verified = sum(await asyncio.gather(*(producer(i,c) for i,c in enumerate(callers))))
    elapsed = time.perf_counter()-start
    assert verified == count
    return {"workload": "completed_requests", "producers": producers, "verified_requests": count, "seconds": round(elapsed,6), "requests_per_second": round(count/elapsed,2)}


async def native_api_rtt(env, mode, samples, size, case_id):
    """Existing products' direct request APIs, without our Agent envelope/adapter."""
    endpoints, task, context, errors = [], None, None, []
    try:
        if mode.startswith("swarmwire"):
            caller = await native_connect(env, mode, case_id + "-caller")
            worker = await native_connect(env, mode, case_id + "-echo")
            endpoints.extend([caller, worker])
            caller_rpc, worker_rpc = RawRPC(caller), RawRPC(worker)
            async def echo(data, message):
                return data
            task = asyncio.create_task(worker_rpc.serve(echo))
            async def exchange(data):
                return await caller_rpc.request(worker.name, data, timeout=None)
        elif mode.startswith("zeromq"):
            import zmq.asyncio
            context = zmq.asyncio.Context()
            caller, worker = context.socket(zmq.REQ), context.socket(zmq.REP)
            endpoints.extend([caller, worker])
            for sock in endpoints:
                sock.setsockopt(zmq.LINGER, 0)
            address = f"inproc://{case_id}" if mode.endswith("inproc") else ("tcp://127.0.0.1:*" if mode.endswith("tcp") else f"ipc://{env.directory}/{case_id}.sock")
            worker.bind(address)
            caller.connect(worker.getsockopt(zmq.LAST_ENDPOINT).decode())
            async def echo():
                while True:
                    await worker.send(await worker.recv())
            task = asyncio.create_task(echo())
            async def exchange(data):
                await caller.send(data)
                return await caller.recv()
        elif mode == "nats_core":
            import nats
            async def on_error(exc):
                errors.append(exc)
            caller = await nats.connect(env.nats_url, error_cb=on_error)
            worker = await nats.connect(env.nats_url, error_cb=on_error)
            endpoints.extend([caller, worker])
            async def echo(message):
                await message.respond(message.data)
            await worker.subscribe("raw." + case_id, cb=echo)
            await worker.flush()
            async def exchange(data):
                return (await caller.request("raw." + case_id, data, timeout=None)).data
        else:
            raise ValueError(mode)
        values = []
        first_exchange = None
        for i in range(samples + 100):
            data = struct.pack("!Q", i) + b"x" * (size - 8)
            start = time.perf_counter_ns()
            reply = await deadline(exchange(data), 5)
            elapsed = (time.perf_counter_ns() - start) / 1000
            if i == 0:
                first_exchange = elapsed
            if reply != data:
                raise AssertionError("direct SDK reply mismatch")
            if i >= 100:
                values.append(elapsed)
        return {"workload": "native_api_rtt", "payload_bytes": size, "samples": samples, "first_exchange_us": round(first_exchange,3), **percentiles(values)}
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if context:
            for sock in endpoints:
                sock.close(0)
            context.destroy(linger=0)
        else:
            await asyncio.gather(*(endpoint.close() for endpoint in endpoints))
        if errors:
            raise RuntimeError("direct SDK transport errors: " + str(errors))


async def native_api_rate(env, mode, count, producers, case_id):
    """Direct native SDK throughput with one outstanding echo per producer."""
    endpoints, task, context, errors = [], None, None, []
    try:
        if mode.startswith("swarmwire"):
            worker = await native_connect(env, mode, case_id + "-echo")
            endpoints.append(worker)
            callers = []
            for i in range(producers):
                caller = await native_connect(env, mode, f"{case_id}-p{i}")
                endpoints.append(caller)
                callers.append(RawRPC(caller))
            worker_rpc = RawRPC(worker)
            async def echo(data, message):
                return data
            task = asyncio.create_task(worker_rpc.serve(echo))
            async def exchange(caller, data):
                return await caller.request(worker.name, data, timeout=None)
        elif mode.startswith("zeromq"):
            import zmq.asyncio
            context = zmq.asyncio.Context()
            worker = context.socket(zmq.REP)
            worker.setsockopt(zmq.LINGER, 0)
            endpoints.append(worker)
            address = f"inproc://{case_id}" if mode.endswith("inproc") else ("tcp://127.0.0.1:*" if mode.endswith("tcp") else f"ipc://{env.directory}/{case_id}.sock")
            worker.bind(address)
            callers = []
            for _ in range(producers):
                caller = context.socket(zmq.REQ)
                caller.setsockopt(zmq.LINGER, 0)
                caller.connect(worker.getsockopt(zmq.LAST_ENDPOINT).decode())
                endpoints.append(caller)
                callers.append(caller)
            async def echo():
                while True:
                    await worker.send(await worker.recv())
            task = asyncio.create_task(echo())
            async def exchange(caller, data):
                await caller.send(data)
                return await caller.recv()
        elif mode == "nats_core":
            import nats
            async def on_error(exc):
                errors.append(exc)
            worker = await nats.connect(env.nats_url, error_cb=on_error)
            endpoints.append(worker)
            callers = []
            for _ in range(producers):
                caller = await nats.connect(env.nats_url, error_cb=on_error)
                endpoints.append(caller)
                callers.append(caller)
            async def echo(message):
                await message.respond(message.data)
            await worker.subscribe("raw." + case_id, cb=echo)
            await worker.flush()
            async def exchange(caller, data):
                return (await caller.request("raw." + case_id, data, timeout=None)).data
        else:
            raise ValueError(mode)
        async def producer(index, caller, warmup=False):
            sequence = [index] if warmup else range(index, count, producers)
            completed = 0
            for i in sequence:
                data = struct.pack("!Q", i) + b"x" * 56
                reply = await deadline(exchange(caller, data), 10)
                if reply != data:
                    raise AssertionError("direct SDK completion mismatch")
                completed += 1
            return completed
        await asyncio.gather(*(producer(i,c,True) for i,c in enumerate(callers)))
        start = time.perf_counter()
        verified = sum(await asyncio.gather(*(producer(i,c) for i,c in enumerate(callers))))
        elapsed = time.perf_counter() - start
        assert verified == count
        return {"workload": "native_api_completed_requests", "payload_bytes": 64, "producers": producers, "verified_requests": count, "seconds": round(elapsed,6), "requests_per_second": round(count/elapsed,2)}
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if context:
            for sock in endpoints:
                sock.close(0)
            context.destroy(linger=0)
        else:
            await asyncio.gather(*(endpoint.close() for endpoint in endpoints))
        if errors:
            raise RuntimeError("direct SDK transport errors: " + str(errors))


async def main(args):
    modes = args.modes.split(",")
    env = Environment(args.nats_server)
    rows = []
    randomizer = random.Random(args.seed)
    try:
        await env.start()
        for trial in range(args.trials):
            order = modes[:]
            randomizer.shuffle(order)
            for mode in order:
                cases = [("rtt", size) for size in (64,4096,32768)] + [("scatter",n) for n in (8,32,128)] + [("rate",n) for n in (1,32,128)]
                randomizer.shuffle(cases)
                for index, (workload, param) in enumerate(cases):
                    session = Session(env, mode, f"t{trial}-m{modes.index(mode)}-c{index}")
                    try:
                        if workload == "rtt":
                            row = await rtt(session,args.samples,param)
                        elif workload == "scatter":
                            row = await scatter(session,args.scatter_samples,param)
                        else:
                            row = await completion_rate(session,args.requests,param)
                        rows.append({"mode":mode,"trial":trial,**row})
                        print(json.dumps(rows[-1]),flush=True)
                    finally:
                        await session.close()
            raw_modes = ("swarmwire_auto_unix", "swarmwire_auto_tcp", "swarmwire_peer_unix", "swarmwire_peer_tcp", "swarmwire_unix_stream", "swarmwire_tcp_stream", "zeromq_ipc", "zeromq_inproc", "zeromq_tcp", "nats_core")
            raw_order = [(mode, workload, param) for mode in raw_modes if mode in modes for workload, param in [("rtt",n) for n in (64,4096,32768)] + [("rate",n) for n in (1,32,128)]]
            randomizer.shuffle(raw_order)
            for index, (mode, workload, param) in enumerate(raw_order):
                if workload == "rtt":
                    row = await native_api_rtt(env, mode, args.samples, param, f"raw-t{trial}-c{index}")
                else:
                    row = await native_api_rate(env, mode, args.requests, param, f"raw-t{trial}-c{index}")
                rows.append({"mode": mode, "trial": trial, **row})
                print(json.dumps(rows[-1]), flush=True)
        versions = {name: importlib.metadata.version(name) for name in ("pyzmq","nats-py","uvloop","orjson","langgraph")}
        import zmq
        nats_version = subprocess.check_output([args.nats_server,"--version"],text=True).strip()
        report = {
            "platform": platform.platform(), "machine": platform.machine(), "python": platform.python_version(),
            "loop": type(asyncio.get_running_loop()).__name__,
            "versions": {**versions, "libzmq": zmq.zmq_version(), "nats_server": nats_version},
            "seed": args.seed, "trial_order_randomized": True,
            "delivery_semantics": "ephemeral application request/reply; every measured request must return its matching result",
            "topologies": {
                "swarmwire_memory": "direct same-process delivery",
                "swarmwire_peer_unix": "direct Unix peer sockets; native discovery/authentication/artifact broker on control plane only",
                "swarmwire_peer_tcp": "direct loopback TCP peer sockets; native Unix discovery/authentication/artifact control plane only",
                "swarmwire_auto_unix": "Unix peers and native Unix broker multiplexing; configurable threshold 16 warm routes, automatic reply path",
                "swarmwire_auto_tcp": "TCP peers and native TCP broker multiplexing; configurable threshold 16 warm routes, automatic reply path",
                "zeromq_inproc": "direct same-process PUSH/PULL mesh; raw SDK uses REQ/REP",
                "swarmwire_unix": "separate native broker, Unix sockets, Python workers in harness process, per-send ACK",
                "swarmwire_unix_stream": "same native Unix broker, no positive per-send ACK",
                "zeromq_ipc": "direct Unix-socket PUSH/PULL mesh; raw SDK uses REQ/REP",
                "zeromq_tcp": "direct loopback TCP PUSH/PULL mesh; raw SDK uses REQ/REP",
                "swarmwire_tcp": "separate native broker, loopback TCP, per-send ACK",
                "swarmwire_tcp_stream": "same native TCP broker, no positive per-send ACK",
                "nats_core": "separate official NATS server, loopback TCP; no JetStream"
            },
            "notes": [
                "Agent workloads share the same Python interpreter, loop, Agent envelope, payloads, handlers, deadlines and verification across modes.",
                "Native API workloads measure direct pyzmq REQ/REP, nats-py request/respond, or SwarmWire RawRPC; no Agent wrapper or JSON. RawRPC adds a 13-byte correlation envelope to the application bytes.",
                "The common outer deadline bounds every direct SDK request; nats-py request(timeout=None) and RawRPC(timeout=None) avoid redundant SDK timers while preserving that deadline.",
                "Topology and per-send acceptance semantics differ; application reply completion is the shared measurement boundary.",
                "SwarmWire acknowledged modes wait for broker acceptance; streaming, ZeroMQ and Core NATS sends do not.",
                "Direct peers reserve one bounded receive slot per connection, return credits with outgoing data, and warm discovery/authentication before timing.",
                "No model/API calls, durable storage, forced competitor delays or omitted losing trials.",
                "All setup and route warmup occur before timing; agent text length excludes JSON metadata; raw payload_bytes is exact."
            ],
            "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted([*(ROOT/'swarmwire').glob('*.py'), ROOT/'native/swarmwire.cpp', ROOT/'compare.py'])},
            "results": rows
        }
        Path(args.output).write_text(json.dumps(report,indent=2)+"\n")
        print("Saved",args.output,flush=True)
    finally:
        await env.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--nats-server",default=shutil.which("nats-server"))
    parser.add_argument("--modes",default="swarmwire_memory,swarmwire_auto_unix,swarmwire_auto_tcp,swarmwire_peer_unix,swarmwire_peer_tcp,zeromq_inproc,swarmwire_unix,swarmwire_unix_stream,zeromq_ipc,zeromq_tcp,swarmwire_tcp,swarmwire_tcp_stream,nats_core")
    parser.add_argument("--samples",type=int,default=500)
    parser.add_argument("--scatter-samples",type=int,default=50)
    parser.add_argument("--requests",type=int,default=4000)
    parser.add_argument("--trials",type=int,default=5)
    parser.add_argument("--seed",type=int,default=20261009)
    parser.add_argument("--output",default="comparison.json")
    args = parser.parse_args()
    if not args.nats_server:
        parser.error("provide --nats-server /path/to/nats-server")
    if min(args.samples,args.scatter_samples,args.requests,args.trials)<1:
        parser.error("counts must be positive")
    import uvloop
    uvloop.install()
    asyncio.run(main(args))
