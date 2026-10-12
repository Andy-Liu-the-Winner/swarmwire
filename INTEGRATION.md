# Integrating existing agents

## Managed local deployment

Use `Swarm(backend='local')` to own one native broker and adaptive transport for
the context. There is no manual port, socket, token, or executable setup. Prefer
`Swarm()`/memory for agents already running in one process. Opting into local
sockets does not automatically move those agents into separate processes.

With `Swarm()` the optional `SWARMWIRE_BACKEND` selects a backend before startup:

| Value | Behavior |
|---|---|
| `memory` (default) | Same-process workers, no extra process |
| `local` | Auto-build/start a private local broker, adaptive data sockets |
| `auto` | Try local; on startup failure use same-process memory |
| `external` | Attach to `SWARMWIRE_SOCKET` or loopback `SWARMWIRE_PORT` |
| `nats` | Attach to an existing `SWARMWIRE_NATS_URL` deployment |

An explicit `backend=` overrides the environment. An explicit transport object
retains its original ownership rules and ignores backend environment selection.
For NATS authentication/TLS settings, continue passing your existing configured
`NatsTransport`; the environment shortcut only selects a server URL.

Wrap an application that starts local worker processes:

```sh
python -m swarmwire run -- python app.py
```

The launcher supplies `SWARMWIRE_BACKEND=external`, a private socket and a generated
connection token to its child. Descendant workers inherit this environment. They
can keep `Swarm()` and send to names registered by another process. The launcher
returns the application's exit code and shuts down its broker on completion.
It does not provision machines or schedule worker processes for you.

To roll back, change deployment configuration before restarting the application:
use `memory` for same-process workers, `nats` for existing distributed workers,
or `external` for an existing local broker. In-flight work is not migrated.
`auto` fallback happens only before the first endpoint is registered and is unsafe
as a substitution for separate-process workers; the launcher never uses it.

Platform wheels contain the executable. Source/editable installs use a local
C++17 compiler and a content-addressed, process-locked build cache. Override the
private cache location with `SWARMWIRE_CACHE_DIR` (directory mode 0700) or use an
explicit `SWARMWIRE_BINARY`. Builds use no network. Unsupported platforms can use
memory/NATS; auto mode can fall back during startup.

Local broker shutdown is tied to context exit, application exceptions, and parent
process death through an inherited pipe. Abrupt parent death can leave an empty
temporary directory, but the broker exits and unlinks its socket. Connections
are not transparently reopened after broker failure, and requests are not replayed.
Remote cancellations still do not terminate already-started model execution.

`swarm.status()` is synchronous and returns only non-secret lifecycle information.
`await swarm.stats()` adds on-demand native/endpoint counters. Lifecycle log records
use the standard `swarmwire` logger with `swarmwire_event` fields. No monitoring
server, new telemetry SDK, background polling, or per-message timers are installed.

Framework execution and message delivery are independent. You can change either
without rewriting the other. This version provides a Python integration API and
an open binary native protocol; other language SDKs have not been implemented.

## Transport choices

| Transport | Scope | Dependency | Send acceptance |
|---|---|---|---|
| `MemoryBus()` | One process/event loop | None | Direct delivery or bounded queue |
| `AdaptiveTransport(unix=...)` | Local peer sockets + native multiplexing | 0.3 native broker | Local write after bounded receive credit |
| `PeerTransport(unix=...)` | Direct local Unix peer sockets | 0.3 native broker for discovery | Local write after receive credit |
| `PeerTransport(network='tcp', port=...)` | Direct loopback TCP peer sockets | 0.3 native broker for discovery | Local write after receive credit |
| `NativeTransport(unix=...)` | Processes on one host | Native broker | Local stream write; rejection closes connection |
| `NativeTransport(port=..., acknowledged=True)` | Loopback processes | Native broker | Broker queue acceptance |
| `ZeroMQMesh(directory=...)` | Local processes sharing directory | pyzmq | Native socket queue acceptance |
| `ZeroMQMesh(tcp=True)` | Loopback TCP mesh in one registry | pyzmq | Native socket queue acceptance |
| `ZeroMQMesh(inproc=True)` | One shared mesh/context | pyzmq | Native socket queue acceptance |
| `NatsTransport(servers=...)` | Existing local/remote deployment | nats-py, NATS server | Client publish acceptance |

`Agent.request()` always waits for a correlated application reply. Per-send
acceptance differs across backends; it is not durable delivery. The native
broker/MemoryBus reject duplicate identities. NATS applications must assign
unique identities; duplicate NATS subscriptions can execute the same task twice.

```python
from swarmwire import Swarm, NatsTransport
from swarmwire.backends import ZeroMQMesh

# Existing NATS deployment; client options such as user_credentials pass through.
async with Swarm(NatsTransport(servers='nats://server:4222')) as swarm:
    result = await swarm.request('existing-worker', {'question': 'hello'})

# Caller owns an explicitly supplied mesh and closes it after its Swarm.
mesh = ZeroMQMesh('/tmp/unique-swarm-directory')
try:
    async with Swarm(mesh) as swarm:
        await swarm.register('worker', existing_async_handler)
        result = await swarm.request('worker', {'question': 'hello'})
finally:
    await mesh.close()
```

## Direct and adaptive infrastructure

```python
from swarmwire import AdaptiveTransport, PeerTransport, Swarm

async with Swarm(AdaptiveTransport(unix='/tmp/swarmwire.sock', capacity=512)) as swarm:
    await swarm.register('worker', existing_async_handler)
    # Registration prepares the caller's route before the first task.
    result = await swarm.request('worker', {'question': 'hello'})
```

The native broker issues one-use, destination-bound route tickets. The receiver
validates the ticket through its registered native connection and obtains the
source identity. Peers then exchange data directly. Tickets expire after ten
seconds; incomplete socket handshakes close after five seconds. This binds
senders in the trusted local swarm; it is not a multi-tenant authorization service.

Each endpoint reserves a quarter of its configured inbox for broker traffic,
one slot per peer link, and one local self slot when capacity > 2. The combined
queue remains bounded by the configured capacity. Peer capacity must be at least
2. The default is 256; use 512 for larger connected sets. Accepted application
bytes in the inbox are bounded to 64 MiB. Closed links with unconsumed messages
keep their slot reserved until those messages are consumed.

`AdaptiveTransport` uses direct sockets for sparse routes and native batching
when at least 16 peer routes are warm. This threshold is configurable with
`multiplex_threshold`. Replies can use the incoming broker path; recent broker
traffic biases automatic routing for 100 ms. This is a heuristic and can be
forced with `route_policy='direct'` or `'broker'`. Unix peers with a TCP-only
control connection keep direct data routing instead of moving it onto the
slower TCP broker path. Benchmark metadata identifies every actual data topology.

Standalone endpoints can call `await endpoint.preconnect('worker')`. Swarm
registration does this during setup, and automatic mode stops adding eager
peer routes once it can multiplex. The native control connection remains
required for registration, tickets and artifacts; its loss closes the endpoint.
A data-peer disconnect fails only requests waiting for that peer. Explicit
peer and routed clients interoperate through the native path.

The public `Client` now uses direct protocol callbacks. `StreamClient` exposes
the previous stream implementation. Fast clients remain compatible with the
0.2 broker; peer/adaptive transports need the upgraded 0.3 executable.

## Binary RPC

Use `RawRPC` when your application already has a byte format such as Protobuf
or MessagePack. It carries a 13-byte correlation header and resolves reply
futures directly, avoiding JSON and receive-queue wakeups for responses.

```python
from swarmwire import PeerEndpoint, RawRPC

endpoint = await PeerEndpoint.connect('caller', unix='/tmp/swarmwire.sock')
rpc = RawRPC(endpoint)
await endpoint.preconnect('worker')
reply = await rpc.request('worker', b'encoded-task')
await endpoint.close()
```

On the worker, create `RawRPC(endpoint)` and await `rpc.serve(handler)`, where
`async handler(data, message)` returns bytes. A maximum application payload is
1 MiB minus the 13-byte header. Handler failures return `RemoteError`. Timeouts
clean pending correlation records; late replies are discarded. Choose one
`Agent` or `RawRPC` facade per endpoint. The generic Agent JSON API remains the
framework-facing option and needs no framework internals to change.

## Generic async frameworks

A handler accepts `(payload, envelope)` and returns a JSON dictionary. The
envelope includes the connection-bound sender and request ID. An async function
with a single input can be wrapped directly:

```python
from swarmwire import CallableAdapter

adapter = CallableAdapter(
    existing_agent.ainvoke,
    input_mapper=lambda payload, message: payload['input'],
    result_mapper=lambda output: {'answer': output},
)
await swarm.register('worker', adapter, concurrency=4)
```

Keep blocking entry points off the event loop with an explicitly managed thread
or process. Choose concurrency based on your framework's mutable state and model
rate limits. This layer does not make stateful agent instances safe for concurrent use.

## LangGraph

```python
from swarmwire import LangGraphAdapter

adapter = LangGraphAdapter(
    compiled_graph,
    input_mapper=lambda p, m: {'messages': p['messages']},
    config_factory=lambda p, m: {
        'configurable': {'thread_id': p['session_id']}
    },
    result_mapper=lambda state: {'answer': state['messages'][-1].content},
)
await swarm.register('graph', adapter)
```

Use distinct thread IDs for independent sessions. Results with Pydantic objects
are normalized recursively, but explicit result mapping keeps messages compact.
`examples/framework_swarm.py` runs a real compiled graph and generic worker with
no model calls. The tests also exercise actual checkpointed graph invocations.

## OpenAI Agents SDK

```python
from swarmwire import OpenAIAgentsAdapter

adapter = OpenAIAgentsAdapter(
    openai_agent,
    options_factory=lambda payload, message: {
        'session': sessions[payload['session_id']],
        'max_turns': 10,
    },
)
await swarm.register('sdk', adapter)
result = await swarm.request('sdk', {'input': 'Explore this question',
                                     'session_id': 'session-a'})
```

The adapter awaits `Runner.run` and maps `final_output`. Supply your own SDK,
model configuration and credentials. It detects interrupted runs and returns a
remote error; approval and resume logic must remain with the framework rather
than treating a paused run as completed. A custom runner/result mapper can
implement that application policy. The adapter's contract is tested with an
injected runner; live Agents SDK/model execution was not tested.

[Official running-agents API](https://developers.openai.com/api/docs/guides/agents/running-agents)

## CrewAI

```python
from swarmwire import CrewAIAdapter

adapter = CrewAIAdapter(
    lambda payload, message: build_fresh_crew(),
    input_mapper=lambda p, m: p['inputs'],
)
await swarm.register('crew', adapter)
```

The adapter uses native async `akickoff(inputs=...)`. CrewOutput's JSON/Pydantic
result is normalized, or raw text becomes `{'text': ...}`. A factory provides a
fresh crew per request; reuse an instance only when the framework allows it.
Older installations without `akickoff` need an explicit CallableAdapter fallback.
This integration has contract tests, not an installed/live CrewAI run.

[Official async entry points](https://docs.crewai.com/en/learn/kickoff-async)

## Artifacts and custom backends

Store large source material with `endpoint.store(bytes)` and send the returned
reference. Native endpoints share the broker's bounded artifact store; MemoryBus
shares an in-process store. For NATS/ZeroMQ across processes, inject a shared
provider implementing async `store(bytes) -> str` and `load(str) -> bytes`.
`MemoryArtifacts` only shares data inside one process; it is not distributed storage.

Implement the `Endpoint` protocol to add another backend: `name`, a bounded
`inbox`, `interceptor`, `on_close` callbacks, and async `send`, `receive`, `store`,
`load`, `stats`, `close`. `BufferedEndpoint` supplies queue/interceptor/closure
handling. A transport factory exposes async `connect(name)`. `Agent` adds the
JSON task/reply contract above that endpoint, and `Swarm` adds worker scheduling.
No framework needs to inherit from a SwarmWire base agent.

Transport identity is the source of `sender`; trusted-peer ZeroMQ/NATS naming
alone is not authentication. Use the deployment's authentication and identity
policy for remote or untrusted workloads. Runtime cancellations stop waits and
clean pending correlation records; remote execution cancellation requires a
separate application protocol.
