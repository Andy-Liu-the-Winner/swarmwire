# Existing tools and the place for SwarmWire

Research checked October 9, 2026. SwarmWire 0.3 includes direct peers, adaptive native multiplexing and binary
RPC. It can demonstrate local messaging wins
against the tested versions/configurations of ZeroMQ and NATS. Other products
below were researched, not benchmarked. This prototype has a much smaller feature
set than established messaging infrastructure.

| Existing tool | What it addresses | Comparison or integration |
|---|---|---|
| [ZeroMQ](https://zeromq.org/socket-api/) | Native sockets, routing patterns, brokerless messaging | Real pyzmq 27.2.0/libzmq 4.3.5 benchmarked. Supported backend. Direct REQ/REP is also measured, separately from our mesh adapter. |
| [NATS](https://docs.nats.io/learn/) | Messaging, deployment infrastructure; JetStream adds persistence | Real Core server 2.15.1 and nats-py 2.16.0 benchmarked. Supported backend. Wins over SwarmWire TCP on measured concurrent throughput. Native NATS request/respond also measured. |
| [Zenoh](https://zenoh.io/blog/2025-10-20-zenoh-imoogi/) | Distributed data routing and shared-memory options | Relevant candidate for distribution/shared memory. Not measured here; no speed ranking against it is claimed. |
| [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) | Shared-memory interprocess communication with language bindings | Candidate for large local payloads and lower copy overhead. Not measured here. SwarmWire makes no claim to beat it. |
| [A2A](https://a2a-protocol.org/latest/specification/) | Cross-system agent discovery, tasks, streaming and cancellation | A standard interoperability layer. SwarmWire is not an A2A implementation; a gateway would require explicit task/state mapping. |
| [AutoGen distributed runtime](https://microsoft.github.io/autogen/stable/user-guide/core-user-guide/framework/distributed-agent-runtime.html) | Distributed framework execution through its gRPC runtime | Existing agent runtime, not an equivalent bare-message benchmark. No whole-framework speedup claim. |

A2A and framework runtimes solve broader problems than this transport. Replacing
an existing transport only improves the part of an application's critical path
that actually waits for communication. Model reasoning, rate limits and unnecessary
sequential dependencies can dominate the total time.

SwarmWire's current advantage is a small API with a same-process path,
broker-authenticated peer sockets, adaptive native multiplexing and interchangeable
existing backends. Framework adapters
use public async entry points rather than modifying framework internals. This
makes attachment easy, but it does not automatically replace every internal
framework handoff or scheduler; applications must route those handoffs through it.

For multi-host work, use the NATS backend and its existing deployment features.
For a future native multi-host transport, clustering, discovery, authentication,
reconnection, cancellation and durable execution need explicit design and new
benchmarks. Future shared-memory or additional language bindings should first be
validated against representative swarm workloads rather than inferred from
language choice alone.

[Official NATS version tested](https://github.com/nats-io/nats-server/releases/tag/v2.15.1).
The external server binary is not redistributed in the source bundle.

The goal is the fastest useful agent communication layer. The defensible current
claim is limited to measured workloads and product configurations. Claims of
being the fastest worldwide would need independent replication, shared-memory
competitors, multi-host tests and equivalent operational guarantees.
