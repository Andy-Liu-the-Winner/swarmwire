# Measured comparison: SwarmWire 0.2

**SwarmWire wins the tested local Unix-socket workloads.** In five randomized
trials, its streaming Unix path had 2.77× lower 64-byte-text Agent RTT than our
ZeroMQ IPC mesh adapter and 3.45× lower RTT than our NATS Core adapter. It completed
4.68× and 2.24× as many verified requests per second, respectively. These are
results on this machine and deployment topology, not universal product rankings.

The direct SDK comparison also favors SwarmWire Unix: 84.21 µs versus 157.12 µs
for pyzmq IPC REQ/REP and 303.17 µs for native nats-py request/respond with an
exact 64-byte raw payload. That experiment removes our Agent JSON wrapper from
all products. **NATS and ZeroMQ also win some comparisons**, shown below.

Recorded October 9, 2026: macOS 26.3 arm64, Python 3.10.12, uvloop 0.23.0,
orjson 3.13.0, Apple Clang 17 C++17 release build. Competitors: pyzmq 27.2.0,
libzmq 4.3.5; nats-py 2.16.0; official NATS server 2.15.1. No model or internet
calls occur in the timed workloads.

## Same Agent interface across transports

Values are **medians across five per-trial medians**, or medians of five
completed-request rates. Lower latency and higher throughput are better.
RTT is a complete task plus matching application reply, not one-way send time.

| Transport | 64 B text RTT | 4 KiB text RTT | 8-worker scatter/gather | 32-worker scatter/gather | Completed requests/s, 32 producers |
|---|---:|---:|---:|---:|---:|
| SwarmWire Unix streaming | **89.12 µs** | **97.96 µs** | **0.398 ms** | **1.274 ms** | **28,935** |
| SwarmWire Unix acknowledged | 101.25 µs | 113.33 µs | 0.486 ms | 1.674 ms | 13,307 |
| ZeroMQ IPC mesh | 247.29 µs | 254.21 µs | 1.298 ms | 5.214 ms | 6,182 |
| NATS Core | 307.29 µs | 316.12 µs | 0.876 ms | 2.452 ms | 12,926 |
| SwarmWire TCP streaming | 244.00 µs | 250.21 µs | 0.959 ms | 3.127 ms | 10,784 |
| SwarmWire TCP acknowledged | 296.08 µs | 302.38 µs | 1.130 ms | 3.954 ms | 5,419 |

All Agent cases use identical Python Agent envelopes, JSON encoding/decoding,
echo handlers, expected-sender/correlation verification, deadline machinery and
event loop. Payload text sizes exclude JSON envelope metadata. Setup and route
warmup are excluded. ZeroMQ uses persistent brokerless PUSH/PULL routes; SwarmWire
and NATS each use a separate broker process. Workers/callers are coroutines in
the same harness process. The native broker can route independent processes;
that mode is covered by integration tests, not by these throughput measurements.

Same-process cases are a separate deployment category:

| Same-process transport | 64 B text RTT | 32-worker scatter/gather | Completed requests/s |
|---|---:|---:|---:|
| SwarmWire MemoryBus | **39.25 µs** | **0.563 ms** | **78,382** |
| ZeroMQ inproc mesh | 200.58 µs | 5.061 ms | 6,590 |

MemoryBus requires all agents to share one event loop. Its gain cannot be claimed
for agents running in separate processes or on separate machines.

## Direct native SDK APIs

This separate experiment uses exact-size opaque byte payloads, no JSON or
SwarmWire Agent schema. It benchmarks `Client.send/receive`, pyzmq `REQ/REP`,
and nats-py `request` with responder `respond`. The responder runs in the same
Python harness; SwarmWire and NATS still use separate broker processes.

| Direct API transport | 64 B median RTT | Median trial p95 | 4 KiB median RTT |
|---|---:|---:|---:|
| SwarmWire Unix streaming Client | **84.21 µs** | **97.33 µs** | **88.08 µs** |
| ZeroMQ IPC REQ/REP | 157.12 µs | 194.29 µs | 157.21 µs |
| NATS Core request/respond | 303.17 µs | 327.38 µs | 311.92 µs |
| SwarmWire loopback TCP streaming Client | 241.21 µs | 274.50 µs | 243.08 µs |
| ZeroMQ inproc REQ/REP | 141.67 µs | 185.25 µs | 141.83 µs |

These are the products' Python async interfaces, not their fastest possible C++
or Go applications. Socket patterns and topology differ; the completed and
verified echo reply is the common measurement boundary. Inproc is shown for
context, not as a distributed-network baseline.

## Repeatability and losing cases

Every trial is retained. Ranges below span all five trials, not confidence
intervals. Desktop scheduling affects results, particularly tail latency.

| 64 B text Agent workload | Trial median RTT range | Completed-request rate range |
|---|---:|---:|
| SwarmWire Unix streaming | 79.88–91.71 µs | 23,072–29,806/s |
| ZeroMQ IPC mesh | 242.08–259.42 µs | 5,848–6,487/s |
| NATS Core | 301.88–308.67 µs | 11,254–13,354/s |

- NATS beat SwarmWire streaming TCP on concurrent throughput: 12,926 vs 10,784
  completed requests/s, and on 32-worker scatter/gather: 2.452 vs 3.127 ms.
- Direct ZeroMQ IPC REQ/REP beat SwarmWire TCP Client RTT: 157.12 vs 241.21 µs.
- Acknowledged Unix messaging sacrifices throughput for a positive queue receipt;
  streaming is the measured faster choice when application replies confirm work.
- Native SwarmWire has no clustering or persistence. These tests do not measure
  NATS JetStream, authentication/TLS overhead, multi-host links, large blob transfer,
  or durable/reliable execution semantics.
- There is no controlled end-to-end comparison against whole agent frameworks,
  no live model latency measurement, and no claim to beat untested shared-memory
  systems such as iceoryx2 or Zenoh.

Streaming `send` completes after a local stream write, ZeroMQ after native socket
acceptance, and NATS after local publish acceptance. SwarmWire acknowledged modes
wait for the broker's positive queue receipt. The shared measured boundary is
always the matching application reply. Negative streaming receipts fail the
connection; all timed requests must complete correctly. Core NATS subscription
errors abort a benchmark instead of silently counting lost messages.

## Reproduction and data

Five trials, seed 20261009. Each Agent RTT case records 500 samples after 100
warmups; scatter/gather records 50 rounds after ten warmups; throughput verifies
4,000 completed requests from 32 producers, each with one outstanding request.
Native SDK RTT likewise records 500 samples after 100 warmups. All 250 result
rows contain **305,000 verified measured request/replies**, excluding warmups.
Mode and workload order are randomized; native SDK case order is also randomized.
All peers/routes are ready before timing. There is no forced competitor delay,
per-message NATS flush, or best-run selection. This is one local benchmark run,
not an independent replication across machines.

```sh
.venv/bin/python -m pip install -r requirements-tested.txt
.venv/bin/python compare.py --nats-server /absolute/path/to/nats-server \
    --trials 5 --samples 500 --scatter-samples 50 --requests 4000 \
    --output comparison.json
.venv/bin/python summarize.py comparison.json --output comparison-summary.json
```

[All trials, versions, topology and source hashes](comparison.json) ·
[Aggregate values and trial ranges](comparison-summary.json) ·
[Full test environment lock](requirements-lock.txt) ·
[Current alternatives](LANDSCAPE.md)
