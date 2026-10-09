# Measured comparison: SwarmWire 0.3

**The adaptive layer won all 45 matched median comparisons in this run.**
That means Unix against ZeroMQ IPC, and loopback TCP against ZeroMQ TCP and NATS
Core, across 15 workloads per pairing. The comparison covers both the same
Agent interface and each product's binary/native RPC API. This is a measured
warm-connection result on one machine, not a claim to be fastest worldwide.

Recorded October 9, 2026, macOS 26.3 arm64, Python 3.10.12, uvloop 0.23.0,
orjson 3.13.0, Apple Clang 17 C++17 release build. Competitors: pyzmq 27.2.0 /
libzmq 4.3.5; nats-py 2.16.0 and official NATS server 2.15.1. Five randomized
trials retained all **885 result rows and 2,098,500 verified measured request/replies**,
excluding warmups. No model or internet calls occurred in the timed workloads.

## What changed in the infrastructure

- Persistent peer links remove the broker hop from unicast. A native discovery
  broker issues one-use, destination-bound tickets and verifies the sender before
  the peer connection becomes active.
- Direct event callbacks replace stream-reader and per-message credit tasks.
  Credits ride on outgoing data when possible; frames use one write.
- Adaptive routing uses the native multiplexer for dense traffic, where native
  batching can beat many independent peer sockets. The default switch threshold
  is 16 established routes and is configurable.
- Binary RawRPC carries a 13-byte correlation header, skips JSON and completes
  reply futures directly. Ready sends avoid unnecessary deadline timers. The
  shared outer request deadline still bounds every operation.
- Swarm registration prepares routes before the first request. Standalone peers
  expose preconnect explicitly. First-exchange measurements below disclose the
  cost when the route has not been prepared.

## Same Agent interface

Numbers are medians across five trial medians or five completed-request rates.
Lower latency and higher throughput are better. Application text lengths exclude
JSON envelope metadata. RTT includes a task and its matching application reply.

| Transport | 64 B text RTT | 4 KiB text RTT | 32 KiB text RTT | 32-worker scatter/gather | Completed requests/s, 32 producers |
|---|---:|---:|---:|---:|---:|
| SwarmWire adaptive Unix | 28.21 µs | 37.79 µs | 116.29 µs | 0.884 ms | 41,204 |
| ZeroMQ IPC | 252.38 µs | 262.21 µs | 324.71 µs | 5.532 ms | 5,662 |
| SwarmWire adaptive TCP | 97.17 µs | 109.29 µs | 209.92 µs | 2.311 ms | 15,367 |
| ZeroMQ TCP | 317.46 µs | 336.08 µs | 465.58 µs | 5.852 ms | 4,851 |
| NATS Core | 314.04 µs | 325.46 µs | 493.12 µs | 2.832 ms | 12,308 |

Adaptive Unix had **8.95× lower 64-byte-text RTT** than ZeroMQ
IPC and **11.13× lower RTT** than local NATS Core. The NATS/Unix
comparison uses different network transports; the pure TCP pairing below is the
matching-network comparison. Adaptive TCP had **3.27× lower RTT** than ZeroMQ TCP
and **3.23× lower RTT** than NATS Core.

All Agent cases share interpreter, loop, Agent JSON schema, handler, deadlines,
expected-sender/correlation verification and application bytes. Worker/caller
coroutines run in one Python harness. Broker processes are separate; peer and
ZeroMQ meshes are brokerless data paths. Separate-process peer operation is
covered by integration tests, not these throughput runs.

## Binary/native SDK APIs

This category removes the JSON Agent layer from all products. SwarmWire uses its
public RawRPC API, ZeroMQ uses direct REQ/REP, and NATS uses native request/respond.
Application bytes match; product-specific transport metadata differs. RawRPC adds
13 bytes to the stated application payload. NATS uses request(timeout=None) with
our common outer deadline, removing a redundant SDK timer while retaining the
same end-to-end deadline. Its request cleanup behavior was checked in the installed
SDK source. No per-message flush or artificial competitor delay is added.

| Native API transport | 64 B RTT | 4 KiB RTT | 32 KiB RTT | Completed requests/s, 32 producers | Completed requests/s, 128 producers |
|---|---:|---:|---:|---:|---:|
| SwarmWire adaptive Unix | 25.83 µs | 30.88 µs | 92.75 µs | 47,245 | 46,489 |
| ZeroMQ IPC | 162.46 µs | 166.21 µs | 190.96 µs | 7,163 | 6,257 |
| SwarmWire adaptive TCP | 96.50 µs | 100.12 µs | 187.83 µs | 15,449 | 15,851 |
| ZeroMQ TCP | 210.04 µs | 217.33 µs | 320.75 µs | 5,552 | 5,917 |
| NATS Core | 289.17 µs | 294.79 µs | 436.00 µs | 12,856 | 11,879 |

These are async Python SDK results, not a ranking against every possible native
C++, Rust or Go application. Replies are verified exactly; accepted local writes
alone never count as completed requests.

## Full matched workload audit

Each pairing covers Agent RTT at 64 B/4 KiB/32 KiB, scatter/gather at 8/32/128
workers, Agent completion rate at 1/32/128 producers, native API RTT at the same
three sizes, and native API completion rate at 1/32/128 producers: 15 cases.

| Pairing | Median wins | Median losses |
|---|---:|---:|
| Adaptive Unix vs ZeroMQ IPC | 15 | 0 |
| Adaptive TCP vs ZeroMQ TCP | 15 | 0 |
| Adaptive TCP vs NATS Core | 15 | 0 |

The audit uses median of trial medians/rates, not selected best runs. It does not
claim every individual trial or every tail percentile wins. Scheduling variation
can produce overlapping rate ranges and outlier tails; all p95/p99 and every
trial are available in the JSON files.

## Other configurations and deployment categories

All measured modes remain in the reports, including losing or non-default
configurations. One same-process path, two adaptive paths, two direct peer paths,
four acknowledged/streaming native broker paths, three ZeroMQ modes and NATS Core
were measured. Same-process memory results cannot be attributed to separate-process
or multi-host agents. Acknowledged sends include an extra positive queue receipt.

| Selected configuration | 64 B text Agent RTT | 32-worker scatter/gather | Completed requests/s, 32 producers |
|---|---:|---:|---:|
| MemoryBus, one process | 39.79 µs | 0.551 ms | 72,499 |
| ZeroMQ inproc, one context | 191.79 µs | 5.099 ms | 6,265 |
| Direct Unix peers | 27.96 µs | 0.984 ms | 43,509 |
| Direct TCP peers | 98.83 µs | 2.895 ms | 15,477 |
| Native Unix streaming broker | 38.00 µs | 0.711 ms | 54,429 |
| Native TCP streaming broker | 164.75 µs | 2.135 ms | 16,229 |
| Native Unix acknowledged broker | 52.46 µs | 1.161 ms | 24,281 |
| Native TCP acknowledged broker | 220.08 µs | 2.925 ms | 6,655 |

Direct TCP peers lose to NATS on some fan-out workloads; adaptive routing is the
configuration that closes that gap. A TCP mode can still lose to a Unix/inproc
mode on another product; those are different deployment categories. The named
layer has several configurations, and its automatic policy is a heuristic.

## First-exchange latency and exceptions

The following is the median of one first exchange per trial after endpoints have
been created. It includes lazy route setup/authentication or SDK response-subscription
setup. It is based on five observations, not a stable cold-start tail estimate.

| Transport | First 64 B text Agent exchange | Warm median | First 64 B native API exchange |
|---|---:|---:|---:|
| SwarmWire adaptive Unix | 235.38 µs | 28.21 µs | 244.17 µs |
| ZeroMQ IPC | 860.58 µs | 252.38 µs | 332.79 µs |
| SwarmWire adaptive TCP | 687.79 µs | 97.17 µs | 681.21 µs |
| ZeroMQ TCP | 1538.88 µs | 317.46 µs | 1240.38 µs |
| NATS Core | 349.96 µs | 314.04 µs | 456.33 µs |

**NATS beat adaptive TCP on first-exchange latency.** Route-ticket validation and
socket setup have a real cost. Preconnect/eager registration moves that work into
startup; it does not erase it. Some larger-payload first-exchange comparisons also
favor an incumbent. Consequently, “all 45 wins” applies to the defined warm median
workloads and is not an all-scenarios speed claim.

## Repeatability and interpretation

| 64 B text Agent workload | Five trial-median range | 32-producer rate range |
|---|---:|---:|
| SwarmWire adaptive Unix | 27.83–30.83 µs | 37,891–44,145/s |
| ZeroMQ IPC | 247.46–265.21 µs | 4,353–5,771/s |
| SwarmWire adaptive TCP | 95.58–98.79 µs | 14,236–15,602/s |
| ZeroMQ TCP | 316.88–331.33 µs | 2,501–5,334/s |
| NATS Core | 305.38–323.83 µs | 5,934–13,453/s |

Ranges span every trial and are not confidence intervals. These are measurements
from one desktop with randomized case order; no CPU affinity or independent
multi-machine replication is claimed. The earlier 0.2 measurement was 89.12 µs
for Unix streaming Agent RTT and 244.00 µs for streaming TCP. The upgraded adaptive
path measures 28.21/97.17 µs here. Those before/after runs used different case
matrices and timing, so they are indicative rather than a controlled version A/B.

Operational semantics remain ephemeral: local write/native socket/publish
acceptance differs across backends, and only matching application replies count.
Native failures can lose work; timeouts may occur after execution. Applications
need their own idempotency, supervision and remote-work cancellation. The native
and peer paths are local; use the NATS backend for an existing remote deployment.

No inference, internet, TLS, persistence, multi-host links, production reliability,
whole-framework scheduling or provider costs are measured. Zenoh/iceoryx2/shared
memory were not benchmarked. This release is the fastest in the listed matched
warm comparisons; it does not establish a universal fastest layer.

## Reproduce and inspect

Five trials, seed 20261009, randomized mode/workload order. Each RTT case records
500 samples after 100 warmups; each scatter case records 50 rounds after ten
warmups; each completion-rate case verifies 4,000 requests, one outstanding per
producer. Peers and routes are warmed before those measurements; first exchanges
are preserved separately. The harness starts and stops its servers and aborts on
incorrect replies or NATS subscription errors.

```sh
.venv/bin/python -m pip install -r requirements-tested.txt
.venv/bin/python compare.py --nats-server /absolute/path/to/nats-server \
    --trials 5 --samples 500 --scatter-samples 50 --requests 4000 \
    --output comparison.json
.venv/bin/python summarize.py comparison.json --output comparison-summary.json
```

[Every trial, topology, versions and source hashes](comparison.json) ·
[Full aggregate and ranges](comparison-summary.json) ·
[Prior 0.2 comparison](history/v0.2/COMPARISON.md) ·
[Validation](VALIDATION.md) · [Integration](INTEGRATION.md) ·
[Environment lock](requirements-lock.txt) · [Other current products](LANDSCAPE.md)
