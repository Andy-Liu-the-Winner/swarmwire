# Historical performance: SwarmWire 0.1

These measurements describe the earlier 0.1 implementation. Use
[COMPARISON.md](COMPARISON.md) for current 0.3 head-to-head results.

Recorded October 9, 2026 on macOS 26.3 arm64. These are local **round-trip**
measurements, not one-way delivery or LLM completion times.

Unix-socket run: Python 3.10.12, uvloop 0.23.0, orjson 3.13.0, MCP SDK 1.30.0.
The router was compiled with Apple Clang 17 and C++17 at `-O3`.
Native/Python latency runs contain 2,000 measured iterations after 100 warmups.
MCP contains 100 measured iterations after ten warmups.

| Path | Application payload | Median RTT | p95 RTT | p99 RTT |
|---|---:|---:|---:|---:|
| Native C++ clients | 64 B | 23.88 µs | 156.33 µs | 588.08 µs |
| Python binary clients | 64 B | 120.25 µs | 370.17 µs | 986.71 µs |
| Python JSON Agent wrappers | 64 B | 164.88 µs | 225.25 µs | 306.21 µs |
| Native C++ clients | 4 KiB | 16.92 µs | 21.88 µs | 39.00 µs |
| Python binary clients | 4 KiB | 120.88 µs | 226.25 µs | 427.54 µs |
| Python JSON Agent wrappers | 4 KiB | 176.42 µs | 232.33 µs | 388.21 µs |
| MCP: four SDK tool calls | 64 B | 7.69 ms | 10.67 ms | 14.24 ms |

The 64-producer fan-in run delivered and verified 20,000 distinct 64-byte
messages in 0.4723 seconds: **42,347 messages/second**. This includes the Python
producers, consumer, acknowledgements, and separate native broker.

## What these numbers establish

The native transport can exchange small messages in tens of microseconds on this
machine. Python wrappers add event-loop and function-call overhead. MCP adds two
stdio server processes, JSON-RPC handling, SDK validation, and four tool requests
per round trip. It is useful for compatibility but should not supervise every
runtime event in a latency-sensitive swarm.

Use Unix sockets and direct `Agent` or binary `Client` messaging for local swarm
traffic. Use MCP or provider tool calls when a model needs to initiate or inspect
a handoff. Keep the runtime's communication plane separate from model reasoning.

## Interpretation limits

- No inference or internet calls occurred. These results do not imply that an
  OpenAI or Anthropic model can respond in microseconds.
- Native clients run synchronously in one process; Python sender and echo worker
  run as coroutines in one process. Both use a separate native broker. Raw
  payload sizes match; JSON wrappers add envelope metadata and serialization.
- MCP uses the actual SDK and two separate stdio adapter processes; the test
  invokes send, receive, send, receive. This is not a model-driven tool loop.
- Desktop scheduling and competing work affect tails. Larger payloads sometimes
  measured faster because runs happened at different times. This is not evidence
  that larger messages are intrinsically faster.
- These are microbenchmarks of the delivered prototype, with no controlled
  baseline from an existing swarm framework. No framework speedup is claimed.
- Recorded TCP results use Python 3.13.1 without the optional extras, and 32
  producers. They change several variables and should not be treated as a
  controlled uvloop/orjson speed comparison.
- There is no durable delivery or multi-host networking in this version.

Full data: [Unix sockets](benchmarks-fast-unix.json),
[dependency-free TCP](benchmarks-standard-tcp.json).

Reproduce the Unix run with:

```sh
.venv/bin/python benchmark.py --fast --unix --mcp --samples 2000 \
    --agents 64 --messages 20000 --output benchmarks-fast-unix.json
```
