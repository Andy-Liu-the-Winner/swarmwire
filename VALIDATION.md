# Validation: SwarmWire 0.3

Validated on macOS 26.3 arm64, Apple Clang 17, and Python 3.10.12 with the
isolated dependencies in `requirements-lock.txt`. Standard-library Python 3.13.1
was also exercised; optional external-framework/product tests require their extras.

The full suite covers 40 tests. The native transport/control tests passed with
AddressSanitizer and UndefinedBehaviorSanitizer. It exercises actual ZeroMQ
IPC/inproc, actual NATS Core, and actual compiled checkpointed LangGraph.

New infrastructure checks include:

- Real Unix and TCP peer task/reply exchanges, concurrent handlers and recovery
  after handler failures.
- A separate-process peer worker completed an application request with zero
  native-broker data-message forwards, demonstrating broker bypass.
- Route tickets are destination-bound and single-use; a forged sender handshake
  is rejected. The implementation also bounds ticket storage and expiry.
- One-slot peer receive windows preserve order for empty, binary and maximum-size
  payloads. Cancelled sends preserve the credit needed for later traffic.
- Peer disconnects fail requests to that peer. Discovery can recover when a
  previously missing worker joins. Python 3.13 shutdown closes connections before
  awaiting the listener, avoiding its changed wait_closed behavior.
- Adaptive dense routing forwards through the native multiplexer. Direct peer
  workers interoperate with routed clients while sharing a bounded inbox.
- Binary RPC verified 100 parallel requests, correlated errors, timeout cleanup,
  discarded late replies, empty payloads and its maximum 1 MiB-minus-13-byte payload.
- Fast protocol clients passed all 17 native transport tests against the prior
  0.2 broker, using capability fallback without credit piggybacking.

Earlier transport checks remain: fragmented/malformed frames, unique identity,
message order, full-inbox control progress, explicit backpressure including
streaming rejection, fan-in from 64 agents, disconnect wakeups, malformed Agent
envelopes, blob integrity/deduplication, batch partial failures and subprocess workers.
Runtime checks cover exploration completion order, early close, timeout cleanup,
closed full receivers, artifact LRU eviction and framework result mapping.

Real official MCP SDK initialized two stdio servers, discovered six tools and
completed 100 measured round trips after ten warmups. The compatibility smoke
is in `compatibility-smoke.json`; it is not a competitive benchmark. OpenAI and
Anthropic tool formats were tested locally. CrewAI and OpenAI Agents adapters
have contract fixtures; installed/live provider runs were not tested. No model
calls were made.

Release flags: C++17, `-O3 -DNDEBUG -Wall -Wextra -Wpedantic`. Sanitizer flags:
`-O1 -g -fsanitize=address,undefined -fno-omit-frame-pointer`. The included native
executable is macOS arm64. Linux epoll source is included but Linux compilation
and execution have not been validated here.

The expanded comparison reports every trial, versions, topologies, source
hashes, matching application replies and first-exchange latency. Model inference,
internet calls, durable storage, TLS and multi-host links remain outside these
measurements. This is an ephemeral local system; validation does not establish
exactly-once execution or production reliability guarantees.
