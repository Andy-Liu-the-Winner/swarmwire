# Validation: SwarmWire 0.4

0.4 managed-lifecycle validation on October 11, 2026:

- 56 tests passed with the Python 3.10 dependency environment, including actual
  ZeroMQ, NATS, and compiled checkpointed LangGraph. Standard-library Python 3.13
  also passed, with three optional-dependency cases skipped.
- Managed startup verified native readiness, concurrent endpoint registration,
  session isolation, one-time cached builds, invalid compiler/cache errors,
  environment rollback, and startup-only fallback with no request replay.
- Application exceptions, cancellation during startup, malformed/late readiness,
  broker death, and abrupt parent exit all exercised process/connection cleanup.
- The one-command launcher connected an actual worker in a separate process and
  preserved nonzero application exit codes.
- A platform wheel was built from its source distribution, installed into a clean
  environment, and run outside the checkout with CXX pointing to a missing compiler.
  Python 3.10 and 3.13 native self-checks and the application launcher completed
  successfully. The bundled binary is executable by non-owner deployment users.
- All 56 tests also passed with AddressSanitizer and UndefinedBehaviorSanitizer
  on the native broker. The real LangGraph example completed using managed startup.
- Official MCP SDK initialization, discovery of six tools, and artifact store/load
  passed through the updated CLI on a managed native broker.
- Public CPU-only CI passed all four Ubuntu 22.04/macOS 14 and Python 3.10/3.13
  combinations, including regression tests, source-distribution builds, and clean
  compiler-disabled native wheel checks. The external NATS test requires a server
  binary and is skipped in CI; it passed in the full local environment.
  [CI results](https://github.com/Andy-Liu-the-Winner/swarmwire/actions/runs/38199937602).

These checks use CPU/local sockets. They allocate no GPUs and call no model API.
The 0.3 speed/cost discussion remains a historical benchmark/model, not a measured
0.4 GPU saving. Cross-machine fleet scheduling and 100,000 agents are not validated.

## Recorded 0.3 validation

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
