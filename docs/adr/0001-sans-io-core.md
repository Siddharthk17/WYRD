# ADR 0001: Sans-IO Core Architecture

## Status
Accepted

## Context
Distributed consensus engines usually intertwine networking, timers, threads, and disk access directly with protocol state transitions. In testing, this makes deterministic reproduction of edge-case bugs notoriously difficult or impossible. Shims that mock operating system interfaces or intercept async runtime calls introduce overhead, timing divergence, and testing artifacts that differ from production execution.

## Options Considered
1. **Async Runtime Interception**: Intercept futures, sockets, and timers at the runtime boundary (similar to Loom or Madsim).
2. **Trait-based Abstract I/O**: Provide `AsyncRead`/`AsyncWrite` trait implementations backed by in-memory channels.
3. **Pure Sans-IO State Machine**: Protocol logic has zero I/O, zero clock access, and zero threads. The engine exposes a pure transition function: `step(input, &mut output)`.

## Decision
ANVIL adopts a pure sans-IO core (`anvil-raft`, `anvil-storage`, and `anvil-kv`).

The consensus core receives explicit inputs:
- `Tick`
- `Recv(from, message)`
- `Propose(command)`
- `Persisted(token)`
- `Applied(index)`

All side-effects are returned in a caller-provided reusable `Output` struct:
- Outbound messages
- Persistence requests
- Committed entries to apply
- Role change notifications
- Trace events

## Consequences
- **Testing**: The deterministic simulator drives the exact identical binary core as the production server without any virtual runtime overhead.
- **Performance**: Eliminates async state machine heap allocations in hot paths. Caller reuses input and output buffers.
- **Ergonomics**: The outer harness (`anvil-node`, `anvild`) is responsible for orchestrating network, disk, and timer drivers.
