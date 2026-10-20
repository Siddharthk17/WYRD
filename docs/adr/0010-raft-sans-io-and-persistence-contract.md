# ADR 0010: Raft Sans-IO Consensus Core and Persistence Contract

## Status
Accepted

## Context
Standard Raft implementations often interlace disk I/O, network sending, and timer handling directly within consensus handlers. In such designs, when a node crashes or when disk fsync stalls, race conditions can cause nodes to acknowledge votes or log entries that have not actually been made durable, leading to silent state loss and split-brain scenarios. Furthermore, synchronous waiting on disk fsync blocks the consensus event loop, severely harming latency.

## Options Considered
1. **Synchronous In-Band Fsync**: Block the consensus loop until disk writes complete before returning responses.
2. **Asynchronous Futures with Runtime Schedulers**: Use Tokio tasks to await disk and network operations inside the consensus engine.
3. **Sans-IO State Machine with Two-Phase Persistence Tokens**: Keep consensus purely computational (`step(input, &mut output)`). When state changes require persistence, emit explicit `PersistOp` commands with unique tokens. Defer releasing acknowledging network messages or counting local match indices until an explicit `Input::Persisted { token }` notification is delivered.

## Decision
ANVIL adopts the **Sans-IO State Machine with Two-Phase Persistence Tokens** in `anvil-raft`:
- **Follower Responses**: When accepting entries or granting votes, the response message is queued in `pending_persists` and emitted only after `Input::Persisted` for that operation is returned by storage.
- **Leader Proposals**: Upon proposal, the leader appends the entry locally and broadcasts `AppendEntriesRequest` to followers immediately in parallel with disk persistence. However, the leader counts its own match index towards quorum commitment only after receiving `Input::Persisted` for its own append.
- **Term & Vote Monotonicity**: Hard state updates are always persisted before any message revealing the new term or vote is released.
- **Trace Projection**: Every step emits a `SpecProjection` capturing current term, role, vote, commit index, and log digest, matching variables in `specs/anvil_raft.tla`.

## Consequences
- **Correctness**: Eliminates all data loss and split-brain windows caused by uncommitted disk buffers across node restarts.
- **Performance**: Pipelined replication operates in parallel with local disk fsync without blocking the consensus loop.
- **Zero Allocations**: Output buffers are caller-owned and reused across steps via `output.clear()`.
- **Determinism**: The core executes identically in the deterministic simulator (`anvil-sim`) and the production daemon (`anvild`).
