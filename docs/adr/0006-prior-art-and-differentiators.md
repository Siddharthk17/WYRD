# ADR 0006: Prior Art and Architecture Differentiators

## Status
Accepted

## Context
Deterministic simulation testing (DST) has become the gold standard for mission-critical distributed systems. Several notable frameworks and industry implementations exist, including Turmoil, Madsim, Shuttle, Loom, FoundationDB, and TigerBeetle. ANVIL evaluates their architectures and identifies why it takes a distinct approach.

## Comparison of Prior Art

### 1. Turmoil and Madsim
- **Approach**: Virtualize Tokio primitives, mock `tokio::net::TcpStream`, and replace system timers within the async runtime.
- **Why ANVIL Differs**: Runtime shims require maintaining complex synchronization state machines that mimic OS sockets and scheduler interleavings. Subtleties in Tokio versions can cause divergence between simulated tests and production execution. ANVIL separates protocol logic from I/O entirely via a pure sans-IO design: the simulator and production daemon call the exact same `step()` functions without runtime shimming.

### 2. Loom and Shuttle
- **Approach**: Systematic concurrency testing and model checking via thread-scheduling permutation and atomic operation interception.
- **Why ANVIL Differs**: Loom and Shuttle target shared-memory multithreading race conditions at the instruction/lock level. ANVIL's consensus core has zero concurrency and zero threads internally; it runs as a single-threaded deterministic state machine driven by explicit event arrivals. Concurrency exists only at the cluster actor level.

### 3. FoundationDB
- **Approach**: Pioneered deterministic simulation testing for distributed ACID databases using Flow (an actor-based C++ dialect compiled to state machines).
- **Why ANVIL Differs**: FoundationDB requires custom compiler toolchains (Flow) and intertwines simulated networking with custom futures. ANVIL achieves deterministic simulation directly in standard Rust 2024 using sans-IO state machines, standard type systems, and decoupled event queues without custom language preprocessors.

### 4. TigerBeetle
- **Approach**: Deterministic state machine replication written in Zig. Operates sans-IO with explicit storage fault injection (read-path corruption, write-path corruption, latent sector errors), zero allocations in steady state, and extensive internal assertion checks.
- **Why ANVIL Differs / Aligns**: ANVIL strongly aligns with TigerBeetle's storage fault philosophy and zero-allocation discipline. However, ANVIL applies this model to standard Raft consensus in safe Rust (`#![forbid(unsafe_code)]`), with an explicit TLA+ specification, BLAKE3 streaming trace hashing, and an independent linearizability checker (`anvil-check`).

## Summary of ANVIL Differentiators
1. **Zero-IO Pure Core**: The consensus core does not know what a socket, thread, or file descriptor is.
2. **Deterministic Replay Guarantee**: Any failure replays bit-for-bit from a single 64-bit seed.
3. **100% Safe Rust**: `#![forbid(unsafe_code)]` across all crates without exceptions.
4. **Independent Stream Derivation**: Adding components or logging never shifts random streams for other actors.
5. **Linearizability Integration**: Every execution trace is validated by an integrated history checker.
