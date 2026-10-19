# ADR 0005: Assertion Policy and Error Handling

## Status
Accepted

## Context
Silent corruption and partial state transitions in distributed systems lead to catastrophic split-brain or data loss. In production builds, `debug_assert!` statements are stripped by the compiler, leaving systems vulnerable to running with corrupted internal invariants. Conversely, panicking on external untrusted network input creates trivial denial-of-service attack vectors.

## Options Considered
1. **Pervasive Result Errors**: Return `Result<T, ProtocolError>` for all internal invariant violations.
2. **Debug-Only Asserts**: Standard Rust idiom using `debug_assert!`.
3. **Strict Invariant Asserts in Release + Typed Errors for External Boundaries**: Unforgiving protocol invariant assertions that crash loudly in both debug and release, coupled with strictly typed error returns for all external data decoders.

## Decision
ANVIL enforces a two-tier boundary:

1. **External Boundary (Network, Disk, User Input)**:
   - Untrusted input never triggers `panic!`, `unwrap()`, or unchecked indexing.
   - Decoders return strongly-typed errors using `thiserror`.
   - Length limits are validated before allocating memory buffers.

2. **Internal Invariant Boundary (Protocol State Machine, Storage State)**:
   - State machine and protocol invariants use `assert!` (not `debug_assert!`).
   - Invariants remain active in release builds with `opt-level = 3`.
   - Any violated invariant indicates a critical implementation bug; the process must crash loudly immediately to avoid persisting invalid state.

## Consequences
- Protects the cluster from running with divergent state.
- Minor runtime overhead for evaluating branch assertions in release builds. All asserted conditions are fast integer comparisons or state checks ($O(1)$).
