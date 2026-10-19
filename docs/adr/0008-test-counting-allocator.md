# ADR 0008: Test Counting Allocator and Unsafe Code Scope

## Status
Accepted

## Context
ANVIL's performance doctrine requires that hot paths allocate nothing in steady state:
- Payloads are `bytes::Bytes` (reference-counted slices).
- Decoding frames and log entries must be zero-copy.
- Output and scratch buffers are reused.
- Regression tests must assert exact allocation counts using a counting global allocator.

Simultaneously, the charter doctrine states:
- `unsafe is forbidden: #![forbid(unsafe_code)] in every crate. An exception needs an ADR and human sign-off.`

Rust's standard library defines `std::alloc::GlobalAlloc` as an `unsafe trait` with `unsafe fn alloc` and `unsafe fn dealloc`. Consequently, any custom global memory allocator in Rust cannot be implemented without the `unsafe` keyword.

## Options Considered
1. **Third-party profiling tools (Valgrind / Massif / DHAT)**: External tools can count allocations, but cannot be asserted programmatically within `cargo test` unit assertions without external process wrappers and platform dependencies.
2. **Dedicated test allocator in test harness (`tests/`)**: Implement a minimal `CountingAlloc` wrapping `std::alloc::System` that uses atomic counters to track allocation counts and allocated bytes.
3. **Allowing `unsafe` in production crates**: Completely rejected per Charter Rule 1 & Rule 5.

## Decision
ANVIL retains `#![forbid(unsafe_code)]` unconditionally on all production crates and source modules (`src/`).

As an approved exception under this ADR, test integration binaries in `tests/` and test benchmarking fixtures may declare a `CountingAlloc` implementing `std::alloc::GlobalAlloc` to enforce the zero-allocation performance doctrine in continuous integration. The implementation is restricted strictly to atomic increments forwarded directly to `std::alloc::System`.

## Consequences
- Production crates maintain 100% safe Rust `#![forbid(unsafe_code)]`.
- Automated unit and integration tests (`cargo test`) directly assert zero allocations on critical hot paths (`Entry::decode_from_bytes`).
- Any allocation regression on the consensus or storage hot path fails CI immediately.
