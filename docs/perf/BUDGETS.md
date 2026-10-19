# ANVIL Performance Budgets

Per the ANVIL performance doctrine:
- All performance budgets are measured on day one.
- Each budget is set at **80 percent** of the measured value.
- Budgets ratchet upward only; regressions fail CI gates.
- Hot paths allocate zero heap bytes in steady state.

## Reference Hardware Baseline

Measurements were captured using `cargo xtask bench` in release mode (`--release`) on 2026-10-20:

- **CPU**: 12th Gen Intel(R) Core(TM) i5-12450H (8 cores / 12 threads, base 2.0 GHz, turbo up to 4.4 GHz)
- **Caches**: L1d 320 KiB, L1i 384 KiB, L2 7 MiB, L3 12 MiB
- **Memory**: 7.4 GiB RAM, DDR4/DDR5
- **Operating System**: Linux 7.2.5-3-omarchy x86_64 PREEMPT_DYNAMIC
- **Rust Toolchain**: `rustc 1.99.0` (edition 2024, opt-level 3)

---

## Performance Budgets (Week 1 Baseline)

| Component / Hot Path | Metric | Charter Floor | Measured Value | Budget (80% Baseline) | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Event Queue** (`BinaryHeapEventQueue`) | Push-Pop Pairs / sec | $\ge 10.00\text{ M}$ | **20.09 M ops/sec** | **16.07 M ops/sec** | **PASSED** (+100.9% over floor) |
| **Random Stream** (`DetRng`) | Raw `u64` draws / sec | $\ge 100.00\text{ M}$ | **345.12 M u64/sec** | **276.10 M u64/sec** | **PASSED** (+245.1% over floor) |
| **Binary Codec** (`Entry::decode_from_bytes`) | 1 KiB Entry Decodes / sec | N/A | **4.76 M entries/sec** | **3.80 M entries/sec** | **PASSED** (4.88 GB/s bandwidth) |
| **Binary Codec** (`Entry::decode_from_bytes`) | Steady-State Heap Allocations | **0** | **0 bytes / 0 allocs** | **0 allocs** | **PASSED** (verified by `CountingAlloc`) |
| **Simulation Kernel** (`SimKernel::run`) | Simulated Events / sec (1 core) | N/A | **11,816,249 events/sec**| **9,452,999 events/sec**| **PASSED** (11.8 M events/sec) |

---

## Allocation Invariants

Hot path zero-allocation invariants are continuously checked by `crates/anvil-types/tests/allocation_test.rs` under `CountingAlloc` (ADR 0008):

1. **`Entry::decode_from_bytes`**:
   - Decoding a 1 KiB entry from a shared `bytes::Bytes` slice performs zero heap allocations.
   - Batch decoding 1,000 entries from a network buffer performs zero heap allocations.
2. **`Frame::decode_from_bytes`**:
   - Decoding frame headers and slicing payloads performs zero heap allocations.
3. **Event Queue Push/Pop**:
   - Steady-state simulation events in `BinaryHeapEventQueue` reuse the backing heap buffer without re-allocation.

---

## Ratcheting Policy

1. When optimizations (such as SIMD varint decoding, 4-ary heap layout, or zero-copy batching) increase measured throughput, new baseline values must be recorded here.
2. The budget is adjusted to 80% of the new measured throughput.
3. Budgets are never lowered to pass a test or gate.
