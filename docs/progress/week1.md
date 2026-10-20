# ANVIL Progress Report: Week 1 of 10

**Date**: 2026-10-20  
**Status**: Week 1 Deliverables Complete  
**Quality Gate**: `cargo xtask ci` PASS (All Gates Green)

---

## 1. What Shipped

### A. Workspace Foundation & Quality Configuration
- Initialized Cargo workspace with 14 crates strictly obeying the downward dependency rule: `anvil-types`, `anvil-raft`, `anvil-kv`, `anvil-storage`, `anvil-node`, `anvil-check`, `anvil-sim`, `anvil-device-fs`, `anvil-server`, `anvil-client`, `anvilctl`, `anvil-viz`, `anvil-bench`, `xtask`.
- Pinned Rust toolchain 1.99.0 with edition 2024 in `rust-toolchain.toml`.
- Configured `.cargo/config.toml`, `clippy.toml`, `deny.toml`, and `rustfmt.toml`.
- Enforced `#![forbid(unsafe_code)]` across all 14 workspace crates.
- Documented Architecture Decision Records [0001](file:///mnt/data2/WYRD/docs/adr/0001-workspace-crate-boundaries.md) through [0008](file:///mnt/data2/WYRD/docs/adr/0008-test-counting-allocator.md).

### B. Hand-Written Binary Codec & Types (`anvil-types`)
- Domain newtypes (`NodeId`, `Term`, `LogIndex`, `GroupId`, `ClusterId`, `SessionId`, `SeqNo`) with checked monotonic arithmetic and zero-cost abstraction.
- Zero-copy hand-written binary wire codec:
  - 16-byte fixed `FrameHeader` with magic bytes (`0x41 0x4E 0x56 0x4C`), versioning, frame types, and CRC32C integrity checksum.
  - Variable-length LEB128 unsigned (`u64`) and Zigzag-encoded signed (`i64`) varint encoders and decoders.
  - Zero-copy entry decoding directly slicing payload `bytes::Bytes` from shared buffers.
- Comprehensive property tests (`proptest`) verifying varint roundtrips, newtype serialization, entry encode/decode fidelity, and panics-free handling of arbitrary byte sequences.
- Three `cargo-fuzz` targets (`fuzz_varint`, `fuzz_frame_header`, `fuzz_entry`) with checked-in seed corpora.

### C. Simulation Kernel Foundation (`anvil-sim`)
- `SimTime`: Nanosecond-resolution discrete logical timestamp preventing physical clock leaks.
- `DetRng`: Pinned ChaCha8 PRNG with independent stream derivation per component, custom unbiased samplers (Lemire's algorithm for uniform bounds, ppm Bernoulli trials, fixed-point exponential and bimodal distributions, and Fisher-Yates shuffles).
- `BinaryHeapEventQueue`: Min-heap priority event queue with deterministic 3-tuple tie-breaking `(time, priority, event_id)` achieving 22.04 M push-pop operations/sec.
- Compact 32-byte `TraceEvent` records and ring-buffer `TraceRecorder` computing streaming incremental BLAKE3 digests.
- `Watchdog` monitoring logical time stagnation and event starvation.
- Thread-local `buggify!` fault injection subsystem with reproducible seed-derived activations.

### D. Consensus Validation Suite & Replicated Counter
- Implemented `ActorContext` trait allowing simulation actors to query time, schedule future deliveries, and record liveness progress.
- Toy replicated-counter consensus actor validating:
  - Byte-for-byte trace determinism across independent runs.
  - Strict FIFO tie-breaking for equal-timestamp events.
  - Time monotonicity invariant (logical time never moves backward).
  - Buggify fault injection and intentional message drops.
  - Watchdog triggering on stalled protocol progress or early queue depletion.

### E. Determinism Gate (`cargo xtask determinism`)
- 10 fixed golden seeds pinned in `tests/golden/seed_hashes.txt`.
- Multi-tier determinism gate verifying:
  - In-process repeatability (two consecutive runs within same memory space).
  - Out-of-process isolation (fresh child process launched under Address Space Layout Randomization (ASLR) and perturbed environment variables).
  - Cryptographic verification against golden BLAKE3 digests.
- Negative test fixture proving that introducing non-deterministic iteration order (via standard `HashMap`) causes the gate to fail immediately.

### F. Performance Budgets & Zero-Allocation Harness
- Test-only counting global allocator ([ADR 0008](file:///mnt/data2/WYRD/docs/adr/0008-test-counting-allocator.md)) in `crates/anvil-types/tests/allocation_test.rs` proving **zero heap allocations** for 1 KiB entry and frame decodes in steady state.
- Performance budgets specification and measurement baseline in `docs/perf/BUDGETS.md` ratcheting 80% budgets against real hardware.
- Standalone benchmark and budget verification CLI `anvil-bench` and `cargo xtask bench`.

### G. Unified Continuous Integration (`cargo xtask ci`)
- Single unified command `cargo xtask ci` enforcing:
  1. Format check (`cargo fmt --all -- --check`)
  2. Strict clippy (`cargo clippy --workspace --all-targets -- -D warnings`)
  3. Workspace test suite (`cargo test --workspace`)
  4. Documentation tests (`cargo test --doc --workspace`)
  5. Documentation build (`cargo doc --workspace --no-deps`)
  6. Zero-unsafe code audit (validating `#![forbid(unsafe_code)]` in all crates and 0 unsafe blocks)
  7. Determinism gate verification
  8. Performance budgets gate
- GitHub Actions PR CI workflow (`.github/workflows/pr.yml`) configured for x86_64 and aarch64 Linux.

---

## 2. Exit-Gate Commands & Real Output

### Gate 1: `cargo xtask ci`
```text
$ cargo xtask ci
===========================================================
               ANVIL Continuous Integration (CI)          
===========================================================

>>> Running Gate: Format Check (`cargo fmt --check`)...
>>> [PASS] Format Check (`cargo fmt --check`) (96.77ms)

>>> Running Gate: Clippy Lints (`cargo clippy -D warnings`)...
    Checking xtask v0.1.0 (/mnt/data2/WYRD/crates/xtask)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.41s
>>> [PASS] Clippy Lints (`cargo clippy -D warnings`) (504.77ms)

>>> Running Gate: Workspace Tests (`cargo test --workspace`)...
     Running unittests src/main.rs (target/debug/deps/anvil_bench-e0933054575fd577)
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/queue_benchmark.rs (target/debug/deps/queue_benchmark-6d244159a8de7314)
test test_queue_empirical_benchmark ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 12.95s

     Running tests/replicated_counter_test.rs (target/debug/deps/replicated_counter_test-99611d1bfd9f47e2)
test test_watchdog_trips_on_early_queue_depletion ... ok
test test_watchdog_trips_on_deliberately_stuck_protocol_stagnation ... ok
test test_replicated_counter_buggify_points_fire ... ok
test test_replicated_counter_tie_ordering ... ok
test test_replicated_counter_time_never_goes_backward ... ok
test test_replicated_counter_determinism_across_runs ... ok
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/sim_kernel_test.rs (target/debug/deps/sim_kernel_test-e45659ed06559cdd)
test test_sim_time_operations ... ok
test test_watchdog_stall_and_early_depletion ... ok
test test_event_queue_tie_breaking_deterministic ... ok
test test_trace_recorder_ring_buffer_and_blake3 ... ok
test test_sim_kernel_run_loop ... ok
test test_det_rng_stream_derivation_isolation ... ok
test test_buggify_registry_and_macro ... ok
test test_det_rng_samplers ... ok
test result: ok. 8 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/allocation_test.rs (target/debug/deps/allocation_test-3767d13660e6bb91)
test test_allocator_tracks_allocations ... ok
test test_codec_decode_1kib_entry_zero_allocations ... ok
test test_codec_decode_batch_1000_entries_zero_allocations ... ok
test test_frame_decode_zero_allocations ... ok
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/generate_seed_corpus.rs (target/debug/deps/generate_seed_corpus-b9e55284a7bab234)
test test_generate_and_verify_seed_corpus ... ok
test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/proptest_codec.rs (target/debug/deps/proptest_codec-49a0ca8dc6b3e277)
test prop_varint_u64_roundtrip ... ok
test prop_varint_i64_roundtrip ... ok
test prop_newtypes_roundtrip ... ok
test prop_entry_roundtrip ... ok
test prop_arbitrary_bytes_never_panics ... ok
test result: ok. 5 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.31s

     Running tests/types_codec_test.rs (target/debug/deps/types_codec_test-3e20eca4a38e9c13)
test test_dependency_allowlist ... ok
test test_group_cluster_session_seq_no ... ok
test test_invalid_magic_rejection ... ok
test test_crc_corruption_rejection ... ok
test test_entry_noop_roundtrip ... ok
test test_entry_roundtrip_bytes_zero_copy ... ok
test test_log_index_arithmetic ... ok
test test_node_id_non_zero ... ok
test test_term_arithmetic ... ok
test test_varint_i64_zigzag ... ok
test test_varint_overflow_rejection ... ok
test test_varint_u64_edge_cases ... ok
test result: ok. 12 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s

     Running tests/determinism_gate_test.rs (target/debug/deps/determinism_gate_test-2ec655f5f09941af)
test test_negative_test_hashmap_fixture_fails_gate ... ok
test test_determinism_gate_golden_seeds ... ok
test result: ok. 2 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.04s
>>> [PASS] Workspace Tests (`cargo test --workspace`) (13.94s)

>>> Running Gate: Documentation Tests (`cargo test --doc`)...
test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
>>> [PASS] Documentation Tests (`cargo test --doc`) (368.65ms)

>>> Running Gate: Documentation Build (`cargo doc`)...
   Generated /mnt/data2/WYRD/target/doc/anvil_bench/index.html and 13 other files
>>> [PASS] Documentation Build (`cargo doc`) (145.08ms)

>>> Running Gate: Zero-Unsafe Code Audit...
>>> [PASS] Zero-Unsafe Code Audit (14 crates audited, 0 unsafe blocks) (5.09ms)

>>> Running Gate: Determinism Verification...
=== ANVIL Determinism Gate ===
Evaluating 10 golden seeds (negative_test=false)...
  Seed 1                    -> 9d45e25baa8ccbe1fe3f8a5f8af3189bc783243f4dddfdc95468277327301bb5 [PASS]
  Seed 2                    -> d617512a0c8bb05188dac1e2787f0f82ab4ac8651d222718b3b6ae6f33a752b8 [PASS]
  Seed 42                   -> df1886a39af0cc5e6795a5af2216099c4b57e50c02e62c69816709fbe1e2a747 [PASS]
  Seed 100                  -> 2b11e8d3ab5b49e6b896e730b3db95981d173bec9e549bb3c79cf031f8a75500 [PASS]
  Seed 1337                 -> 516719cf67107fb7f380accd9b59a83ac4fbd10cdb91007df9c99f4b500f2d27 [PASS]
  Seed 2024                 -> 08a9abfd35e626854c88c9ed47097f26306454f528ac5ce96a256ca3305501da [PASS]
  Seed 54321                -> 2cb7c94d245d8299b6437eba75a374f5e67ee7b64faf140e90e764ff9b6879ed [PASS]
  Seed 999999               -> 656d02251d6b0fd2a97ee36fde7bb24de83e5f79711028a59a460fede834f1c2 [PASS]
  Seed 16045690984503098046 -> 788dcd7d5568c8cfe8a031d8751dce1cf076d35edf2707f3d3395935fa50bb15 [PASS]
  Seed 18369614218089748088 -> 6c9079d39cc56bcdaee3dd8448a4b49576ca24a097866c5e2c7bd9d6e9bbb819 [PASS]

ALL 10 SEEDS PASSED: in-process runs, out-of-process runs, and golden hashes match.
>>> [PASS] Determinism Verification (27.95ms)

>>> Running Gate: Performance Budgets (`cargo xtask bench`)...
===========================================================
             ANVIL Performance Benchmark Suite            
===========================================================
Benchmarking Event Queue (Binary Min-Heap)...
  -> 22.04 M push-pop ops/sec (500000 iterations)
Benchmarking DetRng (ChaCha8 stream derivation)...
  -> 432.54 M u64/sec (50000000 draws)
Benchmarking Codec (Zero-Copy 1 KiB Entry Decode)...
  -> 5.09 M entries/sec (0 allocations, 100000 decodes)
Benchmarking Simulation Kernel (1 Core Simulated Throughput)...
  -> 12182286 simulated events/sec (40000 events processed)

===========================================================
                     Summary vs Budgets                    
===========================================================
| Metric                          | Measured   | Charter Floor | Budget (80%) |
| :---                            | :---       | :---          | :---         |
| Event Queue (M ops/sec)         |    22.04 M | >= 10.00 M    |      17.64 M |
| DetRng Throughput (M u64/sec)   |   432.54 M | >= 100.0 M    |     346.03 M |
| Codec 1 KiB Decode (M/sec)      |     5.09 M | Zero Alloc    |       4.07 M |
| Kernel Throughput (events/sec)  | 12182286   | N/A           |    9745829   |
===========================================================
>>> [PASS] Performance Budgets (`cargo xtask bench`) (337.81ms)

===========================================================
              ALL CI QUALITY GATES PASSED!                
===========================================================
```

---

## 3. Benchmark Numbers & Hardware Profile

### Host Hardware & Environment
- **CPU**: 12th Gen Intel(R) Core(TM) i5-12450H (12 vCPUs: 4 P-cores + 4 E-cores, up to 4.40 GHz)
- **RAM**: 7.4 GiB Physical Memory
- **Operating System**: Linux 7.2.5-3-omarchy x86_64
- **Rust Toolchain**: rustc 1.99.0 (b940084d7 2026-09-28), Edition 2024

### Measured Baselines vs Ratcheted Budgets
| Subsystem / Operation | Charter Floor | Measured (Week 1) | Ratcheted Budget (80%) | Status |
| :--- | :--- | :--- | :--- | :--- |
| **Event Queue Push/Pop** | $\ge 10.00\text{ M ops/sec}$ | **22.04 M ops/sec** | **17.64 M ops/sec** | **PASS** (+120% over floor) |
| **`DetRng` u64 Generation** | $\ge 100.0\text{ M u64/sec}$ | **432.54 M u64/sec** | **346.03 M u64/sec** | **PASS** (+332% over floor) |
| **Codec 1 KiB Entry Decode** | **0 Allocations** | **5.09 M entries/sec** (0 alloc) | **4.07 M entries/sec** | **PASS** (Zero Allocations) |
| **Sim Kernel (1 Core)** | N/A | **12,182,286 events/sec** | **9,745,829 events/sec** | **PASS** |

---

## 4. Architecture Decisions Taken

- [ADR 0001: Workspace Crate Hierarchy and Boundaries](file:///mnt/data2/WYRD/docs/adr/0001-workspace-crate-boundaries.md)
- [ADR 0002: Rust Toolchain and Edition Selection](file:///mnt/data2/WYRD/docs/adr/0002-toolchain-and-edition.md)
- [ADR 0003: Continuous Integration and Gate Automation via xtask](file:///mnt/data2/WYRD/docs/adr/0003-xtask-ci-automation.md)
- [ADR 0004: Strict Sans-IO and Determinism Lints](file:///mnt/data2/WYRD/docs/adr/0004-determinism-lint-enforcement.md)
- [ADR 0005: Hand-written Binary Codec over Serde](file:///mnt/data2/WYRD/docs/adr/0005-handwritten-binary-codec.md)
- [ADR 0006: Fuzz Testing Strategy with Seed Corpora](file:///mnt/data2/WYRD/docs/adr/0006-fuzz-testing-strategy.md)
- [ADR 0007: Event Queue Data Structure Selection](file:///mnt/data2/WYRD/docs/adr/0007-event-queue-data-structure.md)
- [ADR 0008: Counting Allocator for Zero-Allocation Hot Path Verification](file:///mnt/data2/WYRD/docs/adr/0008-test-counting-allocator.md)

---

## 5. Open Items & Future Work

1. **External Cargo Tools (`cargo-deny`, `cargo-audit`, `cargo-llvm-cov`, `cargo-geiger`)**:
   - The configuration files (`deny.toml`, audit rules, etc.) are committed. The standalone binaries are not installed on the local developer machine; in GitHub Actions CI, these will be executed via container/setup action or installed via `cargo binstall` in subsequent infrastructure hardening.
2. **Simulator Nemesis, Fault Schedulers, and Shrinker**:
   - Week 1 established the kernel, RNG, event queue, watchdog, and toy consensus validation. Network partitions, disk fault injection, and delta-debugging shrinker are scheduled for Weeks 2-4 as real Raft replication code is introduced.
3. **Fuzzing Continuous Farm**:
   - Seed corpora are generated and checked into `fuzz/corpus/`. Automated continuous 24/7 cluster fuzzing runs will hook into the scheduled nightly workflow.

---

## 6. Anything Unsure About

- **Bytes Unshared vs Shared Slicing**: When `Bytes::from(Vec<u8>)` is sliced for the first time, `bytes` converts the underlying `BoxSlice` into a reference-counted `Shared` buffer, incurring exactly 1 allocation during the warmup transition. In production steady state, frames arrive from network sockets or disk ring buffers as pre-existing shared buffers, resulting in strictly 0 allocations. The test suite correctly models steady state via a warmup pass.
