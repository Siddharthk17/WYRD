# ANVIL Progress Report: Week 4 of 10

**Date**: 2026-11-26
**Status**: Week 4 Deliverables Complete
**Quality Gate**: `cargo xtask ci` PASS (all gates green, output below)

---

## 1. What Shipped

### A. Full Network Fault Model (`anvil-sim/src/network.rs`, ADR 0017)

- Per-link latency distributions: constant, uniform, exponential tail,
  bimodal. Sampled from pure hash functions, no generator draws mid-run.
- Per-link bandwidth caps with queueing (no artificial delay while budget
  remains).
- Faults: drop, duplicate, reorder, delay spikes, connection resets with
  backoff. Corruption is modeled as drop plus reset.
- Partitions: random bisection, isolate one node, bridge node, asymmetric
  one-way cuts, flapping. `is_cut` is directed; symmetric helpers insert
  both directions so old behavior is unchanged.
- Two transport modes: ordered-lossy (TCP-like FIFO) and unordered
  (UDP-like deterministic hash order). The farm alternates per seed so both
  run in every configuration.
- Process faults: crash with restart delay, pause with stale-timer burst on
  resume, per-node tick-rate skew 0.5x to 2x.
- Recovery oracle bounded by each node's pre-crash commit index, so a
  legitimately divergent uncommitted suffix is not misreported.

### B. Replicated KV State Machine (`anvil-kv`, ADR 0018)

- Commands: RegisterSession, Put, Delete, Cas (expected version), Append
  (list semantics), ClockTick, ExpireSessions. Reads never enter the log.
- State in `imbl::OrdMap`, snapshot is an O(1) clone plus a walk.
- Per-key version is the log index of the last write. Tombstones preserve
  delete versions so reads of deleted keys report the delete index.
- Rolling blake3 state hash over applied commands for replica comparison.
- Bounds checked before allocation: keys at most 1 KiB, values at most
  1 MiB, configurable.
- Time enters only through ClockTick entries.
- Snapshot format v02 with tombstone section, encode and decode roundtrip.

### C. Sessions (Raft thesis chapter 6)

- RegisterSession returns a SessionId equal to its log index.
- Every request carries (session, seq, acked_up_to). Responses memoized per
  outstanding seq and returned on a duplicate instead of re-executing.
- Expiry through ExpireSessions after a configurable virtual idle time with
  bounded memory. Expired sessions answer SessionExpired, never silent
  re-execution.

### D. Read Paths (`anvil-node`)

- Leader ReadIndex with heartbeat-round confirmation; follower ReadIndex
  waits for the leader commit then reads locally; lease reads exist but are
  off by default and bounded by the drift configuration; stale local reads
  are explicit and excluded from checking.
- `anvil-node` owns RaftCore plus KvStore behind environment traits and is
  shared by the simulator and the server. Crash recovery stays in the
  cluster harness, which rebuilds the node from recovered state.

### E. Workload Generator with Swarm Profiles (`anvil-sim/src/workload.rs`)

- Each seed picks client count (1 to 8), key space (1 to 16 keys),
  operation mix, value sizes, think times, timeout and retry policy,
  client crashes, and redirect handling.
- Histories with Range run register-only so the whole-store model stays
  sound; mixed list-plus-range runs are deferred.
- Clients record invoke and return events with virtual time. Timed-out
  operations are recorded as indeterminate.
- Every run applies network, process, clock-skew faults, then heals and
  converges. Storage-fault variant runs SimDevice chaos.

### F. Linearizability Checker (`anvil-check`)

- Wing-Gong-Lowe search with memoization on (linearized set, model-state
  hash) and P-compositionality by key. Single-threaded and deterministic.
- Register model (Put, Get, Delete, Cas) and list-append model (Append,
  Get) with opaque versions from observed responses.
- Indeterminate operations may linearize at any point after invoke, or
  never; indeterminate returns impose no out-edges.
- Range histories use a memoized whole-store model on bounded inputs
  (at most 300 operations, 20M-step failure cap that reports failure,
  never success).
- On failure, output holds the minimal sub-history, the longest
  linearizable prefix, and JSON for the Week 10 visualizer.

### G. Checker Validation

- Brute-force permutation checker for histories of at most 8 operations:
  agrees with the real checker on 2,000 randomized tiny histories in the
  default suite and on 1,000,000 in the release-only ignored run.
- Random linearizable history generator with known verdicts, plus read
  corruptors that must fail (200/200 fail).
- Porcupine-compatible JSON export now carries operation results. The Go
  harness in `crates/anvil-check/tests/porcupine/` mirrors the register
  model with per-key partitioning. 10,000 deterministic histories, all
  verdicts agree. Nightly CI job `porcupine-cross-check` runs it.

### H. Convergence Audit and Farm

- After each run's heal phase, every live replica must share applied index
  and state hash.
- `cargo xtask farm --kv` runs the full loop (faults, workload, checker,
  audit) across seeds in parallel, with a `--storage-faults` variant.

---

## 2. Exit-Gate Commands and Real Output

### Gate 1: Continuous Integration (`cargo xtask ci`)

```text
$ cargo run -p xtask -- ci
>>> [PASS] Format Check (`cargo fmt --check`) (270.97ms)
>>> [PASS] Clippy Lints (`cargo clippy -D warnings`) (1.23s)
>>> [PASS] Workspace Tests (`cargo nextest run --workspace`) (45.59s)
>>> [PASS] Documentation Tests (`cargo test --doc`) (479.60ms)
>>> [PASS] Documentation Build (`cargo doc`) (2.00s)
>>> [PASS] Zero-Unsafe Code Audit (14 crates audited, 0 `unsafe` blocks) (19.49ms)
>>> [PASS] Determinism Verification (42.79ms)
>>> [PASS] Performance Budgets (`cargo xtask bench`) (4.99s)
>>> [PASS] Positive simulation: no invariant violations on any trace.
>>> [PASS] TLC Spec Verification (4.19s)
>>> [PASS] Simulator Smoke Farm (2000 seeds in 1.22s, 1647 seeds/sec)
>>> [PASS] Fuzz Smoke (60s per target) (431.13s)
>>> [PASS] Supply Chain (`cargo deny check`) (1.44s)
>>> [PASS] Advisories (`cargo audit`) (2.34s)
>>> [PASS] Unsafe Scan (`cargo geiger` x14 crates) (0 unsafe)
>>> [PASS] Feature Powerset (`cargo hack check`) (1.93s)
>>> [PASS] Coverage anvil-types (97.97% lines, floor 95.00%) (81.35s)
>>> [PASS] Coverage anvil-raft (96.95% lines, floor 95.00%) (81.35s)
>>> [PASS] Coverage anvil-storage (97.06% lines, floor 95.00%) (81.35s)
>>> [PASS] Coverage anvil-kv (97.95% lines, floor 95.00%) (81.35s)
>>> [PASS] Coverage anvil-check (96.25% lines, floor 95.00%) (81.35s)
>>> [PASS] Coverage anvil-sim (91.55% lines, floor 90.00%) (81.35s)
ALL CI QUALITY GATES PASSED!
```

Determinism golden hashes are unchanged from Week 3 (same 10 seeds,
same hashes, e.g. seed 1 `9d45e25b...01bb5`). The Week 4 network and
workload code paths do not alter the plan-based farm traces.

### Gate 2: Full KV Farm, 100,000 Seeds

```text
$ cargo run --release -p xtask -- farm --seeds 100000 --kv
ANVIL KV Seed Farm (workload plus checker plus audit)
Evaluating 100000 seeds across 12 parallel worker threads...
Farm Result: 100000/100000 passed in 13.10s (7634.9 seeds/sec)
FARM PASSED: All 100000 seeds completed with zero invariant violations.
```

### Gate 3: KV Farm with Storage Faults, 2,000 Seeds

```text
$ cargo run --release -p xtask -- farm --seeds 2000 --kv --storage-faults
Farm Result: 2000/2000 passed in 282.64ms (7076.1 seeds/sec)
FARM PASSED: All 2000 seeds completed with zero invariant violations.
```

### Gate 4: Canary Bug

```text
$ cargo test -p anvil-sim --test kv_workload_test test_canary_stale_read_is_caught
test test_canary_stale_read_is_caught ... ok
```

The canary serves reads from the most lagging live replica without
waiting. A standalone probe finds the first catch at seed 0
(`CANARY_CAUGHT_AT=0 ops=56`), inside the 1,000-seed bound. The canary
switch (`NodeConfig::skip_read_confirmation`) is off in all other runs.

### Gate 5: Checker Validation Suite

```text
$ cargo test -p anvil-check
test_checker_brute_force_agrees_on_tiny_histories ... ok      # 2,000 histories
test_checker_mutated_histories_fail ... ok                    # 200/200 corrupted fail
test_porcupine_export_shape ... ok

$ cargo test --release -p anvil-check --test checker_test -- --ignored
test_checker_brute_force_agrees_on_million_histories ... ok   # 1,000,000 in 1.37s

$ cargo test --release -p anvil-check --test porcupine_cross_test -- --ignored
test_porcupine_cross_check_ten_thousand_histories ... ok      # 10,000 histories, all agree
```

### Gate 6: Coverage and Mutation

Coverage floors are enforced in `cargo xtask ci` (anvil-kv and
anvil-check floors added this week at 95.00). Measured: anvil-kv
97.95%, anvil-check 96.25%, anvil-sim 91.55%.

Mutation testing with cargo-mutants was attempted on this host but the
workspace disk is 99% full (`/mnt/data2` has 1.3 GiB free) and the
mutants runner failed copying the 404 MiB `fuzz/target` tree
(`Disk quota exceeded`). No mutation score is claimed. The nightly
`mutation-report` CI job remains the scoring vehicle, as in Weeks 1-3.

---

## 3. Benchmark Numbers and Hardware Profile

Hardware: 12th Gen Intel i5-12450H, 7.4 GiB RAM, NVMe on /mnt/data2,
Linux 7.2.5-3-omarchy, rustc 1.99.0.

| Subsystem | Result | Gate | Status |
| :--- | :--- | :--- | :--- |
| Checker, 100k ops / 100 keys (release) | 411 ms | under 2 s | PASS |
| Checker, 1M ops bounded memory (release) | 4.28 s | bounded, early exit | PASS |
| Brute-force agreement, 1M tiny (release) | 1.37 s | green | PASS |
| Porcupine cross-check, 10k (release) | 0.43 s | all agree | PASS |
| KV farm throughput (release, 12 threads) | 7,635 seeds/s | smoke | PASS |
| Sim kernel (existing budget suite) | 12.16 M events/s | N/A | PASS |

Simulated seconds per real second: single-core release throughput is
1,220 seeds/s at a mean 141 virtual ticks per seed (15 warmup, measured
schedule span, 1-12 heal ticks; tick counts measured with a temporary
instrumented build, since removed). At 1 tick = 1 ms that is 172
simulated seconds per real second, 14 percent under the 200x
expectation. Cost breakdown per seed: 0.80 ms in sim ticks and workload
bookkeeping versus 0.012 ms in the checker, so the checker is not the
bottleneck. Per-tick cost is 5.8 us across 3 nodes (Raft step plus
store apply plus divergence check). Week 8 owns the optimization; the
gate text allows a miss with a profile, and this is it. At a
deployment-calibrated tick of 2 ms (150-tick election timeouts land at
300 ms, the usual Raft setting) the same measurement is 344x.

---

## 4. Architecture Decisions Taken

- [ADR 0017: Week 4 Network Fault Model](../adr/0017-week4-network-model.md)
- [ADR 0018: Week 4 KV Store, Sessions, Reads, Checker, Workload](../adr/0018-week4-kv-checker-and-workload.md)
- Supply-chain note: `deny.toml` now allows `MPL-2.0` because the
  charter pins imbl 7 and Week 4 is the first week to depend on it
  (anvil-kv snapshots, anvil-check utilities). Weak copyleft at file
  scope; recorded here and in the allowlist comment.

---

## 5. Open Items and Future Work

1. **Mutation scores**: blocked on host disk quota (see Gate 6).
   Nightly `mutation-report` job will score anvil-kv and anvil-check.
2. **Porcupine cross-check**: export format fixed and 10k agreement
   proven locally; the nightly `porcupine-cross-check` job runs it in CI.
3. **Lease reads**: implemented behind a disabled flag with unit tests;
   production tuning and Jepsen-style lease validation belong to Week 8.
4. **Mixed list-plus-range histories**: dedicated Range runs are
   register-only; mixed-model whole-store checking is future work.
5. **Cas in sim workloads**: value/index Cas paths are unit tested and
   checker validated (including the 10k Porcupine Cas batch); the swarm
   exercises Put/Delete/Append/Get plus Range.

---

## 6. Anything Unsure About

- Indeterminate operations impose no happens-before out-edges. This
  matches Elle and Porcupine practice: a timed-out operation may take
  effect after its response, so its return time constrains nothing that
  follows. Completed operations keep all edges.
- Tick-to-wall-clock mapping is ambiguous (see section 3). Readings:
  (a) 1 tick = 1 ms gives 172x, a documented miss with profile;
  (b) election-timeout calibration (150 ticks = 300 ms) gives 2 ms per
  tick and 344x. Recommendation: keep logical ticks wall-free in the
  core and let Week 8 set the mapping from measured election timing.
  No gate was loosened: the numbers above are measured, not adjusted.
- The canary serves reads from the most lagging live replica without
  waiting. That is exactly what removing the heartbeat confirmation does
  to a follower read, and the checker catches it at seed 0.
- During this week the coverage tooling twice reported contradictory
  line counts for one file (889 versus 1,109 instrumented lines).
  Cause: stale `.profraw` data accumulates in `target/llvm-cov-target`
  across runs with different binaries. `cargo llvm-cov clean` before
  the gate run resolves it, and the CI log above is from a cleaned run.
