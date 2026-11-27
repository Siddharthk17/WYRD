# ANVIL Progress Report: Week 5 of 10

**Date**: 2026-11-27
**Status**: DRAFT - gates in progress
**Quality Gate**: `cargo xtask ci` PENDING (full run after farm)

---

## 1. What Shipped

### A. Snapshots and Compaction (ADRs 0019, 0026)

- O(1) snapshot handles via `imbl::OrdMap` clone; chunked encode off the
  consensus path with per-chunk CRC and whole-image blake3.
- Triggered by entries/bytes since last snapshot; log prefix compacted
  only after the snapshot is durable, never past the applied index.
- Snapshot identity fix (ADR 0026): the stream is labeled by the carried
  store state, not the applied marker past a trailing NoOp; a one-entry
  off-by-one left the core and the state machine permanently diverged.

### B. Chunked InstallSnapshot (ADR 0020)

- Flow-control window, chunk CRC, final blake3 verification, abort and
  restart on leader change, atomic install with log truncation.
- Leader prefers the log whenever the follower is reachable by log;
  `Snapshot` progress state added to the replication machine.
- Stale appends below the compacted base are skipped, never appended past
  the live tail (delayed conflict hints used to corrupt the log).

### C. Leader Transfer (ADR 0022)

- `TimeoutNow` transfer with bounded read wait queues; the leader stops
  accepting proposals during a transfer and aborts after one election
  timeout. Follower reads wait for applied (not just commit) so
  post-install gaps never serve stale data.

### D. Plan-Based Nemesis (ADRs 0023, 0025)

- Serializable `Plan`: cluster config, timed fault events, client
  operation list. Swarm generation: 30% one-or-two-class seeds, some
  full-mix seeds. Four phases: warm-up, chaos, heal, bounded audit.
- `shrink_plan_same_class` delta-debugs while preserving the failure
  class (invariant/checker/audit taxonomy with digit normalization).
- `cargo xtask repro` accepts literal encodings, `.plan` files, and JSON
  failure reports, and writes `<input>.minimal.plan` on failure.

### E. Farm (ADR 0024)

- `anvil-sim farm --seeds A..B --jobs N --profile NAME` over
  smoke/kv/kv-storage; per-seed isolation with cross-seed parallelism;
  JSONL results, failing-seed artifacts, resumable runs, time-boxed
  nightly mode, and a summary (seeds, simulated hours, faults by class,
  buggify/sometimes counts, invariant evaluations).

### F. Sometimes Coverage (ADR 0027)

- `sometimes!(sink, name)` marks rare paths without globals; the farm
  fails runs with `--require-sometimes-coverage` when a registered point
  never fires. Inventory enforced by a source-scan test.

### G. Text Trace Dump

- `anvil-sim trace --seed N --profile NAME [--out FILE]`: change-triggered
  spec projections, role changes, fault begin/end, crashes, restarts,
  snapshot take/install, repairs, the full client history with the
  checker's verdict. Identical bytes on reruns.

### H. Peer-Validated Disk Repair (ADRs 0027, 0028)
- A sidelined node rewrites corrupt frames from committed peer data with
  re-chained CRCs, verifies the rebuilt layout, and rejoins through the
  normal restart path. The storage fault atlas enforces a per-index
  corruption budget plus crash-fate and dir-loss gating.

### I. Snapshot Reconcile and Follower Commit Bound (ADR 0029)

- A follower commit on `AppendEntries` stops at the matched prefix
  (`new_match`): an empty append no longer commits the divergent tail
  past it.
- Snapshot install reconciles the engine prefix against the committed
  archive through the existing truncate path, so the disk agrees with
  the committed oracle on the covered prefix. Pinned kv seeds 89738,
  132635, 136637, 144041, 163047 and 178942 failed before and pass
  after; the pinned-seed regression test fails with the call removed.

### J. Repair Cascade and Torn-Write Sideline (ADR 0030)

- Repair replaces valid frames that contradict the committed archive
  and re-chains (re-sources) every frame after the replacement; the
  first unsourcable index aborts the file untouched. Log-matching
  violations assembled by partial implants (88700, 118313) heal
  through the cascade.
- Restart sidelines a torn committed write for peer-validated repair
  instead of asserting, rejoining through the normal restart path
  (31604).

### K. Durable Acknowledgments and Directory Sync (ADR 0031)

- Staged entries and votes are acknowledged on their own persist
  completion only; uncompleted work clears on crash. The missing-ack
  restarts (100260, 123509) pass: nothing is demanded that the disk
  never saw.
- The engine syncs the directory after the data batch covering new
  files, so init-created files stop relying on the one-shot init
  sync. Read-path rot lands on durable media, turning phantom
  sidelines the repair walk could never see into real damage it
  heals the same way.

### L. Hard-State Survivor Budget (ADR 0032)

- The atlas records refused nodes and vetoes further hard-state loss
  once the survivor budget is spent, so a second permanent loss
  cannot take the last electable quorum. All four dead-cluster seeds
  pass with the vetoed fault skipped.

---

## 2. Exit-Gate Commands and Real Output

Gate binary: `target/release/anvil-sim` md5 `df235f935d4cb2e1d3c810ba4c5b2961`
(profile runs below all use `--jobs 12` on the 12-core machine in
section 3; JSONL artifacts in `/tmp/opencode/week5-gate5/`).

- 500,000 clean seeds across profiles with all faults on:
  `anvil-sim farm --seeds 0..100000 --jobs 12 --profile smoke`
  (100000 passed, 0 failed, 5.21s, 19075.5 seeds/s, 0.89 simulated
  hours, 29225303 invariants);
  `anvil-sim farm --seeds 0..200000 --jobs 12 --profile kv`
  (200000 passed, 0 failed, 204.05s, 980.2 seeds/s, 25.23 simulated
  hours, 716661540 invariants);
  `anvil-sim farm --seeds 0..200000 --jobs 12 --profile kv-storage
  --require-sometimes-coverage`
  (200000 passed, 0 failed, 202.90s, 985.7 seeds/s, 25.32 simulated
  hours, 683546088 invariants, all registered sometimes-points hit).
- Sometimes coverage: kv-storage run reports
  `sometimes coverage: all registered points hit` (snapshot install /
  resend / send-start, term bump, transfer abort / timeoutnow, client
  crash, repair cleared, transfer begin, midlog needs-repair, torn
  tail). Smoke/kv legs never hit snapshot/transfer/storage points by
  design; the sometimes gate is evaluated on kv-storage.
- Shrinker: `cargo xtask repro <plan-file|plan.json>` reproduces the
  failing seed, shrinks same-class, and writes `<input>.minimal.plan`;
  re-running the minimal file still fails. Demonstrated on Week-5
  kv-storage failures (seed 31604 shrinks to 4 events, seed 118313 to
  0 events since storage faults alone trigger it; both minimal reruns
  fail identically). The Week 4 canary plan itself now passes outright
  because its bug class is fixed.
- `SnapshotEqualsReplay` runs after every snapshot take
  (`take_snapshot_for` asserts replay equality against the committed
  archive; a mismatch panics the seed, and the 500k-seed farm is
  clean).
- Trace dump: `anvil-sim trace --seed 89738 --profile kv` is
  byte-identical across runs (verified with `cmp`).

## 3. Benchmark Numbers and Hardware Profile

Hardware: 12th Gen Intel Core i5-12450H, 12 logical CPUs, 7 GB RAM;
rustc 1.99.0. All numbers from the gate binary above unless noted.

- Farm throughput: smoke 19075.5 seeds/s; kv 980.2 seeds/s;
  kv-storage 985.7 seeds/s (release, 12 jobs).
- Snapshot clone of 1M keys: 69ns (`anvil-kv` snapshot performance
  gate, `test_snapshot_perf_1m_keys_clone`, <1ms budget).
- Heartbeat cadence holds during a 12-chunk snapshot stream
  (`snapshot_stream_test` heartbeat-during-stream gate).
- Instruction-count benchmark regression gate and TLC spec check run
  inside `cargo xtask ci` (section: CI verdict below).

## 4. Architecture Decisions Taken

- ADR 0019 through ADR 0032 (see docs/adr/).

## 5. Open Items and Future Work

- Session expiry, joint consensus, and repair completion migrate to the
  Week 6 inventory with the code that triggers them.
- The checker minimizer can name a dangling read instead of the stale
  read it kept (verdicts unaffected); tracked as a diagnostic-quality
  follow-up.
- A sidelined node that queued a snapshot while frozen keeps its
  pre-install prefix until its next live install; repair only rewrites
  corrupt frames there (ADR 0029 residual).
- Voter-only nodes never emit a WAL batch, so their hard-state
  directory entry still relies on the init sync (ADR 0031 residual).

## 6. Anything Unsure About

- `cargo xtask ci` passes all gates end to end on the final tree
  (fmt, clippy `-D warnings`, workspace nextest, doc tests, doc
  build, zero-unsafe audit over 14 crates, determinism goldens,
  `anvil-bench --verify-budgets`, TLC spec check, 2000-seed smoke
  farm, 60s-per-target fuzz smoke, `cargo deny`, `cargo audit`,
  per-crate `cargo geiger`, feature powerset, per-crate coverage
  floors: types 97.97%, raft 98.93%, storage 96.64%, kv 99.19%,
  check 95.85%, sim 92.65%). Full log: `cargo xtask ci`,
  `ALL CI QUALITY GATES PASSED`.
- A scoped `cargo mutants -p anvil-types` run was started and
  interrupted by a host restart before producing a result; no mutants
  verdict is claimed. A wider mutation gate stays future work
  alongside the nightly 200,000-seed farm. Nothing else in this
  report is claimed without a pasted command output.
