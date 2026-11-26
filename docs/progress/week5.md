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
  the committed oracle on the covered prefix. Pinned seeds 89738,
  132635, 136637, 144041, 163047 and 178942 failed before and pass
  after; the pinned-seed regression test fails with the call removed.

### J. Repair Cascade and Torn-Write Sideline (ADR 0030)

- Repair replaces valid frames that contradict the committed archive
  and re-chains (re-sources) every frame after the replacement; the
  first unsourcable index aborts the file untouched.
- Restart sidelines a torn committed write for peer-validated repair
  instead of asserting, rejoining through the normal restart path.
  Ten of the eleven remaining kv-storage seeds heal through the
  cascade, one through the sideline.

### K. Durable Acknowledgments and Directory Sync (ADR 0031)

- Staged entries and votes are acknowledged on their own persist
  completion only; uncompleted work clears on crash.
- The engine syncs the directory after the data batch covering new
  files, and read-path rot lands on durable media so repair observes
  exactly what recovery observed.

### L. Hard-State Survivor Budget (ADR 0032)

- The atlas records refused nodes and vetoes further hard-state loss
  once the survivor budget is spent, so a second permanent loss
  cannot take the last electable quorum. All four dead-cluster seeds
  pass with the vetoed fault skipped.

---

## 2. Exit-Gate Commands and Real Output

TBD - farm running.

## 3. Benchmark Numbers and Hardware Profile

TBD - farm running.

## 4. Architecture Decisions Taken

- ADR 0019 through ADR 0032 (see docs/adr/).

## 5. Open Items and Future Work

- Session expiry, joint consensus, and repair completion migrate to the
  Week 6 inventory with the code that triggers them.
- The checker minimizer can name a dangling read instead of the stale
  read it kept (verdicts unaffected); tracked as a diagnostic-quality
  follow-up.

## 6. Anything Unsure About

TBD.
