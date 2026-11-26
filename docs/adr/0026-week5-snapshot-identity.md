# ADR 0026: Week 5 snapshot identity is the carried state

## Status

Accepted.

## Context

The seed-0 canary run converged its logs (every node committed index 28)
but the convergence audit failed: one follower applied only through index
13 while its log base stood at 27. No panic fired and the run stayed
stuck through a 10x drain, which ruled out slowness.

## Decision

- The supplier labeled the snapshot stream with the applied marker (27)
  while the bytes carried state through 26: entry 27 was a leader NoOp,
  and NoOps advance the applied marker without touching the store. The
  follower core verified and installed the stream (base moved to 27)
  while the store install refused the applied-index mismatch, leaving the
  core and the store split permanently.
- `take_snapshot_for` now identifies the stream by the store's snapshot
  index (`KvStore::snapshot_index`, documented as always the applied
  index), asserts it never exceeds the applied marker, skips zero-state
  snapshots the core would reject, looks up the term at the carried
  index, and bounds compaction by the carried index so the retained log
  suffix always chains from the snapshot.
- Rationale: a snapshot is the bytes, and the bytes end where the state
  ends. Anything else lets the log and the machine disagree about what
  is durable.

## Consequences

- Seed 0 with the canary converges and fails only in the checker with
  `linearizability:1:2`, which is the intended canary signal. The audit
  failure is gone.
- The same off-by-one existed at index 1 (NoOp right after election) and
  was masked because no compaction had happened yet.
- Install-side guards stay strict: digest mismatch, decode failure, and
  applied-index mismatch still refuse. The fix removes the mismatch at
  the source instead of loosening the check.
