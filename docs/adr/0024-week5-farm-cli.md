# ADR 0024: Week 5 parallel seed farm

## Status

Accepted.

## Context

Week 5 deliverable 6 asks for a parallel seed farm with profiles, JSONL
results, failing-seed artifacts, resumable runs, a time-boxed mode, and a
summary. This ADR records the design as implemented.

## Decision

- One entry point (`anvil-sim farm --seeds A..B --jobs N --profile NAME`)
  over three profiles: `smoke` (consensus plans, no storage faults),
  `kv` (KV workload), and `kv-storage` (KV workload with chaotic disks).
  The existing `xtask farm` stays as the CI smoke entrypoint and keeps
  calling the old per-harness runners; the new runner lives in
  `anvil-sim::farm` so both CLIs share it.
- Each seed runs isolated and single-threaded; parallelism is across
  seeds with a work-stealing queue so a `--time-budget` stop does not
  strand slow seeds. Panics are caught per seed and recorded with their
  failure class, never aborting the farm.
- Every completed seed appends one JSON object to `--out`
  (hand-rolled serialization, no new supply-chain surface). Failing seeds
  additionally get `<out>.failures/seed_N.plan` (the exact re-runnable
  plan) and `seed_N.json` (failure class plus a `cargo xtask repro`
  command). A final `{"type":"summary"}` object closes the file for
  nightly archiving.
- `--resume` skips seeds already recorded as passed, so interrupted
  nightly runs continue where they left off.
- The summary reports seeds run, simulated hours (1 virtual tick is
  1 ms), faults injected by class (from deterministic plan
  regeneration), buggify points with evaluation and activation counts,
  and total invariant-checker evaluations.
- `SnapshotStats` carries virtual ticks and invariant checks; the
  consensus runner returns a small `PlanRunStats` with the same.
