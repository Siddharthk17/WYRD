# ADR 0025: Week 5 same-class shrinker and failure taxonomy

## Status

Accepted.

## Context

Week 5 deliverable 7 asks for a delta-debugging shrinker over materialized
plans: remove fault events, shorten durations, drop clients and operations,
reduce cluster size where allowed. Every candidate must be re-run and must
keep the same failure class. The farm and `cargo xtask repro` need a stable
name for each violation kind, and the Week 4 canary must shrink to at most
30 events.

## Decision

- Failure classes are short stable strings. Consensus runs classify a caught
  panic by normalizing digit runs to `#`, so indexes and ticks never split
  one violation kind (`panic_failure_class`, `normalize_failure_class`).
  Workload runs classify as `convergence-audit`,
  `linearizability:<failing>:<prefix>`, or the panic class. Numeric detail
  is dropped; the surrounding words identify the kind.
- `shrink_plan_same_class` takes a `run: &dyn Fn(&Plan) -> Option<String>`
  rendezvous: only candidates reporting the original class are kept. A
  candidate that passes or fails differently is rejected, so the minimal
  plan still demonstrates the same violation.
- Soundness rests on per-event RNG streams keyed by event id at generation
  time, plus runtime draws from per-node streams and salt hashes that are
  never positional. Removing an event never reshuffles another's
  parameters, so a smaller plan replays its shared prefix exactly.
- Steps: remove one event, shorten one window, shrink cluster size to 3
  where every member reference already fits, zero network faults, then for
  workload plans drop dead proposals and halve clients and operations, or
  for consensus plans drop proposals one by one and shorten the run.
- The legacy `shrink_plan` bool predicate is kept unchanged for existing
  callers; it delegates nothing and its step order is untouched.
- `cargo xtask repro` accepts a literal encoding, a `.plan` file, or a
  JSON failure report with a `"plan"` field, dispatches workload plans to
  the KV runner and consensus plans to the consensus runner, shrinks
  same-class on failure, prints the minimal encoding, and writes
  `<input>.minimal.plan` next to file inputs with a follow-up repro
  command. Farm failure reports now carry the plan encoding and a
  file-based repro command.

## Consequences

- The Week 4 canary shrinks from 35 events (5 real plus 30 inert
  zero-delay paddings) to zero fault events with the same
  `linearizability:1:2` class, proving inert faults shrink away and the
  stale read needs no fault at all. The test asserts the class, the
  30-event bound, and that the minimal plan re-decodes.
- Coarse classes can under-shrink when two shapes share a name; that is
  conservative (never a wrong-class minimal plan) and documented in the
  shrinker docs.
