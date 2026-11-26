# ADR 0032: Week 5 hard-state survivor budget

## Status

Accepted.

## Context

Four kv-storage seeds (20400, 48019, 101519, 140919) ended with two
nodes permanently refused and the survivor alone: one refusal was a
torn latest hard-state write older than the acknowledgment, the other
a both-slots corruption. Every hard-state fault path (write rot, read
rot, crash tear/loss, directory loss) bypassed the atlas, which only
gated WAL frames, so nothing vetoed the second permanent loss and no
electable quorum could ever form again.

## Decision

1. The atlas records refused nodes and vetoes further hard-state
   destruction once the `N - Q` survivor budget is spent. Recording
   happens at refusal time, when the loss is certain, never
   speculatively at fault time; the veto predicate itself draws no
   randomness, so passing seeds keep byte-identical streams.
2. Crash fate persists whole the hard-state sectors it would otherwise
   tear or lose under veto, and directory reconciliation keeps the
   hard-state file. Write and read rot skip vetoed targets.
3. A first permanent loss is still allowed: one refused node leaves a
   live quorum, which is exactly the recoverable envelope the Week 5
   gate exercises. Simultaneous coincident losses remain possible in
   theory; the farm has not produced one.

## Consequences

- All four dead-cluster seeds elect and converge: the second killing
  fault is vetoed and the survivor quorum carries the run.
- Refusal still means replacement in Week 6. The gate never weakens:
  faults fire everywhere except where they would make recovery
  definitionally impossible.
