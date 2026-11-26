# ADR 0030: Week 5 repair cascade and torn-write sideline

## Status

Accepted.

## Context

The full kv-storage farm leg (200,000 seeds, all faults on) failed 17
seeds in five classes. Seven were the stale snapshot prefix and the
follower commit bound (ADR 0029). The remaining eleven split into four
storage-fault classes with two shared roots: the disk can assemble a
log no replication could produce, and a crash can expose a torn
committed overwrite as durable truth.

Concretely: peer repair implanted a committed tail frame over a valid
but divergent prefix, so the restarted node held a stale fork under
branch truth and violated Log Matching in memory. Separately, a crash
between an in-memory overwrite and its fsync left the old fork term
durable while the node had already committed the new term, so recovery
replayed the stale term against the committed oracle.

## Decision

1. Repair replaces valid frames that contradict the committed archive,
   exactly like corrupt frames. A stale fork prefix is not branch
   truth, and preserving it under an implanted suffix would assemble
   an unreplicable log.
2. A replacement re-chains every frame after it: each following frame
   is re-sourced in turn (archive first, then a peer whose commit
   covers it), and the first unsourcable index aborts the file
   untouched. The old peek-ahead layout check cannot survive a
   re-chain, so the walk continues to unallocated space or the end of
   data instead.
3. Restart sidelines a torn committed write for repair instead of
   asserting. A recovered entry that contradicts the oracle at or below
   the pre-crash commit bound marks the node `NeedsRepair` at the
   mismatch index; the heal-phase repair implants archive truth and
   the node rejoins through the normal restart path. A mismatch the
   repair cannot source stays sidelined and never rejoins.
4. The restart-time oracle and acknowledgment assertions stay in place
   as backstops. Live divergence is still caught by the in-memory
   checker on every step, which re-fires if a core bug recommits a
   healed entry.

## Consequences

- The kv-storage leg went from 17 failures to zero new classes: all
  eleven remaining seeds pass, ten through the repair cascade and one
  through the torn-write sideline.
- Repair now rewrites whole suffixes instead of single tail frames.
  Every implanted byte is still branch truth (committed archive or a
  commit-covering peer), and any gap aborts the file before any write.
- Sidelined nodes still converge: archive-sourced repair completes in
  the heal window, so quorum and liveness audits see the rejoined
  node, not the sidelined one.
