# ADR 0029: Week 5 snapshot reconcile and follower commit bound

## Status

Accepted.

## Context

Six kv-profile farm seeds (89738, 132635, 136637, 144041, 163047,
178942) failed `CommittedNeverLostAcrossCrashes` at restart with the
same signature: index 33 expected term 2, recovered term 1. A commit
trail showed the failing node compacting its log past index 33
(snapshot install jumping commit 32 to 42 in one step), while the disk
kept the pre-snapshot divergent frame. Installing a snapshot wipes the
core log but not the disk (ADR 0021 defers disk prefix deletion), so a
later crash replayed the stale fork term against the committed oracle.
A second, independent bug committed the same class of error in memory:
an empty `AppendEntries` (or duplicate resend) advanced the follower
commit to `min(leader_commit, last_index)`, committing the divergent
tail past the matched prefix instead of stopping at it.

## Decision

1. The follower commit on `AppendEntries` advances only to the matched
   prefix (`new_match`), never past it. An empty append matches only up
   to `prev_log_index`, so the divergent tail stays uncommitted until
   the leader overwrites it. Append and truncate paths already set
   `new_match` to the post-append tip, so their behavior is unchanged.
2. Snapshot install reconciles the engine prefix against the committed
   archive (`SimCluster::reconcile_snapshot_prefix`). The first index
   where the disk term disagrees with archive truth rewinds the engine
   through the existing truncate path and restages the covered range:
   committed entries come from the archive, uncommitted entries the
   core still holds are preserved, and only frames neither source can
   supply are dropped for the leader to re-replicate. Acknowledged
   writes above the restaged tip are pruned so recovery never demands
   an intentionally discarded frame.
3. The engine exposes `term_at` over its staged journal so the sim can
   find the first divergence without guessing.
4. Nodes that are not live keep their bytes: reconcile skips crashed,
   sidelined, and refused nodes, which the repair path owns.

## Consequences

- All six seeds pass with no change to passing seeds' behavior; the
  pinned-seed regression test fails with the reconcile call removed.
- Disk prefix deletion stays future work, but a snapshot install no
  longer leaves contradicting bytes behind: the disk always agrees
  with the committed oracle on the covered prefix.
- The residual hole is a sidelined node that queued a snapshot while
  frozen: its prefix reconciled at its last live install, and repair
  only rewrites corrupt frames, so a valid-but-stale prefix could
  still fail its restart. No farm seed has hit it; membership
  replacement in Week 6 closes it.
