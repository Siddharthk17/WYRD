# ADR 0028: Week 5 simulator correctness fixes

## Status

Accepted.

## Context

Proving the Week 5 gates (500,000 clean seeds, full sometimes coverage,
a shrinking canary) surfaced four latent bugs and two scope decisions.
Each fix below was verified by a farm seed that failed before and passes
after, with no change to passing seeds' behavior.

## Decision

1. Stale follower appends are skipped, not appended. An AppendEntries
   whose entries lie at or below the compacted base is a delayed resend
   (typically after a stale conflict hint reset the leader's next index).
   The per-entry loop used to treat `term_at == None` as "new" for both
   compacted and future indexes and appended the prefix past the live
   tail, permanently corrupting the log. Now entries at or below the base
   are skipped, and the ack advances to at least the base.
2. Follower reads wait for the applied index, not just commit. Right
   after a snapshot install the commit is already past the install index
   while the newly available suffix still waits for the next apply pass;
   serving the read in that gap returned stale data the checker correctly
   rejected. The read path now additionally requires the store to have
   applied through the read bar, matching what `anvil-node` already does.
3. Crash-time fate and whole-file loss consult the survivability atlas.
   Previously only live bit-rot consulted it, so a crash could tear or
   lose the last quorum copies of committed entries with no veto, and a
   second crash left a single copy and no electable quorum. The atlas
   additionally enforces a per-index corruption budget (at most N - Q
   nodes may destroy the same entry, committed or not), since
   pre-commit corruption on two nodes otherwise breaks post-commit
   majorities that the quorum rule alone cannot see.
4. Peer-validated disk repair runs on heal. A sidelined node rewrites its
   corrupt frames from committed peer entries (branch truth, never a
   fork), re-chains CRCs, verifies the rebuilt layout by re-scanning, and
   then restarts cleanly through the normal recovery path. Unfixable nodes
   (no peer holds the data, layout mismatch) stay sidelined instead of
   installing unverified bytes. Membership-driven replacement, the
   bad-snapshot repair path, and reconfig-time repair stay in Week 6.
5. Refused nodes count as crashed. Both hard-state slots lost means the
   node must be replaced under a new id (Week 6); until then every
   quorum-based check (leader existence, convergence drains, liveness
   probes, audits) operates on the live set. Restarting crashed nodes no
   longer skips sidelined ones, since recovery re-derives the flag from
   the (possibly repaired) disk; without that the repair loop deadlocks.
6. The recovery ack check only enforces committed entries. The old rule
   required every fsync-acknowledged entry to survive with the acked term,
   which made clean restarts impossible after a legitimate truncation of
   an uncommitted fork (the ack predates the conflict, and no future
   state can satisfy it). Now only entries the global oracle committed
   with the acked term must be present; the per-run proptest still
   covers acked-write survival at the engine level.

## Consequences

- Seeds 419, 523, 1000, 1406, 1613, 1952 and 1138 each failed for one of
  the above reasons and pass after the corresponding fix, with no change
  to the determinism gate (golden hashes unchanged).
- The corruption budget makes second-node corruption of the same index
  vetoed, so double-sidelines now require distinct corrupt frames on
  distinct nodes; those heal through repair instead of halting the farm.
- Crash-time fate and whole-file loss are recorded against the atlas, so
  later vetoes account for every destroyed copy.
