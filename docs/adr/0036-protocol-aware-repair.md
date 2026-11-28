# ADR 0036: Protocol-Aware Repair

## Status

Accepted.

## Context

Week 5 sidelines a node with mid-log corruption and rewrites its frames
from the committed archive, but the sidelined core is frozen: it answers
nothing, so the leader cannot re-replicate the range and the node
rejoins only through disk surgery. Week 6 deliverable 4 wants repair
through the protocol: the node keeps following, fetches the missing range
from peers with validation, and clears the flag.

## Decision

- The core owns a repair flag (`SetNeedsRepair` / `ClearNeedsRepair`,
  `repair_from`). While flagged the node never campaigns, never grants
  votes or pre-votes, ignores `TimeoutNow`, and clamps every append
  response to the last good index, so the leader re-replicates the
  unverified suffix instead of counting it. A flagged leader hands off
  and steps down immediately. Snapshot acks are not clamped: an install
  is digest-verified and atomic, and the sim clears the flag on success.
- The sim no longer freezes repair nodes. Detection (restart recovery)
  and the bad-snapshot path mark both the storage and the core flag; the
  heal path clears both once the range is verified whole.
- A failed snapshot install (digest, decode, or applied-index mismatch)
  sidelines the node on the same path with `repair_from` at the snapshot
  index. A later install covering the index clears both flags.
- The committed archive now carries entry kinds, so repair reconstructs
  config entries with their true kind instead of guessing `Normal` from
  payload presence. Every fetched entry passes `validate_repair_entry`:
  index match, archive agreement where the archive covers the index,
  otherwise a peer commit covering the index. Mismatches reject the
  entry; peer bytes are never asserted on.
- Repair still rewrites frames from validated sources and re-scans; the
  protocol keeps the core log current so the rewritten disk and the live
  log agree when the flag clears.
- A sidelined core is rebuilt from the recovered prefix so the node keeps
  following while flagged. It never rejoins on that prefix: after a
  successful rewrite the node crashes and the heal pass restarts it, so
  the post-repair core is always rebuilt from verified disk. Rejoining on
  the sideline-time log would risk campaigning without committed entries
  the truncation removed.
- A repair flag set while the node is paused or crashed is re-synced to
  the core on its next step; an unflagged core with a corrupt prefix
  could otherwise win an election it must lose.
- Rejection handling lowers a recorded match the conflict hint proves
  stale (torn tail lost acknowledged bytes after the ack). Without the
  ceiling, `next_index` can never backtrack below `match + 1`, so a
  follower whose disk shrank can never be re-replicated and a phantom
  commit can count the lost suffix. Lowering is always conservative.

## Consequences

- A repair node stays a follower, tracks the leader, and converges its
  in-memory log during repair; only disk bytes wait for the rewrite.
- The leader's recorded match for a repair node can stay stale-high until
  fresh clamped acks arrive, but only committed indexes use it, so no new
  commit counts the unverified suffix. A new election rebuilds progress
  from clamped acks.
