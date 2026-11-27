# ADR 0034: Joint Consensus and Learners in the Core

## Status

Accepted.

## Context

Week 6 deliverable 1 needs safe membership changes. The core must count
quorums across two configs at once, replicate to non-voting learners, and
ignore nodes the config removed.

## Decision

- `RaftConfig` gains `cluster_id` and `learners`. The local node is a
  learner exactly when it appears in `learners`; otherwise it votes.
- `EnterJoint` carries the target config and takes effect on append. The
  leader appends `LeaveJoint` automatically once the joint entry commits.
  The joint marker stays until `LeaveJoint` commits, which serializes
  changes and marks the removed-leader stepdown point.
- Quorums are AND: a majority of the old voters and a majority of the new
  voters. One shared helper counts votes, matches, heartbeat acks, and read
  acks, so elections, commits, CheckQuorum, and ReadIndex agree.
- A change is refused while a config entry is uncommitted or the leader has
  not committed an entry in its own term. Refusals surface as
  `MembershipEvent::Rejected`.
- Only effective voters grant votes and campaign. Learners follow the log
  but never vote and never start elections. Messages from senders outside
  the effective config are dropped and counted as `raft.unknown.sender`.
- A removed leader sends `TimeoutNow` to a remaining voter, steps down,
  and emits `RemovedSteppedDown`. Truncation rolls membership back when it
  removes the joint entry.
- Recovery replays config entries from the stored log, then clears state
  the commit index proves durable.

## Consequences

- Single-config clusters behave exactly as before: the joint path is idle
  and the AND rule reduces to the old majority.
- Snapshots do not yet carry the config; the node layer (S3) embeds it on
  take and restores it on install. Until then a snapshot install keeps the
  live membership.
