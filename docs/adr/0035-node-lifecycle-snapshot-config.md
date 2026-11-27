# ADR 0035: Node Lifecycle and Snapshot-Carried Config

## Status

Accepted.

## Context

Week 6 deliverable 2 needs safe node lifecycle on top of the joint-consensus
core: bootstrap, waiting joiners, adding as learner, promotion, replacement,
and per-message identity checks. Snapshots must carry the config so an
installer that missed config entries still converges.

## Decision

- `Node::bootstrap` writes a `LeaveJoint` entry at index 1 in term 0 with
  the founding voter set. Every founder builds the same bytes, so recovery
  replays the same membership everywhere. It is a `LeaveJoint` (not a new
  kind) so no decoder changes: it commits as a previous-term entry under
  the first leader and serializes like any other config change.
- `Node::joining` starts with empty storage and no voters. An unconfigured
  core accepts appends and snapshots from anyone, never campaigns, and never
  votes. The first `EnterJoint` (by log) or snapshot install (by restore)
  configures it.
- Adding a node is propose-config with the node as learner; the leader
  replicates by log, or by snapshot when the follower is behind the
  compacted base. `Node::propose_promote` moves a learner to the voter set;
  the sim nemesis (not the node) enforces the lag-for-N-ticks policy with
  `replication_match`.
- Replacing a node with lost storage means a new `NodeId`: a node that
  forgot its vote must never reuse its identity.
- `Node::receive` drops messages with a foreign `ClusterId` before the core
  and counts them in `cluster_rejects`. The core drops senders outside the
  effective config. Together every message is identity-checked at the node
  boundary; the sim delivery path applies the same check.
- Snapshots are v04: the v03 body plus a membership section (voters,
  learners, optional joint with its enter index). v03 and v02 still decode.
  The node embeds `membership_snapshot` on take and steps
  `RestoreMembership` on install. The core accepts the restore only when
  its base matches the just-installed snapshot.
- Config entries skip the state machine on apply but still advance applied
  and acknowledge the core, on the node and in the sim.
- `ExpireSessions` is a client-writable command; expiry counts surface in
  `KvResponse::SessionsExpired` and `expired_sessions_total`. The farm
  trigger for the expiry point arrives with the reconfiguration nemesis.

## Consequences

- Bootstrap entries flow through the normal config machinery: no special
  cases in commit, quorum, or recovery.
- Snapshot install is the only path that replaces membership wholesale,
  and only with bytes the core just verified.
