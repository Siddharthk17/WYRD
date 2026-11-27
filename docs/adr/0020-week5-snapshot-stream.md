# ADR 0020: Week 5 chunked InstallSnapshot in the core

## Status

Accepted.

## Context

Week 5 deliverable 2 needs log compaction to actually unblock lagging
followers: once the leader compacts past a follower's next index, the log can
no longer reach it, so the leader must stream a snapshot. The stream must
resume at chunk granularity, verify integrity, prefer the log whenever the
follower is reachable by log, and abort on leader change.

## Decision

- Two wire messages: `InstallSnapshotRequest` (one chunk with its CRC, the
  full-stream BLAKE3 digest, and the snapshot bounds) and
  `InstallSnapshotResponse` (success, installed match index, next wanted
  chunk). Per-chunk CRC catches corruption before buffering; the final digest
  gates the atomic install.
- A third replication state, `ProgressState::Snapshot`, entered only when the
  follower's next index is at or below the compacted base. The log path is
  always tried first. The snapshot stream reuses the in-flight window.
- The core never holds state-machine bytes on its own. The node supplies
  snapshot chunks through `Input::SnapshotReady`; the core asks through
  `Output::snapshot_requests` (one request per peer and need, duplicates
  suppressed) and hands verified streams back through
  `Output::ready_snapshots`. Follower install is atomic: the log is replaced
  by the snapshot's last index and term, commit jumps without emitting
  already-applied entries, and the node installs the state in the same step.
- The follower keeps a sparse chunk buffer, acks the smallest missing chunk,
  and discards the buffer on any stream change (leader, term, index, digest).
  A corrupt chunk or digest fails that stream without touching the log.
- `Input::CompactLog` drops the prefix with core-side bound checks (above
  base, at most applied and last). `RaftLog` records the base term so
  previous-entry checks still resolve at the compacted edge.
- Tick pumps snapshot streams so a full window stays in flight.

## Consequences

- Snapshot traffic is idempotent: duplicates and reordered chunks converge on
  the same install, and stale-term chunks are rejected.
- The node owns durability and install (deliverable S3); the core owns the
  protocol state machine, tested without any node code.
