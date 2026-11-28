# ADR 0037: Replication Requests From Unknown Leaders

## Status

Accepted.

## Context

The core drops messages from senders outside the effective config, so
removed servers and strangers cannot disrupt the cluster. A follower that
was partitioned while the cluster changed membership keeps the old
config, and the new leader is a stranger to it. The leader's appends and
heartbeats were dropped, so the follower never learned the config entry
that would make the leader a member. Seed 868 of the kv farm hit this:
node 1 held {1, 2, 3} while leader 4 and the rest held {1, 2, 4, 5, 6},
and node 1 stayed a PreCandidate with a five-entry log while the cluster
committed at index 20.

## Decision

- `accepts_sender` splits traffic. Election and transfer messages
  (pre-vote, vote, `TimeoutNow`) are still limited to members.
  Replication requests (`AppendEntries`, `Heartbeat`,
  `InstallSnapshot`) are accepted from any sender.
- Nothing else on the receiver changes: term checks, log matching, the
  contiguous-entries check, the commit clamp, and config decode
  validation all still apply. A stranger can append only where the local
  log matches, exactly as a member leader could.
- Vote granting stays restricted by `may_grant_vote`, so a stranger
  cannot win an election or raise its term through pre-vote.

## Consequences

- A stale follower converges once the leader's probe reaches it. The
  config entries it missed make the leader a member, and ordinary
  filtering resumes.
- A removed server can send appends, but it cannot commit them without a
  current quorum. The worst case is a temporary log suffix that the real
  leader truncates.
- `raft.unknown.sender` now counts only dropped election and transfer
  traffic.
