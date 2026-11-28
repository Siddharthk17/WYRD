# ADR 0033: Membership Config Wire Format

## Status

Accepted.

## Context

Week 6 adds joint consensus and learners. Config changes ride as log
entries, so the wire format must precede the consensus logic. The format
must bound every allocation before reserving memory.

## Decision

- Two entry kinds: `EnterJoint = 3` carries the joint view, `LeaveJoint = 4`
  carries the final voter set. Both take effect when appended.
- Payload is `ClusterConfig`: voter count, voter ids, learner count,
  learner ids. Each id is a varint. Counts are bounded by `MAX_VOTERS = 9`
  and `MAX_LEARNERS = 9` before any vector is reserved.
- Quorum is `n / 2 + 1`. Joint quorum needs a majority of the old voters
  and a majority of the new voters (AND, never OR).
- `ClusterId` is verified on every message via `ensure_cluster`. A node
  outside the config never votes and never campaigns.
- `ClusterConfig::new` sorts and dedupes, rejects empty voters, overlap,
  and over-cap sets with `MembershipError`.

## Consequences

- The Raft core (S2) consumes this format without its own decoder.
- Snapshots carry the same struct, so restore reuses the same bounds.
- Caps of 9 voters and 9 learners cover 3, 5, and 7 node clusters with
  headroom while keeping decode-time reservation small.
