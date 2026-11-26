# ADR 0022: Week 5 leader-transfer timeout and bounded read queue

## Status

Accepted.

## Context

Week 5 deliverable 3 needs a leader transfer that cannot wedge the cluster
and follower reads that cannot grow without bound. The core already stopped
proposals during a transfer and sent `TimeoutNow` once the transferee caught
up, but a transferee that never catches up (partitioned, lagging, or gone)
blocked proposals forever. The node's pending-read list was unbounded.

## Decision

- The core records transfer start and aborts a pending transfer after one
  election timeout, resuming proposals. The abort is unilateral and safe:
  a `TimeoutNow` already sent clears the transfer immediately, and any term
  change steps the leader down, which clears it too.
- Transfers to unknown peers or self are ignored instead of parking the
  transfer state: only a known follower can take over.
- Proposal gating during transfer is unchanged (one line already present)
  and is now covered by a regression test, since it is the load-bearing
  safety property of the transfer window.
- The node caps queued reads at 512 (`NodeConfig::max_pending_reads`),
  shedding the oldest waiter when full. The shed client's deadline fires
  and it retries, matching network-drop behavior, instead of the queue
  growing without bound under a stalled quorum.

## Consequences

- A failed transfer degrades to a normal leader serving writes after at
  most one election timeout; the transferee keeps replicating and a later
  transfer can retry.
- Shed reads are indistinguishable from lost messages to callers, so no
  client logic changes.
