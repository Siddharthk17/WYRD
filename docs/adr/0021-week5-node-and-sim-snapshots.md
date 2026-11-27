# ADR 0021: Week 5 node snapshot wiring and sim snapshot flow

## Status

Accepted.

## Context

The core streams snapshot chunks (ADR 0020) but owns no state-machine
bytes, and the simulator never took snapshots: `SimCluster` stepped bare
`RaftCore`s while the KV workload kept separate stores. Three gaps had to
close for snapshots to work end to end: the node must supply and install
snapshots, the sim must take snapshots with compaction and install them on
lagging replicas, and the storage engine must stay chained across installs
that wipe the core log but not the disk.

## Decision

- `anvil-node` owns a `SnapshotPolicy` plus entries/bytes-since-snapshot
  counters. It supplies snapshots on core requests and on policy triggers,
  compacts the covered prefix only after the snapshot is encoded, and
  installs core-verified snapshots atomically (decode, replace, advance
  applied, notify the core). A request the applied state cannot cover is a
  protocol violation and fails loudly instead of stalling.
- The sim workload keeps a committed archive (index to term and payload,
  including no-ops so backfills have no gaps) and per-node counters. After
  every apply pass it installs ready snapshots (with engine backfill from
  the archive and gap-buffer discard first), supplies pending requests, and
  takes policy snapshots with compaction. Every take asserts
  `SnapshotEqualsReplay` against a fresh replay of the archive.
- The apply loop never advances past a compacted prefix: a missing entry at
  or below the base stops the loop so the install carries the effects. A
  divergence with a compacted prefix replays from the node's last snapshot
  plus the live suffix; without one and with an empty base it replays from
  scratch; otherwise it waits for the install.
- The storage engine accepts overlapping appends idempotently (term-checked
  skip, rewind on conflict) and buffers post-install gap appends until the
  backfill lands, with a bounded buffer and token coalescing for duplicate
  deliveries. Truncations and installs discard unacked buffered batches;
  the core resends what it needs.
- The checker skips compacted entries in LogMatching, the oracle, and
  LeaderCompleteness: equality of the compacted prefix is established by
  `SnapshotEqualsReplay` at take time, not by log comparison.
- Core `Applied` notices are capped at the core's commit index so a
  restarted core (commit zero) never sees applied beyond commit.

## Consequences

- Snapshot traffic is verified at three levels: chunk CRC plus final digest
  in the core, replay equality at take time, and convergence plus
  linearizability at audit time.
- Disk prefix deletion stays future work: the engine retains the full WAL
  in the sim and relies on backfill plus the gap buffer for continuity.
- The install path replaces the old divergence replay comment that said
  snapshots would replace it in Week 5; they now do.
