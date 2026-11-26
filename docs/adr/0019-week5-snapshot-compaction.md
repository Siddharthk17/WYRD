# ADR 0019: Week 5 snapshot format v03 and log compaction

## Status

Accepted.

## Context

Week 5 deliverable 1 needs snapshots that restore sessions, compaction that
drops the log prefix only after the snapshot is durable, and a chunk path that
stays off the consensus thread. The v02 format held only keys, tombstones, and
the state hash. The in-memory log assumed index equals position plus one, so no
prefix could be dropped.

## Decision

- Snapshot v03 (`ANVKVS03`) adds the `KvConfig`, the full session table
  (session id, last activity, highest sequence, memoized responses with
  sequence numbers), and the ordering ticks. Decode accepts v02 for old data
  and rejects a v03 whose embedded config differs from the node's config.
- `KvStore::snapshot_handle` is a plain clone. The persistent map shares
  structure, so the handle is `O(1)` and serialization walks live entries
  once, off the consensus path.
- `encode_snapshot_chunked` splits the byte stream into 64 KiB chunks with
  per-chunk CRC32C and a final BLAKE3 digest. `decode_snapshot_chunked`
  verifies CRC, order, and digest before decoding.
- `SnapshotPolicy` triggers on entries or bytes since the last snapshot and
  computes `compaction_index` as snapshot index minus the retained window,
  only when durable, and never beyond the applied index.
- `RaftLog` stores a `base_index`. Compaction drops entries at or below the
  target and rebuilds the digest over live entries. Compacted indexes report no
  term, so the leader must use a snapshot. `truncate_and_append` drops any
  head at or below the base instead of rejecting it.

## Consequences

- Session deduplication survives a snapshot restore. A duplicate replay after
  restore returns the memoized response at its original version.
- Log memory stays bounded through snapshots plus the retained window.
- Chunk corruption or a digest mismatch fails before any state is installed.
