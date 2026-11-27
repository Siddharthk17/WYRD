# ADR 0031: Week 5 durable acknowledgments and directory sync

## Status

Accepted.

## Context

The last two kv-storage failures (seeds 100260, 123509) restarted a
node whose disk held no bytes for index 1 while the acknowledgment
record claimed it persisted, tripping
`RecoveredLogMissingAcknowledgedEntry`. Three independent defects
combined: the sim recorded the whole core log as acknowledged on any
persist completion (even a hard-state-only one); the one-shot init
directory sync left every file droppable when it failed, and the
engine never re-synced the directory afterward; and read-path rot
flipped bits in a throwaway clone the repair walk could never see.

## Decision

1. Acknowledgments track persist tokens. Staged entries and votes are
   recorded per token at submit time and acknowledged on their own
   completion only. Sync, backfill, and reconcile stage under
   sim-internal tokens (counted down from `u64::MAX`, never colliding
   with core tokens) with the same completion accounting.
   Uncompleted work clears on crash; the durable record survives.
2. The engine assumes an unsynced directory at startup and emits one
   `SyncDir` after the data sync of the next WAL batch. Directory
   entries never precede the bytes they name, and this also covers
   files predating the engine. Rollover and rewind unlinks mark the
   flag instead of syncing immediately.
3. Read-path rot lands on durable media. A phantom flip in the return
   clone alone would sideline a node for damage the repair walk reads
   as pristine and can never clear; real media damage is what repair
   is built to rewrite.

## Consequences

- The acknowledgment record now means what it says, and the atlas
  counts only truly persisted copies, so its vetoes stay exact.
- Both missing-prefix seeds pass with no change to passing seeds.
  Directory entries for fresh files still have a one-batch window in
  which entry loss applies, preserving the intended fault surface.
