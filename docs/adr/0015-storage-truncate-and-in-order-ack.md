# ADR 0015: Byte-Exact Storage Truncation and In-Order Sync Acknowledgment

## Status
Accepted

## Context
`handle_truncate_and_append` cleared the pending batch and rewound the log
index without rewinding the segment write offset or the chained CRC seed, and
it never emitted a durable `Truncate`. A conflicting suffix therefore left
stale durable bytes behind the rewind point while new frames chained CRCs
from discarded state. Separately, WAL sync completions acknowledged tokens
out of order, so a later batch could report durable before an earlier one.

## Options Considered
1. **Truncate and rescan**: re-read the segment after every truncate. Sans-IO
   forbids it; the engine has no read path to already-written bytes.
2. **Rebuild offsets by replaying appends**: batch boundaries change segment
   packing, so a replay cannot reproduce the true layout from entries alone.
3. **Journal observed placements, rewind to them**: record every staged
   frame's segment, offset, CRC seeds, term, and length; record the recovery
   scanner's observed placements for pre-existing data; rewind by truncating
   the journal and cutting durable bytes with `Truncate`. Acknowledge WAL
   sync tokens only when every earlier sync completed.

## Decision
ANVIL adopts option 3:
- The journal is a `Vec` in log order so steady-state appends stay
  allocation-free after capacity is reserved; a `BTreeMap` would allocate per
  frame and break the zero-allocation budget the allocation tests enforce.
- Recovery placements recorded by `scan_recovery` seed the journal, so
  truncating into pre-restart segments rewinds to observed truth.
- Rewinding unlinks sealed segments above the target, emits `Truncate` when
  rewound bytes were already flushed, and retains pending tokens: responses
  for replaced entries are stale but idempotent, while dropping tokens would
  leak the Raft core's persist waits.
- WAL sync completions complete a set and acknowledge only the in-order
  head, so tokens always acknowledge a durable prefix.

## Consequences
- Truncation is byte-identical to staging only the kept prefix, verified by a
  regression test comparing offsets and CRCs against a reference engine.
- The journal holds one entry per live log index; Week 6 snapshot retention
  will bound it.
