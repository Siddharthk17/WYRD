# ADR 0012: On-Disk Formats and Recovery Classification

## Status
Accepted

## Context
A crash-resilient storage engine requires deterministic file formats that detect corruptions, torn writes, and misdirected writes at recovery time. Furthermore, the recovery engine must classify failures explicitly (such as distinguishing un-flushed trailing writes from catastrophic mid-log bit rot) rather than applying naive truncation or silent panics.

## Options Considered
1. **Dynamic Length-Prefixed Log**: Append variable-length records directly with a trailing CRC, and truncate from the first invalid record on startup.
2. **SQLite Rollback Journal / Write-Ahead Log Style**: Single-file WAL with atomic header updates.
3. **Sector-Aligned Chained-CRC Segments with Alternating Hard-State Slots**:
   - WAL: Preallocated fixed-size segments, sector-aligned frames zero-padded to sector boundaries, chained CRC32C incorporating segment ID and file offset.
   - Hard State: Fixed 2-sector file with two alternating slots (monotonically increasing sequence number + CRC32C) for atomic updates without renames.
   - Snapshots: Staged via temporary files, chunk CRC32C, and whole-file BLAKE3 cryptographic footer.

## Decision
ANVIL adopts **Sector-Aligned Chained-CRC Segments with Alternating Hard-State Slots**:
- **Torn Write Defense**: All WAL frames align to the sector size ($512$ or $4,096$ bytes). Any non-zero padding or split sector fails verification immediately.
- **Misdirected Write Defense**: Chaining the CRC32C across the previous record's CRC, segment ID, file offset, and entry metadata guarantees that relocated or swapped blocks fail verification.
- **Recovery Classification**:
  - `Clean`: Entire log and active hard state slot are verified intact.
  - `TornTail`: An invalid record followed only by unallocated zero sectors at the end of the log; safely truncate to the last valid index and continue.
  - `MidLogCorruption`: An invalid record followed by valid entries later in the log; mark node as `NeedsRepair`, halt leadership and voting, and await peer repair.
  - `HardState (1 Corrupt Slot)`: Recover cleanly from the alternative valid slot.
  - `HardState (2 Corrupt Slots)`: Halt startup and crash; operator must provision a replacement node under a new node ID to prevent duplicate votes.

## Consequences
- **Predictable Recovery**: Deterministic classification eliminates arbitrary data truncation.
- **Zero Allocations**: Scanning and decoding operate on preallocated sector buffers.
- **Atomic Safety**: Hard state flips atomically without filesystem rename or directory sync overhead.
