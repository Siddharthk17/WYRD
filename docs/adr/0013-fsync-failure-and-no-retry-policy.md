# ADR 0013: Fsync Failure and No-Retry Policy

## Status
Accepted

## Context
In 2018, the PostgreSQL development community uncovered the "fsyncgate" vulnerability in the Linux page cache. When an I/O writeback failure occurs (due to bad blocks, EIO, or controller timeouts), the Linux kernel marks the dirty pages clean or invalidates them from the page cache. If the application subsequently retries `fsync()`, the kernel returns success (`0`) because no dirty pages remain in the cache, even though the unwritten data was permanently dropped without reaching non-volatile physical storage. Retrying a failed `fsync()` therefore silently causes catastrophic data loss.

## Options Considered
1. **Retry `fsync()` on Error**: Loop with exponential backoff on `EIO` or `EAGAIN`.
2. **Re-dirty Buffer and Rewrite**: Attempt to locate the failed offsets in user memory and re-issue `pwrite()`.
3. **Fail-Fast Crash on Any Sync Error**: Treat any `fsync()` or `fdatasync()` error as a fatal condition; panic or terminate immediately without retrying.

## Decision
ANVIL adopts the **Fail-Fast Crash on Any Sync Error** policy:
- **Zero Retries**: The storage engine never retries an `fsync()` or `fdatasync()` that returns an error.
- **Immediate Crash**: Upon receiving an error from a sync operation, the storage engine triggers an immediate crash.
- **Recovery Resolution**: Upon node restart, the recovery scanner inspects durable state on disk, evaluates alternating hard state slots, and validates WAL CRC chains to recover cleanly from known good non-volatile state.
- **Simulation**: In `anvil-sim`, injected fsync errors immediately signal node failure and initiate crash recovery.

## Consequences
- **Safety**: Prevents silent data corruption and phantom durability acknowledgments caused by operating system page cache page invalidation.
- **Simplicity**: Eliminates complex, error-prone user-space write retry logic in the storage layer.
