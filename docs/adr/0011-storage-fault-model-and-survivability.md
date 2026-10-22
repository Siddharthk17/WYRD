# ADR 0011: Storage Fault Model and the Survivability Rule

## Status
Accepted

## Context
Real disk drives and operating system page caches exhibit complex failure modes: dirty page loss on crashes, out-of-order write visibility, torn writes at sector granularity, bit rot, latent sector errors (EIO), misdirected writes, and gray failure stalls. Testing storage engines merely by restarting clean processes ignores the primary real-world causes of database corruption and state machine divergence.

## Options Considered
1. **POSIX File Abstraction Assumption**: Assume the operating system and filesystem handle data integrity, testing only uncorrupted process crashes.
2. **Ad-Hoc Fault Injection**: Randomly corrupt bytes or drop files during testing without formalizing physical failure boundaries or limits.
3. **Formal Storage Fault Catalog with Quorum Survivability Rule**: Catalog the exact physical and OS failure modes (9 fault classes) and implement an explicit Storage Fault Atlas in the simulator that injects these faults while respecting the physical survivability rule.

## Decision
ANVIL adopts the **Formal Storage Fault Catalog with Quorum Survivability Rule**:
- **Defended Faults**: Crash losing unsynced pages, unsynced write reordering, torn writes at sector granularity (512B and 4096B), misdirected writes, bit rot (read and write paths), latent sector errors, ENOSPC, fsync failures, and fsync stalls.
- **The Survivability Rule**: The simulator fault injector guarantees that it never destroys all valid copies of a committed entry across a majority (quorum) of nodes simultaneously. At all times, at least one node in any potential majority retains an uncorrupted copy of committed data.
- **Lying Fsync**: Defined as an optional Byzantine test class to verify that the storage engine detects unexpected data loss rather than silently continuing with corrupted state.

## Consequences
- **Rigorous Validation**: Simulator tests can safely inject high rates of media corruption and crash recovery without triggering false positive failures from physically impossible scenarios.
- **Zero Miracles**: The verification framework tests algorithm correctness and fault tolerance, not data fabrication from non-existent physical state.
