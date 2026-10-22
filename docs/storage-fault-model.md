# ANVIL Storage Fault Model

## 1. Overview and Doctrine

ANVIL relies on durable storage to persist Raft consensus state (term and vote) and committed replicated state machine entries. Storage hardware is fallible: operating systems buffer dirty pages, storage controllers reorder un-flushed sectors, media sectors deteriorate, and power interruptions induce torn writes.

This fault model explicitly formalizes what ANVIL assumes from the storage subsystem, what failure modes ANVIL defends against, and the operational invariants maintained during catastrophic crashes and media corruptions.

ANVIL follows the TigerBeetle and LazyFS philosophy:
- Storage faults must be cataloged prior to implementation.
- Corruption injection must occur on both read and write paths.
- The system must explicitly classify corruptions during recovery.
- Crashes and restarts run identical recovery logic across simulation and real hardware.

---

## 2. Hardware and Environment Assumptions

ANVIL makes the following base assumptions regarding physical disks and filesystems:

1. **Sector Granularity**: Disks read and write in discrete sectors of either 512 bytes or 4,096 bytes (4Kn / Advanced Format). The actual sector size is explicitly declared in each file header.
2. **Volatile Page Cache**: Any write that has not been acknowledged by a successful `fsync` or `fdatasync` system call exists in volatile memory (OS page cache or drive write cache) and is subject to total loss upon power cut or kernel panic.
3. **Flushed Persistence**: Once `fdatasync` or `fsync` returns successfully, all writes submitted prior to the sync call are durable on physical non-volatile media (excluding Byzantine "lying fsync" drives).
4. **Directory Persistence**: In POSIX filesystems, creating, renaming, extending via preallocation, or unlinking a file requires an explicit `fsync` on the containing directory descriptor to guarantee durability across power loss.

---

## 3. Storage Fault Catalog

ANVIL explicitly defends against nine classes of storage faults:

### Fault 1: Unsynced Data Loss on Crash
- **Behavior**: An ungraceful crash (`kill -9`, kernel panic, power outage) abruptly terminates execution. Any data written to the OS or disk cache that has not completed `fdatasync` is permanently lost.
- **Defense**: ANVIL adheres to the strict Raft persistence contract. Neither an append acknowledgment nor a vote response is dispatched over the network until the corresponding `PersistOp` has been completely synced to disk. Recovery treats un-flushed trailing writes as expected torn tails.

### Fault 2: Out-of-Order Unsynced Writes
- **Behavior**: Operating systems and disk controllers reorder dirty pages in the write cache. If a crash occurs between unsynced writes $W_1$ and $W_2$, the disk may contain $W_2$ while $W_1$ was lost, or any arbitrary subset of unsynced pages.
- **Defense**: ANVIL enforces ordering through barriers (`fdatasync`). Furthermore, each WAL record chains its CRC32C from the previous record and incorporates the file offset and segment ID. A modern record written without its predecessor will fail CRC verification and be treated as corruption.

### Fault 3: Torn Writes at Sector Granularity
- **Behavior**: A power outage occurs midway through writing a block across physical sectors. Some sectors within the write contain new data, while others retain old data or garbage.
- **Defense**: All WAL frames are strictly aligned to the configured sector size (512 or 4,096 bytes) and zero-padded. Each frame payload contains an independent CRC32C checksum. If a write is torn across a sector boundary, the CRC or zero-padding check fails immediately.

### Fault 4: Misdirected Writes
- **Behavior**: Due to controller firmware bugs or DMA race conditions, a write intended for block $A$ of file $X$ is written to block $B$ of file $Y$.
- **Defense**: Every WAL frame and snapshot chunk header embeds:
  - Magic byte identifier
  - Segment ID / File UUID
  - Logical log index and term
  - Physical file offset
  - Chained CRC32C incorporating segment ID and offset
  A frame written to the wrong file or offset fails header validation and CRC checks.

### Fault 5: Bit Rot and Corruption (Read & Write Paths)
- **Behavior**: Media degradation, cosmic rays, cable noise, or memory flips alter bytes in transit or at rest.
- **Defense**: 
  - Read Path: Every read verifies CRC32C (for WAL frames and Hard State) or BLAKE3 (for snapshots) before decoding or state mutation.
  - Write Path: Simulation injects corruptions during write staging prior to disk persistence to verify that corrupt frames are flagged on subsequent reads or peer recovery.

### Fault 6: Latent Sector Errors (EIO on Read)
- **Behavior**: Physical media sectors become unreadable (bad blocks), returning `EIO` or `EUNATCH` when accessed.
- **Defense**: I/O errors encountered while reading log segments trigger recovery classification. If the unreadable region is at the log tail, it is handled as a torn tail. If located mid-log, the node transitions to `NeedsRepair` and pauses voting/leadership until repaired from peers.

### Fault 7: Disk Exhaustion (ENOSPC)
- **Behavior**: The storage volume runs out of free space during preallocation or append.
- **Defense**: ANVIL preallocates WAL segments in fixed sizes (e.g., 64 MiB) ahead of time rather than expanding dynamically on the append path. If preallocation fails with `ENOSPC`, the storage engine crashes cleanly, refusing to accept new proposals while preserving committed state.

### Fault 8: fsync Failure (Postgres "fsyncgate" Defense)
- **Behavior**: An `fsync` call returns an error (`EIO`, `EROFS`). The Linux kernel may invalidate dirty page cache pages after an error, meaning a subsequent `fsync` retry could falsely report success while data was lost.
- **Defense**: **ANVIL never retries a failed fsync.** Any `fsync` or `fdatasync` failure immediately panics/crashes the process. The node restarts and executes clean crash recovery from the surviving physical media.

### Fault 9: Gray Failure (fsync Stalls)
- **Behavior**: Storage stalls or cloud EBS volumes freeze, causing `fsync` operations to block for seconds or minutes.
- **Defense**: In the Raft core, leadership lease renewal and peer replication are decoupled from disk wait states: the leader does not block its network event loop on disk I/O. If a follower or leader experiences prolonged storage stalls, Raft heartbeats detect quorum loss and elect a healthy peer.

### Optional Class: Lying fsync
- **Behavior**: Low-grade disks or faulty consumer SSDs acknowledge `fsync` before data reaches non-volatile media.
- **Defense**: This is classified as Byzantine behavior. While ANVIL cannot magically make volatile memory durable on power cut, ANVIL detects data loss upon reboot because missing sequence numbers, broken CRC chains, or invalid hard state slots expose that acknowledged writes vanished.

---

## 4. The Survivability Rule

Fault injection must be rigorous, but physical reality dictates that if every surviving copy of a committed entry is wiped across every node in a quorum, consensus safety cannot restore lost data.

> **The Survivability Rule**:
> The fault injector and nemesis never destroy every valid copy of a committed log entry across a quorum (majority) of nodes. 
> At all times, at least one node in any potential majority retains a valid, uncorrupted copy of every committed entry.

The ANVIL checker validates protocol correctness, crash safety, and corruption recovery—it verifies safety, not miracles. If a quorum loses all data, the cluster halts safely rather than fabricating state.
