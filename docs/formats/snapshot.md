# ANVIL On-Disk Format: State Machine Snapshots

## 1. Overview and Safe Write Protocol

Snapshots capture the committed state machine, client session table, and Raft configuration at a specific `LogIndex` and `Term`, enabling WAL truncation and fast follower catch-up.

To guarantee that a crash midway through snapshot creation never leaves a corrupt snapshot visible to the engine:
1. The snapshot is written to a temporary file: `snapshot-{last_index:016x}-{last_term:016x}.tmp`.
2. All data chunks are written with individual CRC32C checksums.
3. The whole-file BLAKE3 cryptographic hash is computed incrementally and written into the 64-byte footer.
4. The file descriptor is flushed with `fdatasync`.
5. The temporary file is atomically renamed to its permanent name: `snapshot-{last_index:016x}-{last_term:016x}.snap`.
6. The containing directory is synced via `fsync` on the directory file descriptor.

---

## 2. Snapshot File Layout

```text
+-------------------------------------------------------------------------------+
| Snapshot Header (64 bytes)                                                    |
+-------------------------------------------------------------------------------+
| Chunk 0 Header (16B) | Chunk 0 Data (N_0 bytes)                               |
+-------------------------------------------------------------------------------+
| Chunk 1 Header (16B) | Chunk 1 Data (N_1 bytes)                               |
+-------------------------------------------------------------------------------+
| ...                                                                           |
+-------------------------------------------------------------------------------+
| Chunk K Header (16B) | Chunk K Data (N_K bytes)                               |
+-------------------------------------------------------------------------------+
| Whole-File Footer (64 bytes)                                                  |
+-------------------------------------------------------------------------------+
```

---

## 3. Byte Layouts

### A. Snapshot Header (64 bytes)

```text
+-------------------+-------------------+-------------------+-------------------+
| Magic (8B)        | Version (2B)      | Sector Size (2B)  | Reserved (4B)     |
+-------------------+-------------------+-------------------+-------------------+
| Last Index (8B)   | Last Term (8B)    | Cluster ID (8B)   | Group ID (8B)     |
+-------------------+-------------------+-------------------+-------------------+
| Chunk Count (8B)  | Header CRC32C (4B)| Reserved (4B)                         |
+-------------------+-------------------+-------------------+-------------------+
```

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `magic` | `[u8; 8]` | 0 | ASCII `"ANVILSNP"` (`0x41 0x4E 0x56 0x49 0x4C 0x53 0x4E 0x50`) |
| `version` | `u16` (LE) | 8 | Format version (`1`) |
| `sector_size` | `u16` (LE) | 10 | Sector alignment size (`512` or `4096`) |
| `reserved_0` | `u32` (LE) | 12 | Must be 0 |
| `last_index` | `u64` (LE) | 16 | Raft `LogIndex` included in this snapshot |
| `last_term` | `u64` (LE) | 24 | Raft `Term` corresponding to `last_index` |
| `cluster_id` | `u64` (LE) | 32 | Identifier of the cluster |
| `group_id` | `u64` (LE) | 40 | Raft replication group ID |
| `chunk_count` | `u64` (LE) | 48 | Number of data chunks in this snapshot |
| `header_crc` | `u32` (LE) | 56 | CRC32C of bytes 0..56 |
| `reserved_1` | `u32` (LE) | 60 | Must be 0 |

---

### B. Chunk Header and Data

Each chunk contains an application payload (e.g., KV state entries, sessions):

```text
+-------------------+-------------------+-------------------+
| Chunk Index (4B)  | Chunk Len (4B)    | Chunk CRC32C (4B) | Reserved (4B)     |
+-------------------+-------------------+-------------------+-------------------+
| Chunk Payload Bytes (Chunk Len bytes)                                         |
+-------------------------------------------------------------------------------+
```

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `chunk_index` | `u32` (LE) | 0 | Monotonic chunk index ($0, 1, \dots$) |
| `chunk_len` | `u32` (LE) | 4 | Length of chunk payload |
| `chunk_crc` | `u32` (LE) | 8 | CRC32C of chunk payload bytes |
| `reserved` | `u32` (LE) | 12 | Must be 0 |
| `payload` | `[u8; len]`| 16 | Raw chunk content |

---

### C. Snapshot Footer (64 bytes)

The final 64 bytes of the snapshot file contain the cryptographic verification footer:

```text
+-------------------+-------------------+-------------------+-------------------+
| Footer Magic (8B) | Total Length (8B) | BLAKE3 Digest (32B)                   |
+-------------------+-------------------+---------------------------------------+
| Footer CRC32C (4B)| Reserved (12B)                                            |
+-------------------+-----------------------------------------------------------+
```

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `footer_magic` | `[u8; 8]` | 0 | ASCII `"ANVILEND"` (`0x41 0x4E 0x56 0x49 0x4C 0x45 0x4E 0x44`) |
| `total_length` | `u64` (LE) | 8 | Total file length in bytes (including header and footer) |
| `blake3_digest`| `[u8; 32]` | 16 | Cryptographic BLAKE3 hash computed over all preceding bytes |
| `footer_crc` | `u32` (LE) | 48 | CRC32C computed over footer bytes [0..48] |
| `reserved` | `[u8; 12]` | 52 | Must be 0 |

---

## 4. Snapshot Verification and Recovery

When loading or installing a snapshot:
1. Verify the total file length is at least header size + footer size (128 bytes).
2. Read the 64-byte footer and verify `footer_magic` and `footer_crc`.
3. Compute the streaming BLAKE3 digest of the file from byte 0 to `total_length - 64`. Compare against `blake3_digest`. If they do not match, the snapshot is corrupt and rejected.
4. Read the header and verify `magic`, `version`, and `header_crc`.
5. Iterate through `chunk_count` chunks, validating `chunk_index`, length bounds, and individual `chunk_crc`.
