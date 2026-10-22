# ANVIL On-Disk Format: Write-Ahead Log (WAL) Segments

## 1. Overview

ANVIL persists consensus entries to preallocated, fixed-size WAL segment files. Segment files are named monotonically: `wal-{segment_id:016x}.wal`.

To defend against torn writes, misdirected writes, and bit rot, WAL segments adhere to:
1. **Fixed Segment Header**: Stored in the first sector (zero-padded to sector size).
2. **Sector-Aligned Frames**: Every record is stored in a frame aligned to the sector size ($512$ or $4,096$ bytes) and zero-padded.
3. **Chained CRC32C Integrity**: Every record carries a CRC32C that incorporates:
   - The CRC32C of the preceding record (or segment header for the first record)
   - The unique segment ID
   - The absolute file byte offset of the record
   - The record contents (index, term, kind, payload)
4. **Padding Validation**: Unused trailing bytes in each sector frame must be verified zero. Non-zero padding indicates a torn write or corruption.

---

## 2. WAL Segment Header Byte Layout

The segment header occupies exactly one sector (the remainder of the sector is padded with zeroes):

```text
+-------------------+-------------------+-------------------+-------------------+
| Magic (8B)        | Version (2B)      | Sector Size (2B)  | Reserved (4B)     |
+-------------------+-------------------+-------------------+-------------------+
| Segment ID (8B)   | First Index (8B)  | Prev Term (8B)    | Header CRC32C (4B)|
+-------------------+-------------------+-------------------+-------------------+
| Zero Padding to Sector Size (512 or 4096 bytes)                               |
+-------------------------------------------------------------------------------+
```

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `magic` | `[u8; 8]` | 0 | ASCII `"ANVILWAL"` (`0x41 0x4E 0x56 0x49 0x4C 0x57 0x41 0x4C`) |
| `version` | `u16` (LE) | 8 | Format version (currently `1`) |
| `sector_size` | `u16` (LE) | 10 | Target physical sector size (`512` or `4096`) |
| `reserved` | `u32` (LE) | 12 | Must be 0 |
| `segment_id` | `u64` (LE) | 16 | Monotonically increasing segment identifier |
| `first_index` | `u64` (LE) | 24 | Expected `LogIndex` of first record in this segment |
| `prev_term` | `u64` (LE) | 32 | Raft term of the entry at `first_index - 1` |
| `header_crc` | `u32` (LE) | 40 | CRC32C calculated over bytes [0..40] |
| `padding` | `[u8]` | 44..Sector | All zeroes up to sector size boundary |

---

## 3. WAL Record Frame Layout

Following the header sector, records are appended sequentially in sector-aligned frames.

```text
+-------------------+-------------------+-------------------+-------------------+
| Record Index (8B) | Record Term (8B)  | Payload Len (4B)  | Record Kind (1B)  |
+-------------------+-------------------+-------------------+-------------------+
| Chained CRC32C (4B)                   | Payload Bytes (N bytes)               |
+---------------------------------------+---------------------------------------+
| Zero Padding to Sector Size (align to sector_size)                            |
+-------------------------------------------------------------------------------+
```

### Detailed Record Fields

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `index` | `u64` (LE) | 0 | Raft `LogIndex` of the entry |
| `term` | `u64` (LE) | 8 | Raft `Term` of the entry |
| `payload_len` | `u32` (LE) | 16 | Length of raw payload bytes $N$ |
| `record_kind` | `u8` | 20 | Entry kind (`0 = Normal`, `1 = NoOp`, `2 = Configuration`) |
| `crc32c` | `u32` (LE) | 21 | Chained CRC32C checksum (see below) |
| `payload` | `[u8; N]` | 25 | Entry payload data |
| `padding` | `[u8; P]` | 25+N | Zero padding such that total frame length is a multiple of `sector_size` |

### Chained CRC32C Computation

To prevent frame splicing, misdirected block placement, and truncation splicing, the `crc32c` field is computed over:
1. `prev_crc`: The CRC32C of the immediate previous record (or `header_crc` for record 0).
2. `segment_id`: 8 bytes little-endian.
3. `file_offset`: 8 bytes little-endian (byte offset where this record begins in the file).
4. `index`: 8 bytes little-endian.
5. `term`: 8 bytes little-endian.
6. `payload_len`: 4 bytes little-endian.
7. `record_kind`: 1 byte.
8. `payload`: $N$ raw bytes.

If a record is copied from segment $A$ into segment $B$, or relocated to a different offset within segment $A$, `crc32c` verification fails immediately.

---

## 4. End-of-Log and Zero Padding

A preallocated WAL segment contains zeroes beyond the active log head. During scan:
- When a record header starts with `index == 0` and all zero bytes across the sector, it is recognized as unallocated space.
- Any non-zero byte encountered in a region expecting zero padding indicates a torn write or corruption.
