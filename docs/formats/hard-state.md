# ANVIL On-Disk Format: Hard State (`hardstate.bin`)

## 1. Overview and Design

In Raft, a node's persistent state comprises `currentTerm` and `votedFor`. Violating the invariant that a node never forgets its vote within a term leads directly to dual leaders and split-brain safety violations.

To achieve atomic updates without relying on filesystem file renames or directory operations, ANVIL stores hard state in a fixed-size file (`hardstate.bin`) featuring **two alternating slots**.

Each slot occupies exactly one sector (either 512 bytes or 4,096 bytes) and maintains a monotonically increasing `sequence_number` and its own CRC32C.
- Writing slot $A$ does not overwrite slot $B$.
- If a crash occurs during a write to slot $A$, slot $B$ remains intact and valid.
- On startup, the recovery scanner inspects both slots and chooses the valid slot with the highest `sequence_number`.

---

## 2. File Layout

```text
+-------------------------------------------------------------------------------+
| Slot 0 (1 Sector: 512 or 4096 bytes)                                          |
+-------------------------------------------------------------------------------+
| Slot 1 (1 Sector: 512 or 4096 bytes)                                          |
+-------------------------------------------------------------------------------+
```

The file size is exactly $2 \times \text{sector\_size}$ (1,024 bytes for 512-byte sectors, or 8,192 bytes for 4,096-byte sectors).

---

## 3. Slot Byte Layout

Each slot has the following structure:

```text
+-------------------+-------------------+-------------------+-------------------+
| Magic (8B)        | Version (2B)      | Slot Index (2B)   | CRC32C (4B)       |
+-------------------+-------------------+-------------------+-------------------+
| Sequence No (8B)  | Current Term (8B) | Voted For (8B)    | Has Vote (1B)     |
+-------------------+-------------------+-------------------+-------------------+
| Zero Padding to Sector Size (512 or 4096 bytes)                               |
+-------------------------------------------------------------------------------+
```

| Field | Type | Offset | Description |
| :--- | :--- | :--- | :--- |
| `magic` | `[u8; 8]` | 0 | ASCII `"ANVILHST"` (`0x41 0x4E 0x56 0x49 0x4C 0x48 0x53 0x54`) |
| `version` | `u16` (LE) | 8 | Format version (`1`) |
| `slot_index` | `u16` (LE) | 10 | `0` for Slot 0, `1` for Slot 1 |
| `crc32c` | `u32` (LE) | 12 | CRC32C calculated over fields from offset 16 to end of slot |
| `sequence_no` | `u64` (LE) | 16 | Monotonically increasing sequence number |
| `current_term` | `u64` (LE) | 24 | Raft consensus `Term` |
| `voted_for` | `u64` (LE) | 32 | Raft `NodeId` voted for in `current_term` (0 if none) |
| `has_vote` | `u8` | 40 | `1` if `voted_for` is `Some`, `0` if `None` |
| `padding` | `[u8]` | 41..Sector | Verified zero padding to sector boundary |

---

## 4. Atomic Update Procedure

1. Determine the target slot: if the active slot is Slot 0, write to Slot 1; if active is Slot 1, write to Slot 0.
2. Set `sequence_number = active_sequence_number + 1`.
3. Fill slot buffer, calculate CRC32C, and write the sector at offset `target_slot * sector_size`.
4. Call `fdatasync` on the file descriptor.
5. Once `fdatasync` completes, mark the target slot as the active in-memory slot.

---

## 5. Recovery Decision Logic

On startup:
1. Read Slot 0 and verify magic, version, `slot_index == 0`, CRC32C, and zero padding.
2. Read Slot 1 and verify magic, version, `slot_index == 1`, CRC32C, and zero padding.
3. Classify:
   - **Both slots valid**: Pick the slot with `sequence_no = max(slot0.seq, slot1.seq)`.
   - **Slot 0 valid, Slot 1 corrupted**: Use Slot 0. The corrupt write in Slot 1 was a torn write or crash.
   - **Slot 1 valid, Slot 0 corrupted**: Use Slot 1.
   - **Both slots corrupted**: **Refuse to start.** The node must not attempt to guess its term or vote. A node that forgets its vote can vote twice in the same term, causing catastrophic split-brain. The operator must replace the node under a new node id.
