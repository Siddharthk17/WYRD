#![forbid(unsafe_code)]

//! On-disk format for alternating hard state slots (`hardstate.bin`).

use crate::device::SectorSize;
use crate::error::StorageError;
use anvil_types::{NodeId, Term};

/// Hard state header magic bytes: ASCII `"ANVILHST"`.
pub const HARD_STATE_MAGIC: [u8; 8] = *b"ANVILHST";

/// Format version 1.
pub const HARD_STATE_VERSION: u16 = 1;

/// Length of non-padding fields in a hard-state slot (41 bytes).
pub const HARD_STATE_SLOT_FIXED_LEN: usize = 41;

/// Representation of a single sector hard-state slot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HardStateSlot {
    /// Slot index: 0 or 1.
    pub slot_index: u16,
    /// Monotonically increasing sequence number.
    pub sequence_no: u64,
    /// Current consensus term.
    pub term: Term,
    /// Node ID voted for in `term`, if any.
    pub voted_for: Option<NodeId>,
    /// Stored CRC32C checksum.
    pub crc32c: u32,
}

impl HardStateSlot {
    /// Creates a new `HardStateSlot` with computed CRC32C.
    #[must_use]
    pub fn new(slot_index: u16, sequence_no: u64, term: Term, voted_for: Option<NodeId>) -> Self {
        let mut slot = Self {
            slot_index,
            sequence_no,
            term,
            voted_for,
            crc32c: 0,
        };
        slot.crc32c = slot.compute_crc();
        slot
    }

    /// Computes CRC32C over the slot payload fields starting at offset 16.
    #[must_use]
    pub fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 25];
        buf[0..8].copy_from_slice(&self.sequence_no.to_le_bytes());
        buf[8..16].copy_from_slice(&self.term.as_u64().to_le_bytes());
        if let Some(node_id) = self.voted_for {
            buf[16..24].copy_from_slice(&node_id.get().to_le_bytes());
            buf[24] = 1;
        } else {
            buf[16..24].copy_from_slice(&0u64.to_le_bytes());
            buf[24] = 0;
        }
        crc32c::crc32c(&buf)
    }

    /// Encodes this slot into a sector-sized buffer with zero-padded trailing bytes.
    pub fn encode(&self, sector_size: SectorSize, out: &mut [u8]) -> Result<(), StorageError> {
        let sector_len = sector_size.as_usize();
        if out.len() < sector_len {
            return Err(StorageError::BufferTooSmall {
                required: sector_len,
                provided: out.len(),
            });
        }

        out[..sector_len].fill(0);
        out[0..8].copy_from_slice(&HARD_STATE_MAGIC);
        out[8..10].copy_from_slice(&HARD_STATE_VERSION.to_le_bytes());
        out[10..12].copy_from_slice(&self.slot_index.to_le_bytes());
        out[12..16].copy_from_slice(&self.crc32c.to_le_bytes());
        out[16..24].copy_from_slice(&self.sequence_no.to_le_bytes());
        out[24..32].copy_from_slice(&self.term.as_u64().to_le_bytes());

        if let Some(node_id) = self.voted_for {
            out[32..40].copy_from_slice(&node_id.get().to_le_bytes());
            out[40] = 1;
        } else {
            out[32..40].copy_from_slice(&0u64.to_le_bytes());
            out[40] = 0;
        }

        Ok(())
    }

    /// Decodes and validates a `HardStateSlot` from sector bytes.
    pub fn decode(
        buf: &[u8],
        expected_slot_index: u16,
        sector_size: SectorSize,
    ) -> Result<Self, StorageError> {
        let sector_len = sector_size.as_usize();
        if buf.len() < sector_len {
            return Err(StorageError::BufferTooSmall {
                required: sector_len,
                provided: buf.len(),
            });
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != HARD_STATE_MAGIC {
            return Err(StorageError::InvalidMagic {
                expected: HARD_STATE_MAGIC,
                found: magic,
            });
        }

        let version = u16::from_le_bytes([buf[8], buf[9]]);
        if version != HARD_STATE_VERSION {
            return Err(StorageError::UnsupportedVersion { found: version });
        }

        let slot_index = u16::from_le_bytes([buf[10], buf[11]]);
        if slot_index != expected_slot_index {
            return Err(StorageError::CrcMismatch {
                expected: u32::from(expected_slot_index),
                computed: u32::from(slot_index),
                context: "hard_state_slot_index",
            });
        }

        let expected_crc = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        let sequence_no = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let term = Term::from_raw(u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]));
        let raw_voted_for = u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]);
        let has_vote = buf[40];

        let voted_for = match has_vote {
            0 => None,
            1 => Some(NodeId::new(raw_voted_for).ok_or(StorageError::CrcMismatch {
                expected: 0,
                computed: 1,
                context: "hard_state_zero_node_id",
            })?),
            _ => {
                return Err(StorageError::CrcMismatch {
                    expected: 0,
                    computed: u32::from(has_vote),
                    context: "hard_state_has_vote_invalid",
                });
            }
        };

        // Compute CRC over bytes 16..41
        let computed_crc = crc32c::crc32c(&buf[16..HARD_STATE_SLOT_FIXED_LEN]);
        if expected_crc != computed_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
                context: "hard_state_slot_crc",
            });
        }

        // Verify remaining bytes in sector are zero
        for (i, &b) in buf[HARD_STATE_SLOT_FIXED_LEN..sector_len]
            .iter()
            .enumerate()
        {
            if b != 0 {
                return Err(StorageError::NonZeroPadding {
                    offset: HARD_STATE_SLOT_FIXED_LEN + i,
                });
            }
        }

        Ok(Self {
            slot_index,
            sequence_no,
            term,
            voted_for,
            crc32c: expected_crc,
        })
    }
}

/// Outcome of recovering hard state from the two alternating slots.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HardStateRecoveryOutcome {
    /// Both slots are valid; selected the one with the higher sequence number.
    BothValid {
        /// Active slot index (0 or 1).
        active_slot: usize,
        /// Recovered term.
        term: Term,
        /// Recovered vote.
        voted_for: Option<NodeId>,
        /// Recovered sequence number.
        sequence_no: u64,
    },
    /// Exactly one slot was valid; the other was corrupted (torn write or bit rot).
    OneSlotCorrupted {
        /// Valid slot index.
        valid_slot: usize,
        /// Corrupted slot index.
        bad_slot: usize,
        /// Recovered term.
        term: Term,
        /// Recovered vote.
        voted_for: Option<NodeId>,
        /// Recovered sequence number.
        sequence_no: u64,
    },
}

/// Recovers hard state by evaluating both slots in `hardstate.bin`.
pub fn recover_hard_state(
    file_bytes: &[u8],
    sector_size: SectorSize,
) -> Result<HardStateRecoveryOutcome, StorageError> {
    let sector_len = sector_size.as_usize();
    let expected_file_len = sector_len * 2;
    if file_bytes.len() < expected_file_len {
        return Err(StorageError::BufferTooSmall {
            required: expected_file_len,
            provided: file_bytes.len(),
        });
    }

    let slot0_result = HardStateSlot::decode(&file_bytes[..sector_len], 0, sector_size);
    let slot1_result =
        HardStateSlot::decode(&file_bytes[sector_len..expected_file_len], 1, sector_size);

    match (slot0_result, slot1_result) {
        (Ok(s0), Ok(s1)) => {
            if s0.sequence_no >= s1.sequence_no {
                Ok(HardStateRecoveryOutcome::BothValid {
                    active_slot: 0,
                    term: s0.term,
                    voted_for: s0.voted_for,
                    sequence_no: s0.sequence_no,
                })
            } else {
                Ok(HardStateRecoveryOutcome::BothValid {
                    active_slot: 1,
                    term: s1.term,
                    voted_for: s1.voted_for,
                    sequence_no: s1.sequence_no,
                })
            }
        }
        (Ok(s0), Err(_)) => Ok(HardStateRecoveryOutcome::OneSlotCorrupted {
            valid_slot: 0,
            bad_slot: 1,
            term: s0.term,
            voted_for: s0.voted_for,
            sequence_no: s0.sequence_no,
        }),
        (Err(_), Ok(s1)) => Ok(HardStateRecoveryOutcome::OneSlotCorrupted {
            valid_slot: 1,
            bad_slot: 0,
            term: s1.term,
            voted_for: s1.voted_for,
            sequence_no: s1.sequence_no,
        }),
        (Err(_), Err(_)) => Err(StorageError::CorruptHardStateBothSlots),
    }
}
