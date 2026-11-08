#![forbid(unsafe_code)]

//! Error definitions for the ANVIL storage engine and on-disk codecs.

use anvil_types::{NodeId, Term};
use thiserror::Error;

/// Storage and on-disk codec errors.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StorageError {
    /// File header magic bytes do not match expected signature.
    #[error("invalid magic bytes: expected {expected:?}, found {found:?}")]
    InvalidMagic {
        /// Expected 8-byte magic.
        expected: [u8; 8],
        /// Found 8-byte magic.
        found: [u8; 8],
    },

    /// Unsupported on-disk format version.
    #[error("unsupported format version: {found}")]
    UnsupportedVersion {
        /// Discovered version number.
        found: u16,
    },

    /// Unsupported sector size configuration.
    #[error("unsupported sector size: {found}")]
    UnsupportedSectorSize {
        /// Configured sector size.
        found: u16,
    },

    /// Checksum verification failure.
    #[error("CRC32C mismatch in {context}: expected 0x{expected:08x}, computed 0x{computed:08x}")]
    CrcMismatch {
        /// Expected CRC checksum recorded in file.
        expected: u32,
        /// Computed CRC checksum over content.
        computed: u32,
        /// Context or field name where mismatch occurred.
        context: &'static str,
    },

    /// Whole-file BLAKE3 cryptographic digest mismatch.
    #[error("BLAKE3 digest mismatch: expected {expected:?}, computed {computed:?}")]
    Blake3Mismatch {
        /// Expected 32-byte BLAKE3 hash from footer.
        expected: [u8; 32],
        /// Computed 32-byte BLAKE3 hash.
        computed: [u8; 32],
    },

    /// Expected zero padding contained non-zero bytes (indicating torn write or corruption).
    #[error("non-zero padding at byte offset {offset}")]
    NonZeroPadding {
        /// Offset within the buffer where non-zero byte was found.
        offset: usize,
    },

    /// Provided buffer is smaller than required minimum length.
    #[error("buffer too small: required {required} bytes, provided {provided} bytes")]
    BufferTooSmall {
        /// Minimum bytes required.
        required: usize,
        /// Actual length of provided buffer.
        provided: usize,
    },

    /// Both alternating hard-state slots are corrupt.
    #[error("both hard-state slots are corrupt; node cannot safely determine term or vote")]
    CorruptHardStateBothSlots,

    /// Structural field validation failure (not a checksum issue): slot index
    /// mismatch, invalid vote flag, zero node id with vote set, or out-of-order chunk.
    #[error("invalid field: {context}")]
    InvalidField {
        /// Description of the field that failed validation.
        context: &'static str,
    },

    /// Unrecoverable fsync error.
    #[error("fsync failed: {reason}")]
    FsyncFailed {
        /// Detail of the sync failure.
        reason: String,
    },

    /// Requested offset or operation is unaligned to sector boundary.
    #[error("unaligned sector access: offset {offset} not aligned to sector size {sector_size}")]
    UnalignedSector {
        /// Byte offset attempted.
        offset: u64,
        /// Required sector boundary.
        sector_size: u16,
    },

    /// Segment preallocation or append exceeded maximum segment capacity.
    #[error("segment capacity exceeded: segment_id {segment_id}")]
    SegmentFull {
        /// Target segment ID.
        segment_id: u64,
    },

    /// Recovered hard state is older than acknowledged writes: the node forgot
    /// an acknowledged term or vote (silent corruption of the latest slot) and
    /// must be replaced under a new node id, never resumed.
    #[error(
        "stale hard state: recovered {recovered_term}/{recovered_vote:?}, acknowledged {acked_term}/{acked_vote:?}"
    )]
    StaleHardState {
        /// Term found during recovery.
        recovered_term: Term,
        /// Vote found during recovery, if any.
        recovered_vote: Option<NodeId>,
        /// Highest acknowledged term.
        acked_term: Term,
        /// Acknowledged vote for that term, if any.
        acked_vote: Option<NodeId>,
    },

    /// Abstract device I/O error.
    #[error("device error: {0}")]
    DeviceError(String),
}
