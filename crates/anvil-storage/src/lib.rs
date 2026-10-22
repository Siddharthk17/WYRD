#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-storage`: WAL, hard state, and snapshot formats with sans-IO recovery logic.

pub mod device;
pub mod error;
pub mod format;
pub mod recovery;

pub use device::{CompletionPayload, Device, DeviceCompletion, DeviceOp, FileId, SectorSize};
pub use error::StorageError;
pub use format::{
    HARD_STATE_MAGIC, HARD_STATE_SLOT_FIXED_LEN, HARD_STATE_VERSION, HardStateRecoveryOutcome,
    HardStateSlot, SNAPSHOT_CHUNK_HEADER_LEN, SNAPSHOT_FOOTER_LEN, SNAPSHOT_FOOTER_MAGIC,
    SNAPSHOT_HEADER_LEN, SNAPSHOT_MAGIC, SNAPSHOT_VERSION, SnapshotChunk, SnapshotFooter,
    SnapshotHeader, WAL_HEADER_FIXED_LEN, WAL_MAGIC, WAL_RECORD_HEADER_FIXED_LEN, WAL_VERSION,
    WalRecordFrame, WalScanResult, WalSegmentHeader, compute_wal_record_crc, decode_wal_frame,
    recover_hard_state,
};
pub use recovery::{LogRecoveryClassification, RecoveredLog, RecoveredStorageState};
