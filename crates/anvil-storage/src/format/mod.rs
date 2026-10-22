#![forbid(unsafe_code)]

//! On-disk format definitions and codecs for ANVIL storage artifacts.

pub mod hard_state;
pub mod snapshot;
pub mod wal;

pub use hard_state::{
    HARD_STATE_MAGIC, HARD_STATE_SLOT_FIXED_LEN, HARD_STATE_VERSION, HardStateRecoveryOutcome,
    HardStateSlot, recover_hard_state,
};
pub use snapshot::{
    SNAPSHOT_CHUNK_HEADER_LEN, SNAPSHOT_FOOTER_LEN, SNAPSHOT_FOOTER_MAGIC, SNAPSHOT_HEADER_LEN,
    SNAPSHOT_MAGIC, SNAPSHOT_VERSION, SnapshotChunk, SnapshotFooter, SnapshotHeader,
};
pub use wal::{
    WAL_HEADER_FIXED_LEN, WAL_MAGIC, WAL_RECORD_HEADER_FIXED_LEN, WAL_VERSION, WalRecordFrame,
    WalScanResult, WalSegmentHeader, compute_wal_record_crc, decode_wal_frame,
};
