#![forbid(unsafe_code)]

//! Abstract device interface and operations for sans-IO storage engines.

use crate::error::StorageError;
use bytes::Bytes;

/// Physical sector size configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum SectorSize {
    /// Standard 512-byte disk sector.
    Sector512 = 512,
    /// Advanced Format 4,096-byte (4Kn) disk sector.
    Sector4096 = 4096,
}

impl SectorSize {
    /// Returns the sector size in bytes as `u16`.
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self as u16
    }

    /// Returns the sector size in bytes as `usize`.
    #[must_use]
    pub const fn as_usize(self) -> usize {
        self as usize
    }

    /// Returns the sector size in bytes as `u64`.
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self as u64
    }

    /// Checks if a byte offset is aligned to this sector size.
    #[must_use]
    pub fn is_aligned(self, offset: u64) -> bool {
        offset.is_multiple_of(self.as_u64())
    }

    /// Rounds an integer byte length up to the nearest sector multiple.
    #[must_use]
    pub const fn align_up(self, len: usize) -> usize {
        let mask = self.as_usize() - 1;
        (len + mask) & !mask
    }

    /// Parses a raw `u16` into a known `SectorSize`.
    pub const fn from_u16(val: u16) -> Result<Self, StorageError> {
        match val {
            512 => Ok(Self::Sector512),
            4096 => Ok(Self::Sector4096),
            other => Err(StorageError::UnsupportedSectorSize { found: other }),
        }
    }
}

/// Abstract identifier for a managed file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(pub u64);

impl FileId {
    /// Reserved `FileId` for the active Hard State file (`hardstate.bin`).
    pub const HARD_STATE: Self = Self(0);

    /// Constructs a `FileId` corresponding to a WAL segment.
    #[must_use]
    pub const fn wal_segment(segment_id: u64) -> Self {
        // High bit 0 reserved for segments, offset by 1
        Self(segment_id.wrapping_add(1))
    }

    /// Constructs a `FileId` corresponding to a snapshot file.
    #[must_use]
    pub const fn snapshot(snapshot_id: u64) -> Self {
        // High bit set to distinguish snapshots from segments
        Self(snapshot_id | (1u64 << 63))
    }
}

/// Abstract I/O operations submitted by the sans-IO storage engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeviceOp {
    /// Create a new file with preallocated bytes.
    Create {
        /// Monotonically increasing operation token.
        op_id: u64,
        /// Target file identifier.
        file_id: FileId,
        /// Number of bytes to preallocate on disk.
        preallocate_bytes: u64,
    },
    /// Open an existing file.
    Open {
        /// Operation token.
        op_id: u64,
        /// Target file identifier.
        file_id: FileId,
    },
    /// Read `len` bytes from `file_id` at `offset`.
    Read {
        /// Operation token.
        op_id: u64,
        /// Target file identifier.
        file_id: FileId,
        /// Byte offset in file.
        offset: u64,
        /// Number of bytes to read.
        len: usize,
    },
    /// Write sector-aligned bytes to `file_id` at `offset`.
    Write {
        /// Operation token.
        op_id: u64,
        /// Target file identifier.
        file_id: FileId,
        /// Byte offset in file.
        offset: u64,
        /// Payload data to persist.
        data: Bytes,
    },
    /// Synchronize data and metadata to durable physical media.
    Fdatasync {
        /// Operation token.
        op_id: u64,
        /// Target file identifier.
        file_id: FileId,
    },
    /// Synchronize the containing directory (needed after create, rename, unlink).
    SyncDir {
        /// Operation token.
        op_id: u64,
    },
    /// Atomically rename a file.
    Rename {
        /// Operation token.
        op_id: u64,
        /// Original file ID.
        from: FileId,
        /// New file ID.
        to: FileId,
    },
    /// Delete / unlink a file.
    Unlink {
        /// Operation token.
        op_id: u64,
        /// File to unlink.
        file_id: FileId,
    },
    /// Truncate a file to a specific byte length.
    Truncate {
        /// Operation token.
        op_id: u64,
        /// File to truncate.
        file_id: FileId,
        /// Target length in bytes.
        len: u64,
    },
}

impl DeviceOp {
    /// Returns the unique operation token.
    #[must_use]
    pub const fn op_id(&self) -> u64 {
        match *self {
            Self::Create { op_id, .. }
            | Self::Open { op_id, .. }
            | Self::Read { op_id, .. }
            | Self::Write { op_id, .. }
            | Self::Fdatasync { op_id, .. }
            | Self::SyncDir { op_id }
            | Self::Rename { op_id, .. }
            | Self::Unlink { op_id, .. }
            | Self::Truncate { op_id, .. } => op_id,
        }
    }
}

/// Payload returned upon successful completion of a `DeviceOp`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompletionPayload {
    /// File created.
    Created,
    /// File opened.
    Opened,
    /// Read returned bytes.
    Read(Bytes),
    /// Write succeeded with number of bytes written.
    Written(usize),
    /// File data synced.
    Synced,
    /// Directory metadata synced.
    DirSynced,
    /// Rename completed.
    Renamed,
    /// Unlink completed.
    Unlinked,
    /// Truncate completed.
    Truncated,
}

/// Completion event returned by a physical or simulated `Device`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceCompletion {
    /// Operation token corresponding to the submitted `DeviceOp`.
    pub op_id: u64,
    /// Outcome of the operation.
    pub result: Result<CompletionPayload, StorageError>,
}

/// Abstract storage device trait.
///
/// Both `SimDevice` (in `anvil-sim`) and `FsDevice` (in `anvil-device-fs`) implement this trait.
pub trait Device: Send {
    /// Submits an asynchronous operation to the device.
    fn submit(&mut self, op: DeviceOp);

    /// Polls for the next completed operation, if any.
    fn poll_completion(&mut self) -> Option<DeviceCompletion>;
}
