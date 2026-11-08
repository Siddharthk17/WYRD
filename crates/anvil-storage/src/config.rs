#![forbid(unsafe_code)]

//! Configuration settings for the ANVIL storage engine.

use crate::device::SectorSize;

/// Configuration parameters for `StorageEngine`.
///
/// Batching is natural: while one `fdatasync` is in flight, every arriving
/// append coalesces into the next batch. Callers bound pipeline depth;
/// the engine never blocks, errors, or splits a staged batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorageConfig {
    /// Target physical sector size.
    pub sector_size: SectorSize,
    /// Capacity of each preallocated WAL segment in bytes (must be sector aligned).
    pub segment_capacity_bytes: u64,
    /// Whether directory fsync is enforced on file creation, rename, and unlinks.
    pub sync_directories: bool,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            sector_size: SectorSize::Sector512,
            segment_capacity_bytes: 64 * 1024 * 1024,
            sync_directories: true,
        }
    }
}

impl StorageConfig {
    /// Creates a configuration optimized for testing with smaller segments.
    #[must_use]
    pub fn for_test(sector_size: SectorSize, segment_capacity_bytes: u64) -> Self {
        Self {
            sector_size,
            segment_capacity_bytes,
            sync_directories: true,
        }
    }
}
