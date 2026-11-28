#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Snapshot policy, chunking, and log-compaction helpers for `anvil-kv`.
//!
//! The snapshot itself is an `O(1)` clone of the persistent map. Serialization
//! walks live entries once, off the consensus path, and splits the bytes into
//! chunks with per-chunk CRC32C and a final BLAKE3 digest.

use anvil_types::LogIndex;
use bytes::Bytes;

/// Magic for snapshot format v04 (v03 plus the membership section).
pub const SNAPSHOT_MAGIC_V04: &[u8; 8] = b"ANVKVS04";
/// Magic and version for snapshot format v03 (sessions plus config).
pub const SNAPSHOT_MAGIC_V03: &[u8; 8] = b"ANVKVS03";
/// Magic for the legacy v02 format (no sessions), still accepted on decode.
pub const SNAPSHOT_MAGIC_V02: &[u8; 8] = b"ANVKVS02";

/// One serialized snapshot chunk with its integrity checksum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunk {
    /// Zero-based chunk number.
    pub index: u32,
    /// Raw slice of the snapshot byte stream.
    pub payload: Bytes,
    /// CRC32C over `payload`.
    pub crc: u32,
}

/// When to take a snapshot and how much log to keep after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SnapshotPolicy {
    /// Take a snapshot after this many applied entries since the last one.
    pub max_entries_since_snapshot: u64,
    /// Take a snapshot after this many applied payload bytes since the last one.
    pub max_bytes_since_snapshot: u64,
    /// Log entries retained after the snapshot index once durable.
    pub retain_entries: u64,
    /// Maximum bytes in one snapshot chunk.
    pub chunk_bytes: usize,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            max_entries_since_snapshot: 5_000,
            max_bytes_since_snapshot: 4 * 1024 * 1024,
            retain_entries: 500,
            chunk_bytes: 64 * 1024,
        }
    }
}

impl SnapshotPolicy {
    /// Returns true when either the entry or byte threshold is reached.
    #[must_use]
    pub const fn should_snapshot(&self, entries_since: u64, bytes_since: u64) -> bool {
        entries_since >= self.max_entries_since_snapshot
            || bytes_since >= self.max_bytes_since_snapshot
    }

    /// Computes the highest log index safe to drop after a durable snapshot.
    ///
    /// Returns `None` when there is nothing to compact. Never returns an index
    /// beyond `applied`: a snapshot past the applied index has no state behind
    /// it, so compaction refuses instead of guessing.
    #[must_use]
    pub fn compaction_index(
        &self,
        applied: LogIndex,
        snapshot_index: LogIndex,
        durable: bool,
    ) -> Option<LogIndex> {
        if !durable {
            return None;
        }
        if snapshot_index > applied {
            return None;
        }
        let snapshot_raw = snapshot_index.as_u64();
        if snapshot_raw <= self.retain_entries {
            return None;
        }
        let compact_up_to = snapshot_raw - self.retain_entries;
        let applied_raw = applied.as_u64();
        if compact_up_to > applied_raw {
            return None;
        }
        if compact_up_to == 0 {
            return None;
        }
        Some(LogIndex::from_raw(compact_up_to))
    }
}

/// Splits snapshot bytes into chunks with per-chunk CRC32C.
#[must_use]
pub fn split_into_chunks(full: &Bytes, chunk_bytes: usize) -> Vec<SnapshotChunk> {
    let size = chunk_bytes.max(1);
    let mut chunks = Vec::new();
    let mut offset = 0usize;
    let mut index = 0u32;
    while offset < full.len() {
        let end = (offset + size).min(full.len());
        let payload = full.slice(offset..end);
        let crc = crc32c::crc32c(&payload);
        chunks.push(SnapshotChunk {
            index,
            payload,
            crc,
        });
        offset = end;
        index = index.saturating_add(1);
    }
    chunks
}

/// Verifies per-chunk CRC32C and reassembles the byte stream in index order.
pub fn reassemble_chunks(chunks: &[SnapshotChunk]) -> Result<Bytes, ChunkError> {
    if chunks.is_empty() {
        return Err(ChunkError::Empty);
    }
    let mut ordered: Vec<&SnapshotChunk> = chunks.iter().collect();
    ordered.sort_by_key(|c| c.index);
    for (expected, chunk) in ordered.iter().enumerate() {
        let expected_index = u32::try_from(expected).map_err(|_| ChunkError::TooMany)?;
        if chunk.index != expected_index {
            return Err(ChunkError::Gap);
        }
        if crc32c::crc32c(&chunk.payload) != chunk.crc {
            return Err(ChunkError::Corrupt);
        }
    }
    let total: usize = ordered.iter().map(|c| c.payload.len()).sum();
    let mut full = Vec::with_capacity(total);
    for chunk in ordered {
        full.extend_from_slice(&chunk.payload);
    }
    Ok(Bytes::from(full))
}

/// BLAKE3 digest of the full snapshot byte stream.
#[must_use]
pub fn snapshot_digest(full: &[u8]) -> [u8; 32] {
    *blake3::hash(full).as_bytes()
}

/// Chunk reassembly failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChunkError {
    /// No chunks supplied.
    #[error("no snapshot chunks")]
    Empty,
    /// Chunk indexes are not a dense zero-based sequence.
    #[error("missing snapshot chunk")]
    Gap,
    /// Chunk count does not fit in a `u32`.
    #[error("too many snapshot chunks")]
    TooMany,
    /// CRC mismatch.
    #[error("snapshot chunk corrupt")]
    Corrupt,
}
