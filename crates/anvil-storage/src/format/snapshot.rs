#![forbid(unsafe_code)]

//! On-disk format for state machine snapshot files.

use crate::device::SectorSize;
use crate::error::StorageError;
use anvil_types::{ClusterId, GroupId, LogIndex, Term};
use bytes::Bytes;

/// Snapshot header magic bytes: ASCII `"ANVILSNP"`.
pub const SNAPSHOT_MAGIC: [u8; 8] = *b"ANVILSNP";

/// Snapshot footer magic bytes: ASCII `"ANVILEND"`.
pub const SNAPSHOT_FOOTER_MAGIC: [u8; 8] = *b"ANVILEND";

/// Format version 1.
pub const SNAPSHOT_VERSION: u16 = 1;

/// Fixed snapshot header size (64 bytes).
pub const SNAPSHOT_HEADER_LEN: usize = 64;

/// Fixed snapshot chunk header size (16 bytes).
pub const SNAPSHOT_CHUNK_HEADER_LEN: usize = 16;

/// Fixed snapshot footer size (64 bytes).
pub const SNAPSHOT_FOOTER_LEN: usize = 64;

/// Stored metadata header at the beginning of each snapshot file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotHeader {
    /// Format version number.
    pub version: u16,
    /// Sector size in bytes.
    pub sector_size: SectorSize,
    /// Last committed log index included in snapshot.
    pub last_index: LogIndex,
    /// Term corresponding to `last_index`.
    pub last_term: Term,
    /// Cluster identifier.
    pub cluster_id: ClusterId,
    /// Replication group identifier.
    pub group_id: GroupId,
    /// Number of data chunks contained in the snapshot.
    pub chunk_count: u64,
    /// CRC32C calculated over header fields [0..56].
    pub header_crc: u32,
}

impl SnapshotHeader {
    /// Constructs a new `SnapshotHeader` with computed CRC32C.
    #[must_use]
    pub fn new(
        sector_size: SectorSize,
        last_index: LogIndex,
        last_term: Term,
        cluster_id: ClusterId,
        group_id: GroupId,
        chunk_count: u64,
    ) -> Self {
        let mut header = Self {
            version: SNAPSHOT_VERSION,
            sector_size,
            last_index,
            last_term,
            cluster_id,
            group_id,
            chunk_count,
            header_crc: 0,
        };
        header.header_crc = header.compute_crc();
        header
    }

    /// Computes CRC32C over the first 56 bytes of the header.
    #[must_use]
    pub fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 56];
        buf[0..8].copy_from_slice(&SNAPSHOT_MAGIC);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10..12].copy_from_slice(&self.sector_size.as_u16().to_le_bytes());
        buf[12..16].copy_from_slice(&0u32.to_le_bytes());
        buf[16..24].copy_from_slice(&self.last_index.as_u64().to_le_bytes());
        buf[24..32].copy_from_slice(&self.last_term.as_u64().to_le_bytes());
        buf[32..40].copy_from_slice(&self.cluster_id.as_u64().to_le_bytes());
        buf[40..48].copy_from_slice(&self.group_id.as_u64().to_le_bytes());
        buf[48..56].copy_from_slice(&self.chunk_count.to_le_bytes());
        crc32c::crc32c(&buf)
    }

    /// Encodes header into a 64-byte buffer.
    pub fn encode(&self, out: &mut [u8]) -> Result<(), StorageError> {
        if out.len() < SNAPSHOT_HEADER_LEN {
            return Err(StorageError::BufferTooSmall {
                required: SNAPSHOT_HEADER_LEN,
                provided: out.len(),
            });
        }

        out[..SNAPSHOT_HEADER_LEN].fill(0);
        out[0..8].copy_from_slice(&SNAPSHOT_MAGIC);
        out[8..10].copy_from_slice(&self.version.to_le_bytes());
        out[10..12].copy_from_slice(&self.sector_size.as_u16().to_le_bytes());
        out[12..16].copy_from_slice(&0u32.to_le_bytes());
        out[16..24].copy_from_slice(&self.last_index.as_u64().to_le_bytes());
        out[24..32].copy_from_slice(&self.last_term.as_u64().to_le_bytes());
        out[32..40].copy_from_slice(&self.cluster_id.as_u64().to_le_bytes());
        out[40..48].copy_from_slice(&self.group_id.as_u64().to_le_bytes());
        out[48..56].copy_from_slice(&self.chunk_count.to_le_bytes());
        out[56..60].copy_from_slice(&self.header_crc.to_le_bytes());
        out[60..64].copy_from_slice(&0u32.to_le_bytes());
        Ok(())
    }

    /// Decodes and validates header from a buffer.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        if buf.len() < SNAPSHOT_HEADER_LEN {
            return Err(StorageError::BufferTooSmall {
                required: SNAPSHOT_HEADER_LEN,
                provided: buf.len(),
            });
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != SNAPSHOT_MAGIC {
            return Err(StorageError::InvalidMagic {
                expected: SNAPSHOT_MAGIC,
                found: magic,
            });
        }

        let version = u16::from_le_bytes([buf[8], buf[9]]);
        if version != SNAPSHOT_VERSION {
            return Err(StorageError::UnsupportedVersion { found: version });
        }

        let raw_sector_size = u16::from_le_bytes([buf[10], buf[11]]);
        let sector_size = SectorSize::from_u16(raw_sector_size)?;
        let last_index = LogIndex::from_raw(u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]));
        let last_term = Term::from_raw(u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]));
        let cluster_id = ClusterId::from_raw(u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]));
        let group_id = GroupId::from_raw(u64::from_le_bytes([
            buf[40], buf[41], buf[42], buf[43], buf[44], buf[45], buf[46], buf[47],
        ]));
        let chunk_count = u64::from_le_bytes([
            buf[48], buf[49], buf[50], buf[51], buf[52], buf[53], buf[54], buf[55],
        ]);
        let expected_crc = u32::from_le_bytes([buf[56], buf[57], buf[58], buf[59]]);

        let computed_crc = crc32c::crc32c(&buf[0..56]);
        if expected_crc != computed_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
                context: "snapshot_header_crc",
            });
        }

        Ok(Self {
            version,
            sector_size,
            last_index,
            last_term,
            cluster_id,
            group_id,
            chunk_count,
            header_crc: expected_crc,
        })
    }
}

/// An individual chunk of state data within a snapshot file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunk {
    /// Monotonic zero-based index of this chunk.
    pub chunk_index: u32,
    /// Raw payload bytes of this chunk.
    pub payload: Bytes,
}

impl SnapshotChunk {
    /// Encodes chunk header and payload into an output buffer.
    pub fn encode(&self, out: &mut Vec<u8>) {
        let payload_len = u32::try_from(self.payload.len()).unwrap_or(u32::MAX);
        let chunk_crc = crc32c::crc32c(&self.payload);

        let mut header = [0u8; SNAPSHOT_CHUNK_HEADER_LEN];
        header[0..4].copy_from_slice(&self.chunk_index.to_le_bytes());
        header[4..8].copy_from_slice(&payload_len.to_le_bytes());
        header[8..12].copy_from_slice(&chunk_crc.to_le_bytes());
        header[12..16].copy_from_slice(&0u32.to_le_bytes()); // reserved

        out.reserve(SNAPSHOT_CHUNK_HEADER_LEN + self.payload.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(&self.payload);
    }

    /// Decodes a chunk from a buffer, returning the decoded chunk and consumed bytes.
    pub fn decode(buf: &[u8], expected_chunk_index: u32) -> Result<(Self, usize), StorageError> {
        if buf.len() < SNAPSHOT_CHUNK_HEADER_LEN {
            return Err(StorageError::BufferTooSmall {
                required: SNAPSHOT_CHUNK_HEADER_LEN,
                provided: buf.len(),
            });
        }

        let chunk_index = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if chunk_index != expected_chunk_index {
            return Err(StorageError::CrcMismatch {
                expected: expected_chunk_index,
                computed: chunk_index,
                context: "snapshot_chunk_index",
            });
        }

        let payload_len = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]) as usize;
        let expected_crc = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);

        let total_len = SNAPSHOT_CHUNK_HEADER_LEN + payload_len;
        if buf.len() < total_len {
            return Err(StorageError::BufferTooSmall {
                required: total_len,
                provided: buf.len(),
            });
        }

        let payload_slice = &buf[SNAPSHOT_CHUNK_HEADER_LEN..total_len];
        let computed_crc = crc32c::crc32c(payload_slice);
        if expected_crc != computed_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
                context: "snapshot_chunk_crc",
            });
        }

        let payload = Bytes::copy_from_slice(payload_slice);
        Ok((
            Self {
                chunk_index,
                payload,
            },
            total_len,
        ))
    }
}

/// Cryptographic footer placed at the end of each snapshot file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFooter {
    /// Total file length including header and footer.
    pub total_length: u64,
    /// Whole-file BLAKE3 cryptographic hash computed over bytes `0..(total_length - 64)`.
    pub blake3_digest: [u8; 32],
    /// CRC32C calculated over footer bytes [0..48].
    pub footer_crc: u32,
}

impl SnapshotFooter {
    /// Creates a new `SnapshotFooter` with calculated CRC32C.
    #[must_use]
    pub fn new(total_length: u64, blake3_digest: [u8; 32]) -> Self {
        let mut footer = Self {
            total_length,
            blake3_digest,
            footer_crc: 0,
        };
        footer.footer_crc = footer.compute_crc();
        footer
    }

    /// Computes CRC32C over footer bytes [0..48].
    #[must_use]
    pub fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 48];
        buf[0..8].copy_from_slice(&SNAPSHOT_FOOTER_MAGIC);
        buf[8..16].copy_from_slice(&self.total_length.to_le_bytes());
        buf[16..48].copy_from_slice(&self.blake3_digest);
        crc32c::crc32c(&buf)
    }

    /// Encodes footer into a 64-byte buffer.
    pub fn encode(&self, out: &mut [u8]) -> Result<(), StorageError> {
        if out.len() < SNAPSHOT_FOOTER_LEN {
            return Err(StorageError::BufferTooSmall {
                required: SNAPSHOT_FOOTER_LEN,
                provided: out.len(),
            });
        }

        out[..SNAPSHOT_FOOTER_LEN].fill(0);
        out[0..8].copy_from_slice(&SNAPSHOT_FOOTER_MAGIC);
        out[8..16].copy_from_slice(&self.total_length.to_le_bytes());
        out[16..48].copy_from_slice(&self.blake3_digest);
        out[48..52].copy_from_slice(&self.footer_crc.to_le_bytes());
        Ok(())
    }

    /// Decodes and validates footer from a buffer.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        if buf.len() < SNAPSHOT_FOOTER_LEN {
            return Err(StorageError::BufferTooSmall {
                required: SNAPSHOT_FOOTER_LEN,
                provided: buf.len(),
            });
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != SNAPSHOT_FOOTER_MAGIC {
            return Err(StorageError::InvalidMagic {
                expected: SNAPSHOT_FOOTER_MAGIC,
                found: magic,
            });
        }

        let total_length = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        let mut blake3_digest = [0u8; 32];
        blake3_digest.copy_from_slice(&buf[16..48]);
        let expected_crc = u32::from_le_bytes([buf[48], buf[49], buf[50], buf[51]]);

        let computed_crc = crc32c::crc32c(&buf[0..48]);
        if expected_crc != computed_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
                context: "snapshot_footer_crc",
            });
        }

        Ok(Self {
            total_length,
            blake3_digest,
            footer_crc: expected_crc,
        })
    }
}
