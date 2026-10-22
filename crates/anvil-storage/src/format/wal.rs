#![forbid(unsafe_code)]

//! On-disk format codec for Write-Ahead Log (WAL) segments.

use crate::device::SectorSize;
use crate::error::StorageError;
use anvil_types::{Entry, EntryKind, LogIndex, Term};
use bytes::Bytes;

/// WAL header magic bytes: ASCII `"ANVILWAL"`.
pub const WAL_MAGIC: [u8; 8] = *b"ANVILWAL";

/// Format version 1.
pub const WAL_VERSION: u16 = 1;

/// Length of fixed fields in segment header before zero padding (44 bytes).
pub const WAL_HEADER_FIXED_LEN: usize = 44;

/// Length of fixed fields in a WAL record frame header (25 bytes).
pub const WAL_RECORD_HEADER_FIXED_LEN: usize = 25;

/// Header written to the first sector of every WAL segment file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalSegmentHeader {
    /// Format version number.
    pub version: u16,
    /// Sector size in bytes (512 or 4096).
    pub sector_size: SectorSize,
    /// Unique monotonically increasing segment identifier.
    pub segment_id: u64,
    /// Expected log index of the first record in this segment.
    pub first_index: LogIndex,
    /// Raft term of the entry preceding `first_index`.
    pub prev_term: Term,
    /// CRC32C calculated over header fields [0..40].
    pub header_crc: u32,
}

impl WalSegmentHeader {
    /// Constructs a new `WalSegmentHeader` with calculated CRC32C.
    #[must_use]
    pub fn new(
        sector_size: SectorSize,
        segment_id: u64,
        first_index: LogIndex,
        prev_term: Term,
    ) -> Self {
        let mut header = Self {
            version: WAL_VERSION,
            sector_size,
            segment_id,
            first_index,
            prev_term,
            header_crc: 0,
        };
        header.header_crc = header.compute_crc();
        header
    }

    /// Computes CRC32C over the first 40 bytes of the header.
    #[must_use]
    pub fn compute_crc(&self) -> u32 {
        let mut buf = [0u8; 40];
        buf[0..8].copy_from_slice(&WAL_MAGIC);
        buf[8..10].copy_from_slice(&self.version.to_le_bytes());
        buf[10..12].copy_from_slice(&self.sector_size.as_u16().to_le_bytes());
        buf[12..16].copy_from_slice(&0u32.to_le_bytes()); // reserved
        buf[16..24].copy_from_slice(&self.segment_id.to_le_bytes());
        buf[24..32].copy_from_slice(&self.first_index.as_u64().to_le_bytes());
        buf[32..40].copy_from_slice(&self.prev_term.as_u64().to_le_bytes());
        crc32c::crc32c(&buf)
    }

    /// Encodes the segment header into a full sector buffer, zero-padding remainder.
    pub fn encode(&self, out: &mut [u8]) -> Result<(), StorageError> {
        let sector_len = self.sector_size.as_usize();
        if out.len() < sector_len {
            return Err(StorageError::BufferTooSmall {
                required: sector_len,
                provided: out.len(),
            });
        }

        out[..sector_len].fill(0);
        out[0..8].copy_from_slice(&WAL_MAGIC);
        out[8..10].copy_from_slice(&self.version.to_le_bytes());
        out[10..12].copy_from_slice(&self.sector_size.as_u16().to_le_bytes());
        out[12..16].copy_from_slice(&0u32.to_le_bytes());
        out[16..24].copy_from_slice(&self.segment_id.to_le_bytes());
        out[24..32].copy_from_slice(&self.first_index.as_u64().to_le_bytes());
        out[32..40].copy_from_slice(&self.prev_term.as_u64().to_le_bytes());
        out[40..44].copy_from_slice(&self.header_crc.to_le_bytes());
        Ok(())
    }

    /// Decodes and validates a `WalSegmentHeader` from a sector buffer.
    pub fn decode(buf: &[u8], sector_size: SectorSize) -> Result<Self, StorageError> {
        let sector_len = sector_size.as_usize();
        if buf.len() < sector_len {
            return Err(StorageError::BufferTooSmall {
                required: sector_len,
                provided: buf.len(),
            });
        }

        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if magic != WAL_MAGIC {
            return Err(StorageError::InvalidMagic {
                expected: WAL_MAGIC,
                found: magic,
            });
        }

        let version = u16::from_le_bytes([buf[8], buf[9]]);
        if version != WAL_VERSION {
            return Err(StorageError::UnsupportedVersion { found: version });
        }

        let raw_sector_size = u16::from_le_bytes([buf[10], buf[11]]);
        let parsed_sector_size = SectorSize::from_u16(raw_sector_size)?;
        if parsed_sector_size != sector_size {
            return Err(StorageError::UnsupportedSectorSize {
                found: raw_sector_size,
            });
        }

        let segment_id = u64::from_le_bytes([
            buf[16], buf[17], buf[18], buf[19], buf[20], buf[21], buf[22], buf[23],
        ]);
        let first_index = LogIndex::from_raw(u64::from_le_bytes([
            buf[24], buf[25], buf[26], buf[27], buf[28], buf[29], buf[30], buf[31],
        ]));
        let prev_term = Term::from_raw(u64::from_le_bytes([
            buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
        ]));
        let expected_crc = u32::from_le_bytes([buf[40], buf[41], buf[42], buf[43]]);

        let computed_crc = crc32c::crc32c(&buf[0..40]);
        if expected_crc != computed_crc {
            return Err(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: computed_crc,
                context: "wal_segment_header",
            });
        }

        // Verify remaining sector bytes are zero
        for (i, &b) in buf[WAL_HEADER_FIXED_LEN..sector_len].iter().enumerate() {
            if b != 0 {
                return Err(StorageError::NonZeroPadding {
                    offset: WAL_HEADER_FIXED_LEN + i,
                });
            }
        }

        Ok(Self {
            version,
            sector_size: parsed_sector_size,
            segment_id,
            first_index,
            prev_term,
            header_crc: expected_crc,
        })
    }
}

/// Computes the chained CRC32C checksum for a WAL record frame.
#[must_use]
pub fn compute_wal_record_crc(
    prev_crc: u32,
    segment_id: u64,
    file_offset: u64,
    index: LogIndex,
    term: Term,
    kind: EntryKind,
    payload: &[u8],
) -> u32 {
    let mut meta = [0u8; 37];
    meta[0..8].copy_from_slice(&segment_id.to_le_bytes());
    meta[8..16].copy_from_slice(&file_offset.to_le_bytes());
    meta[16..24].copy_from_slice(&index.as_u64().to_le_bytes());
    meta[24..32].copy_from_slice(&term.as_u64().to_le_bytes());
    let payload_len = u32::try_from(payload.len()).unwrap_or(u32::MAX);
    meta[32..36].copy_from_slice(&payload_len.to_le_bytes());
    meta[36] = match kind {
        EntryKind::Normal => 0,
        EntryKind::NoOp => 1,
    };

    let step1 = crc32c::crc32c_append(prev_crc, &meta);
    crc32c::crc32c_append(step1, payload)
}

/// In-memory representation of an individual WAL record frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecordFrame {
    /// Raft log index.
    pub index: LogIndex,
    /// Raft consensus term.
    pub term: Term,
    /// Entry kind.
    pub kind: EntryKind,
    /// Chained CRC32C.
    pub crc32c: u32,
    /// Entry payload slice.
    pub payload: Bytes,
}

impl WalRecordFrame {
    /// Creates a record frame from an `Entry` and chains the CRC32C.
    #[must_use]
    pub fn from_entry(entry: &Entry, prev_crc: u32, segment_id: u64, file_offset: u64) -> Self {
        let crc = compute_wal_record_crc(
            prev_crc,
            segment_id,
            file_offset,
            entry.index,
            entry.term,
            entry.kind,
            &entry.payload,
        );
        Self {
            index: entry.index,
            term: entry.term,
            kind: entry.kind,
            crc32c: crc,
            payload: entry.payload.clone(),
        }
    }

    /// Converts this frame back into a domain `Entry`.
    #[must_use]
    pub fn to_entry(&self) -> Entry {
        Entry {
            index: self.index,
            term: self.term,
            kind: self.kind,
            payload: self.payload.clone(),
        }
    }

    /// Computes total frame size aligned to the configured sector size.
    #[must_use]
    pub fn frame_size(&self, sector_size: SectorSize) -> usize {
        let unaligned = WAL_RECORD_HEADER_FIXED_LEN + self.payload.len();
        sector_size.align_up(unaligned)
    }

    /// Encodes this record frame into the output buffer, appending zero padding.
    pub fn encode_frame(&self, sector_size: SectorSize, out: &mut Vec<u8>) {
        let start_len = out.len();
        let payload_len = u32::try_from(self.payload.len()).unwrap_or(u32::MAX);

        out.reserve(self.frame_size(sector_size));

        // 25-byte fixed header
        let mut header = [0u8; WAL_RECORD_HEADER_FIXED_LEN];
        header[0..8].copy_from_slice(&self.index.as_u64().to_le_bytes());
        header[8..16].copy_from_slice(&self.term.as_u64().to_le_bytes());
        header[16..20].copy_from_slice(&payload_len.to_le_bytes());
        header[20] = match self.kind {
            EntryKind::Normal => 0,
            EntryKind::NoOp => 1,
        };
        header[21..25].copy_from_slice(&self.crc32c.to_le_bytes());
        out.extend_from_slice(&header);

        // Payload
        out.extend_from_slice(&self.payload);

        // Zero padding to sector boundary
        let frame_len = out.len() - start_len;
        let aligned_len = sector_size.align_up(frame_len);
        let padding_needed = aligned_len - frame_len;
        out.resize(out.len() + padding_needed, 0);
    }
}

/// Result of scanning a WAL record frame from disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalScanResult {
    /// Frame is valid and verified.
    Valid {
        /// Decoded record frame.
        frame: WalRecordFrame,
        /// Total sector-aligned bytes consumed.
        bytes_consumed: usize,
        /// Next chained CRC32C seed.
        next_crc: u32,
    },
    /// Reached unallocated or zeroed trailing sectors.
    Unallocated,
    /// Corrupt frame detected (CRC mismatch, invalid kind, non-zero padding).
    Corrupted(StorageError),
}

/// Decodes and validates a single WAL record frame at `file_offset`.
pub fn decode_wal_frame(
    buf: &[u8],
    sector_size: SectorSize,
    prev_crc: u32,
    segment_id: u64,
    file_offset: u64,
) -> WalScanResult {
    let sector_len = sector_size.as_usize();
    if buf.len() < sector_len {
        return WalScanResult::Corrupted(StorageError::BufferTooSmall {
            required: sector_len,
            provided: buf.len(),
        });
    }

    // Check if the initial sector is completely zeroes (unallocated space)
    if buf[..sector_len].iter().all(|&b| b == 0) {
        return WalScanResult::Unallocated;
    }

    if buf.len() < WAL_RECORD_HEADER_FIXED_LEN {
        return WalScanResult::Corrupted(StorageError::BufferTooSmall {
            required: WAL_RECORD_HEADER_FIXED_LEN,
            provided: buf.len(),
        });
    }

    let raw_index = u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    let raw_term = u64::from_le_bytes([
        buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
    ]);
    let payload_len = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]) as usize;
    let kind_byte = buf[20];
    let expected_crc = u32::from_le_bytes([buf[21], buf[22], buf[23], buf[24]]);

    let kind = match kind_byte {
        0 => EntryKind::Normal,
        1 => EntryKind::NoOp,
        _ => {
            return WalScanResult::Corrupted(StorageError::CrcMismatch {
                expected: expected_crc,
                computed: 0,
                context: "wal_record_kind_invalid",
            });
        }
    };

    let total_unpadded = WAL_RECORD_HEADER_FIXED_LEN + payload_len;
    let total_frame_len = sector_size.align_up(total_unpadded);

    if buf.len() < total_frame_len {
        return WalScanResult::Corrupted(StorageError::BufferTooSmall {
            required: total_frame_len,
            provided: buf.len(),
        });
    }

    let payload_bytes = &buf[WAL_RECORD_HEADER_FIXED_LEN..total_unpadded];

    // Compute and verify chained CRC32C
    let index = LogIndex::from_raw(raw_index);
    let term = Term::from_raw(raw_term);
    let computed_crc = compute_wal_record_crc(
        prev_crc,
        segment_id,
        file_offset,
        index,
        term,
        kind,
        payload_bytes,
    );

    if expected_crc != computed_crc {
        return WalScanResult::Corrupted(StorageError::CrcMismatch {
            expected: expected_crc,
            computed: computed_crc,
            context: "wal_record_frame_crc",
        });
    }

    // Verify trailing padding in the aligned frame is strictly zero
    for (i, &b) in buf[total_unpadded..total_frame_len].iter().enumerate() {
        if b != 0 {
            return WalScanResult::Corrupted(StorageError::NonZeroPadding {
                offset: total_unpadded + i,
            });
        }
    }

    let payload = Bytes::copy_from_slice(payload_bytes);
    WalScanResult::Valid {
        frame: WalRecordFrame {
            index,
            term,
            kind,
            crc32c: expected_crc,
            payload,
        },
        bytes_consumed: total_frame_len,
        next_crc: expected_crc,
    }
}
