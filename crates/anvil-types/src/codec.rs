use crate::entry::{Entry, EntryKind};
use crate::ids::{ClusterId, GroupId, LogIndex, NodeId, SeqNo, SessionId, Term};
use bytes::{Buf, Bytes};
use thiserror::Error;

/// Magic 4-byte marker designating ANVIL binary frames: "ANVL"
pub const FRAME_MAGIC: [u8; 4] = *b"ANVL";

/// Current wire protocol codec version.
pub const CODEC_VERSION_1: u16 = 1;

/// Maximum allowable frame payload length (64 MiB) to prevent memory exhaustion attacks.
pub const MAX_FRAME_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// Strongly-typed codec errors returned on malformed or invalid inputs.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum CodecError {
    #[error("unexpected end of input: required {needed} bytes, but only {available} available")]
    UnexpectedEof { needed: usize, available: usize },

    #[error("invalid magic bytes: expected {expected:?}, found {found:?}")]
    InvalidMagic { expected: [u8; 4], found: [u8; 4] },

    #[error("unsupported frame version: {0}")]
    UnsupportedVersion(u16),

    #[error("unknown frame type: {0}")]
    UnknownFrameType(u16),

    #[error("unknown entry kind discriminant: {0}")]
    UnknownEntryKind(u8),

    #[error("LEB128 varint integer overflow: exceeds 64-bit capacity")]
    VarintOverflow,

    #[error("frame payload length {length} exceeds maximum safety limit of {limit}")]
    PayloadTooLarge { length: u64, limit: u64 },

    #[error("CRC32C checksum mismatch: expected {expected:#010x}, computed {computed:#010x}")]
    ChecksumMismatch { expected: u32, computed: u32 },

    #[error("invalid node id: 0 is not a valid non-zero NodeId")]
    ZeroNodeId,

    #[error("trailing unparsed bytes remaining in input: {0} bytes")]
    TrailingData(usize),
}

/// Identifies the category of frame being transmitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u16)]
pub enum FrameType {
    /// Raft log entry frame.
    Entry = 1,
    /// Keep-alive or heartbeat frame.
    Heartbeat = 2,
    /// Client request or response frame.
    Message = 3,
}

impl FrameType {
    /// Converts a raw `u16` tag to a `FrameType`.
    pub const fn from_u16(tag: u16) -> Option<Self> {
        match tag {
            1 => Some(Self::Entry),
            2 => Some(Self::Heartbeat),
            3 => Some(Self::Message),
            _ => None,
        }
    }

    /// Returns the raw `u16` tag.
    pub const fn as_u16(self) -> u16 {
        self as u16
    }
}

/// Fixed 16-byte versioned frame header.
///
/// Wire layout:
/// - `[0..4]`: Magic bytes (`b"ANVL"`)
/// - `[4..6]`: Protocol version (little-endian `u16`)
/// - `[6..8]`: Frame type (little-endian `u16`)
/// - `[8..12]`: Payload length in bytes (little-endian `u32`)
/// - `[12..16]`: CRC32C checksum of the header fields and payload (little-endian `u32`)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub version: u16,
    pub frame_type: u16,
    pub payload_len: u32,
    pub checksum: u32,
}

impl FrameHeader {
    pub const HEADER_LEN: usize = 16;

    /// Encodes the frame header into the provided byte buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&FRAME_MAGIC);
        buf.extend_from_slice(&self.version.to_le_bytes());
        buf.extend_from_slice(&self.frame_type.to_le_bytes());
        buf.extend_from_slice(&self.payload_len.to_le_bytes());
        buf.extend_from_slice(&self.checksum.to_le_bytes());
    }

    /// Decodes a frame header from the input slice, advancing the slice on success.
    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        if buf.len() < Self::HEADER_LEN {
            return Err(CodecError::UnexpectedEof {
                needed: Self::HEADER_LEN,
                available: buf.len(),
            });
        }

        let magic: [u8; 4] = [buf[0], buf[1], buf[2], buf[3]];
        if magic != FRAME_MAGIC {
            return Err(CodecError::InvalidMagic {
                expected: FRAME_MAGIC,
                found: magic,
            });
        }

        let version = u16::from_le_bytes([buf[4], buf[5]]);
        if version != CODEC_VERSION_1 {
            return Err(CodecError::UnsupportedVersion(version));
        }

        let frame_type = u16::from_le_bytes([buf[6], buf[7]]);
        let payload_len = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        if payload_len > MAX_FRAME_PAYLOAD_LEN {
            return Err(CodecError::PayloadTooLarge {
                length: u64::from(payload_len),
                limit: u64::from(MAX_FRAME_PAYLOAD_LEN),
            });
        }

        let checksum = u32::from_le_bytes([buf[12], buf[13], buf[14], buf[15]]);
        *buf = &buf[Self::HEADER_LEN..];

        Ok(Self {
            version,
            frame_type,
            payload_len,
            checksum,
        })
    }
}

/// A framed message containing a validated header and an opaque payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub header: FrameHeader,
    pub payload: Bytes,
}

impl Frame {
    /// Encodes a complete frame (header + payload) with CRC32C verification.
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode(frame_type: FrameType, payload: &[u8], buf: &mut Vec<u8>) {
        assert!(payload.len() <= MAX_FRAME_PAYLOAD_LEN as usize);
        let payload_len = payload.len() as u32;

        let mut crc = crc32c::crc32c(&FRAME_MAGIC);
        crc = crc32c::crc32c_append(crc, &CODEC_VERSION_1.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &frame_type.as_u16().to_le_bytes());
        crc = crc32c::crc32c_append(crc, &payload_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, payload);

        let header = FrameHeader {
            version: CODEC_VERSION_1,
            frame_type: frame_type.as_u16(),
            payload_len,
            checksum: crc,
        };

        header.encode(buf);
        buf.extend_from_slice(payload);
    }

    /// Decodes a frame from a byte slice.
    pub fn decode_from_slice(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let mut slice_cursor = *buf;
        let header = FrameHeader::decode(&mut slice_cursor)?;

        let payload_len = header.payload_len as usize;
        if slice_cursor.len() < payload_len {
            return Err(CodecError::UnexpectedEof {
                needed: payload_len,
                available: slice_cursor.len(),
            });
        }

        let (payload_slice, remaining) = slice_cursor.split_at(payload_len);

        let mut crc = crc32c::crc32c(&FRAME_MAGIC);
        crc = crc32c::crc32c_append(crc, &header.version.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &header.frame_type.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &header.payload_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, payload_slice);

        if crc != header.checksum {
            return Err(CodecError::ChecksumMismatch {
                expected: header.checksum,
                computed: crc,
            });
        }

        *buf = remaining;
        Ok(Self {
            header,
            payload: Bytes::copy_from_slice(payload_slice),
        })
    }

    /// Decodes a frame zero-copy from a `bytes::Bytes` buffer.
    pub fn decode_from_bytes(buf: &mut Bytes) -> Result<Self, CodecError> {
        if buf.len() < FrameHeader::HEADER_LEN {
            return Err(CodecError::UnexpectedEof {
                needed: FrameHeader::HEADER_LEN,
                available: buf.len(),
            });
        }

        let mut header_slice = &buf[..FrameHeader::HEADER_LEN];
        let header = FrameHeader::decode(&mut header_slice)?;

        let total_frame_len = FrameHeader::HEADER_LEN + header.payload_len as usize;
        if buf.len() < total_frame_len {
            return Err(CodecError::UnexpectedEof {
                needed: total_frame_len,
                available: buf.len(),
            });
        }

        let payload_slice = &buf[FrameHeader::HEADER_LEN..total_frame_len];
        let mut crc = crc32c::crc32c(&FRAME_MAGIC);
        crc = crc32c::crc32c_append(crc, &header.version.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &header.frame_type.to_le_bytes());
        crc = crc32c::crc32c_append(crc, &header.payload_len.to_le_bytes());
        crc = crc32c::crc32c_append(crc, payload_slice);

        if crc != header.checksum {
            return Err(CodecError::ChecksumMismatch {
                expected: header.checksum,
                computed: crc,
            });
        }

        buf.advance(FrameHeader::HEADER_LEN);
        let payload = buf.split_to(header.payload_len as usize);

        Ok(Self { header, payload })
    }
}

/// Encodes an unsigned 64-bit integer as LEB128, least-significant group first.
pub fn encode_varint_u64(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// Decodes an unsigned 64-bit integer from LEB128, advancing the slice past it.
pub fn decode_varint_u64(buf: &mut &[u8]) -> Result<u64, CodecError> {
    let mut result: u64 = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    while !buf.is_empty() {
        let byte = buf[0];
        *buf = &buf[1..];
        bytes_read += 1;

        let group = u64::from(byte & 0x7F);
        if shift == 63 && group > 1 {
            return Err(CodecError::VarintOverflow);
        }
        if shift > 63 {
            return Err(CodecError::VarintOverflow);
        }

        result |= group << shift;
        shift += 7;

        if (byte & 0x80) == 0 {
            return Ok(result);
        }
        if bytes_read == 10 {
            return Err(CodecError::VarintOverflow);
        }
    }

    Err(CodecError::UnexpectedEof {
        needed: 1,
        available: 0,
    })
}

/// Decodes a `u64` varint from the head of a slice, returning the value and bytes consumed.
pub fn decode_varint_u64_slice(slice: &[u8]) -> Result<(u64, usize), CodecError> {
    let mut cursor = slice;
    let value = decode_varint_u64(&mut cursor)?;
    let consumed = slice.len() - cursor.len();
    Ok((value, consumed))
}

/// Encodes a signed 64-bit integer with `ZigZag` mapping so small negatives stay short.
#[allow(clippy::cast_sign_loss)]
pub fn encode_varint_i64(value: i64, buf: &mut Vec<u8>) {
    let zigzag = ((value << 1) ^ (value >> 63)) as u64;
    encode_varint_u64(zigzag, buf);
}

/// Decodes a ZigZag-mapped signed 64-bit integer, advancing the slice past it.
#[allow(clippy::cast_possible_wrap)]
pub fn decode_varint_i64(buf: &mut &[u8]) -> Result<i64, CodecError> {
    let raw = decode_varint_u64(buf)?;
    let value = ((raw >> 1) as i64) ^ -((raw & 1) as i64);
    Ok(value)
}

pub fn encode_u16_le(value: u16, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn decode_u16_le(buf: &mut &[u8]) -> Result<u16, CodecError> {
    if buf.len() < 2 {
        return Err(CodecError::UnexpectedEof {
            needed: 2,
            available: buf.len(),
        });
    }
    let value = u16::from_le_bytes([buf[0], buf[1]]);
    *buf = &buf[2..];
    Ok(value)
}

pub fn encode_u32_le(value: u32, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn decode_u32_le(buf: &mut &[u8]) -> Result<u32, CodecError> {
    if buf.len() < 4 {
        return Err(CodecError::UnexpectedEof {
            needed: 4,
            available: buf.len(),
        });
    }
    let value = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    *buf = &buf[4..];
    Ok(value)
}

pub fn encode_u64_le(value: u64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&value.to_le_bytes());
}

pub fn decode_u64_le(buf: &mut &[u8]) -> Result<u64, CodecError> {
    if buf.len() < 8 {
        return Err(CodecError::UnexpectedEof {
            needed: 8,
            available: buf.len(),
        });
    }
    let value = u64::from_le_bytes([
        buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
    ]);
    *buf = &buf[8..];
    Ok(value)
}

impl Entry {
    /// Encodes an entry into a framed wire buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        let mut body = Vec::with_capacity(32 + self.payload.len());
        encode_varint_u64(self.index.as_u64(), &mut body);
        encode_varint_u64(self.term.as_u64(), &mut body);
        body.push(self.kind.as_u8());
        body.extend_from_slice(&self.payload);

        assert!(body.len() <= MAX_FRAME_PAYLOAD_LEN as usize);
        Frame::encode(FrameType::Entry, &body, buf);
    }

    /// Decodes an entry zero-copy from a `Bytes` buffer.
    pub fn decode_from_bytes(buf: &mut Bytes) -> Result<Self, CodecError> {
        let frame = Frame::decode_from_bytes(buf)?;
        if frame.header.frame_type != FrameType::Entry.as_u16() {
            return Err(CodecError::UnknownFrameType(frame.header.frame_type));
        }

        let mut body = frame.payload;
        let (index_val, idx_consumed) = decode_varint_u64_slice(&body)?;
        body.advance(idx_consumed);

        let (term_val, term_consumed) = decode_varint_u64_slice(&body)?;
        body.advance(term_consumed);

        if body.is_empty() {
            return Err(CodecError::UnexpectedEof {
                needed: 1,
                available: 0,
            });
        }
        let kind_byte = body[0];
        body.advance(1);

        let kind = EntryKind::from_u8(kind_byte).ok_or(CodecError::UnknownEntryKind(kind_byte))?;

        Ok(Self {
            index: LogIndex::from_raw(index_val),
            term: Term::from_raw(term_val),
            kind,
            payload: body,
        })
    }

    /// Decodes an entry from a byte slice, copying the payload into `Bytes`.
    pub fn decode_from_slice(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let frame = Frame::decode_from_slice(buf)?;
        if frame.header.frame_type != FrameType::Entry.as_u16() {
            return Err(CodecError::UnknownFrameType(frame.header.frame_type));
        }

        let mut body = &frame.payload[..];
        let index_val = decode_varint_u64(&mut body)?;
        let term_val = decode_varint_u64(&mut body)?;

        if body.is_empty() {
            return Err(CodecError::UnexpectedEof {
                needed: 1,
                available: 0,
            });
        }
        let kind_byte = body[0];
        body = &body[1..];

        let kind = EntryKind::from_u8(kind_byte).ok_or(CodecError::UnknownEntryKind(kind_byte))?;

        Ok(Self {
            index: LogIndex::from_raw(index_val),
            term: Term::from_raw(term_val),
            kind,
            payload: Bytes::copy_from_slice(body),
        })
    }
}

impl NodeId {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.get(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Self::new(raw).ok_or(CodecError::ZeroNodeId)
    }
}

impl Term {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}

impl LogIndex {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}

impl GroupId {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}

impl ClusterId {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}

impl SessionId {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}

impl SeqNo {
    pub fn encode(self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.as_u64(), buf);
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self, CodecError> {
        let raw = decode_varint_u64(buf)?;
        Ok(Self::from_raw(raw))
    }
}
