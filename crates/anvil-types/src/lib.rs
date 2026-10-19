#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-types`: Identifiers, log entries, and zero-copy binary codec.

pub mod codec;
pub mod entry;
pub mod ids;

pub use codec::{
    CODEC_VERSION_1, CodecError, FRAME_MAGIC, Frame, FrameHeader, FrameType, MAX_FRAME_PAYLOAD_LEN,
    decode_u16_le, decode_u32_le, decode_u64_le, decode_varint_i64, decode_varint_u64,
    encode_u16_le, encode_u32_le, encode_u64_le, encode_varint_i64, encode_varint_u64,
};
pub use entry::{Entry, EntryKind};
pub use ids::{ClusterId, GroupId, LogIndex, NodeId, SeqNo, SessionId, Term, ZeroNodeIdError};
