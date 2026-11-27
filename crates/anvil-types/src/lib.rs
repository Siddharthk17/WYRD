#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-types`: Identifiers, log entries, and zero-copy binary codec.

pub mod codec;
pub mod config;
pub mod entry;
pub mod ids;

pub use codec::{
    CODEC_VERSION_1, CodecError, FRAME_MAGIC, Frame, FrameHeader, FrameType, MAX_FRAME_PAYLOAD_LEN,
    decode_u16_le, decode_u32_le, decode_u64_le, decode_varint_i64, decode_varint_u64,
    decode_varint_u64_slice, encode_u16_le, encode_u32_le, encode_u64_le, encode_varint_i64,
    encode_varint_u64,
};
pub use config::{
    ClusterConfig, MAX_LEARNERS, MAX_VOTERS, MembershipError, config_entry, decode_config_entry,
    ensure_cluster,
};
pub use entry::{Entry, EntryKind};
pub use ids::{ClusterId, GroupId, LogIndex, NodeId, SeqNo, SessionId, Term, ZeroNodeIdError};

/// Marks a rare protocol path for farm coverage tracking.
///
/// The sink is an explicit `&mut Vec<&'static str>` carried through sans-IO
/// outputs, never a global: deterministic crates forbid statics with
/// interior mutability, so every hit is recorded into caller-provided
/// storage and drained by the simulator. Each name must appear in
/// `anvil_sim::SOMETIMES_INVENTORY`, enforced by a source-scan test.
#[macro_export]
macro_rules! sometimes {
    ($sink:expr, $name:expr) => {
        $sink.push($name)
    };
}
