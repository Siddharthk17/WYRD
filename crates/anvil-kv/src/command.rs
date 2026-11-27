#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

//! Replicated KV command encoding. Payloads travel inside Raft `Normal`
//! entries so the consensus layer stays opaque to client semantics.

use anvil_types::{LogIndex, SeqNo, SessionId, decode_varint_u64, encode_varint_u64};
use bytes::Bytes;
use thiserror::Error;

/// Wire tags for each log command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum KvRole {
    RegisterSession = 1,
    Put = 2,
    Delete = 3,
    Cas = 4,
    Append = 5,
    ClockTick = 6,
    ExpireSessions = 7,
}

impl KvRole {
    #[must_use]
    pub const fn from_u8(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::RegisterSession),
            2 => Some(Self::Put),
            3 => Some(Self::Delete),
            4 => Some(Self::Cas),
            5 => Some(Self::Append),
            6 => Some(Self::ClockTick),
            7 => Some(Self::ExpireSessions),
            _ => None,
        }
    }
}

/// Errors from command decode and bounds checks.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum KvCommandError {
    #[error("unexpected end of command bytes")]
    UnexpectedEof,
    #[error("unknown command tag {0}")]
    UnknownTag(u8),
    #[error("key too large: {0} bytes")]
    KeyTooLarge(usize),
    #[error("value too large: {0} bytes")]
    ValueTooLarge(usize),
    #[error("trailing bytes after command: {0}")]
    Trailing(usize),
}

/// A replicated write or virtual-time advance. Reads never enter the log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvCommand {
    RegisterSession,
    Put {
        session: SessionId,
        seq: SeqNo,
        acked: SeqNo,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        session: SessionId,
        seq: SeqNo,
        acked: SeqNo,
        key: Bytes,
    },
    Cas {
        session: SessionId,
        seq: SeqNo,
        acked: SeqNo,
        key: Bytes,
        expected_version: LogIndex,
        value: Bytes,
    },
    Append {
        session: SessionId,
        seq: SeqNo,
        acked: SeqNo,
        key: Bytes,
        value: Bytes,
    },
    ClockTick {
        tick: u64,
    },
    ExpireSessions {
        up_to_tick: u64,
    },
}

/// Client-visible result of one applied command or read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KvResponse {
    SessionRegistered {
        session: SessionId,
    },
    Ok,
    Value {
        value: Option<Bytes>,
        version: LogIndex,
    },
    List {
        values: Vec<Bytes>,
        version: LogIndex,
    },
    Range {
        pairs: Vec<(Bytes, Bytes)>,
    },
    CasConflict {
        version: LogIndex,
    },
    SessionExpired,
    UnknownSession,
    Duplicate,
    SessionsExpired {
        removed: u64,
    },
}

impl KvCommand {
    /// Encodes the command into a Raft log payload.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::RegisterSession => buf.push(KvRole::RegisterSession as u8),
            Self::Put {
                session,
                seq,
                acked,
                key,
                value,
            } => {
                buf.push(KvRole::Put as u8);
                encode_varint_u64(session.as_u64(), buf);
                encode_varint_u64(seq.as_u64(), buf);
                encode_varint_u64(acked.as_u64(), buf);
                encode_bytes(key, buf);
                encode_bytes(value, buf);
            }
            Self::Delete {
                session,
                seq,
                acked,
                key,
            } => {
                buf.push(KvRole::Delete as u8);
                encode_varint_u64(session.as_u64(), buf);
                encode_varint_u64(seq.as_u64(), buf);
                encode_varint_u64(acked.as_u64(), buf);
                encode_bytes(key, buf);
            }
            Self::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            } => {
                buf.push(KvRole::Cas as u8);
                encode_varint_u64(session.as_u64(), buf);
                encode_varint_u64(seq.as_u64(), buf);
                encode_varint_u64(acked.as_u64(), buf);
                encode_bytes(key, buf);
                encode_varint_u64(expected_version.as_u64(), buf);
                encode_bytes(value, buf);
            }
            Self::Append {
                session,
                seq,
                acked,
                key,
                value,
            } => {
                buf.push(KvRole::Append as u8);
                encode_varint_u64(session.as_u64(), buf);
                encode_varint_u64(seq.as_u64(), buf);
                encode_varint_u64(acked.as_u64(), buf);
                encode_bytes(key, buf);
                encode_bytes(value, buf);
            }
            Self::ClockTick { tick } => {
                buf.push(KvRole::ClockTick as u8);
                encode_varint_u64(*tick, buf);
            }
            Self::ExpireSessions { up_to_tick } => {
                buf.push(KvRole::ExpireSessions as u8);
                encode_varint_u64(*up_to_tick, buf);
            }
        }
    }

    /// Decodes one command, rejecting trailing bytes and oversized fields.
    pub fn decode(input: &[u8], max_key: usize, max_value: usize) -> Result<Self, KvCommandError> {
        let (tag, mut rest) = input.split_first().ok_or(KvCommandError::UnexpectedEof)?;
        let role = KvRole::from_u8(*tag).ok_or(KvCommandError::UnknownTag(*tag))?;
        let cmd = match role {
            KvRole::RegisterSession => {
                if !rest.is_empty() {
                    return Err(KvCommandError::Trailing(rest.len()));
                }
                Self::RegisterSession
            }
            KvRole::Put => {
                let (session, seq, acked, after) = decode_session_header(&mut rest)?;
                let (key, after) = decode_bytes(after, max_key, true)?;
                let (value, after) = decode_bytes(after, max_value, false)?;
                if !after.is_empty() {
                    return Err(KvCommandError::Trailing(after.len()));
                }
                Self::Put {
                    session,
                    seq,
                    acked,
                    key,
                    value,
                }
            }
            KvRole::Delete => {
                let (session, seq, acked, after) = decode_session_header(&mut rest)?;
                let (key, after) = decode_bytes(after, max_key, true)?;
                if !after.is_empty() {
                    return Err(KvCommandError::Trailing(after.len()));
                }
                Self::Delete {
                    session,
                    seq,
                    acked,
                    key,
                }
            }
            KvRole::Cas => {
                let (session, seq, acked, after) = decode_session_header(&mut rest)?;
                let (key, after) = decode_bytes(after, max_key, true)?;
                let mut tail = after;
                let version =
                    decode_varint_u64(&mut tail).map_err(|_| KvCommandError::UnexpectedEof)?;
                let (value, after) = decode_bytes(tail, max_value, false)?;
                if !after.is_empty() {
                    return Err(KvCommandError::Trailing(after.len()));
                }
                Self::Cas {
                    session,
                    seq,
                    acked,
                    key,
                    expected_version: LogIndex::from_raw(version),
                    value,
                }
            }
            KvRole::Append => {
                let (session, seq, acked, after) = decode_session_header(&mut rest)?;
                let (key, after) = decode_bytes(after, max_key, true)?;
                let (value, after) = decode_bytes(after, max_value, false)?;
                if !after.is_empty() {
                    return Err(KvCommandError::Trailing(after.len()));
                }
                Self::Append {
                    session,
                    seq,
                    acked,
                    key,
                    value,
                }
            }
            KvRole::ClockTick => {
                let mut tail = rest;
                let tick =
                    decode_varint_u64(&mut tail).map_err(|_| KvCommandError::UnexpectedEof)?;
                if !tail.is_empty() {
                    return Err(KvCommandError::Trailing(tail.len()));
                }
                Self::ClockTick { tick }
            }
            KvRole::ExpireSessions => {
                let mut tail = rest;
                let up_to =
                    decode_varint_u64(&mut tail).map_err(|_| KvCommandError::UnexpectedEof)?;
                if !tail.is_empty() {
                    return Err(KvCommandError::Trailing(tail.len()));
                }
                Self::ExpireSessions { up_to_tick: up_to }
            }
        };
        Ok(cmd)
    }
}

fn encode_bytes(data: &[u8], buf: &mut Vec<u8>) {
    encode_varint_u64(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

fn decode_bytes(
    input: &[u8],
    limit: usize,
    is_key: bool,
) -> Result<(Bytes, &[u8]), KvCommandError> {
    let mut tail = input;
    let len = decode_varint_u64(&mut tail).map_err(|_| KvCommandError::UnexpectedEof)?;
    let Ok(len) = usize::try_from(len) else {
        return Err(KvCommandError::UnexpectedEof);
    };
    if len > limit {
        return Err(if is_key {
            KvCommandError::KeyTooLarge(len)
        } else {
            KvCommandError::ValueTooLarge(len)
        });
    }
    if tail.len() < len {
        return Err(KvCommandError::UnexpectedEof);
    }
    let (head, rest) = tail.split_at(len);
    Ok((Bytes::copy_from_slice(head), rest))
}

fn decode_session_header<'a>(
    input: &mut &'a [u8],
) -> Result<(SessionId, SeqNo, SeqNo, &'a [u8]), KvCommandError> {
    let session = decode_varint_u64(input).map_err(|_| KvCommandError::UnexpectedEof)?;
    let seq = decode_varint_u64(input).map_err(|_| KvCommandError::UnexpectedEof)?;
    let acked = decode_varint_u64(input).map_err(|_| KvCommandError::UnexpectedEof)?;
    Ok((
        SessionId::from_raw(session),
        SeqNo::from_raw(seq),
        SeqNo::from_raw(acked),
        input,
    ))
}
