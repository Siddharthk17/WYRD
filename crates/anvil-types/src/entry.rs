use crate::ids::{LogIndex, Term};
use bytes::Bytes;

/// Log entry discriminant kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum EntryKind {
    /// Standard client command entry.
    Normal = 1,
    /// Empty no-op entry appended by leader upon winning election.
    NoOp = 2,
}

impl EntryKind {
    /// Converts a raw `u8` discriminant into an [`EntryKind`].
    pub const fn from_u8(tag: u8) -> Option<Self> {
        match tag {
            1 => Some(Self::Normal),
            2 => Some(Self::NoOp),
            _ => None,
        }
    }

    /// Returns the wire discriminant byte.
    pub const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// A replicated Raft log entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// Monotonic log position.
    pub index: LogIndex,
    /// Raft term during which the entry was proposed.
    pub term: Term,
    /// Discriminant tag indicating the entry purpose.
    pub kind: EntryKind,
    /// Associated opaque payload. Sliced zero-copy from network or disk frames.
    pub payload: Bytes,
}

impl Entry {
    /// Creates a new log entry.
    pub fn new(index: LogIndex, term: Term, kind: EntryKind, payload: Bytes) -> Self {
        Self {
            index,
            term,
            kind,
            payload,
        }
    }

    /// Constructs a `NoOp` log entry with an empty payload.
    pub fn noop(index: LogIndex, term: Term) -> Self {
        Self {
            index,
            term,
            kind: EntryKind::NoOp,
            payload: Bytes::new(),
        }
    }

    /// Constructs a `Normal` log entry with the provided payload.
    pub fn normal(index: LogIndex, term: Term, payload: Bytes) -> Self {
        Self {
            index,
            term,
            kind: EntryKind::Normal,
            payload,
        }
    }
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Entry {
    fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
        let index = LogIndex::from_raw(u.arbitrary()?);
        let term = Term::from_raw(u.arbitrary()?);
        let kind = u.arbitrary()?;
        let raw_bytes: Vec<u8> = u.arbitrary()?;
        Ok(Self {
            index,
            term,
            kind,
            payload: Bytes::from(raw_bytes),
        })
    }
}
