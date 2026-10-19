use core::fmt;
use core::num::NonZeroU64;
use thiserror::Error;

/// Error returned when attempting to construct a [`NodeId`] with a zero value.
#[derive(Debug, Error, PartialEq, Eq, Clone, Copy)]
#[error("node id must be non-zero")]
pub struct ZeroNodeIdError;

/// A non-zero node identifier representing a cluster participant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct NodeId(NonZeroU64);

impl NodeId {
    /// Creates a new `NodeId` if `raw` is non-zero.
    pub const fn new(raw: u64) -> Option<Self> {
        match NonZeroU64::new(raw) {
            Some(nz) => Some(Self(nz)),
            None => None,
        }
    }

    /// Creates a new `NodeId` from a known `NonZeroU64`.
    pub const fn from_nonzero(val: NonZeroU64) -> Self {
        Self(val)
    }

    /// Returns the underlying raw integer.
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the underlying `NonZeroU64`.
    pub const fn as_nonzero(self) -> NonZeroU64 {
        self.0
    }
}

impl TryFrom<u64> for NodeId {
    type Error = ZeroNodeIdError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        Self::new(value).ok_or(ZeroNodeIdError)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// Raft election term.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Term(u64);

impl Term {
    /// Creates a `Term` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value of the term.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns true if the term is 0.
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Returns the next term, returning `None` on overflow.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked term addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked term subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Saturating addition.
    #[must_use]
    pub const fn saturating_add(self, rhs: u64) -> Self {
        Self(self.0.saturating_add(rhs))
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Monotonically increasing Raft log index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct LogIndex(u64);

impl LogIndex {
    /// Creates a `LogIndex` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value of the log index.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns true if the index is zero (representing the dummy root log position).
    pub const fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Returns the immediately following index, returning `None` on overflow.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Returns the predecessor index, returning `None` on underflow below zero.
    pub const fn prev(self) -> Option<Self> {
        match self.0.checked_sub(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked log index addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked log index subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Saturating addition.
    #[must_use]
    pub const fn saturating_add(self, rhs: u64) -> Self {
        Self(self.0.saturating_add(rhs))
    }

    /// Saturating subtraction.
    #[must_use]
    pub const fn saturating_sub(self, rhs: u64) -> Self {
        Self(self.0.saturating_sub(rhs))
    }
}

impl fmt::Display for LogIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Multi-Raft or sharding group identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct GroupId(u64);

impl GroupId {
    /// Creates a `GroupId` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Checked addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for GroupId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// ANVIL cluster identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct ClusterId(u64);

impl ClusterId {
    /// Creates a `ClusterId` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Checked addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Client session identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SessionId(u64);

impl SessionId {
    /// Creates a `SessionId` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Checked addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Client sequence number for linearizable request deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SeqNo(u64);

impl SeqNo {
    /// Creates a `SeqNo` from a raw 64-bit integer.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the raw integer value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next sequence number, returning `None` on overflow.
    pub const fn next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked addition.
    pub const fn checked_add(self, rhs: u64) -> Option<Self> {
        match self.0.checked_add(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction.
    pub const fn checked_sub(self, rhs: u64) -> Option<Self> {
        match self.0.checked_sub(rhs) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }
}

impl fmt::Display for SeqNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
