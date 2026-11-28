//! Stable identities for the 20 planted bugs used by the kill matrix.
//!
//! A mutation is selected once, before a run starts, and is then read-only.
//! The selector is the only static with interior mutability in a
//! deterministic crate and exists solely under the `mutants` feature
//! (ADR 0038). Production builds have no such static.

#[cfg(feature = "mutants")]
use std::sync::Once;
#[cfg(feature = "mutants")]
use std::sync::atomic::{AtomicU8, Ordering};

/// No mutation active.
#[cfg(feature = "mutants")]
const NONE: u8 = 0;

#[cfg(feature = "mutants")]
static ACTIVE: AtomicU8 = AtomicU8::new(NONE);
#[cfg(feature = "mutants")]
static INIT: Once = Once::new();

/// One planted bug, with the stable id used by `cargo xtask mutant ID`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum MutantId {
    /// Commit previous-term entries by counting replicas.
    M01 = 1,
    /// Send a vote response before hard state is persisted.
    M02 = 2,
    /// Acknowledge an append before it is persisted.
    M03 = 3,
    /// Count the leader's own match before it is persisted.
    M04 = 4,
    /// Skip the leadership confirmation in `ReadIndex`.
    M05 = 5,
    /// Ignore the drift bound in a lease read.
    M06 = 6,
    /// Fail to truncate a conflicting suffix correctly.
    M07 = 7,
    /// Apply an uncommitted entry.
    M08 = 8,
    /// Off-by-one in the snapshot index.
    M09 = 9,
    /// Do not restore sessions on `InstallSnapshot`.
    M10 = 10,
    /// Do not restore the vote after restart.
    M11 = 11,
    /// Allow a config change before a current-term commit.
    M12 = 12,
    /// Use OR instead of AND for joint quorums.
    M13 = 13,
    /// Evict the session dedup cache too early.
    M14 = 14,
    /// Accept a torn tail or silently truncate mid-log corruption.
    M15 = 15,
    /// Retry an fsync error.
    M16 = 16,
    /// Do not step down without `CheckQuorum`.
    M17 = 17,
    /// Bypass `PreVote` and disrupt the cluster.
    M18 = 18,
    /// Let a removed node keep campaigning.
    M19 = 19,
    /// Accept an unverified entry during repair.
    M20 = 20,
}

/// All mutants in stable order.
pub const ALL: [MutantId; 20] = [
    MutantId::M01,
    MutantId::M02,
    MutantId::M03,
    MutantId::M04,
    MutantId::M05,
    MutantId::M06,
    MutantId::M07,
    MutantId::M08,
    MutantId::M09,
    MutantId::M10,
    MutantId::M11,
    MutantId::M12,
    MutantId::M13,
    MutantId::M14,
    MutantId::M15,
    MutantId::M16,
    MutantId::M17,
    MutantId::M18,
    MutantId::M19,
    MutantId::M20,
];

impl MutantId {
    /// Parses `M01`..`M20`. `none` and `off` clear the selection.
    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        if matches!(raw.to_ascii_lowercase().as_str(), "none" | "off" | "") {
            return None;
        }
        let digits = raw.strip_prefix('M').unwrap_or(raw);
        Self::from_code(digits.parse().ok()?)
    }

    /// Maps a numeric code to a mutant.
    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::M01),
            2 => Some(Self::M02),
            3 => Some(Self::M03),
            4 => Some(Self::M04),
            5 => Some(Self::M05),
            6 => Some(Self::M06),
            7 => Some(Self::M07),
            8 => Some(Self::M08),
            9 => Some(Self::M09),
            10 => Some(Self::M10),
            11 => Some(Self::M11),
            12 => Some(Self::M12),
            13 => Some(Self::M13),
            14 => Some(Self::M14),
            15 => Some(Self::M15),
            16 => Some(Self::M16),
            17 => Some(Self::M17),
            18 => Some(Self::M18),
            19 => Some(Self::M19),
            20 => Some(Self::M20),
            _ => None,
        }
    }

    /// Stable two-digit name.
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::M01 => "M01",
            Self::M02 => "M02",
            Self::M03 => "M03",
            Self::M04 => "M04",
            Self::M05 => "M05",
            Self::M06 => "M06",
            Self::M07 => "M07",
            Self::M08 => "M08",
            Self::M09 => "M09",
            Self::M10 => "M10",
            Self::M11 => "M11",
            Self::M12 => "M12",
            Self::M13 => "M13",
            Self::M14 => "M14",
            Self::M15 => "M15",
            Self::M16 => "M16",
            Self::M17 => "M17",
            Self::M18 => "M18",
            Self::M19 => "M19",
            Self::M20 => "M20",
        }
    }

    /// The bug this mutant plants, as stated in the Week 6 specification.
    #[must_use]
    pub const fn description(self) -> &'static str {
        match self {
            Self::M01 => "commit previous-term entries by counting replicas",
            Self::M02 => "vote response sent before hard state is persisted",
            Self::M03 => "follower acks an append before it is persisted",
            Self::M04 => "leader counts its own match before it is persisted",
            Self::M05 => "ReadIndex skips leadership confirmation",
            Self::M06 => "lease read ignores the drift bound",
            Self::M07 => "conflicting suffix not truncated correctly",
            Self::M08 => "uncommitted entry applied",
            Self::M09 => "snapshot index off by one",
            Self::M10 => "InstallSnapshot does not restore sessions",
            Self::M11 => "vote not restored after restart",
            Self::M12 => "config change allowed before a current-term commit",
            Self::M13 => "joint quorum uses OR instead of AND",
            Self::M14 => "session dedup cache evicted too early",
            Self::M15 => "torn tail accepted or mid-log corruption silently truncated",
            Self::M16 => "fsync error retried",
            Self::M17 => "leader does not step down without CheckQuorum",
            Self::M18 => "PreVote bypass causes disruption",
            Self::M19 => "removed node keeps campaigning",
            Self::M20 => "repair accepts an unverified entry",
        }
    }
}

/// Selects the active mutant. Called once before any worker thread starts.
pub fn set_active(id: Option<MutantId>) {
    #[cfg(feature = "mutants")]
    ACTIVE.store(id.map_or(NONE, |mutant| mutant as u8), Ordering::Release);
    #[cfg(not(feature = "mutants"))]
    let _ = id;
}

/// Returns the active mutant.
#[must_use]
pub fn active() -> Option<MutantId> {
    #[cfg(feature = "mutants")]
    {
        MutantId::from_code(ACTIVE.load(Ordering::Acquire))
    }
    #[cfg(not(feature = "mutants"))]
    {
        None
    }
}

/// Returns true when `id` is active. This is the guard used at every
/// planted-bug site; without the feature it is always false.
#[must_use]
pub fn is(id: MutantId) -> bool {
    #[cfg(feature = "mutants")]
    {
        INIT.call_once(|| {
            if let Ok(raw) = std::env::var("ANVIL_MUTANT") {
                set_active(MutantId::parse(&raw));
            }
        });
    }
    active() == Some(id)
}
