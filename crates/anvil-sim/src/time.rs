use core::fmt;

/// Simulation virtual time represented in discrete nanoseconds since simulation epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SimTime(u64);

impl SimTime {
    /// The simulation epoch (0 nanoseconds).
    pub const ZERO: Self = Self(0);

    /// Constructs a `SimTime` from raw nanoseconds.
    pub const fn from_nanos(nanos: u64) -> Self {
        Self(nanos)
    }

    /// Constructs a `SimTime` from milliseconds.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis.saturating_mul(1_000_000))
    }

    /// Constructs a `SimTime` from seconds.
    pub const fn from_secs(secs: u64) -> Self {
        Self(secs.saturating_mul(1_000_000_000))
    }

    /// Returns the raw virtual time in nanoseconds.
    pub const fn as_nanos(self) -> u64 {
        self.0
    }

    /// Returns the raw virtual time in milliseconds (truncated).
    pub const fn as_millis(self) -> u64 {
        self.0 / 1_000_000
    }

    /// Checked addition of nanoseconds.
    pub const fn checked_add_nanos(self, nanos: u64) -> Option<Self> {
        match self.0.checked_add(nanos) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Checked subtraction of nanoseconds.
    pub const fn checked_sub_nanos(self, nanos: u64) -> Option<Self> {
        match self.0.checked_sub(nanos) {
            Some(v) => Some(Self(v)),
            None => None,
        }
    }

    /// Saturating addition of nanoseconds.
    #[must_use]
    pub const fn saturating_add_nanos(self, nanos: u64) -> Self {
        Self(self.0.saturating_add(nanos))
    }

    /// Saturating subtraction of nanoseconds.
    #[must_use]
    pub const fn saturating_sub_nanos(self, nanos: u64) -> Self {
        Self(self.0.saturating_sub(nanos))
    }

    /// Returns the duration elapsed between `self` and an earlier `earlier` time.
    /// Returns 0 if `self <= earlier`.
    pub const fn saturating_duration_since(self, earlier: Self) -> u64 {
        self.0.saturating_sub(earlier.0)
    }
}

impl fmt::Display for SimTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let nanos = self.0;
        if nanos >= 1_000_000_000 {
            let secs = nanos / 1_000_000_000;
            let rem_ms = (nanos % 1_000_000_000) / 1_000_000;
            write!(f, "{secs}.{rem_ms:03}s")
        } else if nanos >= 1_000_000 {
            let ms = nanos / 1_000_000;
            let rem_us = (nanos % 1_000_000) / 1_000;
            write!(f, "{ms}.{rem_us:03}ms")
        } else if nanos >= 1_000 {
            let us = nanos / 1_000;
            let rem_ns = nanos % 1_000;
            write!(f, "{us}.{rem_ns:03}µs")
        } else {
            write!(f, "{nanos}ns")
        }
    }
}
