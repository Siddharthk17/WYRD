#![forbid(unsafe_code)]

//! Recovery classifications and recovered state representations.

use crate::format::hard_state::HardStateRecoveryOutcome;
use anvil_types::{Entry, LogIndex, NodeId, Term};

/// Explicit classification of the log scan during crash recovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogRecoveryClassification {
    /// All log records across all segments verified cleanly.
    Clean,
    /// An invalid frame with nothing valid following it; safe to truncate to `truncated_offset`.
    TornTail {
        /// File offset where invalid record began.
        truncated_offset: u64,
        /// Highest log index successfully verified prior to the torn tail.
        last_valid_index: LogIndex,
    },
    /// A corrupted record followed by valid records later in the log.
    /// The node is marked `NeedsRepair` starting at `corrupt_index` and must not vote or lead.
    MidLogCorruption {
        /// Log index where corruption occurred.
        corrupt_index: LogIndex,
        /// Byte offset of the corrupt record.
        corrupt_offset: u64,
    },
}

/// Byte-exact placement of one verified WAL frame, recorded during recovery scan.
///
/// The scanner observes the true on-disk layout, so replays never have to
/// guess it: a later truncation rewinds to these offsets and chained CRC
/// seeds instead of recomputing them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FramePlacement {
    /// Log index stored in the frame.
    pub index: LogIndex,
    /// Segment file holding the frame.
    pub seg_id: u64,
    /// Byte offset of the frame start within the segment.
    pub offset: u64,
    /// Chained CRC seed the frame was verified against.
    pub crc_before: u32,
    /// Chained CRC seed after the frame.
    pub crc_after: u32,
    /// Raft term stored in the frame.
    pub term: Term,
    /// Total sector-aligned frame length in bytes.
    pub frame_len: usize,
}

/// Recovered log entries and classification status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredLog {
    /// Recovered verified sequential entries.
    pub entries: Vec<Entry>,
    /// Highest valid log index recovered.
    pub last_index: LogIndex,
    /// Raft term of the highest recovered entry.
    pub last_term: Term,
    /// Detailed classification of the recovery scan.
    pub classification: LogRecoveryClassification,
    /// Byte-exact placement of every recovered frame, in index order.
    pub placements: Vec<FramePlacement>,
}

impl RecoveredLog {
    /// Returns true if the node encountered mid-log corruption requiring peer repair.
    #[must_use]
    pub fn needs_repair(&self) -> bool {
        matches!(
            self.classification,
            LogRecoveryClassification::MidLogCorruption { .. }
        )
    }
}

/// Complete recovered state produced by scanning disk on node initialization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredStorageState {
    /// Recovered term from active hard-state slot.
    pub current_term: Term,
    /// Recovered vote from active hard-state slot.
    pub voted_for: Option<NodeId>,
    /// Hard-state recovery outcome.
    pub hard_state_outcome: HardStateRecoveryOutcome,
    /// Recovered consensus log.
    pub log: RecoveredLog,
}
