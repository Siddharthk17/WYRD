#![forbid(unsafe_code)]

//! Peer replication progress tracking and flow control for `RaftCore`.

use anvil_types::{LogIndex, Term};

/// Replication state of a follower peer from the leader's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgressState {
    /// Probe state: send at most one replication probe message per round trip.
    Probe,
    /// Replicate state: pipelined optimistic replication up to `max_inflight_messages`.
    Replicate,
}

/// Tracks replication progress and in-flight window for an individual follower.
#[derive(Debug, Clone)]
pub struct PeerProgress {
    /// Current replication state.
    pub state: ProgressState,
    /// Highest log index confirmed replicated on the follower.
    pub match_index: LogIndex,
    /// Next log index to send to the follower.
    pub next_index: LogIndex,
    /// Number of in-flight unacknowledged replication messages.
    pub inflight_count: usize,
    /// Maximum allowable in-flight unacknowledged messages.
    pub max_inflight: usize,
}

impl PeerProgress {
    /// Creates a new progress tracker initialized to Probe state at `next_index`.
    #[must_use]
    pub fn new(next_index: LogIndex, max_inflight: usize) -> Self {
        Self {
            state: ProgressState::Probe,
            match_index: LogIndex::from_raw(0),
            next_index,
            inflight_count: 0,
            max_inflight,
        }
    }

    /// Returns true if flow control permits sending an `AppendEntries` message to this peer.
    #[must_use]
    pub fn can_send(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.inflight_count == 0,
            ProgressState::Replicate => self.inflight_count < self.max_inflight,
        }
    }

    /// Records an outbound replication message and optimistically advances `next_index` in Replicate mode.
    pub fn on_send(&mut self, entries_count: usize, last_sent_index: LogIndex) {
        self.inflight_count = self.inflight_count.saturating_add(1);
        if self.state == ProgressState::Replicate
            && entries_count > 0
            && last_sent_index >= self.next_index
        {
            self.next_index = last_sent_index.next().unwrap_or(last_sent_index);
        }
    }

    /// Handles a successful `AppendEntries` response from the follower.
    ///
    /// Advances `match_index`, adjusts `next_index`, decrements in-flight count,
    /// and transitions from Probe to Replicate.
    pub fn on_success(&mut self, confirmed_match: LogIndex) {
        self.inflight_count = self.inflight_count.saturating_sub(1);
        if confirmed_match > self.match_index {
            self.match_index = confirmed_match;
        }
        let next_after_match = confirmed_match.next().unwrap_or(confirmed_match);
        if next_after_match > self.next_index {
            self.next_index = next_after_match;
        }
        self.state = ProgressState::Replicate;
    }

    /// Handles a rejected `AppendEntries` response using conflict hints to repair log in 1-RTT.
    ///
    /// If `conflict_term` is present and leader has entries in that term, leader sets
    /// `next_index` to the entry following the last leader entry in `conflict_term`.
    /// Otherwise, leader sets `next_index` directly to `conflict_index`.
    pub fn on_rejection(
        &mut self,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
        last_index_for_term: impl Fn(Term) -> Option<LogIndex>,
    ) {
        self.inflight_count = self.inflight_count.saturating_sub(1);
        self.state = ProgressState::Probe;

        let resolved_next = if let Some(c_term) = conflict_term {
            if let Some(leader_last_idx) = last_index_for_term(c_term) {
                leader_last_idx.next().unwrap_or(leader_last_idx)
            } else if let Some(c_idx) = conflict_index {
                c_idx
            } else {
                self.next_index.prev().unwrap_or(LogIndex::from_raw(1))
            }
        } else if let Some(c_idx) = conflict_index {
            c_idx
        } else {
            self.next_index.prev().unwrap_or(LogIndex::from_raw(1))
        };

        // Ensure next_index never regresses below match_index + 1
        let floor = self.match_index.next().unwrap_or(self.match_index);
        self.next_index = if resolved_next < floor {
            floor
        } else {
            resolved_next
        };
    }
}
