#![forbid(unsafe_code)]

//! Consensus protocol message structures for `anvil-raft`.

use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;

/// Messages exchanged between cluster nodes over the transport layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Message {
    /// `PreVote` request broadcast prior to advancing term.
    PreVoteRequest {
        /// Candidate's prospective term (`current_term + 1`).
        term: Term,
        /// Prospective candidate node ID.
        candidate_id: NodeId,
        /// Last log index of the candidate.
        last_log_index: LogIndex,
        /// Last log term of the candidate.
        last_log_term: Term,
    },
    /// Response to `PreVote` solicitation.
    PreVoteResponse {
        /// Local term of the responding peer.
        term: Term,
        /// Whether the prospective vote was granted.
        vote_granted: bool,
        /// Responding node identifier.
        from: NodeId,
    },
    /// Formal `RequestVote` solicitation for a new term.
    RequestVoteRequest {
        /// Candidate's current term.
        term: Term,
        /// Candidate node ID.
        candidate_id: NodeId,
        /// Last log index of candidate.
        last_log_index: LogIndex,
        /// Last log term of candidate.
        last_log_term: Term,
    },
    /// Response to `RequestVote` solicitation.
    RequestVoteResponse {
        /// Term of responding peer.
        term: Term,
        /// Whether the vote was granted.
        vote_granted: bool,
        /// Responding node identifier.
        from: NodeId,
    },
    /// `AppendEntries` request carrying log replication payload and commit progress.
    AppendEntriesRequest {
        /// Leader's term.
        term: Term,
        /// Leader node ID.
        leader_id: NodeId,
        /// Index of log entry immediately preceding new entries.
        prev_log_index: LogIndex,
        /// Term of `prev_log_index` entry.
        prev_log_term: Term,
        /// Batch of entries to replicate (empty for keepalive/heartbeat).
        entries: Vec<Entry>,
        /// Leader's commit index.
        leader_commit: LogIndex,
    },
    /// Response to `AppendEntries` replication request with 1-RTT conflict hints.
    AppendEntriesResponse {
        /// Term of responding follower.
        term: Term,
        /// True if follower contained entry matching `prev_log_index` and `prev_log_term`.
        success: bool,
        /// Match index confirmed by follower.
        match_index: LogIndex,
        /// Conflicting term observed at `prev_log_index` if rejected.
        conflict_term: Option<Term>,
        /// First index of `conflict_term` or follower's log length + 1.
        conflict_index: Option<LogIndex>,
        /// Responding node identifier.
        from: NodeId,
    },
    /// Lightweight heartbeat request for leader liveness and `ReadIndex` quorum probing.
    HeartbeatRequest {
        /// Leader's term.
        term: Term,
        /// Leader node ID.
        leader_id: NodeId,
        /// Current leader commit index.
        commit_index: LogIndex,
        /// `ReadIndex` context identifier if querying linearizable quorum.
        read_context: Option<Bytes>,
    },
    /// Heartbeat response confirming leader contact.
    HeartbeatResponse {
        /// Term of responding follower.
        term: Term,
        /// Responding node ID.
        from: NodeId,
        /// Echoed `ReadIndex` context if present in request.
        read_context: Option<Bytes>,
    },
    /// Message instructing a node to immediately trigger election timeout (e.g. leader transfer via `TimeoutNow`).
    TimeoutNow {
        /// Term of leader requesting transfer.
        term: Term,
        /// Leader node ID.
        leader_id: NodeId,
    },
}

impl Message {
    /// Returns the term field of the message.
    #[must_use]
    pub const fn term(&self) -> Term {
        match *self {
            Self::PreVoteRequest { term, .. }
            | Self::PreVoteResponse { term, .. }
            | Self::RequestVoteRequest { term, .. }
            | Self::RequestVoteResponse { term, .. }
            | Self::AppendEntriesRequest { term, .. }
            | Self::AppendEntriesResponse { term, .. }
            | Self::HeartbeatRequest { term, .. }
            | Self::HeartbeatResponse { term, .. }
            | Self::TimeoutNow { term, .. } => term,
        }
    }
}
