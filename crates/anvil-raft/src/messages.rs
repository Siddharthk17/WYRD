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
    /// One chunk of a snapshot stream. The leader sends chunks in index
    /// order under a flow-control window; the follower acknowledges the
    /// next wanted chunk, so a lost chunk resumes at chunk granularity.
    InstallSnapshotRequest {
        /// Leader's term.
        term: Term,
        /// Leader node ID.
        leader_id: NodeId,
        /// Last log index included in the snapshot.
        last_included_index: LogIndex,
        /// Term of `last_included_index`.
        last_included_term: Term,
        /// Zero-based chunk number carried by this message.
        chunk_index: u32,
        /// Total chunk count of the snapshot.
        total_chunks: u32,
        /// Snapshot payload slice for `chunk_index`.
        chunk: Bytes,
        /// CRC32C over `chunk`.
        chunk_crc: u32,
        /// BLAKE3 digest of the full snapshot byte stream.
        snapshot_digest: [u8; 32],
        /// Total snapshot byte length for bounds checks.
        snapshot_bytes: u64,
    },
    /// Acknowledgment for one snapshot chunk, naming the next wanted chunk.
    InstallSnapshotResponse {
        /// Term of responding follower.
        term: Term,
        /// True when the chunk was accepted (CRC valid, stream still current).
        success: bool,
        /// Last snapshot index installed, or zero while streaming.
        match_index: LogIndex,
        /// Next chunk the follower wants.
        next_chunk: u32,
        /// Responding node identifier.
        from: NodeId,
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
            | Self::TimeoutNow { term, .. }
            | Self::InstallSnapshotRequest { term, .. }
            | Self::InstallSnapshotResponse { term, .. } => term,
        }
    }
}
