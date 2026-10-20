#![forbid(unsafe_code)]

//! Core types, state machines, and public contracts for `anvil-raft`.

use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use std::collections::BTreeSet;

/// Role of a Raft node in the cluster consensus protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// Follower role: passive receiver of heartbeats and appends.
    Follower,
    /// `PreCandidate` role: evaluates whether a quorum would grant a vote before incrementing term.
    PreCandidate,
    /// Candidate role: actively soliciting votes in a new term.
    Candidate,
    /// Leader role: coordinates log replication, commits, and client reads.
    Leader,
}

impl Role {
    /// Returns a static string slice describing the role.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Follower => "Follower",
            Self::PreCandidate => "PreCandidate",
            Self::Candidate => "Candidate",
            Self::Leader => "Leader",
        }
    }
}

/// Configuration settings for a `RaftCore` instance.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// Local node identifier.
    pub node_id: NodeId,
    /// Cluster peer identifiers (excluding local node).
    pub peers: BTreeSet<NodeId>,
    /// Minimum randomized election timeout in logical ticks.
    pub election_timeout_ticks: u64,
    /// Leader heartbeat broadcast interval in logical ticks.
    pub heartbeat_interval_ticks: u64,
    /// Maximum in-flight unacknowledged replication messages per peer.
    pub max_inflight_messages: usize,
    /// Maximum bytes serialized in a single `AppendEntries` message.
    pub max_bytes_per_message: usize,
    /// Whether `PreVote` phase is enabled prior to term increment.
    pub pre_vote_enabled: bool,
    /// Whether `CheckQuorum` is enabled to periodically verify leader connectivity.
    pub check_quorum: bool,
}

impl RaftConfig {
    /// Returns the total cluster size (peers + local node).
    #[must_use]
    pub fn cluster_size(&self) -> usize {
        self.peers.len() + 1
    }

    /// Returns the minimum quorum size needed for consensus (majority).
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        (self.cluster_size() / 2) + 1
    }
}

/// Recovered persistent state loaded from durable storage during node initialization.
#[derive(Debug, Clone, Default)]
pub struct RecoveredState {
    /// Last persisted term.
    pub current_term: Term,
    /// Candidate voted for in `current_term`, if any.
    pub voted_for: Option<NodeId>,
    /// Last committed log index acknowledged by storage.
    pub commit_index: LogIndex,
    /// Recovered log entries.
    pub entries: Vec<Entry>,
}

/// Projection of Raft state corresponding 1:1 to TLA+ formal specification variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpecProjection {
    /// Persistent term variable `currentTerm`.
    pub term: Term,
    /// Volatile role variable `role`.
    pub role: Role,
    /// Persistent voted-for variable `votedFor`.
    pub voted_for: Option<NodeId>,
    /// Cryptographic digest of current log variable `log`.
    pub log_digest: [u8; 32],
    /// Volatile commit index variable `commitIndex`.
    pub commit_index: LogIndex,
}

/// Unique token associated with an asynchronous storage persistence operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PersistToken(pub u64);

/// Storage operations required by `RaftCore` to ensure durable consensus invariants.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PersistOp {
    /// Persist updated hard state (term and vote) before releasing messages revealing them.
    HardState {
        term: Term,
        vote: Option<NodeId>,
        token: PersistToken,
    },
    /// Persist new log entries appended at the specified index.
    AppendEntries {
        prev_index: LogIndex,
        entries: Vec<Entry>,
        token: PersistToken,
    },
    /// Truncate diverging entries starting at `from_index` and append new entries.
    TruncateAndAppend {
        from_index: LogIndex,
        entries: Vec<Entry>,
        token: PersistToken,
    },
}

impl PersistOp {
    /// Returns the persistence completion token.
    #[must_use]
    pub const fn token(&self) -> PersistToken {
        match *self {
            Self::HardState { token, .. }
            | Self::AppendEntries { token, .. }
            | Self::TruncateAndAppend { token, .. } => token,
        }
    }
}

/// Notification emitted when node changes its consensus role.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleChangeEvent {
    /// Previous consensus role.
    pub previous: Role,
    /// New consensus role.
    pub current: Role,
    /// Consensus term at transition.
    pub term: Term,
}

/// Result of a linearizable `ReadIndex` operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadIndexResult {
    /// Caller-provided context correlation bytes.
    pub context: Bytes,
    /// Log index at which read is safe to evaluate against state machine.
    pub read_index: LogIndex,
}

/// Destination-addressed outbound message emitted by `RaftCore`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    /// Target peer node ID.
    pub to: NodeId,
    /// Message payload.
    pub message: crate::messages::Message,
}

/// Caller-provided reusable buffer holding all side-effects emitted by `RaftCore::step`.
#[derive(Debug, Default)]
pub struct Output {
    /// Outbound consensus protocol network messages.
    pub messages: Vec<OutboundMessage>,
    /// Durable storage persistence requests.
    pub persist_ops: Vec<PersistOp>,
    /// Committed log entries ready to apply to state machine.
    pub committed_entries: Vec<Entry>,
    /// Completed linearizable read index queries.
    pub completed_reads: Vec<ReadIndexResult>,
    /// Role transition notifications.
    pub role_changes: Vec<RoleChangeEvent>,
    /// Trace events projecting exact TLA+ spec state.
    pub spec_projections: Vec<SpecProjection>,
}

impl Output {
    /// Creates an empty output struct.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Clears all internal vectors, retaining allocated capacity to eliminate hot-path allocations.
    pub fn clear(&mut self) {
        self.messages.clear();
        self.persist_ops.clear();
        self.committed_entries.clear();
        self.completed_reads.clear();
        self.role_changes.clear();
        self.spec_projections.clear();
    }
}

/// Discrete input events consumed by `RaftCore::step`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    /// Logical clock tick advancing election and heartbeat timers.
    Tick,
    /// Network frame received from a cluster peer.
    Recv {
        from: NodeId,
        message: crate::messages::Message,
    },
    /// Client proposal command submitted to cluster leader.
    Propose { payload: Bytes },
    /// Linearizable read index request with client correlation context.
    ReadIndex { context: Bytes },
    /// Notification from storage engine that a previous `PersistOp` has synced to disk.
    Persisted { token: PersistToken },
    /// Notification from state machine that a log entry has been applied.
    Applied { index: LogIndex },
    /// Administrative request to transfer leadership to target node.
    TransferLeader { to: NodeId },
}
