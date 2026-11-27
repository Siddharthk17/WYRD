#![forbid(unsafe_code)]

//! Core types, state machines, and public contracts for `anvil-raft`.

use anvil_types::{ClusterId, Entry, LogIndex, MembershipError, MembershipSnapshot, NodeId, Term};
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
    /// Non-voting learners: they receive log entries but never vote.
    pub learners: BTreeSet<NodeId>,
    /// Cluster this node belongs to. Senders from another cluster are dropped.
    pub cluster_id: ClusterId,
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

    /// Rejects configurations that deadlock replication or corrupt identity.
    ///
    /// Zero flow-control windows (`max_inflight_messages`, `max_bytes_per_message`)
    /// stall the leader forever, and listing the local node as its own peer
    /// breaks quorum math. Timeouts may be zero: a zero election timeout starts
    /// an election on every tick, which focused unit tests use and which
    /// `SimCluster` and production startup never select.
    /// Learners must be disjoint from the voter set: a node is either a voter
    /// or a learner, never both. The local node is a learner exactly when it
    /// appears in `learners`, and then it is excluded from the voters.
    ///
    /// # Errors
    /// Returns a description when a bound would deadlock or corrupt the core.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_inflight_messages == 0 {
            return Err("max_inflight_messages must be non-zero");
        }
        if self.max_bytes_per_message == 0 {
            return Err("max_bytes_per_message must be non-zero");
        }
        if self.peers.contains(&self.node_id) {
            return Err("peers must not contain local node_id");
        }
        if self.peers.iter().any(|peer| self.learners.contains(peer)) {
            return Err("learners must not overlap peers");
        }
        Ok(())
    }

    /// Returns the initial voter set: peers plus the local node, unless the
    /// local node is a learner.
    #[must_use]
    pub fn initial_voters(&self) -> Vec<NodeId> {
        let mut voters: Vec<NodeId> = self.peers.iter().copied().collect();
        if !self.learners.contains(&self.node_id) {
            voters.push(self.node_id);
        }
        voters.sort();
        voters.dedup();
        voters
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

/// Projection of Raft state onto the TLA+ specification variables.
///
/// The log is represented by its digest rather than its full contents so that
/// emitting a projection on every transition stays allocation-free. Trace
/// validation replays these digests against the spec.
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

/// One snapshot chunk supplied by the node for the leader to stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotChunkData {
    /// Snapshot payload slice for this chunk.
    pub chunk: Bytes,
    /// CRC32C over `chunk`.
    pub chunk_crc: u32,
}

/// A fully reassembled and digest-verified snapshot ready for atomic install.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceivedSnapshot {
    /// Last log index included in the snapshot.
    pub last_included_index: LogIndex,
    /// Term of `last_included_index`.
    pub last_included_term: Term,
    /// Full snapshot byte stream.
    pub data: Bytes,
    /// BLAKE3 digest of `data`, verified by the core before handing over.
    pub snapshot_digest: [u8; 32],
}

/// A core request for snapshot bytes: follower `to` needs the log that is
/// already compacted away, so the node must supply a snapshot at or above
/// `least_index` through `Input::SnapshotReady`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotRequest {
    /// Follower that can only be reached by snapshot.
    pub to: NodeId,
    /// Minimum snapshot index that would unblock the follower.
    pub least_index: LogIndex,
}

/// Destination-addressed outbound message emitted by `RaftCore`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMessage {
    /// Target peer node ID.
    pub to: NodeId,
    /// Message payload.
    pub message: crate::messages::Message,
}

/// Reason a membership change proposal was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigReject {
    /// Proposer is not the leader.
    NotLeader,
    /// A config entry is already appended but not yet committed.
    ConfigInProgress,
    /// The leader has not yet committed an entry in its own term.
    NoCurrentTermCommit,
    /// The proposed voter and learner sets fail validation.
    InvalidConfig(MembershipError),
}

/// Membership lifecycle notification emitted by `RaftCore`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipEvent {
    /// An `EnterJoint` entry was appended and took effect.
    Proposed {
        /// Log index of the joint entry.
        index: LogIndex,
    },
    /// The leader auto-appended `LeaveJoint` after the joint entry committed.
    AutoLeave {
        /// Log index of the leave entry.
        index: LogIndex,
    },
    /// A membership proposal was refused.
    Rejected {
        /// Why the proposal was refused.
        reason: ConfigReject,
    },
    /// A removed leader stepped down after `LeaveJoint` committed.
    RemovedSteppedDown,
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
    /// Core requests for snapshot bytes the node must supply.
    pub snapshot_requests: Vec<SnapshotRequest>,
    /// Fully verified snapshots the node must atomically install.
    pub ready_snapshots: Vec<ReceivedSnapshot>,
    /// Role transition notifications.
    pub role_changes: Vec<RoleChangeEvent>,
    /// Trace events projecting exact TLA+ spec state.
    pub spec_projections: Vec<SpecProjection>,
    /// Rare protocol paths executed since the last drain, for farm coverage.
    /// Entries are `&'static str` markers pushed by `sometimes!`; the
    /// simulator drains them after every step.
    pub sometimes_hits: Vec<&'static str>,
    /// Membership lifecycle notifications since the last drain.
    pub membership_events: Vec<MembershipEvent>,
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
        self.snapshot_requests.clear();
        self.ready_snapshots.clear();
        self.role_changes.clear();
        self.spec_projections.clear();
        self.sometimes_hits.clear();
        self.membership_events.clear();
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
    /// Membership change proposal submitted to cluster leader. The leader
    /// appends an `EnterJoint` entry carrying the new config, then appends
    /// `LeaveJoint` automatically once the joint entry commits.
    ProposeConfig {
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
    },
    /// Linearizable read index request with client correlation context.
    ReadIndex { context: Bytes },
    /// Notification from storage engine that a previous `PersistOp` has synced to disk.
    Persisted { token: PersistToken },
    /// Notification from state machine that a log entry has been applied.
    Applied { index: LogIndex },
    /// Node notifies the core of the membership carried by an installed
    /// snapshot. Accepted only when `base` matches the last installed
    /// snapshot: the bytes on disk decide, and a stale restore is ignored.
    /// Lets a fresh joiner with an empty log adopt the cluster config.
    RestoreMembership {
        base: LogIndex,
        membership: MembershipSnapshot,
    },
    /// Marks the node as needing protocol-aware repair from an index.
    /// A flagged node never campaigns, never grants votes, and answers
    /// appends as if its match index were the last good index, so the
    /// leader re-replicates the missing range through the validated log
    /// path. A flagged leader steps down immediately. Ignored for a zero
    /// index, which repairs nothing.
    SetNeedsRepair { from_index: LogIndex },
    /// Clears the repair flag once the missing range is verified whole.
    ClearNeedsRepair,
    /// Administrative request to transfer leadership to target node.
    TransferLeader { to: NodeId },
    /// Node notifies the core that the log prefix is durable in a snapshot and
    /// may be dropped. Ignored unless `up_to` is above the compacted base and
    /// at most the applied index: snapshots never cover unapplied entries.
    CompactLog { up_to: LogIndex },
    /// Node supplies snapshot bytes for the leader to stream. Chunks share
    /// ownership through `Bytes`, so supplying never copies the payload.
    SnapshotReady {
        /// Last log index included in the snapshot.
        last_included_index: LogIndex,
        /// Term of `last_included_index`.
        last_included_term: Term,
        /// Full snapshot byte length for bounds checks.
        snapshot_bytes: u64,
        /// BLAKE3 digest of the full snapshot byte stream.
        snapshot_digest: [u8; 32],
        /// Chunk payloads in index order.
        chunks: Vec<SnapshotChunkData>,
    },
}
