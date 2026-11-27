#![forbid(unsafe_code)]

//! Cluster membership configuration for joint consensus.
//!
//! A config takes effect when its entry is appended, not when committed.
//! `EnterJoint` carries the union view (old plus new voters); `LeaveJoint`
//! carries the final voter set. Learners receive log entries but never vote.

use crate::codec::{CodecError, decode_varint_u64, encode_varint_u64};
use crate::entry::{Entry, EntryKind};
use crate::ids::{ClusterId, LogIndex, NodeId, Term};
use bytes::Bytes;
use thiserror::Error;

/// Hard cap on voters in one config. Bounds every allocation on decode.
pub const MAX_VOTERS: usize = 9;
/// Hard cap on learners in one config. Bounds every allocation on decode.
pub const MAX_LEARNERS: usize = 9;

/// Membership validation and wire-format failure.
#[derive(Debug, Error, PartialEq, Eq, Clone)]
pub enum MembershipError {
    #[error("config must list at least one voter")]
    EmptyVoters,
    #[error("config lists too many members")]
    TooManyMembers,
    #[error("node {0} is both voter and learner")]
    Overlap(NodeId),
    #[error("config payload is not an EnterJoint or LeaveJoint entry")]
    NotConfigEntry,
    #[error("config payload decode failed: {0}")]
    Decode(CodecError),
    #[error("message cluster {actual} does not match local cluster {expected}")]
    WrongCluster {
        expected: ClusterId,
        actual: ClusterId,
    },
    #[error("sender {0} is not a cluster member")]
    UnknownSender(NodeId),
}

/// Voting plus non-voting membership of one Raft group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterConfig {
    voters: Vec<NodeId>,
    learners: Vec<NodeId>,
}

impl ClusterConfig {
    /// Builds a config from voter and learner sets. Sorts and dedupes.
    ///
    /// # Errors
    /// Returns [`MembershipError`] when voters are empty, a node appears in
    /// both roles, or either role exceeds its cap.
    pub fn new(
        mut voters: Vec<NodeId>,
        mut learners: Vec<NodeId>,
    ) -> Result<Self, MembershipError> {
        voters.sort();
        voters.dedup();
        learners.sort();
        learners.dedup();
        if voters.is_empty() {
            return Err(MembershipError::EmptyVoters);
        }
        if voters.len() > MAX_VOTERS || learners.len() > MAX_LEARNERS {
            return Err(MembershipError::TooManyMembers);
        }
        if let Some(dup) = voters.iter().find(|v| learners.contains(v)) {
            return Err(MembershipError::Overlap(*dup));
        }
        Ok(Self { voters, learners })
    }

    /// Single-node bootstrap config.
    ///
    /// # Errors
    /// Never fails for a valid [`NodeId`]; returns [`MembershipError`] only
    /// to keep bootstrap fallible at the call site.
    pub fn single(node: NodeId) -> Result<Self, MembershipError> {
        Self::new(vec![node], Vec::new())
    }

    /// Sorted voter set.
    #[must_use]
    pub fn voters(&self) -> &[NodeId] {
        &self.voters
    }

    /// Sorted learner set.
    #[must_use]
    pub fn learners(&self) -> &[NodeId] {
        &self.learners
    }

    /// Returns true when `node` votes in this config.
    #[must_use]
    pub fn is_voter(&self, node: NodeId) -> bool {
        self.voters.contains(&node)
    }

    /// Returns true when `node` is a non-voting learner.
    #[must_use]
    pub fn is_learner(&self, node: NodeId) -> bool {
        self.learners.contains(&node)
    }

    /// Returns true when `node` is a voter or a learner.
    #[must_use]
    pub fn is_member(&self, node: NodeId) -> bool {
        self.is_voter(node) || self.is_learner(node)
    }

    /// Majority quorum size for `count` voters.
    #[must_use]
    pub const fn majority_of(count: usize) -> usize {
        count / 2 + 1
    }

    /// Majority quorum size for this config's voters.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        Self::majority_of(self.voters.len())
    }

    /// Returns true when `votes` holds a majority of this config.
    #[must_use]
    pub fn has_quorum(&self, votes: usize) -> bool {
        votes >= self.quorum_size()
    }

    /// Joint quorum: a majority of the old voters and a majority of the new.
    #[must_use]
    pub fn has_joint_quorum(old: &Self, new: &Self, old_votes: usize, new_votes: usize) -> bool {
        old.has_quorum(old_votes) && new.has_quorum(new_votes)
    }

    /// Wire encoding: voter count, voter ids, learner count, learner ids.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint_u64(self.voters.len() as u64, buf);
        for v in &self.voters {
            v.encode(buf);
        }
        encode_varint_u64(self.learners.len() as u64, buf);
        for l in &self.learners {
            l.encode(buf);
        }
    }

    /// Decodes a config, checking length bounds before any reservation.
    ///
    /// # Errors
    /// Returns [`MembershipError::TooManyMembers`] when a count exceeds its
    /// cap, [`CodecError`] wrapped in [`MembershipError::Decode`] on framing
    /// errors, and [`MembershipError::EmptyVoters`] or [`MembershipError::Overlap`]
    /// when the decoded sets fail validation.
    pub fn decode(buf: &mut &[u8]) -> Result<Self, MembershipError> {
        let voters_raw = decode_varint_u64(buf).map_err(MembershipError::Decode)?;
        let voters_len =
            usize::try_from(voters_raw).map_err(|_| MembershipError::TooManyMembers)?;
        if voters_len > MAX_VOTERS {
            return Err(MembershipError::TooManyMembers);
        }
        let mut voters = Vec::with_capacity(voters_len);
        for _ in 0..voters_len {
            let id = NodeId::decode(buf).map_err(MembershipError::Decode)?;
            voters.push(id);
        }
        let learners_raw = decode_varint_u64(buf).map_err(MembershipError::Decode)?;
        let learners_len =
            usize::try_from(learners_raw).map_err(|_| MembershipError::TooManyMembers)?;
        if learners_len > MAX_LEARNERS {
            return Err(MembershipError::TooManyMembers);
        }
        let mut learners = Vec::with_capacity(learners_len);
        for _ in 0..learners_len {
            let id = NodeId::decode(buf).map_err(MembershipError::Decode)?;
            learners.push(id);
        }
        if !buf.is_empty() {
            return Err(MembershipError::Decode(CodecError::TrailingData(buf.len())));
        }
        Self::new(voters, learners)
    }
}

/// Builds a config-change log entry carrying `config` as its payload.
#[must_use]
pub fn config_entry(kind: EntryKind, index: LogIndex, term: Term, config: &ClusterConfig) -> Entry {
    assert!(
        kind.is_config_change(),
        "config_entry requires EnterJoint or LeaveJoint"
    );
    let mut buf = Vec::new();
    config.encode(&mut buf);
    Entry::new(index, term, kind, Bytes::from(buf))
}

/// Decodes the config carried by an `EnterJoint` or `LeaveJoint` entry.
///
/// # Errors
/// Returns [`MembershipError::NotConfigEntry`] for other entry kinds and
/// [`MembershipError::Decode`] when the payload fails validation.
pub fn decode_config_entry(entry: &Entry) -> Result<ClusterConfig, MembershipError> {
    if !entry.kind.is_config_change() {
        return Err(MembershipError::NotConfigEntry);
    }
    let mut cursor = &entry.payload[..];
    ClusterConfig::decode(&mut cursor)
}

/// Rejects messages whose cluster id differs from the local one.
///
/// # Errors
/// Returns [`MembershipError::WrongCluster`] on mismatch.
pub const fn ensure_cluster(expected: ClusterId, actual: ClusterId) -> Result<(), MembershipError> {
    if expected.as_u64() == actual.as_u64() {
        Ok(())
    } else {
        Err(MembershipError::WrongCluster { expected, actual })
    }
}
