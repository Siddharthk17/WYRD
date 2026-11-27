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

/// Joint state carried by a snapshot taken while `EnterJoint` is active.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JointSnapshot {
    /// Voters before the joint entry.
    pub old_voters: Vec<NodeId>,
    /// Voters after the joint entry.
    pub new_voters: Vec<NodeId>,
    /// Learners before the joint entry.
    pub old_learners: Vec<NodeId>,
    /// Learners after the joint entry.
    pub new_learners: Vec<NodeId>,
    /// Log index of the `EnterJoint` entry.
    pub enter_index: LogIndex,
}

/// Membership carried by a snapshot, so a node that installs it (including
/// a fresh joiner with an empty log) ends up in the same config the snapshot
/// was taken under.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MembershipSnapshot {
    /// Effective voters.
    pub voters: Vec<NodeId>,
    /// Effective learners.
    pub learners: Vec<NodeId>,
    /// Joint state when the snapshot was taken inside joint consensus.
    pub joint: Option<JointSnapshot>,
}

impl MembershipSnapshot {
    /// Empty membership: no voters, no learners, no joint state.
    #[must_use]
    pub fn empty() -> Self {
        Self {
            voters: Vec::new(),
            learners: Vec::new(),
            joint: None,
        }
    }

    /// Wire encoding: voter and learner sets, then an optional joint section.
    /// Counts are bounded by [`MAX_VOTERS`] and [`MAX_LEARNERS`] before any
    /// reservation.
    pub fn encode(&self, buf: &mut Vec<u8>) {
        encode_ids(&self.voters, buf);
        encode_ids(&self.learners, buf);
        match &self.joint {
            None => buf.push(0),
            Some(joint) => {
                buf.push(1);
                encode_ids(&joint.old_voters, buf);
                encode_ids(&joint.new_voters, buf);
                encode_ids(&joint.old_learners, buf);
                encode_ids(&joint.new_learners, buf);
                encode_varint_u64(joint.enter_index.as_u64(), buf);
            }
        }
    }

    /// Decodes a membership section, checking bounds before reserving.
    ///
    /// # Errors
    /// Returns [`MembershipError::TooManyMembers`] on over-cap counts,
    /// [`MembershipError::Decode`] on framing errors or trailing bytes.
    pub fn decode(buf: &mut &[u8]) -> Result<Self, MembershipError> {
        let voters = decode_ids(buf, MAX_VOTERS)?;
        let learners = decode_ids(buf, MAX_LEARNERS)?;
        if buf.is_empty() {
            return Err(MembershipError::Decode(CodecError::UnexpectedEof {
                needed: 1,
                available: 0,
            }));
        }
        let flag = buf[0];
        *buf = &buf[1..];
        let joint = match flag {
            0 => None,
            1 => {
                let old_voters = decode_ids(buf, MAX_VOTERS)?;
                let new_voters = decode_ids(buf, MAX_VOTERS)?;
                let old_learners = decode_ids(buf, MAX_LEARNERS)?;
                let new_learners = decode_ids(buf, MAX_LEARNERS)?;
                let enter_raw = decode_varint_u64(buf).map_err(MembershipError::Decode)?;
                Some(JointSnapshot {
                    old_voters,
                    new_voters,
                    old_learners,
                    new_learners,
                    enter_index: LogIndex::from_raw(enter_raw),
                })
            }
            _ => {
                return Err(MembershipError::Decode(CodecError::TrailingData(
                    buf.len() + 1,
                )));
            }
        };
        if !buf.is_empty() {
            return Err(MembershipError::Decode(CodecError::TrailingData(buf.len())));
        }
        Ok(Self {
            voters,
            learners,
            joint,
        })
    }
}

fn encode_ids(ids: &[NodeId], buf: &mut Vec<u8>) {
    encode_varint_u64(ids.len() as u64, buf);
    for id in ids {
        id.encode(buf);
    }
}

fn decode_ids(buf: &mut &[u8], cap: usize) -> Result<Vec<NodeId>, MembershipError> {
    let raw = decode_varint_u64(buf).map_err(MembershipError::Decode)?;
    let len = usize::try_from(raw).map_err(|_| MembershipError::TooManyMembers)?;
    if len > cap {
        return Err(MembershipError::TooManyMembers);
    }
    let mut ids = Vec::with_capacity(len);
    for _ in 0..len {
        ids.push(NodeId::decode(buf).map_err(MembershipError::Decode)?);
    }
    Ok(ids)
}

/// Builds the bootstrap entry: a `LeaveJoint` at index 1 in term 0 carrying
/// the founding voter and learner sets. Every founding node writes the same
/// bytes, so recovery replays the same membership everywhere.
#[must_use]
pub fn bootstrap_config_entry(voters: &[NodeId], learners: &[NodeId]) -> Entry {
    let mut sorted_voters = voters.to_vec();
    sorted_voters.sort();
    sorted_voters.dedup();
    let mut sorted_learners = learners.to_vec();
    sorted_learners.sort();
    sorted_learners.dedup();
    let mut buf = Vec::new();
    encode_ids(&sorted_voters, &mut buf);
    encode_ids(&sorted_learners, &mut buf);
    Entry::new(
        LogIndex::from_raw(1),
        Term::from_raw(0),
        EntryKind::LeaveJoint,
        Bytes::from(buf),
    )
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
