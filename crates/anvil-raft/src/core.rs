#![forbid(unsafe_code)]

//! Core sans-IO Raft consensus state machine (`RaftCore`).

use crate::log::RaftLog;
use crate::messages::Message;
use crate::progress::PeerProgress;
use crate::progress::SnapshotProgress;
use crate::types::{
    ConfigReject, Input, MembershipEvent, OutboundMessage, Output, PersistOp, PersistToken,
    RaftConfig, ReadIndexResult, ReceivedSnapshot, RecoveredState, Role, RoleChangeEvent,
    SnapshotChunkData, SnapshotRequest, SpecProjection,
};
use anvil_types::mutants::{MutantId, is};
use anvil_types::{
    ClusterConfig, Entry, EntryKind, JointSnapshot, LogIndex, MembershipSnapshot, NodeId, Term,
    config_entry, decode_config_entry, sometimes,
};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::Rng;
use std::collections::{BTreeMap, BTreeSet};

/// Returns the smallest not-yet-received chunk index, or `total_chunks` when complete.
fn first_missing(pending: &FollowerSnapshot) -> u32 {
    let mut idx = 0u32;
    for slot in &pending.chunks {
        if slot.is_none() {
            return idx;
        }
        idx = idx.saturating_add(1);
    }
    pending.total_chunks
}

/// Pending actions awaiting storage persistence completion before being released.
#[derive(Debug, Clone)]
enum PendingAction {
    /// Send an outbound network message once local state is durable.
    SendMessage(OutboundMessage),
    /// Advance leader's own match index after entries are durable on local disk.
    LeaderPersisted { entry_index: LogIndex },
}

/// Linearizable `ReadIndex` query awaiting quorum confirmation.
#[derive(Debug, Clone)]
struct PendingRead {
    context: Bytes,
    read_index: LogIndex,
    acks: BTreeSet<NodeId>,
}

/// Snapshot bytes the node supplied for this leader to stream.
#[derive(Debug, Clone)]
struct LeaderSnapshot {
    last_included_index: LogIndex,
    last_included_term: Term,
    snapshot_bytes: u64,
    snapshot_digest: [u8; 32],
    chunks: Vec<SnapshotChunkData>,
}

/// Partially received snapshot stream on a follower.
#[derive(Debug, Clone)]
struct FollowerSnapshot {
    leader: NodeId,
    term: Term,
    last_included_index: LogIndex,
    last_included_term: Term,
    total_chunks: u32,
    snapshot_bytes: u64,
    snapshot_digest: [u8; 32],
    chunks: Vec<Option<Bytes>>,
    received: u32,
}

/// Hard bounds on snapshot streams, checked before any additional reservation.
const MAX_SNAPSHOT_CHUNKS: usize = 65_536;
const MAX_SNAPSHOT_BYTES: u64 = 1024 * 1024 * 1024;
const MAX_SNAPSHOT_CHUNK_BYTES: usize = 16 * 1024 * 1024;

/// Joint-consensus state between an appended `EnterJoint` and the commit of
/// the matching `LeaveJoint`. A quorum needs a majority of the old voters
/// and a majority of the new voters.
#[derive(Debug, Clone)]
struct JointState {
    old_voters: Vec<NodeId>,
    new_voters: Vec<NodeId>,
    old_learners: BTreeSet<NodeId>,
    new_learners: BTreeSet<NodeId>,
    enter_index: LogIndex,
}

/// The sans-IO consensus core implementing election, replication, and safety.
#[derive(Debug)]
pub struct RaftCore {
    config: RaftConfig,
    term: Term,
    voted_for: Option<NodeId>,
    role: Role,
    log: RaftLog,
    commit_index: LogIndex,
    applied_index: LogIndex,
    progress: BTreeMap<NodeId, PeerProgress>,
    votes_granted: BTreeSet<NodeId>,
    pre_votes_granted: BTreeSet<NodeId>,
    heartbeat_acks: BTreeSet<NodeId>,
    election_elapsed_ticks: u64,
    randomized_election_timeout: u64,
    heartbeat_elapsed_ticks: u64,
    rng: ChaCha8Rng,
    lead_transferee: Option<NodeId>,
    pending_reads: Vec<PendingRead>,
    next_persist_token: u64,
    pending_persists: Vec<(PersistToken, PendingAction)>,
    leader_persisted_index: LogIndex,
    leader_sticky: bool,
    transfer_elapsed: u64,
    leader_snapshot: Option<LeaderSnapshot>,
    follower_snapshot: Option<FollowerSnapshot>,
    snapshot_requested: BTreeMap<NodeId, LogIndex>,
    voters: Vec<NodeId>,
    learners: BTreeSet<NodeId>,
    joint: Option<JointState>,
    pending_config: Option<LogIndex>,
    leave_index: Option<LogIndex>,
    /// Index of the committed `LeaveJoint` that removed this node. A removed
    /// node still answers vote requests from candidates whose log includes
    /// that entry; otherwise joint consensus deadlocks when the removed node
    /// is the only old-quorum partner left.
    removed_after: Option<(LogIndex, Term)>,
    committed_current_term: bool,
    installed_base: Option<LogIndex>,
    repair_from: Option<LogIndex>,
}

impl RaftCore {
    /// Constructs a new `RaftCore` instance from configuration, recovered state, and a `ChaCha8` RNG.
    ///
    /// Recovery replays the log into the membership before the log moves
    /// into the store, which keeps one extra clone on this cold path.
    #[allow(clippy::needless_pass_by_value)]
    #[must_use]
    pub fn new(config: RaftConfig, recovered: RecoveredState, mut rng: ChaCha8Rng) -> Self {
        assert!(
            config.validate().is_ok(),
            "RaftCore requires a valid RaftConfig"
        );
        assert!(
            recovered.commit_index
                <= recovered
                    .entries
                    .last()
                    .map_or(LogIndex::from_raw(0), |entry| entry.index),
            "recovered commit index cannot exceed last log index"
        );
        let randomized_election_timeout =
            Self::sample_random_timeout(config.election_timeout_ticks, &mut rng);

        let mut progress = BTreeMap::new();
        let last_log_idx = recovered
            .entries
            .last()
            .map_or(LogIndex::from_raw(0), |entry| entry.index);
        assert!(
            last_log_idx.next().is_some(),
            "recovered log ends at maximum index"
        );
        let initial_next = last_log_idx.next().unwrap_or(LogIndex::from_raw(0));

        for &peer in &config.peers {
            progress.insert(
                peer,
                PeerProgress::new(initial_next, config.max_inflight_messages),
            );
        }
        for &learner in &config.learners {
            progress.insert(
                learner,
                PeerProgress::new(initial_next, config.max_inflight_messages),
            );
        }

        let mut voters: Vec<NodeId> = config.initial_voters();
        voters.sort();
        voters.dedup();
        let learners = config.learners.clone();

        let mut core = Self {
            config,
            term: recovered.current_term,
            voted_for: if is(MutantId::M11) {
                None
            } else {
                recovered.voted_for
            },
            role: Role::Follower,
            log: RaftLog::new(recovered.entries.clone()),
            commit_index: recovered.commit_index,
            applied_index: LogIndex::from_raw(0),
            progress,
            votes_granted: BTreeSet::new(),
            pre_votes_granted: BTreeSet::new(),
            heartbeat_acks: BTreeSet::new(),
            election_elapsed_ticks: 0,
            randomized_election_timeout,
            heartbeat_elapsed_ticks: 0,
            rng,
            lead_transferee: None,
            pending_reads: Vec::new(),
            next_persist_token: 1,
            pending_persists: Vec::with_capacity(64),
            leader_persisted_index: last_log_idx,
            leader_sticky: false,
            transfer_elapsed: 0,
            leader_snapshot: None,
            follower_snapshot: None,
            snapshot_requested: BTreeMap::new(),
            voters,
            learners,
            joint: None,
            pending_config: None,
            leave_index: None,
            removed_after: None,
            committed_current_term: false,
            installed_base: None,
            repair_from: None,
        };
        core.rebuild_membership_from_log(&recovered.entries, recovered.commit_index);
        core
    }

    /// Primary sans-IO transition function: consumes an input event and writes side-effects to `output`.
    pub fn step(&mut self, input: Input, output: &mut Output) {
        match input {
            Input::Tick => self.handle_tick(output),
            Input::Recv { from, message } => self.handle_message(from, message, output),
            Input::Propose { payload } => self.handle_propose(payload, output),
            Input::ProposeConfig { voters, learners } => {
                self.handle_propose_config(voters, learners, output);
            }
            Input::ReadIndex { context } => self.handle_read_index(context, output),
            Input::Persisted { token } => self.handle_persisted(token, output),
            Input::Applied { index } => self.handle_applied(index),
            Input::CompactLog { up_to } => self.handle_compact_log(up_to),
            Input::TransferLeader { to } => self.handle_transfer_leader(to, output),
            Input::RestoreMembership { base, membership } => {
                self.handle_restore_membership(base, membership, output);
            }
            Input::SetNeedsRepair { from_index } => {
                self.handle_set_needs_repair(from_index, output);
            }
            Input::ClearNeedsRepair => {
                self.handle_clear_needs_repair(output);
            }
            Input::SnapshotReady {
                last_included_index,
                last_included_term,
                snapshot_bytes,
                snapshot_digest,
                chunks,
            } => self.handle_snapshot_ready(
                last_included_index,
                last_included_term,
                snapshot_bytes,
                snapshot_digest,
                chunks,
                output,
            ),
        }
        self.emit_spec_projection(output);
    }

    /// Returns the current consensus role.
    #[must_use]
    pub const fn role(&self) -> Role {
        self.role
    }

    /// Returns the current term.
    #[must_use]
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Returns the node identifier that this node voted for in the current term.
    #[must_use]
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the current commit index.
    #[must_use]
    pub const fn commit_index(&self) -> LogIndex {
        self.commit_index
    }

    /// Returns the highest log index applied to the state machine.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the highest index present in the log.
    #[must_use]
    pub fn last_log_index(&self) -> LogIndex {
        self.log.last_index()
    }

    /// Returns a reference to the in-memory log.
    #[must_use]
    pub const fn log(&self) -> &RaftLog {
        &self.log
    }

    /// Returns a reference to peer progress tracking (leader only).
    #[must_use]
    pub const fn progress(&self) -> &BTreeMap<NodeId, PeerProgress> {
        &self.progress
    }

    /// Returns the cluster this node belongs to.
    #[must_use]
    pub const fn cluster_id(&self) -> anvil_types::ClusterId {
        self.config.cluster_id
    }

    /// Returns the current effective voters. During joint consensus the
    /// union of the old and the new voters is returned.
    #[must_use]
    pub fn membership_voters(&self) -> Vec<NodeId> {
        if let Some(joint) = &self.joint {
            let mut union = joint.old_voters.clone();
            for node in &joint.new_voters {
                if !union.contains(node) {
                    union.push(*node);
                }
            }
            union.sort();
            union
        } else {
            self.voters.clone()
        }
    }

    /// Returns the current effective learners (union during joint consensus).
    #[must_use]
    pub fn membership_learners(&self) -> BTreeSet<NodeId> {
        self.learners.clone()
    }

    /// Returns true while an `EnterJoint` is active and its `LeaveJoint`
    /// has not yet committed.
    #[must_use]
    pub const fn in_joint(&self) -> bool {
        self.joint.is_some()
    }

    /// Returns the current membership as a snapshot carrier: effective
    /// voters and learners plus joint state. The node embeds this in
    /// snapshots so installers adopt the same config.
    #[must_use]
    pub fn membership_snapshot(&self) -> MembershipSnapshot {
        let joint = self.joint.as_ref().map(|joint| JointSnapshot {
            old_voters: joint.old_voters.clone(),
            new_voters: joint.new_voters.clone(),
            old_learners: joint.old_learners.iter().copied().collect(),
            new_learners: joint.new_learners.iter().copied().collect(),
            enter_index: joint.enter_index,
        });
        MembershipSnapshot {
            voters: self.membership_voters(),
            learners: self.learners.iter().copied().collect(),
            joint,
        }
    }

    /// Restores membership from an installed snapshot. Only the snapshot
    /// just installed counts; anything older is ignored.
    fn handle_restore_membership(
        &mut self,
        base: LogIndex,
        membership: MembershipSnapshot,
        output: &mut Output,
    ) {
        if self.installed_base != Some(base) {
            return;
        }
        self.installed_base = None;
        if membership.voters.is_empty() {
            return;
        }
        let mut voters = membership.voters.clone();
        voters.sort();
        voters.dedup();
        let mut learners: BTreeSet<NodeId> = membership.learners.iter().copied().collect();
        learners.retain(|learner| !voters.contains(learner));
        let joint = membership.joint.and_then(|joint| {
            if joint.enter_index > base || joint.new_voters.is_empty() {
                return None;
            }
            let mut old_voters = joint.old_voters.clone();
            old_voters.sort();
            old_voters.dedup();
            let mut new_voters = joint.new_voters.clone();
            new_voters.sort();
            new_voters.dedup();
            let old_learners: BTreeSet<NodeId> = joint.old_learners.iter().copied().collect();
            let new_learners: BTreeSet<NodeId> = joint.new_learners.iter().copied().collect();
            Some(JointState {
                old_voters,
                new_voters,
                old_learners,
                new_learners,
                enter_index: joint.enter_index,
            })
        });
        self.voters = voters;
        self.learners = learners;
        self.joint = joint;
        self.pending_config = self.joint.as_ref().map(|state| state.enter_index);
        self.leave_index = None;
        // A snapshot carries the config as of `base`. If it no longer names
        // this node, the removal is committed at `base`, so the node may
        // answer candidates whose logs cover it.
        self.removed_after = if self.joint.is_none()
            && !self.voters.is_empty()
            && !self.voters.contains(&self.config.node_id)
            && !self.learners.contains(&self.config.node_id)
        {
            Some((base, self.log.term_at(base).unwrap_or_default()))
        } else {
            None
        };
        self.resolve_committed_configs(self.commit_index);
        let next = base.next().unwrap_or(base);
        let mut fresh = BTreeMap::new();
        let mut targets = self.membership_voters();
        for learner in &self.learners {
            if !targets.contains(learner) {
                targets.push(*learner);
            }
        }
        for target in targets {
            if target == self.config.node_id {
                continue;
            }
            fresh.insert(
                target,
                PeerProgress::new(next, self.config.max_inflight_messages),
            );
        }
        self.progress = fresh;
        sometimes!(output.sometimes_hits, "raft.config.restore");
    }

    /// Returns the repair base when flagged, `None` once verified whole.
    #[must_use]
    pub const fn repair_from(&self) -> Option<LogIndex> {
        self.repair_from
    }

    /// Marks the node as needing repair from `from_index`. A flagged leader
    /// hands off and steps down: it must not lead while its storage is
    /// unverified.
    fn handle_set_needs_repair(&mut self, from_index: LogIndex, output: &mut Output) {
        if from_index.is_zero() {
            return;
        }
        if self.repair_from == Some(from_index) {
            return;
        }
        self.repair_from = Some(from_index);
        sometimes!(output.sometimes_hits, "raft.repair.enter");
        if self.role == Role::Leader {
            if let Some(&heir) = self
                .membership_voters()
                .iter()
                .find(|voter| **voter != self.config.node_id)
            {
                output.messages.push(OutboundMessage {
                    to: heir,
                    message: Message::TimeoutNow {
                        term: self.term,
                        leader_id: self.config.node_id,
                    },
                });
            }
            self.transition_to_follower(self.term, output);
        }
    }

    /// Clears the repair flag once the missing range is verified whole.
    fn handle_clear_needs_repair(&mut self, output: &mut Output) {
        if self.repair_from.is_none() {
            return;
        }
        self.repair_from = None;
        sometimes!(output.sometimes_hits, "raft.repair.cleared");
    }

    /// Match index this node reports: the last good index while flagged, so
    /// the leader re-replicates the unverified suffix through the validated
    /// path instead of counting it as durable.
    fn report_match(&self, matched: LogIndex) -> LogIndex {
        match self.repair_from {
            Some(from) => {
                let last_good = from.prev().unwrap_or(LogIndex::from_raw(0));
                std::cmp::min(matched, last_good)
            }
            None => matched,
        }
    }

    /// Rebuilds membership by replaying config entries in log order, then
    /// clearing state the commit index proves durable.
    fn rebuild_membership_from_log(&mut self, entries: &[Entry], commit: LogIndex) {
        for entry in entries {
            match entry.kind {
                EntryKind::EnterJoint | EntryKind::LeaveJoint => {
                    self.note_config_appended(entry);
                }
                EntryKind::Normal | EntryKind::NoOp => {}
            }
        }
        self.resolve_committed_configs(commit);
    }

    /// Applies an appended config entry to the effective membership.
    fn note_config_appended(&mut self, entry: &Entry) {
        let Ok(config) = decode_config_entry(entry) else {
            return;
        };
        match entry.kind {
            EntryKind::EnterJoint => {
                let old_learners = self.learners.clone();
                let mut learners = old_learners.clone();
                for learner in config.learners() {
                    learners.insert(*learner);
                }
                for voter in config.voters() {
                    self.ensure_progress(*voter);
                }
                for learner in config.learners() {
                    self.ensure_progress(*learner);
                }
                if config.voters().contains(&self.config.node_id)
                    || config.learners().contains(&self.config.node_id)
                {
                    self.removed_after = None;
                }
                self.joint = Some(JointState {
                    old_voters: self.voters.clone(),
                    new_voters: config.voters().to_vec(),
                    old_learners,
                    new_learners: config.learners().iter().copied().collect(),
                    enter_index: entry.index,
                });
                self.learners = learners;
                self.pending_config = Some(entry.index);
            }
            EntryKind::LeaveJoint => {
                self.voters = config.voters().to_vec();
                self.learners = config.learners().iter().copied().collect();
                self.leave_index = Some(entry.index);
                self.pending_config = Some(entry.index);
                if !self.voters.contains(&self.config.node_id)
                    && !self.learners.contains(&self.config.node_id)
                {
                    self.removed_after = Some((entry.index, entry.term));
                }
            }
            EntryKind::Normal | EntryKind::NoOp => {}
        }
    }

    /// Rolls membership back when conflicting entries are truncated away.
    fn rollback_config_for_truncate(&mut self, trunc_idx: LogIndex) {
        let Some(joint) = self.joint.clone() else {
            return;
        };
        if trunc_idx <= joint.enter_index {
            self.voters = joint.old_voters;
            self.learners = joint.old_learners;
            self.joint = None;
            self.pending_config = None;
            self.leave_index = None;
            self.removed_after = None;
        } else if self.leave_index.is_some_and(|leave| trunc_idx <= leave) {
            self.voters.clone_from(&joint.new_voters);
            self.learners.clone_from(&joint.new_learners);
            self.leave_index = None;
            self.pending_config = Some(joint.enter_index);
            self.removed_after = None;
        }
    }

    /// Clears joint and pending state the commit index proves durable.
    fn resolve_committed_configs(&mut self, commit: LogIndex) {
        if let Some(pending) = self.pending_config
            && pending <= commit
            && self.leave_index.is_none_or(|leave| leave > commit)
        {
            self.pending_config = None;
        }
        if let Some(leave) = self.leave_index
            && leave <= commit
        {
            self.joint = None;
            self.leave_index = None;
            self.pending_config = None;
        }
    }

    /// Ensures a replication target has progress tracking.
    fn ensure_progress(&mut self, node: NodeId) {
        if node == self.config.node_id {
            return;
        }
        if !self.progress.contains_key(&node) {
            let next = self
                .log
                .last_index()
                .next()
                .unwrap_or(self.log.last_index());
            self.progress.insert(
                node,
                PeerProgress::new(next, self.config.max_inflight_messages),
            );
        }
    }

    /// Returns true when `node` votes in the current effective config.
    fn is_effective_voter(&self, node: NodeId) -> bool {
        if let Some(joint) = &self.joint {
            joint.old_voters.contains(&node) || joint.new_voters.contains(&node)
        } else {
            self.voters.contains(&node)
        }
    }

    /// Returns true when `node` is a voter or a learner in the effective
    /// config. A node with an empty voter set and no joint state is still
    /// joining: it accepts traffic from anyone so the cluster can configure
    /// it.
    fn is_member_sender(&self, node: NodeId) -> bool {
        if self.voters.is_empty() && self.joint.is_none() {
            return true;
        }
        self.is_effective_voter(node) || self.learners.contains(&node)
    }

    /// Returns true when `from` may send `message`.
    ///
    /// Election and transfer traffic is limited to members so a removed
    /// server cannot disrupt the cluster. Replication requests are accepted
    /// from any sender: a follower that missed a membership change while
    /// partitioned still holds the old config, where the current leader is a
    /// stranger, and ignoring its appends would strand that follower
    /// forever. Log matching and the receiver-side commit clamp keep such
    /// appends safe.
    fn accepts_sender(&self, from: NodeId, message: &Message) -> bool {
        match message {
            Message::AppendEntriesRequest { .. }
            | Message::HeartbeatRequest { .. }
            | Message::InstallSnapshotRequest { .. } => true,
            _ => self.is_member_sender(from),
        }
    }

    /// Returns true when this node answers a candidate whose last entry is
    /// (`candidate_last`, `candidate_term`). Voters always do. A node removed
    /// by a committed `LeaveJoint` answers only during that leave's term and
    /// only when the candidate's log covers it: the candidate still counts
    /// this node in its joint old quorum, and refusing would deadlock the
    /// handover. A later-term stale-config candidate must not use the
    /// removed node's vote to overwrite committed entries.
    fn may_grant_vote(&self, candidate_last: LogIndex, _candidate_term: Term) -> bool {
        if is(MutantId::M19) || self.is_effective_voter(self.config.node_id) {
            return true;
        }
        self.removed_after
            .is_some_and(|(leave, _)| candidate_last >= leave)
    }

    /// Returns true when this node may campaign: voters only, never learners,
    /// never nodes removed from the effective config, and never nodes under
    /// repair.
    fn can_campaign(&self) -> bool {
        self.repair_from.is_none()
            && (is(MutantId::M19) || self.is_effective_voter(self.config.node_id))
    }

    /// Majority quorum size for `count` voters.
    const fn majority_of(count: usize) -> usize {
        count / 2 + 1
    }

    /// Match index the leader attributes to `node`.
    fn match_of(&self, node: NodeId) -> LogIndex {
        if node == self.config.node_id {
            return if is(MutantId::M04) {
                self.log.last_index()
            } else {
                self.leader_persisted_index
            };
        }
        self.progress
            .get(&node)
            .map_or(LogIndex::from_raw(0), |progress| progress.match_index)
    }

    /// Largest index a majority of `voters` has matched.
    fn quorum_commit_for(&self, voters: &[NodeId]) -> LogIndex {
        let mut matches = [LogIndex::from_raw(0); 32];
        let mut len = 0usize;
        for voter in voters {
            if len < matches.len() {
                matches[len] = self.match_of(*voter);
                len += 1;
            }
        }
        let active = &mut matches[..len];
        active.sort_unstable();
        let quorum = Self::majority_of(active.len());
        if active.len() < quorum {
            return LogIndex::from_raw(0);
        }
        active[active.len() - quorum]
    }

    /// Largest committable index under the effective config.
    fn effective_commit(&self) -> LogIndex {
        if is(MutantId::M01) {
            return self.log.last_index();
        }
        if let Some(joint) = &self.joint {
            let old = self.quorum_commit_for(&joint.old_voters);
            let new = self.quorum_commit_for(&joint.new_voters);
            std::cmp::min(old, new)
        } else {
            self.quorum_commit_for(&self.voters)
        }
    }

    /// Votes inside `voters` from `ids`, counting this node when requested.
    fn count_votes_in(
        &self,
        ids: &BTreeSet<NodeId>,
        include_self: bool,
        voters: &[NodeId],
    ) -> usize {
        let mut count = 0usize;
        for voter in voters {
            if ids.contains(voter) || (include_self && *voter == self.config.node_id) {
                count += 1;
            }
        }
        count
    }

    /// Returns true when `ids` plus this node hold the effective quorum.
    fn effective_quorum_met(&self, ids: &BTreeSet<NodeId>, include_self: bool) -> bool {
        if let Some(joint) = &self.joint {
            let old_votes = self.count_votes_in(ids, include_self, &joint.old_voters);
            let new_votes = self.count_votes_in(ids, include_self, &joint.new_voters);
            let old_ok = old_votes >= Self::majority_of(joint.old_voters.len());
            let new_ok = new_votes >= Self::majority_of(joint.new_voters.len());
            if is(MutantId::M13) {
                old_ok || new_ok
            } else {
                old_ok && new_ok
            }
        } else {
            let votes = self.count_votes_in(ids, include_self, &self.voters);
            votes >= Self::majority_of(self.voters.len())
        }
    }

    /// Copies replication target ids into a stack buffer without allocating.
    fn replica_array(&self, buf: &mut [NodeId; 32]) -> usize {
        let mut len = 0usize;
        for &node in self.progress.keys() {
            if len < buf.len() {
                buf[len] = node;
                len += 1;
            }
        }
        len
    }

    /// Testing helper to set consensus role directly.
    #[doc(hidden)]
    pub fn testing_set_role(&mut self, role: Role) {
        self.role = role;
    }

    /// Testing helper to set consensus term directly.
    #[doc(hidden)]
    pub fn testing_set_term(&mut self, term: Term) {
        self.term = term;
    }

    /// Testing helper to trigger leader election transitions directly.
    #[doc(hidden)]
    pub fn testing_become_leader(&mut self, output: &mut Output) {
        self.become_leader(output);
    }

    fn handle_tick(&mut self, output: &mut Output) {
        match self.role {
            Role::Follower | Role::PreCandidate | Role::Candidate => {
                self.election_elapsed_ticks = self.election_elapsed_ticks.saturating_add(1);
                if self.election_elapsed_ticks >= self.randomized_election_timeout {
                    self.election_elapsed_ticks = 0;
                    self.randomized_election_timeout = Self::sample_random_timeout(
                        self.config.election_timeout_ticks,
                        &mut self.rng,
                    );
                    self.leader_sticky = false;
                    // Learners and removed nodes never campaign.
                    if !self.can_campaign() {
                        return;
                    }
                    if self.config.pre_vote_enabled && !is(MutantId::M18) {
                        self.start_pre_vote(output);
                    } else {
                        self.start_election(output);
                    }
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed_ticks = self.heartbeat_elapsed_ticks.saturating_add(1);
                self.election_elapsed_ticks = self.election_elapsed_ticks.saturating_add(1);

                // A transfer that cannot finish within one election timeout
                // aborts so proposals resume: the transferee is partitioned,
                // too far behind, or gone.
                if self.lead_transferee.is_some() {
                    self.transfer_elapsed = self.transfer_elapsed.saturating_add(1);
                    if self.transfer_elapsed >= self.config.election_timeout_ticks {
                        sometimes!(output.sometimes_hits, "raft.transfer.aborted");
                        self.lead_transferee = None;
                        self.transfer_elapsed = 0;
                    }
                }

                if self.heartbeat_elapsed_ticks >= self.config.heartbeat_interval_ticks {
                    self.heartbeat_elapsed_ticks = 0;
                    self.broadcast_heartbeat(output);
                }

                // Pump snapshot streams within the flow-control window so a
                // full window is in flight even before the first ack returns.
                let mut replicas = [NodeId::from_nonzero(core::num::NonZeroU64::MIN); 32];
                let replica_len = self.replica_array(&mut replicas);
                for &peer in &replicas[..replica_len] {
                    self.resend_snapshot_chunk(peer, output);
                    self.send_snapshot_chunk(peer, output);
                }

                if self.config.check_quorum
                    && self.election_elapsed_ticks >= self.config.election_timeout_ticks
                    && !is(MutantId::M17)
                {
                    self.election_elapsed_ticks = 0;
                    if !self.effective_quorum_met(&self.heartbeat_acks, true) {
                        self.transition_to_follower(self.term, output);
                    }
                    self.heartbeat_acks.clear();
                }

                // A committed membership change can shrink the quorum after
                // the last ack or persist completed. Recheck on every tick so
                // a leader that already holds a majority commits without
                // waiting for another network event.
                self.maybe_advance_commit(output);
            }
        }
    }

    fn start_pre_vote(&mut self, output: &mut Output) {
        if self.membership_voters().len() <= 1 {
            self.start_election(output);
            return;
        }

        self.transition_role(Role::PreCandidate, output);
        self.pre_votes_granted.clear();
        self.pre_votes_granted.insert(self.config.node_id);

        assert!(self.term.next().is_some(), "term overflow on pre-vote");
        let prospective_term = self.term.next().unwrap_or(self.term);
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        for peer in self.election_targets() {
            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::PreVoteRequest {
                    term: prospective_term,
                    candidate_id: self.config.node_id,
                    last_log_index,
                    last_log_term,
                },
            });
        }
    }

    /// Other effective voters this node solicits votes from.
    fn election_targets(&self) -> Vec<NodeId> {
        self.membership_voters()
            .into_iter()
            .filter(|node| *node != self.config.node_id)
            .collect()
    }

    fn handle_pre_vote_request(
        &mut self,
        from: NodeId,
        term: Term,
        _candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        output: &mut Output,
    ) {
        let is_log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
        // Only voters grant pre-votes. Learners, removed nodes, and nodes
        // under repair answer false so they can never feed a quorum.
        let grant = self.repair_from.is_none()
            && self.may_grant_vote(last_log_index, last_log_term)
            && term >= self.term
            && is_log_ok
            && (!self.leader_sticky || self.role == Role::Leader);

        output.messages.push(OutboundMessage {
            to: from,
            message: Message::PreVoteResponse {
                term: self.term,
                vote_granted: grant,
                from: self.config.node_id,
            },
        });
    }

    fn handle_pre_vote_response(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        output: &mut Output,
    ) {
        if self.role != Role::PreCandidate {
            return;
        }

        if term > self.term {
            self.transition_to_follower(term, output);
            return;
        }

        // A lagging peer grants the prospective term carried in the
        // request but answers with its own older term. Counting that reply
        // keeps a candidate that fell back from a failed election from
        // deadlocking against a peer that never saw its newer term.
        if vote_granted {
            self.pre_votes_granted.insert(from);
            if self.effective_quorum_met(&self.pre_votes_granted, true) {
                self.start_election(output);
            }
        }
    }

    fn start_election(&mut self, output: &mut Output) {
        assert!(self.term.next().is_some(), "term overflow on election");
        let new_term = self.term.next().unwrap_or(self.term);
        self.term = new_term;
        self.voted_for = Some(self.config.node_id);
        self.votes_granted.clear();
        self.votes_granted.insert(self.config.node_id);
        self.transition_role(Role::Candidate, output);

        let token = self.next_token();
        output.persist_ops.push(PersistOp::HardState {
            term: self.term,
            vote: self.voted_for,
            token,
        });

        let targets = self.election_targets();
        if targets.is_empty() {
            self.become_leader(output);
            return;
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();
        let term = self.term;
        let candidate_id = self.config.node_id;

        // Term and vote reach disk before any message reveals them.
        for peer in targets {
            let request = OutboundMessage {
                to: peer,
                message: Message::RequestVoteRequest {
                    term,
                    candidate_id,
                    last_log_index,
                    last_log_term,
                },
            };
            self.queue_pending_action(token, PendingAction::SendMessage(request));
        }
    }

    fn handle_request_vote_request(
        &mut self,
        from: NodeId,
        term: Term,
        candidate_id: NodeId,
        last_log_index: LogIndex,
        last_log_term: Term,
        output: &mut Output,
    ) {
        let mut persisted_token: Option<PersistToken> = None;
        if term > self.term {
            persisted_token = self.transition_to_follower(term, output);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
        let is_log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
        // Only voters grant votes. Learners, removed nodes, and nodes under
        // repair never vote.
        let grant = term == self.term
            && can_vote
            && is_log_ok
            && self.repair_from.is_none()
            && self.may_grant_vote(last_log_index, last_log_term);

        if grant {
            self.voted_for = Some(candidate_id);
            self.election_elapsed_ticks = 0;

            let token = self.next_token();
            output.persist_ops.push(PersistOp::HardState {
                term: self.term,
                vote: self.voted_for,
                token,
            });

            let resp = OutboundMessage {
                to: from,
                message: Message::RequestVoteResponse {
                    term: self.term,
                    vote_granted: true,
                    from: self.config.node_id,
                },
            };
            if is(MutantId::M02) {
                output.messages.push(resp);
            } else {
                self.queue_pending_action(token, PendingAction::SendMessage(resp));
            }
        } else if let Some(token) = persisted_token {
            let resp = OutboundMessage {
                to: from,
                message: Message::RequestVoteResponse {
                    term: self.term,
                    vote_granted: false,
                    from: self.config.node_id,
                },
            };
            self.queue_pending_action(token, PendingAction::SendMessage(resp));
        } else {
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::RequestVoteResponse {
                    term: self.term,
                    vote_granted: false,
                    from: self.config.node_id,
                },
            });
        }
    }

    fn handle_request_vote_response(
        &mut self,
        from: NodeId,
        term: Term,
        vote_granted: bool,
        output: &mut Output,
    ) {
        if term > self.term {
            self.transition_to_follower(term, output);
            return;
        }

        if self.role != Role::Candidate || term != self.term {
            return;
        }

        if vote_granted {
            self.votes_granted.insert(from);
            if self.effective_quorum_met(&self.votes_granted, true) {
                self.become_leader(output);
            }
        }
    }

    fn become_leader(&mut self, output: &mut Output) {
        self.transition_role(Role::Leader, output);
        if self.lead_transferee.is_some() {
            sometimes!(output.sometimes_hits, "raft.transfer.aborted");
        }
        self.lead_transferee = None;
        self.transfer_elapsed = 0;
        self.heartbeat_elapsed_ticks = 0;
        self.election_elapsed_ticks = 0;
        self.heartbeat_acks.clear();
        self.committed_current_term = false;
        // Snapshot streams abort on leader change. A new leader streams only
        // snapshots its own node supplies for this term.
        self.leader_snapshot = None;
        self.follower_snapshot = None;
        self.snapshot_requested.clear();

        assert!(
            self.log.last_index().next().is_some(),
            "log index overflow on leader no-op"
        );
        let next_idx = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        // Rebuild progress for every replication target: voters, learners,
        // and both sides of a joint config. Entries the new config dropped
        // lose their tracking here.
        let mut targets = self.membership_voters();
        for learner in &self.learners {
            if !targets.contains(learner) {
                targets.push(*learner);
            }
        }
        let mut fresh = BTreeMap::new();
        for target in targets {
            if target == self.config.node_id {
                continue;
            }
            fresh.insert(
                target,
                PeerProgress::new(next_idx, self.config.max_inflight_messages),
            );
        }
        self.progress = fresh;

        // Leader appends a no-op entry upon election (charter requirement)
        let noop_index = next_idx;
        let noop_entry = Entry::noop(noop_index, self.term);
        self.log.append(noop_entry.clone());

        let token = self.next_token();
        output.persist_ops.push(PersistOp::AppendEntries {
            prev_index: noop_index.prev().unwrap_or(LogIndex::from_raw(0)),
            entries: vec![noop_entry],
            token,
        });

        // Parallel persist: queue match advance for self once disk confirms
        self.queue_pending_action(
            token,
            PendingAction::LeaderPersisted {
                entry_index: noop_index,
            },
        );

        self.broadcast_append_entries(output);
    }

    fn handle_propose(&mut self, payload: Bytes, output: &mut Output) {
        if self.role != Role::Leader || self.lead_transferee.is_some() {
            return;
        }

        assert!(
            self.log.last_index().next().is_some(),
            "log index overflow on propose"
        );
        let entry_index = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        let entry = Entry::normal(entry_index, self.term, payload);
        self.log.append(entry.clone());

        let token = self.next_token();
        if !is(MutantId::M04) {
            output.persist_ops.push(PersistOp::AppendEntries {
                prev_index: entry_index.prev().unwrap_or(LogIndex::from_raw(0)),
                entries: vec![entry],
                token,
            });
        }

        // Parallel persist contract: count match for self only after disk completes
        self.queue_pending_action(token, PendingAction::LeaderPersisted { entry_index });

        self.broadcast_append_entries(output);
    }

    fn handle_propose_config(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        output: &mut Output,
    ) {
        if self.role != Role::Leader || self.lead_transferee.is_some() {
            output.membership_events.push(MembershipEvent::Rejected {
                reason: ConfigReject::NotLeader,
            });
            return;
        }
        // Serialized changes: one uncommitted config entry at a time.
        if self.joint.is_some()
            || self
                .pending_config
                .is_some_and(|pending| pending > self.commit_index)
        {
            sometimes!(output.sometimes_hits, "raft.config.rejected");
            output.membership_events.push(MembershipEvent::Rejected {
                reason: ConfigReject::ConfigInProgress,
            });
            return;
        }
        // The leader must have committed an entry in its own term first, so
        // the new config cannot commit over an uncommitted past.
        if !self.committed_current_term && !is(MutantId::M12) {
            sometimes!(output.sometimes_hits, "raft.config.rejected");
            output.membership_events.push(MembershipEvent::Rejected {
                reason: ConfigReject::NoCurrentTermCommit,
            });
            return;
        }
        let config = match ClusterConfig::new(voters, learners) {
            Ok(config) => config,
            Err(err) => {
                output.membership_events.push(MembershipEvent::Rejected {
                    reason: ConfigReject::InvalidConfig(err),
                });
                return;
            }
        };

        assert!(
            self.log.last_index().next().is_some(),
            "log index overflow on config propose"
        );
        let entry_index = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        let entry = config_entry(EntryKind::EnterJoint, entry_index, self.term, &config);
        self.log.append(entry.clone());

        let token = self.next_token();
        output.persist_ops.push(PersistOp::AppendEntries {
            prev_index: entry_index.prev().unwrap_or(LogIndex::from_raw(0)),
            entries: vec![entry.clone()],
            token,
        });
        self.queue_pending_action(token, PendingAction::LeaderPersisted { entry_index });
        self.note_config_appended(&entry);
        sometimes!(output.sometimes_hits, "raft.config.enter");
        output
            .membership_events
            .push(MembershipEvent::Proposed { index: entry_index });

        self.broadcast_append_entries(output);
    }

    /// Appends `LeaveJoint` carrying the final config once the joint entry
    /// commits. Takes effect on append; the joint marker stays until commit
    /// so concurrent changes stay refused and the removed leader knows when
    /// to step down.
    fn append_leave_joint(&mut self, output: &mut Output) {
        let Some(joint) = self.joint.clone() else {
            return;
        };
        let Ok(config) = ClusterConfig::new(
            joint.new_voters.clone(),
            joint.new_learners.iter().copied().collect(),
        ) else {
            return;
        };
        assert!(
            self.log.last_index().next().is_some(),
            "log index overflow on auto leave"
        );
        let entry_index = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        let entry = config_entry(EntryKind::LeaveJoint, entry_index, self.term, &config);
        self.log.append(entry.clone());

        let token = self.next_token();
        output.persist_ops.push(PersistOp::AppendEntries {
            prev_index: entry_index.prev().unwrap_or(LogIndex::from_raw(0)),
            entries: vec![entry.clone()],
            token,
        });
        self.queue_pending_action(token, PendingAction::LeaderPersisted { entry_index });
        self.note_config_appended(&entry);
        sometimes!(output.sometimes_hits, "raft.config.leave.auto");
        output
            .membership_events
            .push(MembershipEvent::AutoLeave { index: entry_index });

        self.broadcast_append_entries(output);
    }

    /// Steps a removed leader down after `LeaveJoint` commits, handing
    /// leadership to a remaining voter first.
    fn step_down_if_removed(&mut self, output: &mut Output) {
        if self.role != Role::Leader || self.is_effective_voter(self.config.node_id) {
            return;
        }
        sometimes!(output.sometimes_hits, "raft.config.stepdown.removed");
        if let Some(&heir) = self
            .voters
            .iter()
            .find(|voter| **voter != self.config.node_id)
        {
            output.messages.push(OutboundMessage {
                to: heir,
                message: Message::TimeoutNow {
                    term: self.term,
                    leader_id: self.config.node_id,
                },
            });
        }
        self.transition_to_follower(self.term, output);
        output
            .membership_events
            .push(MembershipEvent::RemovedSteppedDown);
    }

    fn send_append_entries(&mut self, peer: NodeId, output: &mut Output) {
        if let Some(progress) = self.progress.get_mut(&peer) {
            if !progress.can_send() {
                return;
            }

            let prev_index = progress.next_index.prev().unwrap_or(LogIndex::from_raw(0));
            let prev_term = self.log.term_at(prev_index).unwrap_or(Term::from_raw(0));
            let entries = self
                .log
                .slice_from(progress.next_index, self.config.max_bytes_per_message);
            let entries_count = entries.len();
            let last_sent = if entries_count > 0 {
                entries.last().map_or(prev_index, |entry| entry.index)
            } else {
                prev_index
            };

            progress.on_send(entries_count, last_sent);

            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::AppendEntriesRequest {
                    term: self.term,
                    leader_id: self.config.node_id,
                    prev_log_index: prev_index,
                    prev_log_term: prev_term,
                    entries,
                    leader_commit: self.commit_index,
                },
            });
        }
    }

    fn broadcast_append_entries(&mut self, output: &mut Output) {
        if self.role != Role::Leader {
            return;
        }

        let mut replicas = [NodeId::from_nonzero(core::num::NonZeroU64::MIN); 32];
        let replica_len = self.replica_array(&mut replicas);
        for &peer in &replicas[..replica_len] {
            self.replicate_to(peer, output);
        }
    }

    /// Sends to one peer, preferring the log whenever the follower is
    /// reachable by log and falling back to a snapshot stream only when the
    /// needed prefix is compacted away.
    fn replicate_to(&mut self, peer: NodeId, output: &mut Output) {
        let needs_snapshot = self
            .progress
            .get(&peer)
            .is_some_and(|progress| progress.next_index <= self.log.base_index());
        if needs_snapshot {
            self.ensure_snapshot(peer, output);
            self.send_snapshot_chunk(peer, output);
        } else {
            self.send_append_entries(peer, output);
        }
    }

    /// Enters snapshot streaming for a peer behind the compacted prefix, or
    /// asks the node for snapshot bytes when none usable is buffered.
    fn ensure_snapshot(&mut self, peer: NodeId, output: &mut Output) {
        let least = self
            .progress
            .get(&peer)
            .map_or(LogIndex::from_raw(0), |progress| progress.next_index);
        let usable = self.leader_snapshot.as_ref().is_some_and(|snap| {
            snap.last_included_index >= least.prev().unwrap_or(LogIndex::from_raw(0))
                && snap.last_included_index >= self.log.base_index()
        });
        if usable {
            let already = self.progress.get(&peer).is_some_and(|progress| {
                matches!(progress.state, crate::progress::ProgressState::Snapshot(_))
            });
            if !already
                && let Some(snap) = self.leader_snapshot.as_ref()
                && let Some(progress) = self.progress.get_mut(&peer)
            {
                sometimes!(output.sometimes_hits, "raft.snapshot.send.start");
                let total = u32::try_from(snap.chunks.len()).unwrap_or(u32::MAX);
                progress.enter_snapshot(snap.last_included_index, total, 0);
            }
            return;
        }
        let requested = self.snapshot_requested.get(&peer);
        if requested.is_none_or(|known| *known != least) {
            self.snapshot_requested.insert(peer, least);
            output.snapshot_requests.push(SnapshotRequest {
                to: peer,
                least_index: least,
            });
        }
    }

    /// Sends the next never-yet-sent snapshot chunk within the window.
    fn send_snapshot_chunk(&mut self, peer: NodeId, output: &mut Output) {
        let cursor = self.progress.get(&peer).and_then(|progress| {
            if !progress.can_send_snapshot() {
                return None;
            }
            if let crate::progress::ProgressState::Snapshot(state) = progress.state {
                Some(state)
            } else {
                None
            }
        });
        let Some(mut cursor) = cursor else {
            return;
        };
        let Some(snap) = self.leader_snapshot.clone() else {
            return;
        };
        let total = u32::try_from(snap.chunks.len()).unwrap_or(u32::MAX);
        if total == 0 {
            return;
        }
        if cursor.last_included_index == snap.last_included_index {
            cursor.total_chunks = total;
        } else {
            cursor = SnapshotProgress {
                last_included_index: snap.last_included_index,
                send_next: 0,
                acked_chunk: 0,
                total_chunks: total,
            };
        }
        if cursor.send_next >= total {
            return;
        }
        let chunk_idx = cursor.send_next;
        let slot = usize::try_from(chunk_idx).unwrap_or(usize::MAX);
        let Some(data) = snap.chunks.get(slot).cloned() else {
            return;
        };
        if let Some(progress) = self.progress.get_mut(&peer) {
            progress.state = crate::progress::ProgressState::Snapshot(cursor);
            progress.on_snapshot_send_new();
        }
        output.messages.push(OutboundMessage {
            to: peer,
            message: Message::InstallSnapshotRequest {
                term: self.term,
                leader_id: self.config.node_id,
                last_included_index: cursor.last_included_index,
                last_included_term: snap.last_included_term,
                chunk_index: chunk_idx,
                total_chunks: total,
                chunk: data.chunk,
                chunk_crc: data.chunk_crc,
                snapshot_digest: snap.snapshot_digest,
                snapshot_bytes: snap.snapshot_bytes,
            },
        });
    }

    /// Resends the oldest unacked chunk without consuming window: the chunk
    /// is already counted in flight, so a lost ack or packet can always be
    /// recovered on Tick even when the window is full.
    fn resend_snapshot_chunk(&mut self, peer: NodeId, output: &mut Output) {
        let cursor = self.progress.get(&peer).and_then(|progress| {
            if let crate::progress::ProgressState::Snapshot(state) = progress.state {
                Some(state)
            } else {
                None
            }
        });
        let Some(cursor) = cursor else {
            return;
        };
        let Some(snap) = self.leader_snapshot.clone() else {
            return;
        };
        if cursor.last_included_index != snap.last_included_index {
            return;
        }
        let total = u32::try_from(snap.chunks.len()).unwrap_or(u32::MAX);
        let outstanding = cursor.send_next.min(total);
        if cursor.acked_chunk >= outstanding {
            return;
        }
        let slot = usize::try_from(cursor.acked_chunk).unwrap_or(usize::MAX);
        let Some(data) = snap.chunks.get(slot).cloned() else {
            return;
        };
        sometimes!(output.sometimes_hits, "raft.snapshot.resend");
        output.messages.push(OutboundMessage {
            to: peer,
            message: Message::InstallSnapshotRequest {
                term: self.term,
                leader_id: self.config.node_id,
                last_included_index: cursor.last_included_index,
                last_included_term: snap.last_included_term,
                chunk_index: cursor.acked_chunk,
                total_chunks: total,
                chunk: data.chunk,
                chunk_crc: data.chunk_crc,
                snapshot_digest: snap.snapshot_digest,
                snapshot_bytes: snap.snapshot_bytes,
            },
        });
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn handle_append_entries_request(
        &mut self,
        from: NodeId,
        term: Term,
        _leader_id: NodeId,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        entries: Vec<Entry>,
        leader_commit: LogIndex,
        output: &mut Output,
    ) {
        let mut term_persist_token: Option<PersistToken> = None;
        if term > self.term {
            sometimes!(output.sometimes_hits, "raft.term.bump.append");
            term_persist_token = self.transition_to_follower(term, output);
        }

        if term < self.term {
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    match_index: self.report_match(self.log.last_index()),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            });
            return;
        }

        self.election_elapsed_ticks = 0;
        self.leader_sticky = true;
        if self.role != Role::Follower {
            let same_term_token = self.transition_to_follower(term, output);
            assert!(
                same_term_token.is_none(),
                "same-term follower transition must not require persistence"
            );
        }

        let local_prev_term = self.log.term_at(prev_log_index);
        let matches = match local_prev_term {
            Some(t) => t == prev_log_term,
            None => false,
        };

        if !matches {
            let (conflict_term, conflict_index) = match local_prev_term {
                Some(conf_term) => {
                    let first_idx = self
                        .log
                        .find_first_index_of_term(conf_term)
                        .unwrap_or(prev_log_index);
                    (Some(conf_term), Some(first_idx))
                }
                None => (
                    None,
                    Some(
                        self.log
                            .last_index()
                            .next()
                            .unwrap_or(self.log.last_index()),
                    ),
                ),
            };

            let rejection = OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    match_index: self.report_match(self.log.last_index()),
                    conflict_term,
                    conflict_index,
                    from: self.config.node_id,
                },
            };
            if let Some(token) = term_persist_token {
                self.queue_pending_action(token, PendingAction::SendMessage(rejection));
            } else {
                output.messages.push(rejection);
            }
            return;
        }

        if !entries.is_empty() {
            let contiguous = match prev_log_index.next() {
                Some(expected_first) => entries[0].index == expected_first,
                None => false,
            };
            if !contiguous {
                let hint = prev_log_index.next().unwrap_or(prev_log_index);
                let rejection = OutboundMessage {
                    to: from,
                    message: Message::AppendEntriesResponse {
                        term: self.term,
                        success: false,
                        match_index: self.report_match(self.log.last_index()),
                        conflict_term: None,
                        conflict_index: Some(hint),
                        from: self.config.node_id,
                    },
                };
                if let Some(token) = term_persist_token {
                    self.queue_pending_action(token, PendingAction::SendMessage(rejection));
                } else {
                    output.messages.push(rejection);
                }
                return;
            }
        }

        let mut new_match = prev_log_index.max(self.log.base_index());
        let mut entries_to_append = Vec::new();
        let mut truncate_from = None;

        for entry in entries {
            if entry.index <= self.log.base_index() {
                // Already snapshotted: a stale resend of the compacted
                // prefix (for example after a delayed conflict hint reset
                // the leader's next index) must be skipped, never appended
                // past the live tail. The snapshot covers this prefix.
                continue;
            }
            if truncate_from.is_some() {
                entries_to_append.push(entry);
            } else if let Some(existing_term) = self.log.term_at(entry.index) {
                if existing_term != entry.term {
                    truncate_from = Some(entry.index);
                    entries_to_append.push(entry);
                }
            } else {
                entries_to_append.push(entry);
            }
        }

        if let Some(trunc_idx) = truncate_from {
            if !is(MutantId::M07) {
                self.log
                    .truncate_and_append(trunc_idx, entries_to_append.clone());
            }
            new_match = self.log.last_index();
            // A truncated joint entry never took effect on this follower.
            self.rollback_config_for_truncate(trunc_idx);
            for entry in &entries_to_append {
                self.note_config_appended(entry);
            }

            let token = self.next_token();
            output.persist_ops.push(PersistOp::TruncateAndAppend {
                from_index: trunc_idx,
                entries: entries_to_append,
                token,
            });

            // Persistence contract: release success response only after disk sync
            let resp = OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: true,
                    match_index: self.report_match(new_match),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            };
            if is(MutantId::M03) {
                output.persist_ops.clear();
                output.messages.push(resp);
            } else {
                self.queue_pending_action(token, PendingAction::SendMessage(resp));
            }
        } else if !entries_to_append.is_empty() {
            let start_append_idx = entries_to_append[0].index;
            let prev_idx = start_append_idx.prev().unwrap_or(LogIndex::from_raw(0));
            for entry in &entries_to_append {
                self.log.append(entry.clone());
            }
            new_match = self.log.last_index();
            for entry in &entries_to_append {
                self.note_config_appended(entry);
            }

            let token = self.next_token();
            output.persist_ops.push(PersistOp::AppendEntries {
                prev_index: prev_idx,
                entries: entries_to_append,
                token,
            });

            let resp = OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: true,
                    match_index: self.report_match(new_match),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            };
            if is(MutantId::M03) {
                output.persist_ops.clear();
                output.messages.push(resp);
            } else {
                self.queue_pending_action(token, PendingAction::SendMessage(resp));
            }
        } else if let Some(token) = term_persist_token {
            let resp = OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: true,
                    match_index: self.report_match(new_match),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            };
            self.queue_pending_action(token, PendingAction::SendMessage(resp));
        } else {
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: true,
                    match_index: self.report_match(new_match),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            });
        }

        if leader_commit > self.commit_index {
            // Commit only up to the matched prefix: an empty AppendEntries
            // (or a duplicate resend) carries no new entries, so the
            // follower's tail beyond `new_match` is still divergent and
            // must not be committed. `new_match` is the last index the
            // leader and follower are known to agree on.
            let target_commit = std::cmp::min(leader_commit, new_match);
            if target_commit > self.commit_index {
                self.commit_to(target_commit, output);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn handle_append_entries_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        conflict_term: Option<Term>,
        conflict_index: Option<LogIndex>,
        output: &mut Output,
    ) {
        if term > self.term {
            self.transition_to_follower(term, output);
            return;
        }

        if self.role != Role::Leader || term != self.term {
            return;
        }

        let Some(progress) = self.progress.get_mut(&from) else {
            return;
        };

        if success {
            progress.on_success(match_index);
            self.heartbeat_acks.insert(from);
            self.maybe_advance_commit(output);

            if self.lead_transferee == Some(from) && match_index >= self.log.last_index() {
                sometimes!(output.sometimes_hits, "raft.transfer.timeoutnow.sent");
                output.messages.push(OutboundMessage {
                    to: from,
                    message: Message::TimeoutNow {
                        term: self.term,
                        leader_id: self.config.node_id,
                    },
                });
                self.lead_transferee = None;
                self.transfer_elapsed = 0;
            }
        } else {
            let last_idx_fn = |term| self.log.find_last_index_of_term(term);
            progress.on_rejection(conflict_term, conflict_index, last_idx_fn);
            self.broadcast_append_entries(output);
        }
    }

    fn maybe_advance_commit(&mut self, output: &mut Output) {
        if self.role != Role::Leader {
            return;
        }

        let candidate = self.effective_commit();
        if candidate <= self.commit_index {
            return;
        }
        // Raft Section 5.4.2 rule: leader only commits entries from its CURRENT term
        if self.log.term_at(candidate) != Some(self.term) && !is(MutantId::M01) {
            return;
        }
        self.commit_to(candidate, output);
        self.committed_current_term = true;
        self.after_commit_membership(output);
    }

    fn commit_to(&mut self, new_commit: LogIndex, output: &mut Output) {
        let start_idx = self.commit_index.next().unwrap_or(self.commit_index);
        self.commit_index = new_commit;
        self.resolve_committed_configs(new_commit);

        let raw_start = start_idx.as_u64();
        let raw_end = new_commit.as_u64();
        for idx in raw_start..=raw_end {
            if let Some(entry) = self.log.entry_at(LogIndex::from_raw(idx)) {
                output.committed_entries.push(entry.clone());
            }
        }
    }

    /// Membership work after the leader advances the commit index: leave the
    /// joint config automatically once it commits, and step down when this
    /// leader was removed by the committed `LeaveJoint`.
    fn after_commit_membership(&mut self, output: &mut Output) {
        if self.joint.is_some() && self.leave_index.is_none() {
            let enter = self
                .joint
                .as_ref()
                .map_or(LogIndex::from_raw(0), |joint| joint.enter_index);
            if self.commit_index >= enter {
                self.append_leave_joint(output);
            }
        }
        if self.joint.is_none() {
            self.step_down_if_removed(output);
        }
    }

    fn broadcast_heartbeat(&mut self, output: &mut Output) {
        let term = self.term;
        let leader_id = self.config.node_id;
        let commit_index = self.commit_index;
        for &peer in self.progress.keys() {
            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::HeartbeatRequest {
                    term,
                    leader_id,
                    commit_index,
                    read_context: None,
                },
            });
        }
    }

    fn handle_heartbeat_request(
        &mut self,
        from: NodeId,
        term: Term,
        _leader_id: NodeId,
        commit_index: LogIndex,
        read_context: Option<Bytes>,
        output: &mut Output,
    ) {
        let mut term_persist_token: Option<PersistToken> = None;
        if term > self.term {
            term_persist_token = self.transition_to_follower(term, output);
        }

        if term >= self.term {
            self.election_elapsed_ticks = 0;
            self.leader_sticky = true;
            if self.role != Role::Follower {
                let same_term_token = self.transition_to_follower(term, output);
                assert!(
                    same_term_token.is_none(),
                    "same-term follower transition must not require persistence"
                );
            }
            // Heartbeat carries no prev_log check, so commit advances only
            // when the local entry at the target already matches the
            // leader's term. The leader's commit index always points at a
            // current-term entry (commit rule), so a mismatch means the log
            // has diverged and the entry arrives via AppendEntries instead.
            if commit_index > self.commit_index {
                let target = std::cmp::min(commit_index, self.log.last_index());
                if target > self.commit_index && self.log.term_at(target) == Some(term) {
                    self.commit_to(target, output);
                }
            }
        }

        let response = OutboundMessage {
            to: from,
            message: Message::HeartbeatResponse {
                term: self.term,
                from: self.config.node_id,
                read_context,
            },
        };
        if let Some(token) = term_persist_token {
            self.queue_pending_action(token, PendingAction::SendMessage(response));
        } else {
            output.messages.push(response);
        }
    }

    fn handle_heartbeat_response(
        &mut self,
        from: NodeId,
        term: Term,
        read_context: Option<Bytes>,
        output: &mut Output,
    ) {
        if term > self.term {
            self.transition_to_follower(term, output);
            return;
        }

        if self.role != Role::Leader || term != self.term {
            return;
        }

        self.heartbeat_acks.insert(from);

        let should_send_append = if let Some(progress) = self.progress.get_mut(&from) {
            if progress.state == crate::progress::ProgressState::Probe {
                progress.inflight_count = 0;
            }
            progress.match_index < self.log.last_index()
        } else {
            false
        };

        if should_send_append {
            self.replicate_to(from, output);
        }

        if let Some(ctx) = read_context {
            for pending in &mut self.pending_reads {
                if pending.context == ctx {
                    pending.acks.insert(from);
                }
            }
            let mut i = 0;
            while i < self.pending_reads.len() {
                let is_ctx = self.pending_reads[i].context == ctx;
                if is_ctx && self.effective_quorum_met(&self.pending_reads[i].acks, true) {
                    let completed = self.pending_reads.swap_remove(i);
                    output.completed_reads.push(ReadIndexResult {
                        context: completed.context,
                        read_index: completed.read_index,
                    });
                } else {
                    i += 1;
                }
            }
        }
    }

    fn handle_read_index(&mut self, context: Bytes, output: &mut Output) {
        if self.role != Role::Leader {
            return;
        }

        if self.membership_voters().len() <= 1 {
            output.completed_reads.push(ReadIndexResult {
                context,
                read_index: self.commit_index,
            });
            return;
        }

        let mut acks = BTreeSet::new();
        acks.insert(self.config.node_id);

        self.pending_reads.push(PendingRead {
            context: context.clone(),
            read_index: self.commit_index,
            acks,
        });

        // Broadcast heartbeat probing quorum connectivity
        for peer in self.election_targets() {
            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::HeartbeatRequest {
                    term: self.term,
                    leader_id: self.config.node_id,
                    commit_index: self.commit_index,
                    read_context: Some(context.clone()),
                },
            });
        }
    }

    fn handle_compact_log(&mut self, up_to: LogIndex) {
        let base = self.log.base_index().as_u64();
        let raw = up_to.as_u64();
        if raw <= base || raw > self.applied_index.as_u64() || raw > self.log.last_index().as_u64()
        {
            return;
        }
        self.log.compact(up_to);
    }

    fn handle_snapshot_ready(
        &mut self,
        last_included_index: LogIndex,
        last_included_term: Term,
        snapshot_bytes: u64,
        snapshot_digest: [u8; 32],
        chunks: Vec<SnapshotChunkData>,
        output: &mut Output,
    ) {
        if self.role != Role::Leader {
            return;
        }
        if last_included_index.is_zero()
            || chunks.is_empty()
            || chunks.len() > MAX_SNAPSHOT_CHUNKS
            || snapshot_bytes == 0
            || snapshot_bytes > MAX_SNAPSHOT_BYTES
        {
            return;
        }
        let mut total: u64 = 0;
        for entry in &chunks {
            if entry.chunk.is_empty() || entry.chunk.len() > MAX_SNAPSHOT_CHUNK_BYTES {
                return;
            }
            total = total.saturating_add(entry.chunk.len() as u64);
        }
        if total != snapshot_bytes {
            return;
        }
        self.leader_snapshot = Some(LeaderSnapshot {
            last_included_index,
            last_included_term,
            snapshot_bytes,
            snapshot_digest,
            chunks,
        });
        let mut replicas = [NodeId::from_nonzero(core::num::NonZeroU64::MIN); 32];
        let replica_len = self.replica_array(&mut replicas);
        for peer in &replicas[..replica_len] {
            self.replicate_to(*peer, output);
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    fn handle_install_snapshot_request(
        &mut self,
        from: NodeId,
        term: Term,
        leader_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        chunk_index: u32,
        total_chunks: u32,
        chunk: Bytes,
        chunk_crc: u32,
        snapshot_digest: [u8; 32],
        snapshot_bytes: u64,
        output: &mut Output,
    ) {
        if term < self.term {
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::InstallSnapshotResponse {
                    term: self.term,
                    success: false,
                    match_index: LogIndex::from_raw(0),
                    next_chunk: 0,
                    from: self.config.node_id,
                },
            });
            return;
        }
        let mut term_persist_token: Option<PersistToken> = None;
        if term > self.term {
            term_persist_token = self.transition_to_follower(term, output);
        }
        if term >= self.term {
            self.election_elapsed_ticks = 0;
            self.leader_sticky = true;
            if self.role != Role::Follower {
                let same_term_token = self.transition_to_follower(term, output);
                assert!(
                    same_term_token.is_none(),
                    "same-term follower transition must not require persistence"
                );
            }
        }

        let valid = !last_included_index.is_zero()
            && total_chunks > 0
            && usize::try_from(total_chunks).is_ok_and(|n| n <= MAX_SNAPSHOT_CHUNKS)
            && snapshot_bytes > 0
            && snapshot_bytes <= MAX_SNAPSHOT_BYTES
            && chunk_index < total_chunks
            && chunk.len() <= MAX_SNAPSHOT_CHUNK_BYTES
            && (chunk.len() as u64) <= snapshot_bytes;
        if !valid {
            let rejection = OutboundMessage {
                to: from,
                message: Message::InstallSnapshotResponse {
                    term: self.term,
                    success: false,
                    match_index: LogIndex::from_raw(0),
                    next_chunk: 0,
                    from: self.config.node_id,
                },
            };
            if let Some(token) = term_persist_token {
                self.queue_pending_action(token, PendingAction::SendMessage(rejection));
            } else {
                output.messages.push(rejection);
            }
            return;
        }

        if last_included_index <= self.commit_index {
            let ack = OutboundMessage {
                to: from,
                message: Message::InstallSnapshotResponse {
                    term: self.term,
                    success: true,
                    match_index: last_included_index,
                    next_chunk: total_chunks,
                    from: self.config.node_id,
                },
            };
            if let Some(token) = term_persist_token {
                self.queue_pending_action(token, PendingAction::SendMessage(ack));
            } else {
                output.messages.push(ack);
            }
            return;
        }

        let same_stream = self.follower_snapshot.as_ref().is_some_and(|pending| {
            pending.leader == leader_id
                && pending.term == term
                && pending.last_included_index == last_included_index
                && pending.last_included_term == last_included_term
                && pending.total_chunks == total_chunks
                && pending.snapshot_digest == snapshot_digest
                && pending.snapshot_bytes == snapshot_bytes
        });
        if !same_stream {
            let total = usize::try_from(total_chunks).unwrap_or(0);
            let mut slots = Vec::new();
            slots.resize_with(total, || None);
            self.follower_snapshot = Some(FollowerSnapshot {
                leader: leader_id,
                term,
                last_included_index,
                last_included_term,
                total_chunks,
                snapshot_bytes,
                snapshot_digest,
                chunks: slots,
                received: 0,
            });
        }

        let mut wanted = total_chunks;
        let mut complete = false;
        let mut chunk_ok = true;
        if let Some(pending) = self.follower_snapshot.as_mut() {
            let crc_ok = crc32c::crc32c(&chunk) == chunk_crc;
            chunk_ok = crc_ok;
            if crc_ok {
                let slot = usize::try_from(chunk_index).unwrap_or(pending.chunks.len());
                if slot < pending.chunks.len() && pending.chunks[slot].is_none() {
                    pending.chunks[slot] = Some(chunk);
                    pending.received = pending.received.saturating_add(1);
                }
                if pending.received >= pending.total_chunks {
                    complete = true;
                } else {
                    wanted = first_missing(pending);
                }
            }
        }

        if complete {
            let assembled = self.follower_snapshot.as_ref().map(|pending| {
                let mut full = Vec::new();
                for bytes in pending.chunks.iter().flatten() {
                    full.extend_from_slice(bytes);
                }
                Bytes::from(full)
            });
            if let Some(data) = assembled {
                if *blake3::hash(&data).as_bytes() != snapshot_digest {
                    self.follower_snapshot = None;
                    let rejection = OutboundMessage {
                        to: from,
                        message: Message::InstallSnapshotResponse {
                            term: self.term,
                            success: false,
                            match_index: LogIndex::from_raw(0),
                            next_chunk: 0,
                            from: self.config.node_id,
                        },
                    };
                    if let Some(token) = term_persist_token {
                        self.queue_pending_action(token, PendingAction::SendMessage(rejection));
                    } else {
                        output.messages.push(rejection);
                    }
                    return;
                }
                self.log
                    .install_snapshot(last_included_index, last_included_term);
                self.installed_base = Some(last_included_index);
                sometimes!(output.sometimes_hits, "raft.snapshot.install.complete");
                if last_included_index > self.commit_index {
                    self.commit_index = last_included_index;
                }
                output.ready_snapshots.push(ReceivedSnapshot {
                    last_included_index,
                    last_included_term,
                    data,
                    snapshot_digest,
                });
                self.follower_snapshot = None;
                let ack = OutboundMessage {
                    to: from,
                    message: Message::InstallSnapshotResponse {
                        term: self.term,
                        success: true,
                        match_index: last_included_index,
                        next_chunk: total_chunks,
                        from: self.config.node_id,
                    },
                };
                if let Some(token) = term_persist_token {
                    self.queue_pending_action(token, PendingAction::SendMessage(ack));
                } else {
                    output.messages.push(ack);
                }
                return;
            }
        }

        let ack = OutboundMessage {
            to: from,
            message: Message::InstallSnapshotResponse {
                term: self.term,
                success: chunk_ok,
                match_index: LogIndex::from_raw(0),
                next_chunk: wanted,
                from: self.config.node_id,
            },
        };
        if let Some(token) = term_persist_token {
            self.queue_pending_action(token, PendingAction::SendMessage(ack));
        } else {
            output.messages.push(ack);
        }
    }

    fn handle_install_snapshot_response(
        &mut self,
        from: NodeId,
        term: Term,
        success: bool,
        match_index: LogIndex,
        next_chunk: u32,
        output: &mut Output,
    ) {
        if term > self.term {
            self.transition_to_follower(term, output);
            return;
        }
        if self.role != Role::Leader || term != self.term {
            return;
        }
        let snapshot_state = self.progress.get(&from).and_then(|progress| {
            if let crate::progress::ProgressState::Snapshot(state) = progress.state {
                Some(state)
            } else {
                None
            }
        });
        let Some(state) = snapshot_state else {
            return;
        };
        if let Some(progress) = self.progress.get_mut(&from) {
            progress.on_snapshot_ack(next_chunk);
        }
        if success && match_index >= state.last_included_index {
            if let Some(progress) = self.progress.get_mut(&from) {
                progress.exit_snapshot(match_index);
            }
            self.heartbeat_acks.insert(from);
            self.maybe_advance_commit(output);
            self.replicate_to(from, output);
            return;
        }
        // The follower wants an older chunk back: resend exactly that one
        // without touching the send pointer, then keep filling the window
        // with new chunks.
        let send_next = self.progress.get(&from).and_then(|progress| {
            if let crate::progress::ProgressState::Snapshot(snapshot) = progress.state {
                Some((snapshot.send_next, snapshot.total_chunks))
            } else {
                None
            }
        });
        if let Some((send_next, total)) = send_next {
            let resend = next_chunk.min(total.saturating_sub(1));
            if resend < send_next
                && let Some(snap) = self.leader_snapshot.clone()
                && snap.last_included_index == state.last_included_index
                && let Some(data) = snap
                    .chunks
                    .get(usize::try_from(resend).unwrap_or(usize::MAX))
                    .cloned()
            {
                sometimes!(output.sometimes_hits, "raft.snapshot.resend");
                output.messages.push(OutboundMessage {
                    to: from,
                    message: Message::InstallSnapshotRequest {
                        term: self.term,
                        leader_id: self.config.node_id,
                        last_included_index: state.last_included_index,
                        last_included_term: snap.last_included_term,
                        chunk_index: resend,
                        total_chunks: total,
                        chunk: data.chunk,
                        chunk_crc: data.chunk_crc,
                        snapshot_digest: snap.snapshot_digest,
                        snapshot_bytes: snap.snapshot_bytes,
                    },
                });
            }
        }
        while self.snapshot_has_unsent(from)
            && self
                .progress
                .get(&from)
                .is_some_and(crate::progress::PeerProgress::can_send_snapshot)
        {
            self.send_snapshot_chunk(from, output);
        }
    }

    /// Returns true when the peer's snapshot stream still has never-sent
    /// chunks, so a fill loop makes progress on every iteration.
    fn snapshot_has_unsent(&self, peer: NodeId) -> bool {
        self.progress.get(&peer).is_some_and(|progress| {
            if let crate::progress::ProgressState::Snapshot(state) = progress.state {
                state.send_next < state.total_chunks
            } else {
                false
            }
        })
    }

    fn handle_transfer_leader(&mut self, to: NodeId, output: &mut Output) {
        if self.role != Role::Leader || to == self.config.node_id {
            return;
        }
        // Leadership transfers only to voters. Learners can never lead.
        if !self.is_effective_voter(to) {
            return;
        }
        if !self.progress.contains_key(&to) {
            return;
        }

        self.lead_transferee = Some(to);
        self.transfer_elapsed = 0;

        if let Some(peer) = self.progress.get(&to) {
            if peer.match_index >= self.log.last_index() {
                sometimes!(output.sometimes_hits, "raft.transfer.timeoutnow.sent");
                output.messages.push(OutboundMessage {
                    to,
                    message: Message::TimeoutNow {
                        term: self.term,
                        leader_id: self.config.node_id,
                    },
                });
                self.lead_transferee = None;
                self.transfer_elapsed = 0;
            } else {
                self.broadcast_append_entries(output);
            }
        }
    }

    fn handle_timeout_now(&mut self, term: Term, _leader_id: NodeId, output: &mut Output) {
        if term < self.term {
            return;
        }
        if term > self.term {
            let _ = self.transition_to_follower(term, output);
        }
        // Learners and removed nodes never campaign, even when told to.
        if !self.can_campaign() {
            return;
        }
        self.start_election(output);
    }

    fn handle_persisted(&mut self, token: PersistToken, output: &mut Output) {
        let mut i = 0;
        let mut leader_advanced = false;
        while i < self.pending_persists.len() {
            if self.pending_persists[i].0 == token {
                let (_, action) = self.pending_persists.swap_remove(i);
                match action {
                    PendingAction::SendMessage(msg) => {
                        output.messages.push(msg);
                    }
                    PendingAction::LeaderPersisted { entry_index } => {
                        if entry_index > self.leader_persisted_index {
                            self.leader_persisted_index = entry_index;
                            leader_advanced = true;
                        }
                    }
                }
            } else {
                i += 1;
            }
        }
        if leader_advanced {
            self.maybe_advance_commit(output);
        }
    }

    fn handle_applied(&mut self, index: LogIndex) {
        if index > self.applied_index {
            self.applied_index = index;
        }
    }

    #[allow(clippy::too_many_lines)]
    fn handle_message(&mut self, from: NodeId, message: Message, output: &mut Output) {
        // Removed servers and strangers cannot disrupt the cluster: only
        // voters and learners in the effective config get a hearing. The
        // sole exception is replication from a leader this node does not
        // know yet.
        if !self.accepts_sender(from, &message) {
            sometimes!(output.sometimes_hits, "raft.unknown.sender");
            return;
        }
        match message {
            Message::PreVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_pre_vote_request(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                output,
            ),
            Message::PreVoteResponse {
                term, vote_granted, ..
            } => self.handle_pre_vote_response(from, term, vote_granted, output),
            Message::RequestVoteRequest {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => self.handle_request_vote_request(
                from,
                term,
                candidate_id,
                last_log_index,
                last_log_term,
                output,
            ),
            Message::RequestVoteResponse {
                term, vote_granted, ..
            } => self.handle_request_vote_response(from, term, vote_granted, output),
            Message::AppendEntriesRequest {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
            } => self.handle_append_entries_request(
                from,
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                output,
            ),
            Message::AppendEntriesResponse {
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
                ..
            } => self.handle_append_entries_response(
                from,
                term,
                success,
                match_index,
                conflict_term,
                conflict_index,
                output,
            ),
            Message::HeartbeatRequest {
                term,
                leader_id,
                commit_index,
                read_context,
            } => self.handle_heartbeat_request(
                from,
                term,
                leader_id,
                commit_index,
                read_context,
                output,
            ),
            Message::HeartbeatResponse {
                term, read_context, ..
            } => self.handle_heartbeat_response(from, term, read_context, output),
            Message::TimeoutNow { term, leader_id } => {
                self.handle_timeout_now(term, leader_id, output);
            }
            Message::InstallSnapshotRequest {
                term,
                leader_id,
                last_included_index,
                last_included_term,
                chunk_index,
                total_chunks,
                chunk,
                chunk_crc,
                snapshot_digest,
                snapshot_bytes,
            } => self.handle_install_snapshot_request(
                from,
                term,
                leader_id,
                last_included_index,
                last_included_term,
                chunk_index,
                total_chunks,
                chunk,
                chunk_crc,
                snapshot_digest,
                snapshot_bytes,
                output,
            ),
            Message::InstallSnapshotResponse {
                term,
                success,
                match_index,
                next_chunk,
                ..
            } => self.handle_install_snapshot_response(
                from,
                term,
                success,
                match_index,
                next_chunk,
                output,
            ),
        }
    }

    fn transition_role(&mut self, new_role: Role, output: &mut Output) {
        if self.role != new_role {
            output.role_changes.push(RoleChangeEvent {
                previous: self.role,
                current: new_role,
                term: self.term,
            });
            self.role = new_role;
        }
    }

    fn transition_to_follower(&mut self, term: Term, output: &mut Output) -> Option<PersistToken> {
        let term_changed = term != self.term;
        self.transition_role(Role::Follower, output);
        // A vote cast in the current term survives a same-term stepdown.
        // Clearing it would let this node vote twice in one term after a
        // restart where disk still records the earlier vote.
        if term_changed {
            self.term = term;
            self.voted_for = None;
        }
        self.committed_current_term = false;
        if self.lead_transferee.is_some() {
            sometimes!(output.sometimes_hits, "raft.transfer.aborted");
        }
        self.lead_transferee = None;
        self.transfer_elapsed = 0;
        self.pre_votes_granted.clear();
        self.votes_granted.clear();
        self.leader_snapshot = None;
        self.snapshot_requested.clear();
        if term_changed {
            self.follower_snapshot = None;
        }

        if term_changed {
            let token = self.next_token();
            output.persist_ops.push(PersistOp::HardState {
                term: self.term,
                vote: self.voted_for,
                token,
            });
            Some(token)
        } else {
            None
        }
    }

    fn is_log_up_to_date(&self, candidate_index: LogIndex, candidate_term: Term) -> bool {
        let local_term = self.log.last_term();
        let local_index = self.log.last_index();

        candidate_term > local_term
            || (candidate_term == local_term && candidate_index >= local_index)
    }

    fn next_token(&mut self) -> PersistToken {
        let token = PersistToken(self.next_persist_token);
        self.next_persist_token = self.next_persist_token.wrapping_add(1);
        token
    }

    fn queue_pending_action(&mut self, token: PersistToken, action: PendingAction) {
        self.pending_persists.push((token, action));
    }

    fn emit_spec_projection(&self, output: &mut Output) {
        output.spec_projections.push(SpecProjection {
            term: self.term,
            role: self.role,
            voted_for: self.voted_for,
            log_digest: self.log.compute_digest(),
            commit_index: self.commit_index,
        });
    }

    /// Draws an unbiased randomized election timeout in `[low, low + base_ticks)`.
    ///
    /// The sampler duplicates `DetRng::gen_range` on purpose: `anvil-raft` sits below
    /// `anvil-sim` in the crate DAG and cannot depend on it (ADR 0004).
    #[allow(clippy::cast_possible_truncation)]
    fn sample_random_timeout(base_ticks: u64, rng: &mut ChaCha8Rng) -> u64 {
        let low = base_ticks;
        let high = base_ticks.saturating_mul(2).max(low.saturating_add(1));
        let span = high.saturating_sub(low);
        if span <= 1 {
            return low;
        }

        let mut sample = rng.next_u64();
        let mut product = u128::from(sample) * u128::from(span);
        let mut low_bits = product as u64;

        if low_bits < span {
            let threshold = span.wrapping_neg() % span;
            while low_bits < threshold {
                sample = rng.next_u64();
                product = u128::from(sample) * u128::from(span);
                low_bits = product as u64;
            }
        }

        low + (product >> 64) as u64
    }
}
