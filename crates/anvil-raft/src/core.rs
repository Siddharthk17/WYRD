#![forbid(unsafe_code)]

//! Core sans-IO Raft consensus state machine (`RaftCore`).

use crate::log::RaftLog;
use crate::messages::Message;
use crate::progress::PeerProgress;
use crate::types::{
    Input, OutboundMessage, Output, PersistOp, PersistToken, RaftConfig, ReadIndexResult,
    RecoveredState, Role, RoleChangeEvent, SpecProjection,
};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::Rng;
use std::collections::{BTreeMap, BTreeSet};

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
    pending_persists: BTreeMap<PersistToken, Vec<PendingAction>>,
    leader_persisted_index: LogIndex,
}

impl RaftCore {
    /// Constructs a new `RaftCore` instance from configuration, recovered state, and a `ChaCha8` RNG.
    #[must_use]
    pub fn new(config: RaftConfig, recovered: RecoveredState, mut rng: ChaCha8Rng) -> Self {
        let randomized_election_timeout =
            Self::sample_random_timeout(config.election_timeout_ticks, &mut rng);

        let mut progress = BTreeMap::new();
        let last_log_idx = recovered
            .entries
            .last()
            .map_or(LogIndex::from_raw(0), |e| e.index);
        let initial_next = last_log_idx.next().unwrap_or(last_log_idx);

        for &peer in &config.peers {
            progress.insert(
                peer,
                PeerProgress::new(initial_next, config.max_inflight_messages),
            );
        }

        Self {
            config,
            term: recovered.current_term,
            voted_for: recovered.voted_for,
            role: Role::Follower,
            log: RaftLog::new(recovered.entries),
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
            pending_persists: BTreeMap::new(),
            leader_persisted_index: last_log_idx,
        }
    }

    /// Primary sans-IO transition function: consumes an input event and writes side-effects to `output`.
    pub fn step(&mut self, input: Input, output: &mut Output) {
        match input {
            Input::Tick => self.handle_tick(output),
            Input::Recv { from, message } => self.handle_message(from, message, output),
            Input::Propose { payload } => self.handle_propose(payload, output),
            Input::ReadIndex { context } => self.handle_read_index(context, output),
            Input::Persisted { token } => self.handle_persisted(token, output),
            Input::Applied { index } => self.handle_applied(index),
            Input::TransferLeader { to } => self.handle_transfer_leader(to, output),
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

    // ------------------------------------------------------------------------
    // Tick Handling
    // ------------------------------------------------------------------------

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
                    if self.config.pre_vote_enabled {
                        self.start_pre_vote(output);
                    } else {
                        self.start_election(output);
                    }
                }
            }
            Role::Leader => {
                self.heartbeat_elapsed_ticks = self.heartbeat_elapsed_ticks.saturating_add(1);
                self.election_elapsed_ticks = self.election_elapsed_ticks.saturating_add(1);

                if self.heartbeat_elapsed_ticks >= self.config.heartbeat_interval_ticks {
                    self.heartbeat_elapsed_ticks = 0;
                    self.broadcast_heartbeat(output);
                }

                // CheckQuorum: leader steps down if quorum contact is lost
                if self.config.check_quorum
                    && self.election_elapsed_ticks >= self.config.election_timeout_ticks
                {
                    self.election_elapsed_ticks = 0;
                    // Cluster quorum requires majority including self
                    let acks_count = self.heartbeat_acks.len() + 1;
                    if acks_count < self.config.quorum_size() {
                        self.transition_to_follower(self.term, output);
                    }
                    self.heartbeat_acks.clear();
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    // PreVote Protocol
    // ------------------------------------------------------------------------

    fn start_pre_vote(&mut self, output: &mut Output) {
        if self.config.peers.is_empty() {
            // Single-node cluster: directly become leader
            self.start_election(output);
            return;
        }

        self.transition_role(Role::PreCandidate, output);
        self.pre_votes_granted.clear();
        self.pre_votes_granted.insert(self.config.node_id);

        let prospective_term = self.term.next().unwrap_or(self.term);
        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        for &peer in &self.config.peers {
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
        let grant = term >= self.term && is_log_ok;

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

        if vote_granted && term == self.term {
            self.pre_votes_granted.insert(from);
            if self.pre_votes_granted.len() >= self.config.quorum_size() {
                self.start_election(output);
            }
        }
    }

    // ------------------------------------------------------------------------
    // Election Protocol
    // ------------------------------------------------------------------------

    fn start_election(&mut self, output: &mut Output) {
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

        if self.config.peers.is_empty() {
            // Single node cluster: immediately become leader
            self.become_leader(output);
            return;
        }

        let last_log_index = self.log.last_index();
        let last_log_term = self.log.last_term();

        for &peer in &self.config.peers {
            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::RequestVoteRequest {
                    term: self.term,
                    candidate_id: self.config.node_id,
                    last_log_index,
                    last_log_term,
                },
            });
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
        if term > self.term {
            self.transition_to_follower(term, output);
        }

        let can_vote = self.voted_for.is_none() || self.voted_for == Some(candidate_id);
        let is_log_ok = self.is_log_up_to_date(last_log_index, last_log_term);
        let grant = term == self.term && can_vote && is_log_ok;

        if grant {
            self.voted_for = Some(candidate_id);
            self.election_elapsed_ticks = 0;

            let token = self.next_token();
            output.persist_ops.push(PersistOp::HardState {
                term: self.term,
                vote: self.voted_for,
                token,
            });

            // Persistence contract: release vote response only after HardState is synced
            let resp = OutboundMessage {
                to: from,
                message: Message::RequestVoteResponse {
                    term: self.term,
                    vote_granted: true,
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
            if self.votes_granted.len() >= self.config.quorum_size() {
                self.become_leader(output);
            }
        }
    }

    fn become_leader(&mut self, output: &mut Output) {
        self.transition_role(Role::Leader, output);
        self.lead_transferee = None;
        self.heartbeat_elapsed_ticks = 0;
        self.election_elapsed_ticks = 0;
        self.heartbeat_acks.clear();

        let next_idx = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        for p in self.progress.values_mut() {
            p.next_index = next_idx;
            p.match_index = LogIndex::from_raw(0);
            p.state = crate::progress::ProgressState::Probe;
            p.inflight_count = 0;
        }

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

        // Replicate to all peers immediately
        self.broadcast_append_entries(output);
    }

    // ------------------------------------------------------------------------
    // Client Proposal & Log Replication
    // ------------------------------------------------------------------------

    fn handle_propose(&mut self, payload: Bytes, output: &mut Output) {
        if self.role != Role::Leader || self.lead_transferee.is_some() {
            return;
        }

        let entry_index = self
            .log
            .last_index()
            .next()
            .unwrap_or(self.log.last_index());
        let entry = Entry::normal(entry_index, self.term, payload);
        self.log.append(entry.clone());

        let token = self.next_token();
        output.persist_ops.push(PersistOp::AppendEntries {
            prev_index: entry_index.prev().unwrap_or(LogIndex::from_raw(0)),
            entries: vec![entry],
            token,
        });

        // Parallel persist contract: count match for self only after disk completes
        self.queue_pending_action(token, PendingAction::LeaderPersisted { entry_index });

        self.broadcast_append_entries(output);
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
                entries.last().map_or(prev_index, |e| e.index)
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

        let peers: Vec<NodeId> = self.progress.keys().copied().collect();
        for peer in peers {
            self.send_append_entries(peer, output);
        }
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
        if term > self.term {
            self.transition_to_follower(term, output);
        }

        if term < self.term {
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    match_index: self.log.last_index(),
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            });
            return;
        }

        // Valid leader confirmed: reset election timer
        self.election_elapsed_ticks = 0;
        if self.role != Role::Follower {
            self.transition_to_follower(term, output);
        }

        // 1. Check if log contains an entry at prev_log_index with prev_log_term
        let local_prev_term = self.log.term_at(prev_log_index);
        let matches = match local_prev_term {
            Some(t) => t == prev_log_term,
            None => false,
        };

        if !matches {
            // Compute conflict hint to repair log in 1-RTT
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

            output.messages.push(OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: false,
                    match_index: self.log.last_index(),
                    conflict_term,
                    conflict_index,
                    from: self.config.node_id,
                },
            });
            return;
        }

        // 2. Append/truncate entries
        let mut new_match = prev_log_index;
        let mut entries_to_append = Vec::new();
        let mut truncate_from = None;

        for entry in entries {
            if let Some(existing_term) = self.log.term_at(entry.index) {
                if existing_term != entry.term {
                    // Divergence: truncate from this entry
                    truncate_from = Some(entry.index);
                    entries_to_append.push(entry);
                }
            } else {
                entries_to_append.push(entry);
            }
        }

        if let Some(trunc_idx) = truncate_from {
            self.log
                .truncate_and_append(trunc_idx, entries_to_append.clone());
            new_match = self.log.last_index();

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
                    match_index: new_match,
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            };
            self.queue_pending_action(token, PendingAction::SendMessage(resp));
        } else if !entries_to_append.is_empty() {
            let start_append_idx = entries_to_append[0].index;
            let prev_idx = start_append_idx.prev().unwrap_or(LogIndex::from_raw(0));
            for e in &entries_to_append {
                self.log.append(e.clone());
            }
            new_match = self.log.last_index();

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
                    match_index: new_match,
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            };
            self.queue_pending_action(token, PendingAction::SendMessage(resp));
        } else {
            // No new entries: immediate response (heartbeat)
            output.messages.push(OutboundMessage {
                to: from,
                message: Message::AppendEntriesResponse {
                    term: self.term,
                    success: true,
                    match_index: new_match,
                    conflict_term: None,
                    conflict_index: None,
                    from: self.config.node_id,
                },
            });
        }

        // 3. Advance follower commit index
        if leader_commit > self.commit_index {
            let target_commit = std::cmp::min(leader_commit, self.log.last_index());
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
            self.maybe_advance_commit(output);

            // Check if leader transfer is pending for this node
            if self.lead_transferee == Some(from) && match_index >= self.log.last_index() {
                output.messages.push(OutboundMessage {
                    to: from,
                    message: Message::TimeoutNow {
                        term: self.term,
                        leader_id: self.config.node_id,
                    },
                });
                self.lead_transferee = None;
            }
        } else {
            let last_idx_fn = |t| self.log.find_last_index_of_term(t);
            progress.on_rejection(conflict_term, conflict_index, last_idx_fn);
            // Trigger immediate retry with updated next_index
            self.broadcast_append_entries(output);
        }
    }

    // ------------------------------------------------------------------------
    // Commit Advancement (Raft Current-Term Commit Rule)
    // ------------------------------------------------------------------------

    fn maybe_advance_commit(&mut self, output: &mut Output) {
        if self.role != Role::Leader {
            return;
        }

        let mut matches: Vec<LogIndex> = self.progress.values().map(|p| p.match_index).collect();
        // Include leader's own persisted match index
        matches.push(self.leader_persisted_index);
        matches.sort_unstable();

        // Majority index in sorted array (cluster_size / 2 from top)
        let median_pos = matches.len() - self.config.quorum_size();
        let median_commit = matches[median_pos];

        if median_commit > self.commit_index {
            // Raft Section 5.4.2 rule: leader only commits entries from its CURRENT term
            if self.log.term_at(median_commit) == Some(self.term) {
                self.commit_to(median_commit, output);
            }
        }
    }

    fn commit_to(&mut self, new_commit: LogIndex, output: &mut Output) {
        let start_idx = self.commit_index.next().unwrap_or(self.commit_index);
        self.commit_index = new_commit;

        let raw_start = start_idx.as_u64();
        let raw_end = new_commit.as_u64();
        for idx in raw_start..=raw_end {
            if let Some(entry) = self.log.entry_at(LogIndex::from_raw(idx)) {
                output.committed_entries.push(entry.clone());
            }
        }
    }

    // ------------------------------------------------------------------------
    // Heartbeats & Linearizable ReadIndex
    // ------------------------------------------------------------------------

    fn broadcast_heartbeat(&mut self, output: &mut Output) {
        for &peer in &self.config.peers {
            output.messages.push(OutboundMessage {
                to: peer,
                message: Message::HeartbeatRequest {
                    term: self.term,
                    leader_id: self.config.node_id,
                    commit_index: self.commit_index,
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
        if term > self.term {
            self.transition_to_follower(term, output);
        }

        if term >= self.term {
            self.election_elapsed_ticks = 0;
            if self.role != Role::Follower {
                self.transition_to_follower(term, output);
            }
            if commit_index > self.commit_index {
                let target = std::cmp::min(commit_index, self.log.last_index());
                if target > self.commit_index && self.log.term_at(target) == Some(term) {
                    self.commit_to(target, output);
                }
            }
        }

        output.messages.push(OutboundMessage {
            to: from,
            message: Message::HeartbeatResponse {
                term: self.term,
                from: self.config.node_id,
                read_context,
            },
        });
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
            self.send_append_entries(from, output);
        }

        if let Some(ctx) = read_context {
            let mut satisfied_indices = Vec::new();
            for (i, pending) in self.pending_reads.iter_mut().enumerate() {
                if pending.context == ctx {
                    pending.acks.insert(from);
                    if pending.acks.len() >= self.config.quorum_size() {
                        output.completed_reads.push(ReadIndexResult {
                            context: pending.context.clone(),
                            read_index: pending.read_index,
                        });
                        satisfied_indices.push(i);
                    }
                }
            }
            for i in satisfied_indices.into_iter().rev() {
                self.pending_reads.swap_remove(i);
            }
        }
    }

    fn handle_read_index(&mut self, context: Bytes, output: &mut Output) {
        if self.role != Role::Leader {
            return;
        }

        if self.config.peers.is_empty() {
            // Single node cluster: immediately linearizable
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
        for &peer in &self.config.peers {
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

    // ------------------------------------------------------------------------
    // Leadership Transfer
    // ------------------------------------------------------------------------

    fn handle_transfer_leader(&mut self, to: NodeId, output: &mut Output) {
        if self.role != Role::Leader || to == self.config.node_id {
            return;
        }

        self.lead_transferee = Some(to);

        if let Some(prog) = self.progress.get(&to) {
            if prog.match_index >= self.log.last_index() {
                // Follower is already up to date: trigger TimeoutNow immediately
                output.messages.push(OutboundMessage {
                    to,
                    message: Message::TimeoutNow {
                        term: self.term,
                        leader_id: self.config.node_id,
                    },
                });
                self.lead_transferee = None;
            } else {
                // Catch up transferee log
                self.broadcast_append_entries(output);
            }
        }
    }

    fn handle_timeout_now(&mut self, term: Term, _leader_id: NodeId, output: &mut Output) {
        if term >= self.term {
            self.start_election(output);
        }
    }

    // ------------------------------------------------------------------------
    // Persistence Callbacks
    // ------------------------------------------------------------------------

    fn handle_persisted(&mut self, token: PersistToken, output: &mut Output) {
        if let Some(actions) = self.pending_persists.remove(&token) {
            for action in actions {
                match action {
                    PendingAction::SendMessage(msg) => {
                        output.messages.push(msg);
                    }
                    PendingAction::LeaderPersisted { entry_index } => {
                        if entry_index > self.leader_persisted_index {
                            self.leader_persisted_index = entry_index;
                            self.maybe_advance_commit(output);
                        }
                    }
                }
            }
        }
    }

    fn handle_applied(&mut self, index: LogIndex) {
        if index > self.applied_index {
            self.applied_index = index;
        }
    }

    // ------------------------------------------------------------------------
    // Message Dispatch
    // ------------------------------------------------------------------------

    fn handle_message(&mut self, from: NodeId, message: Message, output: &mut Output) {
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
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

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

    fn transition_to_follower(&mut self, term: Term, output: &mut Output) {
        self.transition_role(Role::Follower, output);
        self.term = term;
        self.voted_for = None;
        self.lead_transferee = None;
        self.pre_votes_granted.clear();
        self.votes_granted.clear();
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
        self.pending_persists.entry(token).or_default().push(action);
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

    /// Draws an unbiased randomized election timeout in `[base_ticks, 2 * base_ticks]`
    /// using Lemire's nearly divisionless algorithm.
    #[allow(clippy::cast_possible_truncation)]
    fn sample_random_timeout(base_ticks: u64, rng: &mut ChaCha8Rng) -> u64 {
        let low = base_ticks;
        let high = base_ticks.saturating_mul(2).max(low + 1);
        let span = high - low;
        if span <= 1 {
            return low;
        }

        let mut x = rng.next_u64();
        let mut m = u128::from(x) * u128::from(span);
        let mut l = m as u64;

        if l < span {
            let threshold = span.wrapping_neg() % span;
            while l < threshold {
                x = rng.next_u64();
                m = u128::from(x) * u128::from(span);
                l = m as u64;
            }
        }

        low + (m >> 64) as u64
    }
}
