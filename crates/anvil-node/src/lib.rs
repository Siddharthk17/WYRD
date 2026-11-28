#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation)]

//! `anvil-node`: integration glue owning `RaftCore` and the `KvStore`.
//!
//! The node is sans-IO over environment traits: callers drive ticks,
//! deliver messages, submit persistence operations to a `Device`, and feed
//! completions back. The simulator and the production server share this code.

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore, SnapshotPolicy};
use anvil_raft::{
    Input, Output, PersistOp, RaftConfig, RaftCore, ReadIndexResult, ReceivedSnapshot, Role,
    SnapshotChunkData,
};
use anvil_types::{ClusterId, Entry, LogIndex, NodeId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;

/// Read consistency levels for client reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadConsistency {
    /// Linearizable read through the leader heartbeat round.
    Linearizable,
    /// Linearizable read issued on a follower.
    FollowerLinearizable,
    /// Local lease read. Only safe under the configured drift bound.
    Lease,
    /// Explicitly non-linearizable stale read, excluded from checking.
    Stale,
}

/// Node behavior switches.
#[derive(Debug, Clone, Copy)]
pub struct NodeConfig {
    /// State machine limits.
    pub kv: KvConfig,
    /// Snapshot and compaction policy.
    pub snapshot_policy: SnapshotPolicy,
    /// Whether lease reads are permitted. Off by default.
    pub lease_enabled: bool,
    /// Lease duration in ticks. Must be shorter than the minimum election
    /// timeout minus the drift bound when enabled.
    pub lease_ticks: u64,
    /// Maximum clock drift bound in ticks for lease safety.
    pub drift_bound_ticks: u64,
    /// Canary switch for Week 4 validation: skip the heartbeat round and
    /// answer leader reads immediately. The checker must catch the stale
    /// read. Never enabled in production.
    pub skip_read_confirmation: bool,
    /// Bound on queued linearizable and follower reads waiting for quorum
    /// confirmation or apply. When full the oldest waiter is shed (its
    /// client times out and retries) instead of growing without bound.
    pub max_pending_reads: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            kv: KvConfig::default(),
            snapshot_policy: SnapshotPolicy::default(),
            lease_enabled: false,
            lease_ticks: 0,
            drift_bound_ticks: 0,
            skip_read_confirmation: false,
            max_pending_reads: 512,
        }
    }
}

/// A client write request. `RegisterSession` carries no session yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeWrite {
    RegisterSession,
    ExpireSessions {
        up_to_tick: u64,
    },
    Put {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        value: Bytes,
    },
    Delete {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
    },
    Cas {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        expected_version: LogIndex,
        value: Bytes,
    },
    Append {
        session: anvil_types::SessionId,
        seq: anvil_types::SeqNo,
        acked: anvil_types::SeqNo,
        key: Bytes,
        value: Bytes,
    },
}

/// A client read request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeRead {
    Get {
        key: Bytes,
    },
    List {
        key: Bytes,
    },
    Range {
        start: Bytes,
        end: Bytes,
        limit: usize,
    },
}

/// Client-visible response with the serving read index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientResponse {
    /// Response payload.
    pub response: KvResponse,
    /// Raft log index the read was served at.
    pub read_index: LogIndex,
}

/// Effects emitted by one node step for the environment to execute.
#[derive(Debug, Default)]
pub struct NodeOutput {
    /// Raft messages to the network.
    pub messages: Vec<anvil_raft::OutboundMessage>,
    /// Persistence operations for the storage engine.
    pub persist_ops: Vec<PersistOp>,
    /// Completed client writes in completion order.
    pub client_responses: Vec<ClientResponse>,
    /// Completed client reads in completion order.
    pub read_responses: Vec<ClientResponse>,
}

/// Pending linearizable read waiting for quorum confirmation or apply.
#[derive(Debug, Clone)]
struct PendingRead {
    read: NodeRead,
    consistency: ReadConsistency,
    read_index: Option<LogIndex>,
    context: Option<Bytes>,
}

/// The shared node: Raft core plus applied state machine.
#[allow(clippy::struct_field_names)]
pub struct Node {
    id: NodeId,
    cluster_id: ClusterId,
    raft: RaftCore,
    kv: KvStore,
    config: NodeConfig,
    raft_output: Output,
    pending_reads: Vec<PendingRead>,
    read_seq: u64,
    applied: LogIndex,
    last_leader_contact: u64,
    now_tick: u64,
    entries_since_snapshot: u64,
    bytes_since_snapshot: u64,
    last_snapshot_index: LogIndex,
    cluster_rejects: u64,
    expired_sessions_total: u64,
}

impl Node {
    /// Creates a node from recovered state and an isolated RNG stream.
    #[must_use]
    pub fn new(
        node_id: NodeId,
        raft_config: RaftConfig,
        recovered: anvil_raft::RecoveredState,
        node_config: NodeConfig,
        rng: ChaCha8Rng,
    ) -> Self {
        assert_eq!(
            node_id, raft_config.node_id,
            "node id must match the Raft config"
        );
        Self {
            id: node_id,
            cluster_id: raft_config.cluster_id,
            raft: RaftCore::new(raft_config, recovered, rng),
            kv: KvStore::new(node_config.kv),
            config: node_config,
            raft_output: Output::new(),
            pending_reads: Vec::new(),
            read_seq: 0,
            applied: LogIndex::from_raw(0),
            last_leader_contact: 0,
            now_tick: 0,
            entries_since_snapshot: 0,
            bytes_since_snapshot: 0,
            last_snapshot_index: LogIndex::from_raw(0),
            cluster_rejects: 0,
            expired_sessions_total: 0,
        }
    }

    /// Creates a founding node. Every founder runs this with the same voter
    /// set, so each writes the identical initial config entry at index 1.
    #[must_use]
    pub fn bootstrap(raft_config: RaftConfig, node_config: NodeConfig, rng: ChaCha8Rng) -> Self {
        let node_id = raft_config.node_id;
        let voters = raft_config.initial_voters();
        let learners: Vec<NodeId> = raft_config.learners.iter().copied().collect();
        let bootstrap = anvil_types::bootstrap_config_entry(&voters, &learners);
        let recovered = anvil_raft::RecoveredState {
            current_term: anvil_types::Term::from_raw(0),
            voted_for: None,
            commit_index: LogIndex::from_raw(0),
            entries: vec![bootstrap],
        };
        Self::new(node_id, raft_config, recovered, node_config, rng)
    }

    /// Creates a node with empty storage that waits to be added. It never
    /// campaigns and never votes until a snapshot or config entry configures
    /// it. Callers build the config with no peers and the local node as the
    /// only learner.
    #[must_use]
    pub fn joining(raft_config: RaftConfig, node_config: NodeConfig, rng: ChaCha8Rng) -> Self {
        assert!(
            raft_config.initial_voters().is_empty(),
            "a joining node starts with no voters"
        );
        let node_id = raft_config.node_id;
        Self::new(
            node_id,
            raft_config,
            anvil_raft::RecoveredState::default(),
            node_config,
            rng,
        )
    }

    /// Returns the cluster id.
    #[must_use]
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// Returns messages rejected for a foreign cluster id.
    #[must_use]
    pub const fn cluster_rejects(&self) -> u64 {
        self.cluster_rejects
    }

    /// Returns sessions expired by applied `ExpireSessions` entries.
    #[must_use]
    pub const fn expired_sessions_total(&self) -> u64 {
        self.expired_sessions_total
    }

    /// Returns the current effective voters.
    #[must_use]
    pub fn membership_voters(&self) -> Vec<NodeId> {
        self.raft.membership_voters()
    }

    /// Returns true while joint consensus is active.
    #[must_use]
    pub fn in_joint(&self) -> bool {
        self.raft.in_joint()
    }

    /// Returns the leader's match index for a replication target.
    #[must_use]
    pub fn replication_match(&self, peer: NodeId) -> Option<LogIndex> {
        self.raft
            .progress()
            .get(&peer)
            .map(|progress| progress.match_index)
    }

    /// Proposes a membership change to the cluster. Only the leader's call
    /// has an effect; refusal reasons surface in the core's membership
    /// events. Returns false when this node is not the leader.
    pub fn propose_config(
        &mut self,
        voters: Vec<NodeId>,
        learners: Vec<NodeId>,
        output: &mut NodeOutput,
    ) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        self.raft_output.clear();
        self.raft.step(
            Input::ProposeConfig { voters, learners },
            &mut self.raft_output,
        );
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
        true
    }

    /// Promotes a caught-up learner to voter. Returns false when this node
    /// is not the leader or the target is not a learner.
    pub fn propose_promote(&mut self, target: NodeId, output: &mut NodeOutput) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        let membership = self.raft.membership_snapshot();
        if !membership.learners.contains(&target) || membership.voters.contains(&target) {
            return false;
        }
        let mut voters = membership.voters.clone();
        voters.push(target);
        let learners: Vec<NodeId> = membership
            .learners
            .iter()
            .copied()
            .filter(|learner| *learner != target)
            .collect();
        self.propose_config(voters, learners, output)
    }

    /// Returns the log entry at an index, for bootstrap and debug flows.
    #[must_use]
    pub fn log_entry(&self, index: LogIndex) -> Option<Entry> {
        self.raft.log().entry_at(index).cloned()
    }

    /// Returns the node id.
    #[must_use]
    pub const fn node_id(&self) -> NodeId {
        self.id
    }

    /// Returns the Raft role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.raft.role()
    }

    /// Returns the commit index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.raft.commit_index()
    }

    /// Returns the applied index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied
    }

    /// Returns the rolling state hash.
    #[must_use]
    pub fn state_hash(&self) -> [u8; 32] {
        self.kv.state_hash()
    }

    /// Returns the term.
    #[must_use]
    pub fn term(&self) -> anvil_types::Term {
        self.raft.term()
    }

    /// Advances the logical clock.
    pub fn tick(&mut self, output: &mut NodeOutput) {
        self.now_tick = self.now_tick.saturating_add(1);
        self.raft_output.clear();
        self.raft.step(Input::Tick, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Delivers a Raft message. Messages carrying a foreign cluster id are
    /// dropped before reaching the core and counted for operators.
    pub fn receive(
        &mut self,
        from: NodeId,
        cluster_id: ClusterId,
        message: anvil_raft::Message,
        output: &mut NodeOutput,
    ) {
        if cluster_id != self.cluster_id {
            self.cluster_rejects = self.cluster_rejects.saturating_add(1);
            return;
        }
        self.last_leader_contact = self.now_tick;
        self.raft_output.clear();
        self.raft
            .step(Input::Recv { from, message }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Submits a client write to the leader. Returns false when not leader.
    pub fn client_write(&mut self, write: NodeWrite, output: &mut NodeOutput) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        let mut buf = Vec::new();
        let cmd = match write {
            NodeWrite::RegisterSession => KvCommand::RegisterSession,
            NodeWrite::ExpireSessions { up_to_tick } => KvCommand::ExpireSessions { up_to_tick },
            NodeWrite::Put {
                session,
                seq,
                acked,
                key,
                value,
            } => KvCommand::Put {
                session,
                seq,
                acked,
                key,
                value,
            },
            NodeWrite::Delete {
                session,
                seq,
                acked,
                key,
            } => KvCommand::Delete {
                session,
                seq,
                acked,
                key,
            },
            NodeWrite::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            } => KvCommand::Cas {
                session,
                seq,
                acked,
                key,
                expected_version,
                value,
            },
            NodeWrite::Append {
                session,
                seq,
                acked,
                key,
                value,
            } => KvCommand::Append {
                session,
                seq,
                acked,
                key,
                value,
            },
        };
        cmd.encode(&mut buf);
        self.raft_output.clear();
        self.raft.step(
            Input::Propose {
                payload: Bytes::from(buf),
            },
            &mut self.raft_output,
        );
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
        true
    }

    /// Serves a client read at the requested consistency level.
    pub fn client_read(
        &mut self,
        read: NodeRead,
        consistency: ReadConsistency,
        output: &mut NodeOutput,
    ) -> Option<ClientResponse> {
        match consistency {
            ReadConsistency::Stale => Some(ClientResponse {
                response: self.eval_read(&read),
                read_index: self.applied,
            }),
            ReadConsistency::Lease => self.read_lease(&read),
            ReadConsistency::Linearizable => self.read_leader(&read, output),
            ReadConsistency::FollowerLinearizable => {
                self.enqueue_pending_read(PendingRead {
                    read,
                    consistency,
                    read_index: Some(self.raft.commit_index()),
                    context: None,
                });
                self.serve_ready_reads(output);
                None
            }
        }
    }

    /// Returns the commit index a follower should wait for. The simulator
    /// supplies the leader's commit index; the server resolves it through
    /// the same call after contacting the leader.
    pub fn follower_read_target(&self) -> LogIndex {
        self.raft.commit_index()
    }

    /// Returns the number of reads currently waiting for confirmation.
    #[must_use]
    pub fn pending_read_count(&self) -> usize {
        self.pending_reads.len()
    }

    /// Notifies the storage layer that a persist operation completed.
    pub fn on_persisted(&mut self, token: anvil_raft::PersistToken, output: &mut NodeOutput) {
        self.raft_output.clear();
        self.raft
            .step(Input::Persisted { token }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
    }

    /// Proposes a virtual-time tick. Leaders call this periodically so time
    /// enters the log deterministically.
    pub fn propose_clock_tick(&mut self, tick: u64, output: &mut NodeOutput) -> bool {
        if self.raft.role() != Role::Leader {
            return false;
        }
        let mut buf = Vec::new();
        KvCommand::ClockTick { tick }.encode(&mut buf);
        self.raft_output.clear();
        self.raft.step(
            Input::Propose {
                payload: Bytes::from(buf),
            },
            &mut self.raft_output,
        );
        self.drain_raft_output(output);
        true
    }

    /// Directly applies a committed entry payload, for tests that bypass Raft.
    pub fn apply_entry_for_test(&mut self, index: LogIndex, payload: &[u8]) -> KvResponse {
        self.kv.apply(index, payload).unwrap_or(KvResponse::Ok)
    }

    /// Queues a pending read, shedding the oldest waiter when the
    /// bounded wait queue is full.
    fn enqueue_pending_read(&mut self, pending: PendingRead) {
        while self.pending_reads.len() >= self.config.max_pending_reads.max(1) {
            self.pending_reads.remove(0);
        }
        self.pending_reads.push(pending);
    }

    fn read_lease(&mut self, read: &NodeRead) -> Option<ClientResponse> {
        if !self.config.lease_enabled {
            return None;
        }
        if self.raft.role() != Role::Leader {
            return None;
        }
        let since_contact = self.now_tick.saturating_sub(self.last_leader_contact);
        if since_contact > self.config.lease_ticks {
            return None;
        }
        Some(ClientResponse {
            response: self.eval_read(read),
            read_index: self.applied,
        })
    }

    fn read_leader(&mut self, read: &NodeRead, output: &mut NodeOutput) -> Option<ClientResponse> {
        if self.raft.role() != Role::Leader {
            return None;
        }
        if self.config.skip_read_confirmation {
            return Some(ClientResponse {
                response: self.eval_read(read),
                read_index: self.applied,
            });
        }
        self.read_seq = self.read_seq.wrapping_add(1);
        let mut ctx = Vec::new();
        ctx.extend_from_slice(&self.read_seq.to_le_bytes());
        ctx.extend_from_slice(&self.now_tick.to_le_bytes());
        let context = Bytes::from(ctx);
        self.enqueue_pending_read(PendingRead {
            read: read.clone(),
            consistency: ReadConsistency::Linearizable,
            read_index: None,
            context: Some(context.clone()),
        });
        self.raft_output.clear();
        self.raft
            .step(Input::ReadIndex { context }, &mut self.raft_output);
        self.drain_raft_output(output);
        self.serve_ready_reads(output);
        None
    }

    fn eval_read(&self, read: &NodeRead) -> KvResponse {
        match read {
            NodeRead::Get { key } => self.kv.read_register(key),
            NodeRead::List { key } => self.kv.read_list(key),
            NodeRead::Range { start, end, limit } => self.kv.read_range(start, end, *limit),
        }
    }

    fn serve_ready_reads(&mut self, output: &mut NodeOutput) {
        let mut i = 0;
        while i < self.pending_reads.len() {
            let ready = match self.pending_reads[i].consistency {
                ReadConsistency::Linearizable => false,
                ReadConsistency::FollowerLinearizable => {
                    let target = self.pending_reads[i].read_index.unwrap_or(self.applied);
                    self.applied >= target
                }
                ReadConsistency::Lease | ReadConsistency::Stale => true,
            };
            if ready {
                let pending = self.pending_reads.remove(i);
                let response = self.eval_read(&pending.read);
                output.read_responses.push(ClientResponse {
                    response,
                    read_index: self.applied,
                });
            } else {
                i += 1;
            }
        }
    }

    fn drain_raft_output(&mut self, output: &mut NodeOutput) {
        output.messages.append(&mut self.raft_output.messages);
        output.persist_ops.append(&mut self.raft_output.persist_ops);
        let committed = std::mem::take(&mut self.raft_output.committed_entries);
        for entry in committed {
            self.entries_since_snapshot = self.entries_since_snapshot.saturating_add(1);
            self.bytes_since_snapshot = self
                .bytes_since_snapshot
                .saturating_add(entry.payload.len() as u64);
            if entry.kind == anvil_types::EntryKind::NoOp || entry.kind.is_config_change() {
                // No-ops and config entries carry no state-machine effect:
                // they advance applied and acknowledge the core without
                // touching the store or answering a client.
                self.applied = entry.index;
                let mut nested = Output::new();
                self.raft
                    .step(Input::Applied { index: entry.index }, &mut nested);
                output.messages.extend(nested.messages);
                output.persist_ops.extend(nested.persist_ops);
                continue;
            }
            let index = entry.index;
            let response = self
                .kv
                .apply(index, &entry.payload)
                .unwrap_or(KvResponse::Ok);
            if let KvResponse::SessionsExpired { removed } = response {
                self.expired_sessions_total = self.expired_sessions_total.saturating_add(removed);
            }
            self.applied = index;
            output.client_responses.push(ClientResponse {
                response,
                read_index: index,
            });
            let mut nested = Output::new();
            self.raft.step(Input::Applied { index }, &mut nested);
            output.messages.extend(nested.messages);
            output.persist_ops.extend(nested.persist_ops);
        }
        let completed = std::mem::take(&mut self.raft_output.completed_reads);
        for item in &completed {
            self.serve_confirmed_read(item, output);
        }
        self.process_snapshot_effects(output);
        self.raft_output.clear();
    }

    /// Takes snapshots on policy triggers or core requests, and installs
    /// verified snapshots the core reassembled. Bounded per drain: each
    /// supply satisfies a need, so the loop terminates.
    fn process_snapshot_effects(&mut self, output: &mut NodeOutput) {
        if self
            .config
            .snapshot_policy
            .should_snapshot(self.entries_since_snapshot, self.bytes_since_snapshot)
        {
            self.take_snapshot(output);
        }
        for _ in 0..8 {
            let requests = std::mem::take(&mut self.raft_output.snapshot_requests);
            let ready = std::mem::take(&mut self.raft_output.ready_snapshots);
            if requests.is_empty() && ready.is_empty() {
                break;
            }
            for snap in &ready {
                self.install_snapshot(snap, output);
            }
            for request in &requests {
                self.supply_snapshot(request.least_index, output);
            }
        }
    }

    /// Encodes the state machine at the applied index, hands the chunks to
    /// the core for streaming, and compacts the log prefix that the durable
    /// snapshot now covers. Never snapshots beyond the applied index: the
    /// handle is taken at `self.applied` by construction. The snapshot
    /// carries the live membership so installers adopt the same config.
    fn take_snapshot(&mut self, output: &mut NodeOutput) {
        let handle = self.kv.snapshot_handle();
        let applied = self.applied;
        let Some(term) = self.raft.log().term_at(applied) else {
            return;
        };
        let membership = self.raft.membership_snapshot();
        let (chunks, digest) =
            handle.encode_snapshot_chunked(self.config.snapshot_policy.chunk_bytes, &membership);
        let mut total: u64 = 0;
        let mut data = Vec::with_capacity(chunks.len());
        for chunk in &chunks {
            total = total.saturating_add(chunk.payload.len() as u64);
            data.push(SnapshotChunkData {
                chunk: chunk.payload.clone(),
                chunk_crc: chunk.crc,
            });
        }
        self.raft_output.clear();
        self.raft.step(
            Input::SnapshotReady {
                last_included_index: applied,
                last_included_term: term,
                snapshot_bytes: total,
                snapshot_digest: digest,
                chunks: data,
            },
            &mut self.raft_output,
        );
        output.messages.append(&mut self.raft_output.messages);
        output.persist_ops.append(&mut self.raft_output.persist_ops);
        self.entries_since_snapshot = 0;
        self.bytes_since_snapshot = 0;
        self.last_snapshot_index = applied;
        if let Some(up_to) = self
            .config
            .snapshot_policy
            .compaction_index(applied, applied, true)
        {
            self.raft_output.clear();
            self.raft
                .step(Input::CompactLog { up_to }, &mut self.raft_output);
            output.messages.append(&mut self.raft_output.messages);
            output.persist_ops.append(&mut self.raft_output.persist_ops);
        }
        self.serve_ready_reads(output);
    }

    /// Supplies a snapshot for a lagging peer when the core has no usable
    /// bytes buffered. The least index is always reachable: a need implies
    /// the peer is behind the compacted base, and the base never passes the
    /// applied index, so a snapshot at applied covers it.
    fn supply_snapshot(&mut self, least_index: LogIndex, output: &mut NodeOutput) {
        assert!(
            self.applied.as_u64() + 1 >= least_index.as_u64(),
            "snapshot need beyond applied state"
        );
        self.take_snapshot(output);
    }

    /// Atomically installs a core-verified snapshot: the state machine is
    /// replaced, applied jumps, and the core is told so its bound checks
    /// stay exact. Stale snapshots at or below applied never regress state.
    /// The embedded membership restores the core config, which lets a fresh
    /// joiner adopt the cluster through a snapshot.
    fn install_snapshot(&mut self, snap: &ReceivedSnapshot, output: &mut NodeOutput) {
        if snap.last_included_index <= self.applied {
            return;
        }
        if anvil_kv::snapshot_digest(&snap.data) != snap.snapshot_digest {
            return;
        }
        let Ok(restored) = self.kv_snapshot_decode(&snap.data) else {
            return;
        };
        if restored.applied_index() != snap.last_included_index {
            return;
        }
        let Ok(membership) = anvil_kv::KvStore::decode_membership(self.config.kv, &snap.data)
        else {
            return;
        };
        self.kv = restored;
        self.applied = snap.last_included_index;
        self.entries_since_snapshot = 0;
        self.bytes_since_snapshot = 0;
        self.last_snapshot_index = snap.last_included_index;
        self.raft_output.clear();
        self.raft.step(
            Input::Applied {
                index: snap.last_included_index,
            },
            &mut self.raft_output,
        );
        self.raft_output.clear();
        self.raft.step(
            Input::RestoreMembership {
                base: snap.last_included_index,
                membership,
            },
            &mut self.raft_output,
        );
        self.serve_ready_reads(output);
    }

    /// Decodes install bytes against this node's state-machine limits.
    fn kv_snapshot_decode(&self, data: &[u8]) -> Result<KvStore, anvil_kv::KvError> {
        KvStore::decode_snapshot(self.config.kv, data)
    }

    fn serve_confirmed_read(&mut self, completed: &ReadIndexResult, output: &mut NodeOutput) {
        let mut i = 0;
        while i < self.pending_reads.len() {
            let matches = self.pending_reads[i].context.as_ref() == Some(&completed.context);
            if matches {
                let pending = self.pending_reads.remove(i);
                if self.applied < completed.read_index {
                    self.pending_reads.push(PendingRead {
                        read_index: Some(completed.read_index),
                        ..pending
                    });
                    i += 1;
                    continue;
                }
                let response = self.eval_read(&pending.read);
                output.read_responses.push(ClientResponse {
                    response,
                    read_index: completed.read_index,
                });
            } else {
                i += 1;
            }
        }
    }
}
