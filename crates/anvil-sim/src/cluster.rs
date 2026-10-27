#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines, clippy::similar_names)]

//! In-memory multi-node `SimCluster` test harness for Raft consensus with real storage engine.
//!
//! Models 3, 5, and 7-node clusters with:
//! - Full storage engine integration (`StorageEngine` + `SimDevice`) with crash recovery
//! - In-memory network message routing with drop, duplicate, and delay hooks
//! - Process crashes and restart recovery with invariant verification
//! - Per-step online invariant verification across all nodes

use crate::device::{SimDevice, SimDeviceConfig};
use crate::raft_invariants::RaftInvariantChecker;
use crate::rng::{DetRng, derive_component_seed};
use anvil_raft::{Input, Message, Output, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_storage::{
    Device, DeviceOp, EngineInput, EngineOutput, FileId, HardStateSlot, LogRecoveryClassification,
    RecoveredStorageState, SectorSize, StorageConfig, StorageEngine, StorageError, WalRecordFrame,
    WalSegmentHeader,
};
use anvil_types::{LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};

/// Hook to drop selected packets `(from, to, message) -> bool`.
pub type NetworkFilterHook = Box<dyn Fn(NodeId, NodeId, &Message) -> bool>;

/// Hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
pub type NetworkDelayHook = Box<dyn Fn(NodeId, NodeId, &Message) -> u64>;

/// In-flight network packet queued for discrete logical time delivery.
#[derive(Debug, Clone)]
pub struct InFlightPacket {
    /// Originating sender node ID.
    pub from: NodeId,
    /// Destination recipient node ID.
    pub to: NodeId,
    /// Consensus message payload.
    pub message: Message,
    /// Logical tick at which packet becomes deliverable.
    pub deliver_at_tick: u64,
}

/// Simulated storage layer for a single Raft node, holding its real `StorageEngine` and `SimDevice`.
pub struct SimNodeStorage {
    /// Simulated disk device with fault injection and volatile page cache.
    pub device: SimDevice,
    /// Storage engine running natural batching group commit and WAL segment management.
    pub engine: StorageEngine,
    /// Reusable engine output scratch buffer to avoid allocations.
    pub engine_output: EngineOutput,
    /// Whether this node is currently crashed.
    pub crashed: bool,
    /// Whether this node is marked `NeedsRepair` due to mid-log corruption.
    pub needs_repair: bool,
    /// Whether this node refused to start due to unrecoverable hard-state corruption.
    pub refused_start: bool,
    /// Record of all log entries whose persistence has been acknowledged.
    pub acknowledged_entries: Vec<(LogIndex, Term)>,
    /// Record of all votes persisted to disk per term.
    pub persisted_votes: BTreeMap<Term, Option<NodeId>>,
    /// Active storage configuration.
    pub config: StorageConfig,
}

/// Simulated Raft cluster harness for deterministic consensus testing.
pub struct SimCluster {
    /// Active Raft consensus cores indexed by node ID.
    pub nodes: BTreeMap<NodeId, RaftCore>,
    /// Node configurations indexed by `NodeId`.
    pub configs: BTreeMap<NodeId, RaftConfig>,
    /// Per-node persistent storage layer with real `StorageEngine` and `SimDevice`.
    pub storage: BTreeMap<NodeId, SimNodeStorage>,
    /// Master simulation seed.
    pub master_seed: u64,
    /// Global cluster logical tick counter.
    pub current_tick: u64,
    /// In-flight message queue.
    pub in_flight: Vec<InFlightPacket>,
    /// Network partition pairs `(from, to)` where traffic is unconditionally dropped.
    pub partitions: BTreeSet<(NodeId, NodeId)>,
    /// Global online invariant checker called after every node transition.
    pub checker: RaftInvariantChecker,
    /// Scratch output buffer reused across steps to eliminate allocations.
    output_scratch: Output,
    /// Optional hook to drop selected packets `(from, to, message) -> bool`.
    pub drop_hook: Option<NetworkFilterHook>,
    /// Optional hook to duplicate packets `(from, to, message) -> bool`.
    pub duplicate_hook: Option<NetworkFilterHook>,
    /// Optional hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
    pub delay_hook: Option<NetworkDelayHook>,
}

impl SimCluster {
    /// Constructs a new `SimCluster` with `size` nodes (typically 3, 5, or 7) seeded from `master_seed`.
    #[must_use]
    pub fn new(size: usize, master_seed: u64) -> Self {
        assert!(
            (1..=9).contains(&size),
            "cluster size must be between 1 and 9 nodes"
        );

        let mut node_ids = Vec::with_capacity(size);
        for i in 1..=size {
            let id_raw = u64::try_from(i).unwrap_or(1);
            if let Some(id) = NodeId::new(id_raw) {
                node_ids.push(id);
            }
        }

        let mut nodes = BTreeMap::new();
        let mut configs = BTreeMap::new();
        let mut storage = BTreeMap::new();

        for &id in &node_ids {
            let mut peers = BTreeSet::new();
            for &peer_id in &node_ids {
                if peer_id != id {
                    peers.insert(peer_id);
                }
            }

            let config = RaftConfig {
                node_id: id,
                peers,
                election_timeout_ticks: 10,
                heartbeat_interval_ticks: 3,
                max_inflight_messages: 256,
                max_bytes_per_message: 64 * 1024,
                pre_vote_enabled: true,
                check_quorum: true,
            };

            let path = format!("cluster/node/{}", id.get());
            let seed = derive_component_seed(master_seed, &path);
            let rng = ChaCha8Rng::from_seed(seed);

            let core = RaftCore::new(config.clone(), RecoveredState::default(), rng);
            nodes.insert(id, core);
            configs.insert(id, config);

            // Initialize storage layer with real SimDevice and StorageEngine
            let storage_config = StorageConfig::for_test(SectorSize::Sector512, 64 * 1024);
            let dev_config = SimDeviceConfig::for_test(SectorSize::Sector512);
            let dev_rng = DetRng::new(master_seed, &format!("cluster/storage/{}", id.get()));
            let mut sim_dev = SimDevice::new(dev_config, dev_rng);
            init_node_storage(&mut sim_dev, &storage_config);
            let engine = StorageEngine::new_empty(storage_config.clone());

            storage.insert(
                id,
                SimNodeStorage {
                    device: sim_dev,
                    engine,
                    engine_output: EngineOutput::default(),
                    crashed: false,
                    needs_repair: false,
                    refused_start: false,
                    acknowledged_entries: Vec::new(),
                    persisted_votes: BTreeMap::new(),
                    config: storage_config,
                },
            );
        }

        Self {
            nodes,
            configs,
            storage,
            master_seed,
            current_tick: 0,
            in_flight: Vec::new(),
            partitions: BTreeSet::new(),
            checker: RaftInvariantChecker::new(),
            output_scratch: Output::new(),
            drop_hook: None,
            duplicate_hook: None,
            delay_hook: None,
        }
    }

    /// Advances the cluster by one logical tick.
    ///
    /// 1. Steps `Input::Tick` on all live nodes.
    /// 2. Delivers packets scheduled for `current_tick`.
    /// 3. Evaluates online invariants across all nodes.
    pub fn tick(&mut self) {
        self.current_tick = self.current_tick.saturating_add(1);

        // 1. Tick every live node
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in node_ids {
            if !self.is_node_crashed(id) {
                self.step_node_internal(id, Input::Tick);
            }
        }

        // 2. Deliver all in-flight packets scheduled for now or earlier
        self.deliver_due_messages();
    }

    /// Advances the cluster by `n` ticks.
    pub fn tick_n(&mut self, n: usize) {
        for _ in 0..n {
            self.tick();
        }
    }

    /// Steps a specific node with an `Input` event, processes all emitted side-effects,
    /// and immediately validates online invariants.
    pub fn step_node(&mut self, id: NodeId, input: Input) {
        self.step_node_internal(id, input);
    }

    /// Returns whether the specified node is currently crashed.
    #[must_use]
    pub fn is_node_crashed(&self, id: NodeId) -> bool {
        self.storage.get(&id).is_some_and(|s| s.crashed)
    }

    /// Returns whether the specified node is marked `NeedsRepair` (mid-log corruption).
    #[must_use]
    pub fn is_node_needs_repair(&self, id: NodeId) -> bool {
        self.storage.get(&id).is_some_and(|s| s.needs_repair)
    }

    /// Returns whether the node refused to start (both hard state slots corrupt).
    #[must_use]
    pub fn is_node_refused_start(&self, id: NodeId) -> bool {
        self.storage.get(&id).is_some_and(|s| s.refused_start)
    }

    /// Synchronizes pre-existing entries in `RaftCore` into `StorageEngine` and `SimDevice`
    /// if the node's core was replaced or pre-populated in a test scenario.
    pub fn sync_node_storage_with_core(&mut self, id: NodeId) {
        if let (Some(core), Some(storage)) = (self.nodes.get(&id), self.storage.get_mut(&id)) {
            let core_last = core.last_log_index();
            let engine_last = storage.engine.last_entry_index();
            if core_last > engine_last {
                let mut entries_to_sync = Vec::new();
                for idx in (engine_last.as_u64() + 1)..=core_last.as_u64() {
                    if let Some(entry) = core.log().entry_at(LogIndex::from_raw(idx)) {
                        entries_to_sync.push(entry.clone());
                    }
                }
                if !entries_to_sync.is_empty() {
                    storage.engine_output.clear();
                    storage.engine.step(
                        EngineInput::AppendEntries {
                            prev_index: engine_last,
                            entries: entries_to_sync,
                            token: 0,
                        },
                        &mut storage.engine_output,
                    );
                    let ops = std::mem::take(&mut storage.engine_output.device_ops);
                    for op in ops {
                        storage.device.submit(op);
                    }
                    while let Some(comp) = storage.device.poll_completion() {
                        storage
                            .engine
                            .step(EngineInput::Completion(comp), &mut storage.engine_output);
                    }
                    storage.engine_output.clear();
                    storage.acknowledged_entries = core
                        .log()
                        .entries()
                        .iter()
                        .map(|e| (e.index, e.term))
                        .collect();
                    if let Some(v) = core.voted_for() {
                        storage.persisted_votes.insert(core.term(), Some(v));
                    }
                }
            }
        }
    }

    fn step_node_internal(&mut self, id: NodeId, input: Input) {
        if self.is_node_crashed(id)
            || self.is_node_refused_start(id)
            || self.is_node_needs_repair(id)
        {
            return;
        }

        self.sync_node_storage_with_core(id);
        self.output_scratch.clear();

        // Step the core
        if let Some(core) = self.nodes.get_mut(&id) {
            core.step(input, &mut self.output_scratch);
        }

        // Enqueue outbound network messages into simulated network
        let outbound = std::mem::take(&mut self.output_scratch.messages);
        for msg in outbound {
            self.route_message(id, msg.to, msg.message);
        }

        // Check invariants immediately after step
        self.checker.check_invariants(&self.nodes);

        // Process persistence operations through real StorageEngine and SimDevice
        let mut completed_tokens = Vec::new();
        let mut should_crash = false;

        if let Some(storage) = self.storage.get_mut(&id) {
            if !self.output_scratch.persist_ops.is_empty() {
                let persist_ops = std::mem::take(&mut self.output_scratch.persist_ops);
                storage.engine_output.clear();

                for op in persist_ops {
                    match op {
                        anvil_raft::PersistOp::HardState { term, vote, token } => {
                            storage.engine.step(
                                EngineInput::WriteHardState {
                                    term,
                                    vote,
                                    token: token.0,
                                },
                                &mut storage.engine_output,
                            );
                        }
                        anvil_raft::PersistOp::AppendEntries {
                            prev_index,
                            entries,
                            token,
                        } => {
                            storage.engine.step(
                                EngineInput::AppendEntries {
                                    prev_index,
                                    entries,
                                    token: token.0,
                                },
                                &mut storage.engine_output,
                            );
                        }
                        anvil_raft::PersistOp::TruncateAndAppend {
                            from_index,
                            entries,
                            token,
                        } => {
                            storage.engine.step(
                                EngineInput::TruncateAndAppend {
                                    from_index,
                                    entries,
                                    token: token.0,
                                },
                                &mut storage.engine_output,
                            );
                        }
                    }
                }

                // Submit emitted device operations to SimDevice
                let device_ops = std::mem::take(&mut storage.engine_output.device_ops);
                for dev_op in device_ops {
                    storage.device.submit(dev_op);
                }

                // Drain completions from SimDevice
                while let Some(comp) = storage.device.poll_completion() {
                    storage
                        .engine
                        .step(EngineInput::Completion(comp), &mut storage.engine_output);
                }

                // If fatal error occurred in engine (e.g. fsync failure injected), mark for crash
                if storage.engine.fatal_error().is_some() {
                    should_crash = true;
                } else {
                    completed_tokens = std::mem::take(&mut storage.engine_output.persisted_tokens);
                }
            }
        } else {
            // Fallback for standalone nodes without storage configuration
            let persist_tokens: Vec<_> = self
                .output_scratch
                .persist_ops
                .iter()
                .map(anvil_raft::PersistOp::token)
                .collect();
            for token in persist_tokens {
                self.output_scratch.clear();
                if let Some(core) = self.nodes.get_mut(&id) {
                    core.step(Input::Persisted { token }, &mut self.output_scratch);
                }
                let follow_up = std::mem::take(&mut self.output_scratch.messages);
                for msg in follow_up {
                    self.route_message(id, msg.to, msg.message);
                }
                self.checker.check_invariants(&self.nodes);
            }
        }

        if should_crash {
            self.crash_node(id);
            return;
        }

        if !completed_tokens.is_empty() {
            let mut all_follow_ups = Vec::new();
            let mut latest_log_and_vote = None;

            for token_val in completed_tokens {
                let token = anvil_raft::PersistToken(token_val);
                self.output_scratch.clear();
                if let Some(core) = self.nodes.get_mut(&id) {
                    core.step(Input::Persisted { token }, &mut self.output_scratch);
                    latest_log_and_vote = Some((
                        core.log()
                            .entries()
                            .iter()
                            .map(|e| (e.index, e.term))
                            .collect::<Vec<_>>(),
                        core.term(),
                        core.voted_for(),
                    ));
                }
                all_follow_ups.append(&mut self.output_scratch.messages);
            }

            if let (Some((entries, term, vote)), Some(storage)) =
                (latest_log_and_vote, self.storage.get_mut(&id))
            {
                storage.acknowledged_entries = entries;
                if let Some(cand) = vote {
                    storage.persisted_votes.insert(term, Some(cand));
                }
            }

            for msg in all_follow_ups {
                self.route_message(id, msg.to, msg.message);
            }
            self.checker.check_invariants(&self.nodes);
        }
    }

    /// Crashes a specific node: invokes `SimDevice::crash()`, removes volatile `RaftCore`,
    /// and drops all in-flight network messages to/from this node.
    pub fn crash_node(&mut self, id: NodeId) {
        if let Some(storage) = self.storage.get_mut(&id) {
            storage.crashed = true;
            storage.device.crash();
        }
        self.nodes.remove(&id);
        self.in_flight.retain(|pkt| pkt.to != id && pkt.from != id);
    }

    /// Restarts a crashed node: runs `scan_recovery` against the node's `SimDevice`,
    /// verifies invariants (`VoteNeverForgotten`, `CommittedNeverLostAcrossCrashes`,
    /// and `RecoveredLogEqualsAcknowledgedPlusSurvived`), and recovers `RaftCore` if clean or torn tail.
    ///
    /// # Errors
    /// Returns `StorageError` if hard-state recovery fails (both slots corrupted).
    pub fn restart_node(&mut self, id: NodeId) -> Result<RecoveredStorageState, StorageError> {
        let storage = self.storage.get_mut(&id).ok_or_else(|| {
            StorageError::DeviceError(format!("node {id} not found in cluster storage"))
        })?;

        let sector_size = storage.config.sector_size;
        let hard_state_bytes = storage
            .device
            .get_file_durable_bytes(FileId::HARD_STATE)
            .unwrap_or(&[]);

        // Collect all WAL segments present in durable storage
        let mut segments = Vec::new();
        let mut seg_id = 1u64;
        loop {
            let file_id = FileId::wal_segment(seg_id);
            let Some(seg_bytes) = storage.device.get_file_durable_bytes(file_id) else {
                break;
            };
            if seg_bytes.len() < sector_size.as_usize() {
                break;
            }
            let Ok(hdr) =
                WalSegmentHeader::decode(&seg_bytes[..sector_size.as_usize()], sector_size)
            else {
                break;
            };
            segments.push((hdr, seg_bytes));
            seg_id += 1;
        }

        let recovery_res = StorageEngine::scan_recovery(hard_state_bytes, &segments, sector_size);

        let recovered = match recovery_res {
            Ok(rec) => rec,
            Err(e) => {
                // Both slots corrupted: refuse to start under ADR 0012
                storage.refused_start = true;
                storage.crashed = false;
                return Err(e);
            }
        };

        match &recovered.log.classification {
            LogRecoveryClassification::MidLogCorruption { .. } => {
                // Mid-log corruption: marked NeedsRepair, does not vote or lead
                storage.needs_repair = true;
                storage.crashed = false;
                Ok(recovered)
            }
            LogRecoveryClassification::Clean | LogRecoveryClassification::TornTail { .. } => {
                // Invariant 1: VoteNeverForgotten
                let vote_res = self.checker.check_vote_never_forgotten(
                    id,
                    recovered.current_term,
                    recovered.voted_for,
                );
                assert!(
                    vote_res.is_ok(),
                    "VoteNeverForgotten invariant failed: {:?}",
                    vote_res.err()
                );

                if let Some(&Some(expected_vote)) =
                    storage.persisted_votes.get(&recovered.current_term)
                {
                    assert_eq!(
                        recovered.voted_for,
                        Some(expected_vote),
                        "VoteNeverForgotten: recovered vote for term {} ({:?}) != expected ({:?})",
                        recovered.current_term,
                        recovered.voted_for,
                        expected_vote
                    );
                }

                // Invariant 2: Recovered log contains all acknowledged entries
                for &(ack_idx, ack_term) in &storage.acknowledged_entries {
                    let entry_opt = recovered.log.entries.iter().find(|e| e.index == ack_idx);
                    assert!(
                        entry_opt.is_some_and(|e| e.term == ack_term),
                        "RecoveredLogMissingAcknowledgedEntry: node {id} missing acknowledged entry ({ack_idx}, {ack_term})"
                    );
                }

                // Invariant 3: CommittedNeverLostAcrossCrashes
                let commit_res = self
                    .checker
                    .check_recovered_log_against_oracle(id, &recovered.log.entries);
                assert!(
                    commit_res.is_ok(),
                    "CommittedNeverLostAcrossCrashes invariant failed: {:?}",
                    commit_res.err()
                );

                // Re-initialize RaftCore
                let path = format!("cluster/node/{}", id.get());
                let seed = derive_component_seed(self.master_seed, &path);
                let rng = ChaCha8Rng::from_seed(seed);

                let config = self
                    .configs
                    .get(&id)
                    .cloned()
                    .unwrap_or_else(|| RaftConfig {
                        node_id: id,
                        peers: self.configs.keys().copied().filter(|&p| p != id).collect(),
                        election_timeout_ticks: 10,
                        heartbeat_interval_ticks: 3,
                        max_inflight_messages: 256,
                        max_bytes_per_message: 64 * 1024,
                        pre_vote_enabled: true,
                        check_quorum: true,
                    });

                let recovered_state = RecoveredState {
                    current_term: recovered.current_term,
                    voted_for: recovered.voted_for,
                    commit_index: LogIndex::from_raw(0),
                    entries: recovered.log.entries.clone(),
                };
                let core = RaftCore::new(config, recovered_state, rng);
                self.nodes.insert(id, core);

                // Re-initialize StorageEngine from recovered state
                let active_seg_id = if segments.is_empty() {
                    1
                } else {
                    segments.len() as u64
                };
                let (write_offset, last_crc) = if let Some((last_hdr, _)) = segments.last() {
                    let mut cur_offset = sector_size.as_u64();
                    let mut cur_crc = last_hdr.header_crc;
                    for e in &recovered.log.entries {
                        let frame =
                            WalRecordFrame::from_entry(e, cur_crc, active_seg_id, cur_offset);
                        cur_crc = frame.crc32c;
                        cur_offset += frame.frame_size(sector_size) as u64;
                    }
                    (cur_offset, cur_crc)
                } else {
                    (sector_size.as_u64(), 0)
                };

                storage.engine = StorageEngine::new(
                    storage.config.clone(),
                    &recovered,
                    active_seg_id,
                    write_offset,
                    last_crc,
                );
                storage.crashed = false;
                storage.refused_start = false;
                storage.needs_repair = false;

                self.checker.check_invariants(&self.nodes);

                Ok(recovered)
            }
        }
    }

    /// Routes an outbound message through network hooks, partitions, and delay scheduling.
    fn route_message(&mut self, from: NodeId, to: NodeId, message: Message) {
        if self.is_node_crashed(from) || self.is_node_crashed(to) {
            return;
        }

        // 1. Partition filter
        if self.partitions.contains(&(from, to)) || self.partitions.contains(&(to, from)) {
            return;
        }

        // 2. Drop hook
        if self
            .drop_hook
            .as_ref()
            .is_some_and(|h| h(from, to, &message))
        {
            return;
        }

        // 3. Delay calculation
        let delay = self
            .delay_hook
            .as_ref()
            .map_or(0, |h| h(from, to, &message));
        let deliver_at = self.current_tick.saturating_add(delay);

        // 4. Duplicate hook
        let duplicate = self
            .duplicate_hook
            .as_ref()
            .is_some_and(|h| h(from, to, &message));

        self.in_flight.push(InFlightPacket {
            from,
            to,
            message: message.clone(),
            deliver_at_tick: deliver_at,
        });

        if duplicate {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at.saturating_add(1),
            });
        }
    }

    /// Delivers all messages whose `deliver_at_tick <= current_tick`.
    pub fn deliver_due_messages(&mut self) {
        let mut loop_count = 0usize;
        loop {
            loop_count += 1;
            assert!(
                loop_count < 10_000,
                "message storm detected: delivered over 10,000 messages in one tick"
            );

            let mut due_indices = Vec::new();
            for (i, pkt) in self.in_flight.iter().enumerate() {
                if pkt.deliver_at_tick <= self.current_tick {
                    due_indices.push(i);
                }
            }

            if due_indices.is_empty() {
                break;
            }

            // Extract due packets in FIFO order
            let mut packets_to_deliver = Vec::new();
            for &idx in due_indices.iter().rev() {
                packets_to_deliver.push(self.in_flight.swap_remove(idx));
            }
            packets_to_deliver.reverse();

            for pkt in packets_to_deliver {
                if self.is_node_crashed(pkt.to) {
                    continue;
                }
                // Drop if partition was created in the interim
                if self.partitions.contains(&(pkt.from, pkt.to))
                    || self.partitions.contains(&(pkt.to, pkt.from))
                {
                    continue;
                }
                self.step_node_internal(
                    pkt.to,
                    Input::Recv {
                        from: pkt.from,
                        message: pkt.message,
                    },
                );
            }
        }
    }

    /// Delivers the in-flight packet at `index % in_flight.len()`, removing it from the queue.
    /// Returns `true` if a packet was delivered, `false` if `in_flight` was empty or partition applied.
    pub fn deliver_packet_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() {
            return false;
        }
        let actual_idx = index % self.in_flight.len();
        let pkt = self.in_flight.remove(actual_idx);
        if self.is_node_crashed(pkt.to) {
            return false;
        }
        if self.partitions.contains(&(pkt.from, pkt.to))
            || self.partitions.contains(&(pkt.to, pkt.from))
        {
            return false;
        }
        self.step_node_internal(
            pkt.to,
            Input::Recv {
                from: pkt.from,
                message: pkt.message,
            },
        );
        true
    }

    /// Drops the in-flight packet at `index % in_flight.len()`, removing it without delivery.
    /// Returns `true` if a packet was dropped, `false` if `in_flight` was empty.
    pub fn drop_packet_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() {
            return false;
        }
        let actual_idx = index % self.in_flight.len();
        self.in_flight.remove(actual_idx);
        true
    }

    /// Duplicates the in-flight packet at `index % in_flight.len()`, queuing an extra copy.
    /// Returns `true` if a packet was duplicated, `false` if `in_flight` was empty or exceeded bounds.
    pub fn duplicate_packet_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() || self.in_flight.len() >= 512 {
            return false;
        }
        let actual_idx = index % self.in_flight.len();
        let pkt = self.in_flight[actual_idx].clone();
        self.in_flight.push(pkt);
        true
    }

    /// Drains all in-flight messages by advancing logical time until the network is quiescent.
    pub fn drain_network(&mut self, max_ticks: usize) {
        for _ in 0..max_ticks {
            if self.in_flight.is_empty() {
                break;
            }
            self.tick();
        }
    }

    /// Proposes a client command payload to a target node (typically the leader).
    pub fn propose(&mut self, node_id: NodeId, payload: Bytes) {
        if !self.is_node_crashed(node_id) {
            self.step_node_internal(node_id, Input::Propose { payload });
        }
    }

    /// Returns the currently elected leader node ID, or `None` if no single leader exists.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        let mut leader = None;
        for (&id, core) in &self.nodes {
            if !self.is_node_crashed(id) && core.role() == Role::Leader {
                if leader.is_some() {
                    return None;
                }
                leader = Some(id);
            }
        }
        leader
    }

    /// Isolates a node completely by cutting bidirectional network traffic to all peers.
    pub fn isolate_node(&mut self, isolated: NodeId) {
        for &peer in self.nodes.keys() {
            if peer != isolated {
                self.partitions.insert((isolated, peer));
                self.partitions.insert((peer, isolated));
            }
        }
    }

    /// Partitions the cluster into two disconnected components.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partitions.insert((a, b));
                self.partitions.insert((b, a));
            }
        }
    }

    /// Heals all network partitions, restoring full peer connectivity.
    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    /// Helper to tick the cluster until a leader is elected, up to `max_ticks`.
    pub fn elect_leader(&mut self, max_ticks: usize) -> NodeId {
        for _ in 0..max_ticks {
            if let Some(leader) = self.leader() {
                return leader;
            }
            self.tick();
        }
        let leader = self.leader();
        assert!(
            leader.is_some(),
            "failed to elect leader within {max_ticks} ticks"
        );
        leader.unwrap_or(NodeId::from_nonzero(core::num::NonZeroU64::MIN))
    }

    /// Returns the highest committed log index agreed across all live replicas.
    #[must_use]
    pub fn highest_commit_index(&self) -> LogIndex {
        self.nodes
            .iter()
            .filter(|&(id, _)| !self.is_node_crashed(*id))
            .map(|(_, core)| core.commit_index())
            .max()
            .unwrap_or(LogIndex::from_raw(0))
    }
}

/// Initializes the physical on-disk layout on a fresh `SimDevice`.
fn init_node_storage(device: &mut SimDevice, config: &StorageConfig) {
    let sector_size = config.sector_size;
    let sector_len = sector_size.as_usize();

    // 1. Create and preallocate Hard State (2 sectors for 2 alternating slots)
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 2 * sector_size.as_u64(),
    });
    // Write initial slot 0: term 0, seq 0, vote None
    let slot0 = HardStateSlot::new(0, 0, Term::from_raw(0), None);
    let mut slot_buf = vec![0u8; sector_len];
    let _ = slot0.encode(sector_size, &mut slot_buf);
    device.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        data: Bytes::from(slot_buf),
    });
    device.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: FileId::HARD_STATE,
    });

    // 2. Create and preallocate WAL Segment 1
    device.submit(DeviceOp::Create {
        op_id: 4,
        file_id: FileId::wal_segment(1),
        preallocate_bytes: config.segment_capacity_bytes,
    });
    let header = WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(0));
    let mut hdr_buf = vec![0u8; sector_len];
    let _ = header.encode(&mut hdr_buf);
    device.submit(DeviceOp::Write {
        op_id: 5,
        file_id: FileId::wal_segment(1),
        offset: 0,
        data: Bytes::from(hdr_buf),
    });
    device.submit(DeviceOp::Fdatasync {
        op_id: 6,
        file_id: FileId::wal_segment(1),
    });

    // 3. Sync directory metadata
    device.submit(DeviceOp::SyncDir { op_id: 7 });

    // Drain all initialization completions
    while device.poll_completion().is_some() {}
}
