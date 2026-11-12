#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines, clippy::similar_names)]

//! In-memory multi-node `SimCluster` test harness for Raft consensus with real storage engine.
//!
//! Models 3, 5, and 7-node clusters with:
//! - Full storage engine integration (`StorageEngine` + `SimDevice`) with crash recovery
//! - In-memory network message routing with drop, duplicate, and delay hooks
//! - Process crashes and restart recovery with invariant verification
//! - Per-step online invariant verification across all nodes

use crate::device::{SimDevice, SimDeviceConfig, StorageFaultAtlas};
use crate::network::SimNetwork;
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
pub use crate::network::NetworkFilterHook;

/// Hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
pub use crate::network::NetworkDelayHook;

/// In-flight network packet queued for discrete logical time delivery.
pub use crate::network::InFlightPacket;

/// Network fault parameters for plan execution, applied as pure hash
/// functions with no generator draws mid-run.
pub use crate::network::PlanNet;

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
    /// Deterministic packet queue with partitions and fault hooks.
    pub network: SimNetwork,
    /// Plan-hash network faults for plan execution. When set, packet faults
    /// come from pure hash functions instead of installed hooks.
    pub plan_net: Option<PlanNet>,
    /// Global online invariant checker called after every node transition.
    pub checker: RaftInvariantChecker,
    /// Storage fault atlas enforcing the quorum survivability rule.
    /// `None` unless the cluster was built with storage faults enabled.
    pub fault_atlas: Option<StorageFaultAtlas>,
    /// Restart count per node, mixed into the restart RNG seed so a restarted
    /// node continues a fresh election-timeout sequence instead of replaying it.
    pub restart_counts: BTreeMap<NodeId, u64>,
    /// Scratch output buffer reused across steps to eliminate allocations.
    output_scratch: Output,
}

impl SimCluster {
    /// Constructs a new `SimCluster` with `size` nodes (typically 3, 5, or 7) seeded from `master_seed`.
    #[must_use]
    pub fn new(size: usize, master_seed: u64) -> Self {
        Self::build(
            size,
            master_seed,
            &SimDeviceConfig::for_test(SectorSize::Sector512),
            None,
        )
    }

    /// Constructs a `SimCluster` with storage fault injection enabled.
    ///
    /// Every node device uses `dev_config` (chaotic fault rates) and shares one
    /// `StorageFaultAtlas`, so injected read/write corruption can never destroy
    /// every surviving copy of a committed entry on a quorum.
    #[must_use]
    pub fn new_with_storage_faults(
        size: usize,
        master_seed: u64,
        dev_config: &SimDeviceConfig,
    ) -> Self {
        let atlas = StorageFaultAtlas::new(size);
        Self::build(size, master_seed, dev_config, Some(atlas))
    }

    fn build(
        size: usize,
        master_seed: u64,
        dev_config: &SimDeviceConfig,
        atlas: Option<StorageFaultAtlas>,
    ) -> Self {
        assert!(
            (1..=9).contains(&size),
            "cluster size must be between 1 and 9 nodes"
        );

        let mut node_ids = Vec::with_capacity(size);
        for i in 1..=size {
            let id_raw = u64::try_from(i).unwrap_or(1);
            if let Some(node_id) = NodeId::new(id_raw) {
                node_ids.push(node_id);
            }
        }

        let mut nodes = BTreeMap::new();
        let mut configs = BTreeMap::new();
        let mut storage = BTreeMap::new();

        for &node_id in &node_ids {
            let mut peers = BTreeSet::new();
            for &peer_id in &node_ids {
                if peer_id != node_id {
                    peers.insert(peer_id);
                }
            }

            let config = RaftConfig {
                node_id,
                peers,
                election_timeout_ticks: 10,
                heartbeat_interval_ticks: 3,
                max_inflight_messages: 256,
                max_bytes_per_message: 64 * 1024,
                pre_vote_enabled: true,
                check_quorum: true,
            };
            assert!(
                config.validate().is_ok(),
                "SimCluster builds only valid RaftConfig values"
            );

            let path = format!("cluster/node/{}", node_id.get());
            let seed = derive_component_seed(master_seed, &path);
            let rng = ChaCha8Rng::from_seed(seed);

            let core = RaftCore::new(config.clone(), RecoveredState::default(), rng);
            nodes.insert(node_id, core);
            configs.insert(node_id, config);

            // Initialize storage layer with real SimDevice and StorageEngine
            let storage_config = StorageConfig::for_test(SectorSize::Sector512, 64 * 1024);
            let dev_rng = DetRng::new(master_seed, &format!("cluster/storage/{}", node_id.get()));
            let mut sim_dev = SimDevice::new(dev_config.clone(), dev_rng);
            if let Some(shared_atlas) = &atlas {
                sim_dev = sim_dev.with_atlas(shared_atlas.clone(), node_id);
            }
            init_node_storage(&mut sim_dev, &storage_config);
            let engine = StorageEngine::new_empty(storage_config.clone());

            storage.insert(
                node_id,
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
            network: SimNetwork::new(),
            plan_net: None,
            checker: RaftInvariantChecker::new(),
            fault_atlas: atlas,
            restart_counts: BTreeMap::new(),
            output_scratch: Output::new(),
        }
    }

    /// Advances the cluster by one logical tick.
    ///
    /// 1. Steps `Input::Tick` on all live nodes.
    /// 2. Delivers packets scheduled for `current_tick`.
    /// 3. Evaluates online invariants across all nodes.
    pub fn tick(&mut self) {
        self.current_tick = self.current_tick.saturating_add(1);

        // Device completions are timestamped on each device's own clock, so the
        // cluster carries the tick forward before any node steps.
        for storage in self.storage.values_mut() {
            storage.device.set_time(self.current_tick);
        }

        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for node_id in node_ids {
            if !self.is_node_crashed(node_id) {
                self.step_node_internal(node_id, Input::Tick);
            }
        }

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
    pub fn step_node(&mut self, node_id: NodeId, input: Input) {
        self.step_node_internal(node_id, input);
    }

    /// Returns whether the specified node is currently crashed.
    #[must_use]
    pub fn is_node_crashed(&self, node_id: NodeId) -> bool {
        self.storage.get(&node_id).is_some_and(|s| s.crashed)
    }

    /// Returns whether the specified node is marked `NeedsRepair` (mid-log corruption).
    #[must_use]
    pub fn is_node_needs_repair(&self, node_id: NodeId) -> bool {
        self.storage.get(&node_id).is_some_and(|s| s.needs_repair)
    }

    /// Returns whether the node refused to start (both hard state slots corrupt).
    #[must_use]
    pub fn is_node_refused_start(&self, node_id: NodeId) -> bool {
        self.storage.get(&node_id).is_some_and(|s| s.refused_start)
    }

    /// Synchronizes pre-existing entries in `RaftCore` into `StorageEngine` and `SimDevice`
    /// if the node's core was replaced or pre-populated in a test scenario.
    pub fn sync_node_storage_with_core(&mut self, node_id: NodeId) {
        if let (Some(core), Some(storage)) =
            (self.nodes.get(&node_id), self.storage.get_mut(&node_id))
        {
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

    fn step_node_internal(&mut self, node_id: NodeId, input: Input) {
        if self.is_node_crashed(node_id)
            || self.is_node_refused_start(node_id)
            || self.is_node_needs_repair(node_id)
        {
            return;
        }

        self.sync_node_storage_with_core(node_id);
        self.output_scratch.clear();

        if let Some(core) = self.nodes.get_mut(&node_id) {
            core.step(input, &mut self.output_scratch);
        }

        let outbound = std::mem::take(&mut self.output_scratch.messages);
        for msg in outbound {
            self.route_message(node_id, msg.to, msg.message);
        }

        self.checker.check_invariants(&self.nodes);

        // Process persistence operations through real StorageEngine and SimDevice
        if let Some(storage) = self.storage.get_mut(&node_id) {
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
            }
        } else {
            // Nodes registered without storage (public maps allow manual insertion)
            // acknowledge persistence immediately to keep pure-consensus harnesses live.
            let persist_tokens: Vec<_> = self
                .output_scratch
                .persist_ops
                .iter()
                .map(anvil_raft::PersistOp::token)
                .collect();
            for token in persist_tokens {
                self.output_scratch.clear();
                if let Some(core) = self.nodes.get_mut(&node_id) {
                    core.step(Input::Persisted { token }, &mut self.output_scratch);
                }
                let follow_up = std::mem::take(&mut self.output_scratch.messages);
                for msg in follow_up {
                    self.route_message(node_id, msg.to, msg.message);
                }
                self.checker.check_invariants(&self.nodes);
            }
        }

        // Drive pending storage work to quiescence (including completions that
        // became ready while the node was idle) and collect acknowledgements.
        let mut completed_tokens = Vec::new();
        if self.storage.contains_key(&node_id) {
            if self.drive_storage(node_id) {
                self.crash_node(node_id);
                return;
            }
            if let Some(storage) = self.storage.get_mut(&node_id) {
                completed_tokens = std::mem::take(&mut storage.engine_output.persisted_tokens);
            }
        }

        if !completed_tokens.is_empty() {
            let mut all_follow_ups = Vec::new();
            let mut latest_log_and_vote = None;

            for token_val in completed_tokens {
                let token = anvil_raft::PersistToken(token_val);
                self.output_scratch.clear();
                if let Some(core) = self.nodes.get_mut(&node_id) {
                    core.step(Input::Persisted { token }, &mut self.output_scratch);
                    latest_log_and_vote = Some((
                        core.log()
                            .entries()
                            .iter()
                            .map(|entry| (entry.index, entry.term))
                            .collect::<Vec<_>>(),
                        core.term(),
                        core.voted_for(),
                    ));
                }
                all_follow_ups.append(&mut self.output_scratch.messages);
            }

            if let (Some((entries, term, vote)), Some(storage)) =
                (latest_log_and_vote, self.storage.get_mut(&node_id))
            {
                storage.acknowledged_entries = entries;
                if let Some(cand) = vote {
                    storage.persisted_votes.insert(term, Some(cand));
                }
            }

            if let Some(atlas) = &self.fault_atlas
                && let Some(node_storage) = self.storage.get(&node_id)
            {
                for &(idx, _) in &node_storage.acknowledged_entries {
                    atlas.record_persisted(node_id, idx);
                }
                atlas.advance_commit_index(self.highest_commit_index());
            }

            for msg in all_follow_ups {
                self.route_message(node_id, msg.to, msg.message);
            }
            self.checker.check_invariants(&self.nodes);
        }
    }

    /// Drives a node's storage to quiescence: submits buffered device operations,
    /// drains ready completions through the engine (which may emit follow-up
    /// operations such as batched syncs or verification reads), and repeats
    /// until no work remains. Each round either submits new work or observes
    /// none, so the loop always terminates.
    ///
    /// Returns true if the engine latched a fatal error, in which case the
    /// caller must crash the node.
    fn drive_storage(&mut self, node_id: NodeId) -> bool {
        loop {
            let mut submitted = false;
            if let Some(storage) = self.storage.get_mut(&node_id) {
                for dev_op in std::mem::take(&mut storage.engine_output.device_ops) {
                    storage.device.submit(dev_op);
                    submitted = true;
                }
            } else {
                return false;
            }

            let mut drained = false;
            loop {
                let completion = self
                    .storage
                    .get_mut(&node_id)
                    .and_then(|storage| storage.device.poll_completion());
                match completion {
                    None => break,
                    Some(comp) => {
                        drained = true;
                        if let Some(storage) = self.storage.get_mut(&node_id) {
                            storage
                                .engine
                                .step(EngineInput::Completion(comp), &mut storage.engine_output);
                            if storage.engine.fatal_error().is_some() {
                                return true;
                            }
                        }
                    }
                }
            }

            if !submitted && !drained {
                break;
            }
        }
        false
    }

    /// Crashes a specific node: invokes `SimDevice::crash()`, removes volatile `RaftCore`,
    /// and drops all in-flight network messages to/from this node.
    pub fn crash_node(&mut self, node_id: NodeId) {
        if let Some(storage) = self.storage.get_mut(&node_id) {
            storage.crashed = true;
            storage.device.crash();
        }
        self.nodes.remove(&node_id);
        self.network
            .in_flight
            .retain(|pkt| pkt.to != node_id && pkt.from != node_id);
    }

    /// Restarts a crashed node: runs `scan_recovery` against the node's `SimDevice`,
    /// verifies invariants (`VoteNeverForgotten`, `CommittedNeverLostAcrossCrashes`,
    /// and `RecoveredLogEqualsAcknowledgedPlusSurvived`), and recovers `RaftCore` if clean or torn tail.
    ///
    /// # Errors
    /// Returns `StorageError` if hard-state recovery fails (both slots corrupted).
    pub fn restart_node(&mut self, node_id: NodeId) -> Result<RecoveredStorageState, StorageError> {
        let storage = self.storage.get_mut(&node_id).ok_or_else(|| {
            StorageError::DeviceError(format!("node {node_id} not found in cluster storage"))
        })?;

        let sector_size = storage.config.sector_size;
        let sector_len = sector_size.as_usize();
        let hard_state_bytes = storage.device.read_durable(FileId::HARD_STATE)?;

        // Collect all WAL segments present in durable storage, verifying each
        // header as it is read. A header that fails to decode ends collection:
        // a later segment means mid-log corruption, otherwise the segment is
        // a torn tail whose header is rewritten below.
        let mut segments: Vec<(WalSegmentHeader, Bytes)> = Vec::new();
        let mut seg_id = 1u64;
        let mut torn_header_seg: Option<u64> = None;
        let mut header_gap_before_next = false;
        loop {
            let file_id = FileId::wal_segment(seg_id);
            let durable_len = storage
                .device
                .get_file_durable_bytes(file_id)
                .map_or(0, <[u8]>::len);
            if durable_len < sector_len {
                break;
            }
            let seg_bytes = storage.device.read_durable(file_id)?;
            if let Ok(hdr) = WalSegmentHeader::decode(&seg_bytes[..sector_len], sector_size) {
                segments.push((hdr, seg_bytes));
                seg_id += 1;
                continue;
            }
            let next_len = storage
                .device
                .get_file_durable_bytes(FileId::wal_segment(seg_id + 1))
                .map_or(0, <[u8]>::len);
            if next_len >= sector_len {
                header_gap_before_next = true;
            } else {
                torn_header_seg = Some(seg_id);
            }
            break;
        }

        let seg_refs: Vec<(WalSegmentHeader, &[u8])> = segments
            .iter()
            .map(|(hdr, bytes)| (hdr.clone(), bytes.as_ref()))
            .collect();
        let recovery_res = StorageEngine::scan_recovery(&hard_state_bytes, &seg_refs, sector_size);

        let mut recovered = match recovery_res {
            Ok(rec) => rec,
            Err(e) => {
                // Both slots corrupted: refuse to start under ADR 0012
                storage.refused_start = true;
                storage.crashed = false;
                return Err(e);
            }
        };

        // A recovered term or vote older than the latest acknowledged write means
        // silent corruption destroyed the latest hard-state slot. The node forgot
        // an acknowledged vote and must be replaced, never resumed: rejoining
        // could let it vote twice in one term.
        if let Some((&max_term, &max_vote)) =
            storage.persisted_votes.iter().max_by_key(|(term, _)| *term)
            && (recovered.current_term < max_term
                || (recovered.current_term == max_term && recovered.voted_for != max_vote))
        {
            storage.refused_start = true;
            storage.crashed = false;
            return Err(StorageError::StaleHardState {
                recovered_term: recovered.current_term,
                recovered_vote: recovered.voted_for,
                acked_term: max_term,
                acked_vote: max_vote,
            });
        }

        if header_gap_before_next {
            recovered.log.classification = LogRecoveryClassification::MidLogCorruption {
                corrupt_index: recovered
                    .log
                    .last_index
                    .next()
                    .unwrap_or(recovered.log.last_index),
                corrupt_offset: 0,
            };
        }

        let max_acked = storage
            .acknowledged_entries
            .iter()
            .map(|&(idx, _)| idx)
            .max();
        let lost_acked_bound = match &recovered.log.classification {
            LogRecoveryClassification::TornTail {
                last_valid_index, ..
            } => Some(*last_valid_index),
            LogRecoveryClassification::Clean
            | LogRecoveryClassification::MidLogCorruption { .. } => None,
        };
        let torn_acked = torn_header_seg.is_some()
            && max_acked.is_some_and(|max_ack| max_ack > recovered.log.last_index);
        let truncated_acked =
            lost_acked_bound.is_some_and(|bound| max_acked.is_some_and(|max_ack| max_ack > bound));
        if header_gap_before_next || torn_acked || truncated_acked {
            let corrupt_index = lost_acked_bound
                .unwrap_or(recovered.log.last_index)
                .next()
                .unwrap_or(recovered.log.last_index);
            recovered.log.classification = LogRecoveryClassification::MidLogCorruption {
                corrupt_index,
                corrupt_offset: 0,
            };
        } else if torn_header_seg.is_some() {
            recovered.log.classification = LogRecoveryClassification::TornTail {
                truncated_offset: 0,
                last_valid_index: recovered.log.last_index,
            };
        }

        match &recovered.log.classification {
            LogRecoveryClassification::MidLogCorruption { .. } => {
                // Mid-log corruption: marked NeedsRepair, does not vote or lead
                storage.needs_repair = true;
                storage.crashed = false;
                Ok(recovered)
            }
            LogRecoveryClassification::Clean | LogRecoveryClassification::TornTail { .. } => {
                // Invariant 2: Recovered log contains all acknowledged entries
                for &(ack_idx, ack_term) in &storage.acknowledged_entries {
                    let entry_opt = recovered.log.entries.iter().find(|e| e.index == ack_idx);
                    assert!(
                        entry_opt.is_some_and(|e| e.term == ack_term),
                        "RecoveredLogMissingAcknowledgedEntry: node {node_id} missing acknowledged entry ({ack_idx}, {ack_term})"
                    );
                }

                // Invariant 3: CommittedNeverLostAcrossCrashes
                let commit_res = self
                    .checker
                    .check_recovered_log_against_oracle(node_id, &recovered.log.entries);
                assert!(
                    commit_res.is_ok(),
                    "CommittedNeverLostAcrossCrashes invariant failed: {:?}",
                    commit_res.err()
                );

                // Re-initialize RaftCore with a restart-aware seed so the
                // restarted node draws a fresh election-timeout sequence.
                let restarts = self.restart_counts.get(&node_id).copied().unwrap_or(0);
                self.restart_counts.insert(node_id, restarts + 1);
                let path = format!("cluster/node/{}/restart-{restarts}", node_id.get());
                let seed = derive_component_seed(self.master_seed, &path);
                let rng = ChaCha8Rng::from_seed(seed);

                let config = self
                    .configs
                    .get(&node_id)
                    .cloned()
                    .unwrap_or_else(|| RaftConfig {
                        node_id,
                        peers: self
                            .configs
                            .keys()
                            .copied()
                            .filter(|&p| p != node_id)
                            .collect(),
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
                self.nodes.insert(node_id, core);

                // Crash discarded volatile terms/votes that never reached disk.
                // Reset checker history to the recovered durable state before
                // validating, or pre-crash in-memory values would read as
                // regressions no other node could have observed. A mismatch
                // against an acknowledged vote means durable state lost a real
                // vote, so the node must not resume.
                if let Err((tracked, durable)) = self.checker.reset_for_restart(
                    node_id,
                    recovered.current_term,
                    recovered.voted_for,
                    &storage.persisted_votes,
                ) {
                    storage.refused_start = true;
                    storage.crashed = false;
                    return Err(StorageError::StaleHardState {
                        recovered_term: recovered.current_term,
                        recovered_vote: recovered.voted_for,
                        acked_term: recovered.current_term,
                        acked_vote: tracked.or(durable),
                    });
                }

                // Re-initialize StorageEngine from recovered state.
                // Only entries at or after the active segment's first index are
                // replayed to recompute the write offset and CRC seed, so the
                // values exactly match a fresh scan of the active segment.
                let (active_seg_id, resume_index, resume_crc) = match torn_header_seg {
                    Some(torn_id) => {
                        let resume_index = recovered
                            .log
                            .last_index
                            .next()
                            .unwrap_or(recovered.log.last_index);
                        let resume_header = WalSegmentHeader::new(
                            sector_size,
                            torn_id,
                            resume_index,
                            recovered.log.last_term,
                        );
                        let mut hdr_buf = vec![0u8; sector_len];
                        assert!(
                            resume_header.encode(&mut hdr_buf).is_ok(),
                            "resume header buffer is sector sized"
                        );
                        storage
                            .device
                            .overwrite_durable(FileId::wal_segment(torn_id), 0, &hdr_buf);
                        (torn_id, resume_index, resume_header.header_crc)
                    }
                    None => {
                        if let Some((last_hdr, _)) = segments.last() {
                            (
                                last_hdr.segment_id,
                                last_hdr.first_index,
                                last_hdr.header_crc,
                            )
                        } else {
                            let fresh = WalSegmentHeader::new(
                                sector_size,
                                1,
                                LogIndex::from_raw(1),
                                Term::from_raw(0),
                            );
                            (1, fresh.first_index, fresh.header_crc)
                        }
                    }
                };
                let mut write_offset = sector_size.as_u64();
                let mut last_crc = resume_crc;
                for e in &recovered.log.entries {
                    if e.index >= resume_index {
                        let frame =
                            WalRecordFrame::from_entry(e, last_crc, active_seg_id, write_offset);
                        last_crc = frame.crc32c;
                        write_offset += frame.frame_size(sector_size) as u64;
                    }
                }

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

    /// Routes an outbound message through partitions and fault hooks into the network.
    fn route_message(&mut self, from: NodeId, to: NodeId, message: Message) {
        if self.is_node_crashed(from) || self.is_node_crashed(to) {
            return;
        }

        if let Some(plan_net) = self.plan_net {
            self.network
                .schedule_plan(from, to, message, self.current_tick, &plan_net);
        } else {
            self.network.schedule(from, to, message, self.current_tick);
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

            if !self.network.has_due(self.current_tick) {
                break;
            }

            // Drain in queue order so surviving packets keep FIFO stability.
            let packets_to_deliver = self.network.take_due(self.current_tick);

            for pkt in packets_to_deliver {
                if self.is_node_crashed(pkt.to) {
                    continue;
                }
                // Drop if partition was created in the interim
                if self.network.is_cut(pkt.from, pkt.to) {
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

    /// Delivers the in-flight packet at `index % len`, removing it from the queue.
    /// Returns `true` if a packet was delivered, `false` if empty or partition applied.
    pub fn deliver_packet_at(&mut self, index: usize) -> bool {
        let Some(pkt) = self.network.take_at(index) else {
            return false;
        };
        if self.is_node_crashed(pkt.to) {
            return false;
        }
        if self.network.is_cut(pkt.from, pkt.to) {
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

    /// Drops the in-flight packet at `index % len`, removing it without delivery.
    /// Returns `true` if a packet was dropped, `false` if the queue was empty.
    pub fn drop_packet_at(&mut self, index: usize) -> bool {
        self.network.drop_at(index)
    }

    /// Duplicates the in-flight packet at `index % len`, queuing an extra copy.
    /// Returns `true` if a packet was duplicated, `false` if empty or at the bound.
    pub fn duplicate_packet_at(&mut self, index: usize) -> bool {
        self.network.duplicate_at(index)
    }

    /// Drains all in-flight messages by advancing logical time until the network is quiescent.
    pub fn drain_network(&mut self, max_ticks: usize) {
        for _ in 0..max_ticks {
            if self.network.is_empty() {
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
        for (&node_id, core) in &self.nodes {
            if !self.is_node_crashed(node_id) && core.role() == Role::Leader {
                if leader.is_some() {
                    return None;
                }
                leader = Some(node_id);
            }
        }
        leader
    }

    /// Isolates a node completely by cutting bidirectional network traffic to all peers.
    pub fn isolate_node(&mut self, isolated: NodeId) {
        let peers: Vec<NodeId> = self.nodes.keys().copied().collect();
        self.network.isolate(isolated, &peers);
    }

    /// Partitions the cluster into two disconnected components.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        self.network.partition(group_a, group_b);
    }

    /// Heals all network partitions, restoring full peer connectivity.
    pub fn heal_all(&mut self) {
        self.network.heal_all();
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
            .filter(|&(node_id, _)| !self.is_node_crashed(*node_id))
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
    assert!(
        slot0.encode(sector_size, &mut slot_buf).is_ok(),
        "initial hard-state slot buffer is sector sized"
    );
    device.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        payload: Bytes::from(slot_buf),
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
    assert!(
        header.encode(&mut hdr_buf).is_ok(),
        "initial segment header buffer is sector sized"
    );
    device.submit(DeviceOp::Write {
        op_id: 5,
        file_id: FileId::wal_segment(1),
        offset: 0,
        payload: Bytes::from(hdr_buf),
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
