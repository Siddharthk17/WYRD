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
use crate::raft_invariants::{InvariantViolation, RaftInvariantChecker};
use crate::rng::{DetRng, derive_component_seed};
use anvil_raft::{
    Input, Message, Output, RaftConfig, RaftCore, ReceivedSnapshot, RecoveredState, Role,
    SnapshotChunkData, SnapshotRequest,
};
use anvil_storage::{
    Device, DeviceOp, EngineInput, EngineOutput, FileId, HardStateSlot, LogRecoveryClassification,
    RecoveredStorageState, SectorSize, StorageConfig, StorageEngine, StorageError, WalRecordFrame,
    WalSegmentHeader,
    format::wal::{WAL_RECORD_HEADER_FIXED_LEN, WalScanResult, decode_wal_frame},
};
use anvil_types::{Entry, LogIndex, NodeId, Term, sometimes};
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
    /// First log index needing peer repair. Set when the node is sidelined,
    /// cleared when recovery succeeds cleanly or repair completes. Survives
    /// crashes: a crashed sidelined node still knows what to fetch.
    pub repair_from: Option<LogIndex>,
    /// Whether this node refused to start due to unrecoverable hard-state corruption.
    pub refused_start: bool,
    /// Record of all log entries whose persistence has been acknowledged.
    /// Entries land here only when their own persist operation completes:
    /// a hard-state completion never acknowledges log entries, and an
    /// in-flight batch is never counted early. The restart check treats
    /// this as the durable record, so overstating it would demand bytes
    /// the disk never saw.
    pub acknowledged_entries: Vec<(LogIndex, Term)>,
    /// Log entries staged but not yet acknowledged, keyed by persist
    /// token. Core tokens belong to one core persist operation each;
    /// internal tokens (allocated from `next_internal_token`) belong to
    /// one sim-internal staging each. Cleared on crash: completions from
    /// a dead engine incarnation never arrive.
    pub pending_ack_entries: BTreeMap<u64, Vec<(LogIndex, Term)>>,
    /// Hard-state votes staged but not yet acknowledged, keyed by persist
    /// token. A vote counts as persisted only when its own write
    /// completes, never when an unrelated batch does.
    pub pending_ack_votes: BTreeMap<u64, (Term, Option<NodeId>)>,
    /// Next sim-internal persist token, allocated downward from `u64::MAX`
    /// so it never collides with core tokens. Sim-internal stagings
    /// (sync, backfill, reconcile) need completion granularity the shared
    /// token zero cannot give.
    pub next_internal_token: u64,
    /// Record of all votes persisted to disk per term.
    pub persisted_votes: BTreeMap<Term, Option<NodeId>>,
    /// Active storage configuration.
    pub config: StorageConfig,
}

/// Renders the first four digest bytes as hex for compact trace lines.
fn short_digest(digest: &[u8; 32]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(8);
    for byte in digest.iter().take(4) {
        let _ = write!(out, "{byte:02x}");
    }
    out
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
    /// Last commit index observed per node before its crash. Bounds the
    /// recovery oracle check to entries the node itself had committed.
    last_commit_before_crash: BTreeMap<NodeId, LogIndex>,
    /// Completed linearizable read results per node, drained from the core
    /// after every step so workloads can serve reads.
    pub completed_reads: BTreeMap<NodeId, Vec<anvil_raft::ReadIndexResult>>,
    /// Core snapshot-byte requests per node, drained after every step for the
    /// workload to satisfy from its state machines.
    pub snapshot_requests: BTreeMap<NodeId, Vec<SnapshotRequest>>,
    /// Core-verified snapshots per node, drained after every step for the
    /// workload to install into its state machines.
    pub ready_snapshots: BTreeMap<NodeId, Vec<ReceivedSnapshot>>,
    /// Rare protocol paths executed anywhere in the cluster, drained from
    /// every core step. The farm aggregates these per seed for the
    /// sometimes-coverage gate.
    pub sometimes_hits: Vec<&'static str>,
    /// Human-readable trace lines for `anvil-sim trace`. Recorded only when
    /// `trace_enabled` is set, so farm runs pay nothing.
    pub trace_enabled: bool,
    /// Trace lines in emission order, each prefixed with the cluster tick.
    pub trace_lines: Vec<String>,
    /// Last emitted spec projection per node, for change-triggered tracing.
    last_projection: BTreeMap<NodeId, anvil_raft::SpecProjection>,
    /// Per-node tick rate in ticks per mille. 1000 is real time, 500 is half
    /// speed, 2000 is double speed. Models clock drift between 0.5x and 2x.
    pub tick_rate_per_mille: BTreeMap<NodeId, u64>,
    /// Fractional tick credit accumulated for skewed nodes.
    tick_credit_mille: BTreeMap<NodeId, u64>,
    /// Nodes currently paused. A paused node steps no ticks and drops no
    /// state; missed ticks fire as a burst on resume.
    paused: BTreeSet<NodeId>,
    /// Missed tick count per paused node.
    paused_missed: BTreeMap<NodeId, u64>,
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
                    repair_from: None,
                    refused_start: false,
                    acknowledged_entries: Vec::new(),
                    pending_ack_entries: BTreeMap::new(),
                    pending_ack_votes: BTreeMap::new(),
                    next_internal_token: u64::MAX,
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
            tick_rate_per_mille: BTreeMap::new(),
            tick_credit_mille: BTreeMap::new(),
            paused: BTreeSet::new(),
            paused_missed: BTreeMap::new(),
            completed_reads: BTreeMap::new(),
            snapshot_requests: BTreeMap::new(),
            ready_snapshots: BTreeMap::new(),
            sometimes_hits: Vec::new(),
            trace_enabled: false,
            trace_lines: Vec::new(),
            last_projection: BTreeMap::new(),
            last_commit_before_crash: BTreeMap::new(),
            output_scratch: Output::new(),
        }
    }

    /// Advances the cluster by one logical tick.
    ///
    /// 1. Steps `Input::Tick` on all live nodes, honoring per-node tick-rate
    ///    skew and pause. 2. Delivers packets scheduled for `current_tick`.
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
            if self.is_node_crashed(node_id) || self.paused.contains(&node_id) {
                if self.paused.contains(&node_id) {
                    let missed = self.paused_missed.entry(node_id).or_insert(0);
                    *missed = missed.saturating_add(1);
                }
                continue;
            }
            let rate = self
                .tick_rate_per_mille
                .get(&node_id)
                .copied()
                .unwrap_or(1000);
            let credit = self.tick_credit_mille.entry(node_id).or_insert(0);
            *credit = credit.saturating_add(rate);
            let mut steps = *credit / 1000;
            *credit %= 1000;
            if steps > 4 {
                steps = 4;
            }
            for _ in 0..steps {
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
            // Start past the compacted prefix: the core may have installed a
            // snapshot (or compacted) while the engine still needs the
            // backfill, which arrives through the workload in the same tick.
            // The engine gap-buffers the suffix until the prefix lands.
            let sync_from = engine_last
                .as_u64()
                .saturating_add(1)
                .max(core.log().base_index().as_u64().saturating_add(1));
            if core_last > engine_last {
                let mut entries_to_sync = Vec::new();
                for idx in sync_from..=core_last.as_u64() {
                    if let Some(entry) = core.log().entry_at(LogIndex::from_raw(idx)) {
                        entries_to_sync.push(entry.clone());
                    }
                }
                let mut completed = Vec::new();
                if !entries_to_sync.is_empty() {
                    let sync_prev = LogIndex::from_raw(sync_from.saturating_sub(1));
                    let token = storage.next_internal_token;
                    storage.next_internal_token =
                        storage.next_internal_token.saturating_sub(1);
                    storage.pending_ack_entries.insert(
                        token,
                        entries_to_sync.iter().map(|e| (e.index, e.term)).collect(),
                    );
                    storage.engine_output.clear();
                    storage.engine.step(
                        EngineInput::AppendEntries {
                            prev_index: sync_prev,
                            entries: entries_to_sync,
                            token,
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
                    completed = std::mem::take(&mut storage.engine_output.persisted_tokens);
                    storage.engine_output.clear();
                }
                for token_val in completed {
                    self.acknowledge_token(node_id, token_val);
                }
            }
        }
    }

    /// Moves core snapshot outputs from the scratch buffer into the
    /// per-node queues the workload drains.
    fn stash_snapshot_outputs(&mut self, node_id: NodeId) {
        if !self.output_scratch.snapshot_requests.is_empty() {
            let requests = std::mem::take(&mut self.output_scratch.snapshot_requests);
            self.snapshot_requests
                .entry(node_id)
                .or_default()
                .extend(requests);
        }
        if !self.output_scratch.ready_snapshots.is_empty() {
            let ready = std::mem::take(&mut self.output_scratch.ready_snapshots);
            self.ready_snapshots
                .entry(node_id)
                .or_default()
                .extend(ready);
        }
    }

    fn step_node_internal(&mut self, node_id: NodeId, input: Input) {
        if self.is_node_crashed(node_id)
            || self.is_node_refused_start(node_id)
            || self.is_node_needs_repair(node_id)
            || self.paused.contains(&node_id)
        {
            return;
        }

        self.sync_node_storage_with_core(node_id);
        self.output_scratch.clear();
        let mut fresh_reads = Vec::new();

        if let Some(core) = self.nodes.get_mut(&node_id) {
            core.step(input, &mut self.output_scratch);
        }
        self.sometimes_hits
            .append(&mut self.output_scratch.sometimes_hits);
        fresh_reads.append(&mut self.output_scratch.completed_reads);
        self.stash_snapshot_outputs(node_id);

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
                            storage.pending_ack_votes.insert(token.0, (term, vote));
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
                            storage.pending_ack_entries.insert(
                                token.0,
                                entries.iter().map(|e| (e.index, e.term)).collect(),
                            );
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
                            storage.pending_ack_entries.insert(
                                token.0,
                                entries.iter().map(|e| (e.index, e.term)).collect(),
                            );
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
                self.sometimes_hits
                    .append(&mut self.output_scratch.sometimes_hits);
                self.stash_snapshot_outputs(node_id);
                let follow_up = std::mem::take(&mut self.output_scratch.messages);
                for msg in follow_up {
                    self.route_message(node_id, msg.to, msg.message);
                }
                let reads = std::mem::take(&mut self.output_scratch.completed_reads);
                if !reads.is_empty() {
                    fresh_reads.extend(reads);
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

            for &token_val in &completed_tokens {
                let token = anvil_raft::PersistToken(token_val);
                self.output_scratch.clear();
                if let Some(core) = self.nodes.get_mut(&node_id) {
                    core.step(Input::Persisted { token }, &mut self.output_scratch);
                }
                all_follow_ups.append(&mut self.output_scratch.messages);
                self.sometimes_hits
                    .append(&mut self.output_scratch.sometimes_hits);
                fresh_reads.append(&mut self.output_scratch.completed_reads);
                self.stash_snapshot_outputs(node_id);
            }

            // Acknowledge exactly what each completed operation persisted:
            // log entries staged under a completed WAL token, votes under
            // a completed hard-state token. Anything else is still
            // in flight and must not read as durable.
            for token_val in completed_tokens {
                self.acknowledge_token(node_id, token_val);
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
        if !fresh_reads.is_empty() {
            self.completed_reads
                .entry(node_id)
                .or_default()
                .extend(fresh_reads);
        }
        let extra_reads = std::mem::take(&mut self.output_scratch.completed_reads);
        if !extra_reads.is_empty() {
            self.completed_reads
                .entry(node_id)
                .or_default()
                .extend(extra_reads);
        }
        self.stash_snapshot_outputs(node_id);
        self.trace_step(node_id);
    }

    /// Feeds snapshot bytes the workload encoded from its state machine.
    /// Steps the core, routes messages, and checks invariants like any input.
    pub fn supply_snapshot(
        &mut self,
        node_id: NodeId,
        last_included_index: LogIndex,
        last_included_term: Term,
        snapshot_bytes: u64,
        snapshot_digest: [u8; 32],
        chunks: Vec<SnapshotChunkData>,
    ) {
        self.step_node_internal(
            node_id,
            Input::SnapshotReady {
                last_included_index,
                last_included_term,
                snapshot_bytes,
                snapshot_digest,
                chunks,
            },
        );
    }

    /// Drops the node's log prefix after its snapshot proved durable.
    pub fn compact_log(&mut self, node_id: NodeId, up_to: LogIndex) {
        self.step_node_internal(node_id, Input::CompactLog { up_to });
    }

    /// Backfills log entries the core discarded through a snapshot install
    /// but the engine never persisted, so future appends chain. Entries come
    /// from the workload's committed archive and are therefore stable. Uses
    /// token zero like the storage sync path: completions are consumed
    /// without notifying the core. Returns false when storage fault
    /// injection crashed the node.
    pub fn backfill_log_prefix(&mut self, node_id: NodeId, entries: Vec<Entry>) -> bool {
        if entries.is_empty() {
            return !self.is_node_crashed(node_id);
        }
        let token = self.internal_persist_token(node_id);
        let Some(token) = token else {
            return false;
        };
        let pending: Vec<(LogIndex, Term)> =
            entries.iter().map(|e| (e.index, e.term)).collect();
        let Some(storage) = self.storage.get_mut(&node_id) else {
            return false;
        };
        storage.pending_ack_entries.insert(token, pending);
        let prev_index = entries[0].index.prev().unwrap_or(LogIndex::from_raw(0));
        storage.engine_output.clear();
        storage.engine.step(
            EngineInput::AppendEntries {
                prev_index,
                entries,
                token,
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
        let completed: Vec<u64> = std::mem::take(&mut storage.engine_output.persisted_tokens);
        storage.engine_output.clear();
        for token_val in completed {
            self.acknowledge_token(node_id, token_val);
        }
        if self.drive_storage(node_id) {
            self.crash_node(node_id);
            return false;
        }
        if let Some(storage) = self.storage.get_mut(&node_id) {
            let completed: Vec<u64> =
                std::mem::take(&mut storage.engine_output.persisted_tokens);
            storage.engine_output.clear();
            for token_val in completed {
                self.acknowledge_token(node_id, token_val);
            }
        }
        !self.is_node_crashed(node_id)
    }

    /// Allocates a sim-internal persist token for one staging, or `None`
    /// when the node has no storage. Counts down from `u64::MAX` so it
    /// never collides with core tokens.
    fn internal_persist_token(&mut self, node_id: NodeId) -> Option<u64> {
        let storage = self.storage.get_mut(&node_id)?;
        let token = storage.next_internal_token;
        storage.next_internal_token = storage.next_internal_token.saturating_sub(1);
        Some(token)
    }

    /// Moves one completed persist token into the durable record: staged
    /// log entries join the acknowledgments, staged votes join the
    /// persisted votes. Unknown tokens (core-internal or already
    /// consumed) are ignored. The atlas learns the full vector in the
    /// main completion flow, which re-records idempotently.
    pub fn acknowledge_token(&mut self, node_id: NodeId, token: u64) {
        if let Some(storage) = self.storage.get_mut(&node_id) {
            if let Some(entries) = storage.pending_ack_entries.remove(&token) {
                for acked in entries {
                    if !storage.acknowledged_entries.contains(&acked) {
                        storage.acknowledged_entries.push(acked);
                    }
                }
            }
            if let Some((term, vote)) = storage.pending_ack_votes.remove(&token) {
                storage.persisted_votes.insert(term, vote);
            }
        }
    }

    /// Returns the engine's durable tip for one node.
    #[must_use]
    pub fn engine_last_index(&self, node_id: NodeId) -> Option<LogIndex> {
        self.storage
            .get(&node_id)
            .map(|storage| storage.engine.last_entry_index())
    }

    /// Replaces a stale divergent disk prefix the snapshot at `base` just
    /// superseded. Installing a snapshot wipes the core log but not the
    /// disk, so frames at or below the snapshot may still carry terms from
    /// a fork the installer never joined; a later crash would replay them
    /// and contradict the committed oracle. The committed archive is branch
    /// truth for the covered prefix, so the first divergent index rewinds
    /// the engine through the existing truncate path and restages archive
    /// truth: committed suffix entries come from the archive, uncommitted
    /// ones the core still holds are preserved, and only frames neither
    /// source can supply are dropped for the leader to re-replicate.
    /// Acknowledged writes above the restaged tip are pruned so recovery
    /// never demands a frame the reconcile intentionally discarded.
    /// Nodes that are not live keep their bytes for the repair path.
    pub fn reconcile_snapshot_prefix(
        &mut self,
        node_id: NodeId,
        archive: &BTreeMap<LogIndex, (Term, Option<Bytes>)>,
        base: LogIndex,
    ) {
        if self.is_node_crashed(node_id)
            || self.is_node_needs_repair(node_id)
            || self.is_node_refused_start(node_id)
        {
            return;
        }
        let Some(tip) = self.engine_last_index(node_id) else {
            return;
        };
        let end = base.min(tip);
        let mut divergent = None;
        for raw in 1..=end.as_u64() {
            let at = LogIndex::from_raw(raw);
            let disk_term = self
                .storage
                .get(&node_id)
                .and_then(|storage| storage.engine.term_at(at));
            let archive_term = archive.get(&at).map(|(term, _)| *term);
            match (disk_term, archive_term) {
                (Some(disk), Some(archived)) if disk == archived => {}
                _ => {
                    divergent = Some(at);
                    break;
                }
            }
        }
        let Some(first) = divergent else {
            return;
        };
        let mut entries = Vec::new();
        let mut idx = first.as_u64();
        while idx <= tip.as_u64() {
            let at = LogIndex::from_raw(idx);
            if let Some((term, payload)) = archive.get(&at) {
                let (kind, bytes) = match payload {
                    Some(bytes) => (anvil_types::EntryKind::Normal, bytes.clone()),
                    None => (anvil_types::EntryKind::NoOp, Bytes::new()),
                };
                entries.push(Entry {
                    index: at,
                    term: *term,
                    kind,
                    payload: bytes,
                });
            } else if let Some(core) = self.nodes.get(&node_id)
                && let Some(entry) = core.log().entry_at(at)
            {
                entries.push(entry.clone());
            } else {
                break;
            }
            idx += 1;
        }
        assert!(
            !entries.is_empty(),
            "snapshot reconcile found nothing to restage"
        );
        let new_tip = first.as_u64() + entries.len() as u64 - 1;
        let token = self.internal_persist_token(node_id);
        let Some(token) = token else {
            return;
        };
        let pending: Vec<(LogIndex, Term)> =
            entries.iter().map(|e| (e.index, e.term)).collect();
        let Some(storage) = self.storage.get_mut(&node_id) else {
            return;
        };
        storage.pending_ack_entries.insert(token, pending);
        storage.engine_output.clear();
        storage.engine.step(
            EngineInput::TruncateAndAppend {
                from_index: first,
                entries,
                token,
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
        let completed: Vec<u64> = std::mem::take(&mut storage.engine_output.persisted_tokens);
        storage.engine_output.clear();
        for token_val in completed {
            self.acknowledge_token(node_id, token_val);
        }
        if self.drive_storage(node_id) {
            self.crash_node(node_id);
            return;
        }
        if let Some(storage) = self.storage.get_mut(&node_id) {
            let completed: Vec<u64> =
                std::mem::take(&mut storage.engine_output.persisted_tokens);
            storage.engine_output.clear();
            storage
                .acknowledged_entries
                .retain(|(acked, _)| acked.as_u64() <= new_tip);
            for token_val in completed {
                self.acknowledge_token(node_id, token_val);
            }
        }
    }

    /// Tells the core a log entry was applied to the state machine, keeping
    /// the never-beyond-applied bound exact for compaction checks.
    pub fn notify_applied(&mut self, node_id: NodeId, index: LogIndex) {
        self.step_node_internal(node_id, Input::Applied { index });
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
        if let Some(core) = self.nodes.get(&node_id) {
            self.last_commit_before_crash
                .insert(node_id, core.commit_index());
        }
        if let Some(storage) = self.storage.get_mut(&node_id) {
            storage.crashed = true;
            storage.device.crash();
            // Completions from the dead engine incarnation never arrive:
            // staged-but-unacknowledged entries and votes stay unacked.
            // The durable acknowledgment record survives the crash.
            storage.pending_ack_entries.clear();
            storage.pending_ack_votes.clear();
        }
        self.nodes.remove(&node_id);
        self.network
            .in_flight
            .retain(|pkt| pkt.to != node_id && pkt.from != node_id);
        self.trace_line(format!("{node_id:?} crashed"));
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
                // Both slots corrupted: refuse to start under ADR 0012.
                // The node stays down until Week 6 replaces it, so mark it
                // crashed: every quorum-based check already skips crashed nodes.
                storage.refused_start = true;
                storage.crashed = true;
                self.trace_line(format!("{node_id:?} restart refused: unrecoverable"));
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
            storage.crashed = true;
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

        if matches!(
            recovered.log.classification,
            LogRecoveryClassification::TornTail { .. }
        ) {
            sometimes!(self.sometimes_hits, "storage.torn.tail");
        }

        match &recovered.log.classification {
            LogRecoveryClassification::MidLogCorruption { corrupt_index, .. } => {
                // Mid-log corruption: marked NeedsRepair, does not vote or lead
                sometimes!(self.sometimes_hits, "storage.midlog.needsrepair");
                let failed_index = *corrupt_index;
                if let Some(storage) = self.storage.get_mut(&node_id) {
                    storage.needs_repair = true;
                    storage.repair_from = Some(failed_index);
                    storage.crashed = false;
                }
                self.trace_line(format!(
                    "{node_id:?} sidelined for repair at {failed_index:?}"
                ));
                Ok(recovered)
            }
            LogRecoveryClassification::Clean | LogRecoveryClassification::TornTail { .. } => {
                // Invariant 2: Recovered log contains all acknowledged entries
                // that were committed with the acknowledged term. An ack for
                // a different term (or an uncommitted index) is a stale
                // record of a legitimately truncated fork, not lost data:
                // requiring it would make clean restarts impossible.
                for &(ack_idx, ack_term) in &storage.acknowledged_entries {
                    if self.checker.committed_term_at(ack_idx) != Some(ack_term) {
                        continue;
                    }
                    let entry_opt = recovered.log.entries.iter().find(|e| e.index == ack_idx);
                    assert!(
                        entry_opt.is_some_and(|e| e.term == ack_term),
                        "RecoveredLogMissingAcknowledgedEntry: node {node_id} missing acknowledged entry ({ack_idx}, {ack_term})"
                    );
                }

                // Invariant 3: CommittedNeverLostAcrossCrashes, bounded by the
                // node's own pre-crash commit index. Entries above it were
                // uncommitted from this node's perspective and may hold a
                // legitimate divergent suffix. A mismatch at or below the
                // bound with a stale acknowledgment is a torn write the
                // crash exposed: the overwrite reached a quorum in memory
                // but never reached this disk. The committed archive holds
                // branch truth, so the node is sidelined for peer-validated
                // repair instead of rejoining with contradicting bytes.
                // A mismatch the repair cannot source stays sidelined and
                // never rejoins, so no silent violation survives.
                let commit_bound = self
                    .last_commit_before_crash
                    .get(&node_id)
                    .copied()
                    .unwrap_or(LogIndex::from_raw(0));
                let commit_res = self.checker.check_recovered_log_against_oracle(
                    node_id,
                    &recovered.log.entries,
                    commit_bound,
                );
                if let Err(InvariantViolation::CommittedNeverLostAcrossCrashes {
                    index: failed_index,
                    ..
                }) = commit_res
                {
                    sometimes!(self.sometimes_hits, "storage.midlog.needsrepair");
                    if let Some(storage) = self.storage.get_mut(&node_id) {
                        storage.needs_repair = true;
                        storage.repair_from = Some(failed_index);
                        storage.crashed = false;
                    }
                    self.trace_line(format!(
                        "{node_id:?} sidelined for repair at {failed_index:?} (torn committed write)"
                    ));
                    return Ok(recovered);
                }

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
                    storage.crashed = true;
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
                storage.repair_from = None;
                self.trace_line(format!(
                    "{node_id:?} restarted clean at {:?}",
                    recovered.log.last_index
                ));

                self.checker.check_invariants(&self.nodes);

                Ok(recovered)
            }
        }
    }

    /// Peer-validated disk repair for sidelined nodes. A node marked
    /// `NeedsRepair` rejoins once the corrupt range is rewritten from a
    /// live peer and the node re-replicates the suffix through the core's
    /// validated paths. Each rewritten frame is re-chained and the file
    /// re-scanned, so a failed repair leaves the node sidelined instead of
    /// installing unverified bytes. Repair is a heal-phase activity;
    /// mid-chaos faults keep faulting.
    pub fn repair_step(&mut self, archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>) {
        // Crashed sidelined nodes are included: repair fixes their disk
        // first, and the heal pass restarts them right after (crashed nodes
        // restart unconditionally now that recovery re-derives the flag).
        let targets: Vec<NodeId> = self
            .storage
            .iter()
            .filter(|(_, storage)| !storage.refused_start && storage.repair_from.is_some())
            .map(|(id, _)| *id)
            .collect();
        for id in targets {
            self.repair_node(id, archive);
        }
    }

    /// Repairs one sidelined node from peer data. Returns true when the
    /// node rejoined.
    fn repair_node(
        &mut self,
        node_id: NodeId,
        archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>,
    ) -> bool {
        if !self.rewrite_repair_frames(node_id, archive) {
            return false;
        }
        if let Some(storage) = self.storage.get_mut(&node_id) {
            storage.needs_repair = false;
            storage.repair_from = None;
            if !self.nodes.contains_key(&node_id) {
                // No core exists: the crash that sidelined this node removed
                // it and recovery never rebuilt it. Mark crashed so the heal
                // pass restarts it from the repaired disk.
                storage.crashed = true;
            }
        }
        sometimes!(self.sometimes_hits, "sim.repair.cleared");
        true
    }

    /// Finds a validated copy of the missing entry. Prefers the committed
    /// archive (never compacted); otherwise requires a live peer whose
    /// commit covers the entry, which makes it branch truth by leader
    /// completeness.
    fn repair_source(
        &self,
        node_id: NodeId,
        repair_from: LogIndex,
        archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>,
    ) -> Option<Entry> {
        if let Some((term, payload)) = archive.and_then(|archive| archive.get(&repair_from)) {
            return Some(Entry {
                index: repair_from,
                term: *term,
                kind: if payload.is_some() {
                    anvil_types::EntryKind::Normal
                } else {
                    anvil_types::EntryKind::NoOp
                },
                payload: payload.clone().unwrap_or_default(),
            });
        }
        let mut best: Option<(LogIndex, Entry)> = None;
        for (peer_id, core) in &self.nodes {
            if *peer_id == node_id
                || self.is_node_crashed(*peer_id)
                || self.is_node_needs_repair(*peer_id)
                || self.is_node_refused_start(*peer_id)
            {
                continue;
            }
            if core.commit_index() < repair_from {
                continue;
            }
            let Some(entry) = core.log().entry_at(repair_from) else {
                continue;
            };
            if best
                .as_ref()
                .is_some_and(|(best_commit, _)| *best_commit >= core.commit_index())
            {
                continue;
            }
            best = Some((core.commit_index(), entry.clone()));
        }
        best.map(|(_, entry)| entry)
    }

    /// Rewrites corrupt frames in the node's WAL files from validated peer
    /// data, re-chaining CRCs. Only committed entries are implanted, so the
    /// bytes are branch truth wherever they land. Skips files that do not
    /// need repair; aborts a file on the first frame no peer can supply.
    /// Returns true when at least one frame was rewritten and the repaired
    /// files re-scan clean.
    fn rewrite_repair_frames(
        &mut self,
        node_id: NodeId,
        archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>,
    ) -> bool {
        let Some(sector_size) = self
            .storage
            .get(&node_id)
            .map(|storage| storage.config.sector_size)
        else {
            return false;
        };
        let file_ids: Vec<FileId> = match self.storage.get(&node_id) {
            Some(storage) => storage
                .device
                .list_durable_files()
                .into_iter()
                .filter(|file| file.is_wal_segment())
                .collect(),
            None => return false,
        };
        let mut repaired_any = false;
        for file_id in file_ids {
            if self.try_repair_file(node_id, file_id, sector_size, archive) {
                repaired_any = true;
            }
        }
        repaired_any
    }

    /// Rebuilds an undecodable segment header from known constants, the
    /// first frame's index, and a live peer's term for the junction.
    /// Verifies the anchor frame decodes under the rebuilt chain; anything
    /// else leaves the file alone.
    fn rebuild_segment_header(
        &self,
        node_id: NodeId,
        file_id: FileId,
        data: &[u8],
        sector_size: SectorSize,
    ) -> Option<WalSegmentHeader> {
        let sector_len = sector_size.as_usize();
        let segment_id = file_id.wal_segment_id()?;
        if data.len() < sector_len + WAL_RECORD_HEADER_FIXED_LEN {
            return None;
        }
        let mut index_bytes = [0u8; 8];
        index_bytes.copy_from_slice(&data[sector_len..sector_len + 8]);
        let first_index = LogIndex::from_raw(u64::from_le_bytes(index_bytes));
        if first_index.is_zero() {
            return None;
        }
        let prev_term = if first_index == LogIndex::from_raw(1) {
            Term::from_raw(0)
        } else {
            match self.junction_term(node_id, first_index) {
                Some(t) => t,
                None => {
                    return None;
                }
            }
        };
        let header = WalSegmentHeader::new(sector_size, segment_id, first_index, prev_term);
        match decode_wal_frame(
            &data[sector_len..],
            sector_size,
            header.header_crc,
            segment_id,
            sector_len as u64,
        ) {
            WalScanResult::Valid { frame, .. } if frame.index == first_index => Some(header),
            _ => None,
        }
    }

    /// Finds a live peer's term for the entry just before `first_index`,
    /// preferring peers that already committed it.
    fn junction_term(&self, node_id: NodeId, first_index: LogIndex) -> Option<Term> {
        let want = first_index.prev().unwrap_or(LogIndex::from_raw(0));
        if want.is_zero() {
            return Some(Term::from_raw(0));
        }
        let mut best: Option<(LogIndex, Term)> = None;
        for (peer_id, core) in &self.nodes {
            if *peer_id == node_id
                || self.is_node_crashed(*peer_id)
                || self.is_node_needs_repair(*peer_id)
                || self.is_node_refused_start(*peer_id)
            {
                continue;
            }
            let term = core.log().term_at(want)?;
            if best
                .as_ref()
                .is_some_and(|(best_commit, _)| *best_commit >= core.commit_index())
            {
                continue;
            }
            best = Some((core.commit_index(), term));
        }
        best.map(|(_, term)| term)
    }

    /// Sources branch truth for one frame under repair and encodes it at
    /// the rebuilt offset, returning the bytes with the chained CRC.
    /// Returns `None` when no source can supply the index, which aborts
    /// the file untouched.
    #[allow(clippy::too_many_arguments)]
    fn sourced_repair_frame(
        &self,
        node_id: NodeId,
        expected: LogIndex,
        archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>,
        last_crc: u32,
        segment_id: u64,
        offset: u64,
        sector_size: SectorSize,
    ) -> Option<(Vec<u8>, u32)> {
        let entry = self.repair_source(node_id, expected, archive)?;
        assert!(
            entry.index == expected,
            "repair peer returned a different index",
        );
        let frame = WalRecordFrame::from_entry(&entry, last_crc, segment_id, offset);
        let mut frame_bytes = Vec::new();
        frame.encode_frame(sector_size, &mut frame_bytes);
        Some((frame_bytes, frame.crc32c))
    }

    /// Repairs one segment file in place, replacing undecodable frames with
    /// committed peer entries and rebuilding an undecodable header from
    /// known constants, the first frame, and peer terms. A frame that
    /// decodes but disagrees with the committed archive is a stale fork
    /// the repair must not preserve: implanting only the corrupt tail
    /// would assemble a log that violates Log Matching (stale prefix
    /// under branch truth). The replacement re-chains every frame after
    /// it, so each of those is re-sourced in turn; any frame no source
    /// can supply aborts the file untouched.
    /// Verifies the rebuilt layout by re-scanning; any failure leaves the file untouched.
    #[allow(clippy::too_many_lines)]
    fn try_repair_file(
        &mut self,
        node_id: NodeId,
        file_id: FileId,
        sector_size: SectorSize,
        archive: Option<&BTreeMap<LogIndex, (Term, Option<Bytes>)>>,
    ) -> bool {
        let sector_len = sector_size.as_usize();
        let Some(data) = self
            .storage
            .get(&node_id)
            .and_then(|storage| storage.device.get_file_durable_bytes(file_id))
            .map(<[u8]>::to_vec)
        else {
            return false;
        };
        if data.len() < sector_len {
            return false;
        }
        let header = match WalSegmentHeader::decode(&data[..sector_len], sector_size) {
            Ok(header) => header,
            Err(_) => match self.rebuild_segment_header(node_id, file_id, &data, sector_size) {
                Some(header) => header,
                None => return false,
            },
        };
        let mut rebuilt = vec![0u8; sector_len];
        if header.encode(&mut rebuilt).is_err() {
            return false;
        }
        // A rebuilt header alone is a repair even when every frame below
        // already verifies: the on-disk header was undecodable.
        let mut replaced = rebuilt != data[..sector_len].to_vec();
        let mut last_crc = header.header_crc;
        let mut offset = sector_len;
        let mut expected = header.first_index;
        while offset + sector_len <= data.len() {
            match decode_wal_frame(
                &data[offset..],
                sector_size,
                last_crc,
                header.segment_id,
                offset as u64,
            ) {
                WalScanResult::Valid {
                    frame,
                    bytes_consumed,
                    next_crc,
                } => {
                    if frame.index != expected {
                        return false;
                    }
                    // A valid frame that contradicts the committed archive
                    // is a stale fork prefix, not branch truth. Keeping it
                    // under an implanted committed suffix would assemble a
                    // log no replication could produce, so it is replaced
                    // exactly like a corrupt frame.
                    let stale = archive
                        .as_ref()
                        .and_then(|archive| archive.get(&expected))
                        .is_some_and(|(term, _)| *term != frame.term);
                    if !stale {
                        rebuilt.extend_from_slice(&data[offset..offset + bytes_consumed]);
                        last_crc = next_crc;
                        offset += bytes_consumed;
                        expected = LogIndex::from_raw(expected.as_u64() + 1);
                        continue;
                    }
                    let Some((frame_bytes, frame_crc)) = self.sourced_repair_frame(
                        node_id,
                        expected,
                        archive,
                        last_crc,
                        header.segment_id,
                        offset as u64,
                        sector_size,
                    ) else {
                        return false;
                    };
                    rebuilt.extend_from_slice(&frame_bytes);
                    last_crc = frame_crc;
                    offset += frame_bytes.len();
                    expected = LogIndex::from_raw(expected.as_u64() + 1);
                    replaced = true;
                }
                WalScanResult::Unallocated => break,
                WalScanResult::Corrupted(_) => {
                    let Some((frame_bytes, frame_crc)) = self.sourced_repair_frame(
                        node_id,
                        expected,
                        archive,
                        last_crc,
                        header.segment_id,
                        offset as u64,
                        sector_size,
                    ) else {
                        return false;
                    };
                    rebuilt.extend_from_slice(&frame_bytes);
                    last_crc = frame_crc;
                    offset += frame_bytes.len();
                    expected = LogIndex::from_raw(expected.as_u64() + 1);
                    replaced = true;
                }
            }
        }
        if !replaced {
            return false;
        }
        rebuilt.extend_from_slice(&data[offset..]);
        let Some(storage) = self.storage.get_mut(&node_id) else {
            return false;
        };
        storage.device.overwrite_durable(file_id, 0, &rebuilt);
        true
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
            self.trace_line(format!("{node_id:?} propose {}B", payload.len()));
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

    /// Takes completed read results for a node, clearing the buffer.
    pub fn take_completed_reads(&mut self, node: NodeId) -> Vec<anvil_raft::ReadIndexResult> {
        self.completed_reads.remove(&node).unwrap_or_default()
    }

    /// Appends a timestamped line to the text trace when tracing is on.
    /// Rendering is plain strings so the trace stays deterministic and
    /// dependency-free. Takes the owned string because every call site
    /// formats its line inline.
    #[allow(clippy::needless_pass_by_value)]
    pub fn trace_line(&mut self, line: String) {
        if self.trace_enabled {
            self.trace_lines
                .push(format!("t={} {line}", self.current_tick));
        }
    }

    /// Drains one step's spec projections and role changes into the text
    /// trace, emitting only transitions (term, role, vote, commit, or log
    /// digest changed since the node's last line).
    fn trace_step(&mut self, node_id: NodeId) {
        if !self.trace_enabled {
            self.output_scratch.spec_projections.clear();
            self.output_scratch.role_changes.clear();
            return;
        }
        for change in self.output_scratch.role_changes.drain(..) {
            self.trace_lines.push(format!(
                "t={} {node_id:?} role {:?} -> {:?} at {:?}",
                self.current_tick, change.previous, change.current, change.term
            ));
        }
        for projection in self.output_scratch.spec_projections.drain(..) {
            let changed = self
                .last_projection
                .get(&node_id)
                .is_none_or(|previous| *previous != projection);
            if changed {
                self.trace_lines.push(format!(
                    "t={} {node_id:?} term={:?} role={:?} vote={:?} commit={:?} log={}",
                    self.current_tick,
                    projection.term,
                    projection.role,
                    projection.voted_for,
                    projection.commit_index,
                    short_digest(&projection.log_digest),
                ));
                self.last_projection.insert(node_id, projection);
            }
        }
    }

    /// Sets a per-node tick rate in ticks per mille (500 to 2000). Models
    /// clock drift: a node at 500 advances once every two global ticks,
    /// a node at 2000 advances twice per global tick.
    pub fn set_tick_skew(&mut self, node: NodeId, rate_per_mille: u64) {
        assert!(
            (500..=2000).contains(&rate_per_mille),
            "tick skew must be within 0.5x to 2x (500 to 2000 per mille)"
        );
        self.tick_rate_per_mille.insert(node, rate_per_mille);
    }

    /// Returns the tick rate for a node, defaulting to real time.
    #[must_use]
    pub fn tick_rate(&self, node: NodeId) -> u64 {
        self.tick_rate_per_mille.get(&node).copied().unwrap_or(1000)
    }

    /// Pauses a node: it steps no ticks and processes no messages while its
    /// timers go stale.
    pub fn pause_node(&mut self, node: NodeId) {
        self.paused.insert(node);
        self.paused_missed.entry(node).or_insert(0);
    }

    /// Returns true while a node is paused.
    #[must_use]
    pub fn is_paused(&self, node: NodeId) -> bool {
        self.paused.contains(&node)
    }

    /// Resumes a paused node, firing stale timers as a bounded burst of ticks
    /// so an expired election timer triggers promptly.
    pub fn resume_node(&mut self, node: NodeId) {
        if !self.paused.remove(&node) {
            return;
        }
        let missed = self.paused_missed.remove(&node).unwrap_or(0);
        let burst = missed.min(100);
        for _ in 0..burst {
            if self.is_node_crashed(node) {
                break;
            }
            self.step_node_internal(node, Input::Tick);
        }
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
