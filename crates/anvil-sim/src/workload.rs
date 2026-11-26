#![forbid(unsafe_code)]
#![allow(
    clippy::too_many_lines,
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::redundant_closure_for_method_calls,
    clippy::trivially_copy_pass_by_ref,
    clippy::collapsible_if,
    clippy::single_match,
    clippy::needless_borrow,
    clippy::manual_is_multiple_of,
    clippy::match_same_arms,
    clippy::collapsible_match,
    clippy::op_ref,
    clippy::ifs_same_cond,
    clippy::if_same_then_else,
    clippy::too_many_arguments
)]

//! Deterministic KV workload with swarm clients over `SimCluster`.
//!
//! Each seed pre-generates its client list and operation list through an
//! isolated `DetRng` stream before the run starts. Execution draws no
//! generator state: network faults come from the plan salt hash, and client
//! timing comes from the pre-generated schedule.

use crate::cluster::{PlanNet, SimCluster};
use crate::device::SimDeviceConfig;
use crate::plan::{FaultKind, Plan, WorkloadSpec};
use crate::rng::DetRng;
use anvil_check::{ClientOp, History, HistoryOp, OpResult};
use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore, SnapshotPolicy};
use anvil_raft::{Input, ReceivedSnapshot, SnapshotChunkData};
use anvil_types::{LogIndex, NodeId, SeqNo, SessionId};
use bytes::Bytes;
use std::collections::BTreeMap;

/// Swarm profile pre-generated from a seed.
#[derive(Debug, Clone)]
pub struct SwarmProfile {
    /// Number of clients.
    pub clients: usize,
    /// Key space size.
    pub keys: usize,
    /// Operations per client.
    pub ops_per_client: usize,
    /// Percentage of list-append keys (rest are registers).
    pub list_pct: u64,
    /// Think ticks between operations.
    pub think_ticks: u64,
    /// Timeout ticks before an operation is indeterminate.
    pub timeout_ticks: u64,
    /// Whether clients crash mid-run.
    pub client_crashes: bool,
    /// Transport alternation marker.
    pub unordered: bool,
}

/// One scheduled client operation.
#[derive(Debug, Clone)]
struct ScheduledOp {
    client: usize,
    seq_in_client: usize,
    key_idx: usize,
    kind: ScheduledKind,
    invoke_tick: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScheduledKind {
    Put,
    Get,
    Delete,
    Append,
    Range,
}

/// Generates the swarm profile deterministically from the seed.
#[must_use]
pub fn swarm_profile(seed: u64) -> SwarmProfile {
    let mut rng = DetRng::new(seed, "sim/kv-workload");
    let clients = (rng.gen_range(0..8) + 1) as usize;
    let keys = (rng.gen_range(0..16) + 1) as usize;
    let ops_per_client = (rng.gen_range(0..6) + 2) as usize;
    let list_pct = rng.gen_range(0..40);
    let think_ticks = rng.gen_range(1..6);
    let timeout_ticks = 25;
    let client_crashes = rng.bernoulli(1, 4);
    let unordered = seed % 2 == 1;
    SwarmProfile {
        clients,
        keys,
        ops_per_client,
        list_pct,
        think_ticks,
        timeout_ticks,
        client_crashes,
        unordered,
    }
}

/// Snapshot flow state for one KV seed: a committed-payload archive for the
/// `SnapshotEqualsReplay` check, per-node snapshot counters, and the policy.
struct SnapshotHarness {
    policy: SnapshotPolicy,
    chunk_bytes: usize,
    archive: BTreeMap<LogIndex, (anvil_types::Term, Option<Bytes>)>,
    since_entries: BTreeMap<NodeId, u64>,
    since_bytes: BTreeMap<NodeId, u64>,
    last_snapshot: BTreeMap<NodeId, LogIndex>,
    snapshots_taken: u64,
    installs: u64,
    snapshots: BTreeMap<NodeId, (LogIndex, Bytes)>,
}

impl SnapshotHarness {
    fn for_sim() -> Self {
        Self {
            policy: SnapshotPolicy {
                max_entries_since_snapshot: 20,
                max_bytes_since_snapshot: 8 * 1024,
                retain_entries: 5,
                chunk_bytes: 256,
            },
            chunk_bytes: 256,
            archive: BTreeMap::new(),
            since_entries: BTreeMap::new(),
            since_bytes: BTreeMap::new(),
            last_snapshot: BTreeMap::new(),
            snapshots_taken: 0,
            installs: 0,
            snapshots: BTreeMap::new(),
        }
    }
}

/// Replays the archived committed prefix into a fresh store and asserts the
/// state hash equals the snapshot's: `SnapshotEqualsReplay`.
fn assert_snapshot_equals_replay(
    snapshot: &KvStore,
    archive: &BTreeMap<LogIndex, (anvil_types::Term, Option<Bytes>)>,
) {
    let mut replay = KvStore::new(snapshot.config());
    let mut idx = 1u64;
    let applied = snapshot.applied_index().as_u64();
    while idx <= applied {
        if let Some((_, payload)) = archive.get(&LogIndex::from_raw(idx)) {
            if let Some(bytes) = payload {
                // Committed commands can legitimately fail application: a
                // client may pipeline writes against a session whose
                // registration was truncated before committing, and every
                // replica rejects them identically. Only the state hash
                // must match.
                let _ = replay.apply(LogIndex::from_raw(idx), bytes);
            }
        }
        idx += 1;
    }
    assert_eq!(
        replay.state_hash(),
        snapshot.state_hash(),
        "SnapshotEqualsReplay violated at applied {applied}"
    );
}

/// Timed plan events flattened into a tick-ordered action list. Built once
/// before the run; execution draws no generator state.
struct FaultWindow {
    actions: Vec<(u64, bool, usize)>,
}

impl FaultWindow {
    fn new(plan: &Plan) -> Self {
        let mut actions = Vec::new();
        for (index, event) in plan.events.iter().enumerate() {
            actions.push((event.start, false, index));
            actions.push((event.end, true, index));
        }
        actions.sort_unstable();
        Self { actions }
    }
}

/// Live client session state.
struct WorkloadClient {
    session: Option<SessionId>,
    next_seq: u64,
    acked: u64,
    crashed_until: u64,
    key_versions: BTreeMap<Vec<u8>, u64>,
}

/// The live replica that has applied the least. A read served from it
/// without waiting is the canary's stale read.
fn most_lagging_live(
    cluster: &SimCluster,
    applied: &BTreeMap<NodeId, LogIndex>,
    fallback: NodeId,
) -> NodeId {
    cluster
        .nodes
        .keys()
        .copied()
        .filter(|id| !cluster.is_node_crashed(*id))
        .min_by_key(|id| applied.get(id).copied().unwrap_or_default())
        .unwrap_or(fallback)
}

/// Returns the leader's commit index only when the leader has committed at
/// least one entry in its own term. A freshly elected leader reports commit
/// zero (or a stale pre-election index) until its no-op replicates; handing
/// that number to a follower would let the follower stop waiting while it
/// is still missing committed writes.
fn leader_interm_commit(cluster: &SimCluster, leader: NodeId) -> Option<LogIndex> {
    if cluster.leader() != Some(leader) {
        return None;
    }
    let core = cluster.nodes.get(&leader)?;
    let commit = core.commit_index();
    let interm = core
        .log()
        .entries()
        .iter()
        .any(|entry| entry.term == core.term() && entry.index <= commit);
    interm.then_some(commit)
}

/// Runs one deterministic KV seed and returns its history.
/// When `stale_canary` is true, reads are served stale but recorded as
/// linearizable, which the checker must reject.
pub fn run_kv_seed(seed: u64, stale_canary: bool) -> (History, bool) {
    let (history, audit, _) = run_kv_seed_inner(seed, false, stale_canary);
    (history, audit)
}

/// Runs one KV seed with storage faults enabled.
pub fn run_kv_seed_with_storage(seed: u64, stale_canary: bool) -> (History, bool) {
    let (history, audit, _) = run_kv_seed_inner(seed, true, stale_canary);
    (history, audit)
}

/// Snapshot flow counters for one KV seed, also consumed by the farm
/// summary in a later subtask.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotStats {
    /// Snapshots taken, verified against a log replay, and supplied.
    pub snapshots_taken: u64,
    /// Core-verified snapshots installed into a state machine.
    pub installs: u64,
}

/// Runs one deterministic KV seed and returns its history.
pub fn run_kv_seed_with_stats(seed: u64, storage_faults: bool) -> (History, bool, SnapshotStats) {
    run_kv_seed_inner(seed, storage_faults, false)
}

#[allow(clippy::too_many_lines)]
fn run_kv_seed_inner(
    seed: u64,
    storage_faults: bool,
    stale_canary: bool,
) -> (History, bool, SnapshotStats) {
    let profile = swarm_profile(seed);
    let workload_spec = WorkloadSpec {
        clients: profile.clients,
        keys: profile.keys,
        ops_per_client: profile.ops_per_client,
        list_pct: profile.list_pct,
        think_ticks: profile.think_ticks,
        timeout_ticks: profile.timeout_ticks,
        client_crashes: profile.client_crashes,
        unordered: profile.unordered,
    };
    let plan = Plan::generate_for_workload(seed, storage_faults, workload_spec);
    let fault_window = FaultWindow::new(&plan);
    let mut cluster = if storage_faults {
        let dev =
            SimDeviceConfig::chaotic(anvil_storage::SectorSize::Sector512, 10_000, 5_000, 200);
        SimCluster::new_with_storage_faults(3, seed, &dev)
    } else {
        SimCluster::new(3, seed)
    };
    if profile.unordered {
        cluster
            .network
            .set_transport(crate::network::TransportMode::Unordered);
        cluster.network.set_fault_rates(30, 5, 4, 1);
    } else {
        cluster
            .network
            .set_transport(crate::network::TransportMode::OrderedLossy);
        cluster.network.set_fault_rates(0, 5, 4, 1);
    }
    cluster.network.set_fault_salt(seed ^ 0x51AB_3C4D_9E77_11AA);
    cluster
        .network
        .set_latency_model(crate::network::LatencyModel::Uniform { min: 0, max: 2 });
    cluster.network.set_bandwidth_per_tick(64);
    cluster.plan_net = Some(PlanNet {
        salt: plan.salt,
        drop_pct: plan.drop_pct,
        dup_pct: plan.dup_pct,
        max_delay: plan.max_delay,
    });

    let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    let mut base_skew = BTreeMap::new();
    for (i, id) in node_ids.iter().enumerate() {
        let rate = match (seed >> (i * 2)) % 4 {
            0 => 500,
            1 => 1000,
            2 => 1000,
            _ => 2000,
        };
        cluster.set_tick_skew(*id, rate);
        base_skew.insert(*id, rate);
    }

    let mut stores: BTreeMap<NodeId, KvStore> = node_ids
        .iter()
        .map(|id| (*id, KvStore::new(KvConfig::default())))
        .collect();
    let mut applied: BTreeMap<NodeId, LogIndex> = node_ids
        .iter()
        .map(|id| (*id, LogIndex::from_raw(0)))
        .collect();
    let mut applied_terms: BTreeMap<NodeId, (LogIndex, anvil_types::Term)> = BTreeMap::new();
    let mut harness = SnapshotHarness::for_sim();

    let mut clients: Vec<WorkloadClient> = (0..profile.clients)
        .map(|_| WorkloadClient {
            session: None,
            next_seq: 1,
            acked: 0,
            crashed_until: 0,
            key_versions: BTreeMap::new(),
        })
        .collect();

    let schedule = build_schedule(seed, &profile);
    let list_pct = if schedule.iter().any(|op| op.kind == ScheduledKind::Range) {
        0
    } else {
        profile.list_pct
    };
    let mut history = History::new();
    let mut tick: u64 = 0;
    let end_tick = 400;

    let mut op_cursor = 0;
    let mut fault_cursor = 0;
    // Canary state: the last two observed register values per key, with the
    // completion tick of the latest write. A canary read returns the older
    // pair once its successor completed, which is exactly the stale read a
    // ReadIndex without heartbeat confirmation would serve.
    let mut canary_last: BTreeMap<Vec<u8>, (Option<Bytes>, u64, u64)> = BTreeMap::new();
    let mut canary_prev: BTreeMap<Vec<u8>, (Option<Bytes>, u64)> = BTreeMap::new();
    cluster.tick_n(15);
    tick += 15;
    apply_all(
        &mut cluster,
        &mut stores,
        &mut applied,
        &mut applied_terms,
        &mut harness,
    );
    snapshot_maintenance(
        &mut cluster,
        &mut stores,
        &mut applied,
        &mut applied_terms,
        &mut harness,
    );

    // Run the full horizon even after the client schedule exhausts: fault
    // events live in the chaos window and need ticks to fire, and snapshots
    // need applied entries to trigger.
    while tick < end_tick {
        if op_cursor < schedule.len() && tick >= schedule[op_cursor].invoke_tick {
            let op = &schedule[op_cursor];
            tick = advance_with_faults(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &mut harness,
                &node_ids,
                &plan,
                &fault_window,
                &base_skew,
                &mut fault_cursor,
                tick,
            );
            execute_scheduled(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &mut harness,
                &mut clients,
                &profile,
                list_pct,
                op,
                &mut history,
                tick,
                stale_canary,
                &mut canary_last,
                &mut canary_prev,
            );
            op_cursor += 1;
            cluster.tick();
            tick += 1;
            apply_all(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &mut harness,
            );
            snapshot_maintenance(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &mut harness,
            );
        } else {
            tick = advance_with_faults(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &mut harness,
                &node_ids,
                &plan,
                &fault_window,
                &base_skew,
                &mut fault_cursor,
                tick,
            );
        }
    }

    cluster.plan_net = None;
    cluster.heal_all();
    heal_step(&mut cluster, &node_ids, &base_skew);
    snapshot_maintenance(
        &mut cluster,
        &mut stores,
        &mut applied,
        &mut applied_terms,
        &mut harness,
    );
    drain_to_convergence(
        &mut cluster,
        &mut stores,
        &mut applied,
        &mut applied_terms,
        &mut harness,
        &node_ids,
        &base_skew,
    );

    liveness_check(
        &mut cluster,
        &mut stores,
        &mut applied,
        &mut applied_terms,
        &mut harness,
        &node_ids,
        &base_skew,
        seed,
    );

    let audit = convergence_audit(&cluster, &stores, &applied);
    let stats = SnapshotStats {
        snapshots_taken: harness.snapshots_taken,
        installs: harness.installs,
    };
    (history, audit, stats)
}

/// Runs one round of post-heal maintenance: heal partitions, resume paused
/// nodes, restore base tick rates, and restart crashed nodes that can
/// restart. Storage faults can crash nodes at any time, so convergence and
/// liveness loops repeat this instead of relying on a single heal.
fn heal_step(cluster: &mut SimCluster, node_ids: &[NodeId], base_skew: &BTreeMap<NodeId, u64>) {
    cluster.heal_all();
    for id in node_ids.iter().copied() {
        if cluster.is_paused(id) {
            cluster.resume_node(id);
        }
        if let Some(base) = base_skew.get(&id) {
            cluster.set_tick_skew(id, *base);
        }
        if cluster.is_node_crashed(id)
            && !cluster.is_node_needs_repair(id)
            && !cluster.is_node_refused_start(id)
        {
            let _ = cluster.restart_node(id);
        }
    }
}

/// Ticks a healed cluster until every live replica shares the same commit
/// and applied index, or a fixed bound lapses. The bound keeps replay
/// deterministic; the early exit only shortens idle ticks.
fn drain_to_convergence(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
    node_ids: &[NodeId],
    base_skew: &BTreeMap<NodeId, u64>,
) {
    for _ in 0..500 {
        heal_step(cluster, node_ids, base_skew);
        cluster.tick();
        apply_all(cluster, stores, applied, applied_terms, harness);
        snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
        let live: Vec<NodeId> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|id| !cluster.is_node_crashed(*id))
            .collect();
        let commits: Vec<LogIndex> = live
            .iter()
            .filter_map(|id| cluster.nodes.get(id).map(|core| core.commit_index()))
            .collect();
        let applieds: Vec<LogIndex> = live
            .iter()
            .map(|id| applied.get(id).copied().unwrap_or_default())
            .collect();
        if !live.is_empty()
            && commits.windows(2).all(|pair| pair[0] == pair[1])
            && applieds.windows(2).all(|pair| pair[0] == pair[1])
            && applieds.first().copied().unwrap_or_default()
                == commits.first().copied().unwrap_or_default()
        {
            break;
        }
    }
}

/// Advances one outer tick, firing any due pre-generated plan faults first.
#[allow(clippy::too_many_arguments)]
fn advance_with_faults(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
    node_ids: &[NodeId],
    plan: &Plan,
    fault_window: &FaultWindow,
    base_skew: &BTreeMap<NodeId, u64>,
    fault_cursor: &mut usize,
    tick: u64,
) -> u64 {
    while *fault_cursor < fault_window.actions.len() {
        let (at, is_end, index) = fault_window.actions[*fault_cursor];
        if at > tick {
            break;
        }
        let event = &plan.events[index];
        if is_end {
            end_workload_event(cluster, node_ids, plan, tick, event, base_skew);
        } else {
            begin_workload_event(cluster, node_ids, event);
        }
        *fault_cursor += 1;
    }
    let spike: u64 = plan
        .events
        .iter()
        .filter(|event| {
            matches!(event.fault, FaultKind::DelaySpike { .. })
                && event.start <= tick
                && tick < event.end
        })
        .map(|event| match event.fault {
            FaultKind::DelaySpike { extra_ticks } => extra_ticks,
            _ => 0,
        })
        .sum();
    cluster.plan_net = Some(PlanNet {
        salt: plan.salt,
        drop_pct: plan.drop_pct,
        dup_pct: plan.dup_pct,
        max_delay: plan.max_delay.saturating_add(spike),
    });
    cluster.tick();
    apply_all(cluster, stores, applied, applied_terms, harness);
    snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
    tick + 1
}

/// Starts one workload fault event.
fn begin_workload_event(
    cluster: &mut SimCluster,
    node_ids: &[NodeId],
    event: &crate::plan::FaultEvent,
) {
    let at = |member: usize| node_ids[member % node_ids.len()];
    match event.fault {
        FaultKind::Partition { member } => {
            if let Some(id) = node_ids.get(member) {
                let peers: Vec<NodeId> = node_ids.to_vec();
                cluster.network.isolate(*id, &peers);
            }
        }
        FaultKind::Asymmetric { from, to } => {
            cluster.network.cut_one_way(at(from), at(to));
        }
        FaultKind::Crash { node } => {
            if let Some(id) = node_ids.get(node) {
                cluster.crash_node(*id);
            }
        }
        FaultKind::Pause { node } => {
            if let Some(id) = node_ids.get(node) {
                cluster.pause_node(*id);
            }
        }
        FaultKind::SlowClock {
            node,
            rate_per_mille,
        } => {
            if let Some(id) = node_ids.get(node) {
                cluster.set_tick_skew(*id, rate_per_mille);
            }
        }
        FaultKind::DelaySpike { .. } => {}
    }
}

/// Ends one workload fault event, restoring the pre-event condition.
#[allow(clippy::too_many_arguments)]
fn end_workload_event(
    cluster: &mut SimCluster,
    node_ids: &[NodeId],
    plan: &Plan,
    tick: u64,
    event: &crate::plan::FaultEvent,
    base_skew: &BTreeMap<NodeId, u64>,
) {
    match event.fault {
        FaultKind::Partition { .. } | FaultKind::Asymmetric { .. } => {
            let other_cuts = plan.events.iter().any(|other| {
                other.id != event.id
                    && matches!(
                        other.fault,
                        FaultKind::Partition { .. } | FaultKind::Asymmetric { .. }
                    )
                    && other.start <= tick
                    && tick < other.end
            });
            if !other_cuts {
                cluster.network.heal_all();
            }
        }
        FaultKind::Crash { node } => {
            if let Some(id) = node_ids.get(node)
                && !cluster.is_node_needs_repair(*id)
                && !cluster.is_node_refused_start(*id)
            {
                let _ = cluster.restart_node(*id);
            }
        }
        FaultKind::Pause { node } => {
            if let Some(id) = node_ids.get(node)
                && cluster.is_paused(*id)
            {
                cluster.resume_node(*id);
            }
        }
        FaultKind::SlowClock { node, .. } => {
            if let Some(id) = node_ids.get(node) {
                cluster.set_tick_skew(*id, base_skew.get(id).copied().unwrap_or(1000));
            }
        }
        FaultKind::DelaySpike { .. } => {}
    }
}

fn key_bytes(list_pct: u64, keys: usize, key_idx: usize) -> (Bytes, bool) {
    let is_list = (key_idx as u64 * 100 / keys.max(1) as u64) < list_pct;
    let name = format!("k{:04}", key_idx % keys.max(1));
    (Bytes::from(name.into_bytes()), is_list)
}

fn build_schedule(seed: u64, profile: &SwarmProfile) -> Vec<ScheduledOp> {
    let mut rng = DetRng::new(seed, "sim/kv-schedule");
    let mut out = Vec::new();
    for client in 0..profile.clients {
        let mut tick = 16 + rng.gen_range(0..8);
        for seq in 0..profile.ops_per_client {
            let roll = rng.gen_range(0..100);
            let kind = if roll < 40 {
                ScheduledKind::Put
            } else if roll < 70 {
                ScheduledKind::Get
            } else if roll < 78 {
                ScheduledKind::Delete
            } else if roll < 92 {
                ScheduledKind::Append
            } else {
                ScheduledKind::Range
            };
            let key_idx = rng.gen_range(0..profile.keys.max(1) as u64) as usize;
            out.push(ScheduledOp {
                client,
                seq_in_client: seq,
                key_idx,
                kind,
                invoke_tick: tick,
            });
            tick += profile.think_ticks + rng.gen_range(0..4);
        }
    }
    out.sort_by_key(|op| (op.invoke_tick, op.client));
    out
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn execute_scheduled(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
    clients: &mut [WorkloadClient],
    profile: &SwarmProfile,
    list_pct: u64,
    op: &ScheduledOp,
    history: &mut History,
    tick: u64,
    stale_canary: bool,
    canary_last: &mut BTreeMap<Vec<u8>, (Option<Bytes>, u64, u64)>,
    canary_prev: &mut BTreeMap<Vec<u8>, (Option<Bytes>, u64)>,
) {
    let client = &mut clients[op.client];
    if profile.client_crashes && tick < client.crashed_until {
        return;
    }
    if profile.client_crashes && op.seq_in_client == 1 && op.client % 3 == 0 {
        client.crashed_until = tick + 10;
        return;
    }
    let Some(leader) = cluster.leader() else {
        let (key, _) = key_bytes(list_pct, profile.keys, op.key_idx);
        history.push(HistoryOp {
            id: 0,
            client: op.client as u64,
            key,
            op: ClientOp::Get,
            invoke: tick,
            ret: Some(tick + 1),
            result: None,
            indeterminate: true,
        });
        return;
    };
    if client.session.is_none() {
        let before_idx = cluster
            .nodes
            .get(&leader)
            .map_or(0, |core| core.last_log_index().as_u64());
        let mut buf = Vec::new();
        KvCommand::RegisterSession.encode(&mut buf);
        cluster.propose(leader, Bytes::from(buf));
        for _ in 0..12 {
            if cluster.leader() != Some(leader) {
                break;
            }
            cluster.tick();
            apply_all(cluster, stores, applied, applied_terms, harness);
            snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
            if let Some(id) = find_register_session(cluster, leader, before_idx) {
                client.session = Some(id);
                break;
            }
        }
        let Some(session) = client.session else {
            let (key, _) = key_bytes(list_pct, profile.keys, op.key_idx);
            history.push(HistoryOp {
                id: 0,
                client: op.client as u64,
                key,
                op: ClientOp::Get,
                invoke: tick,
                ret: Some(tick + 1),
                result: None,
                indeterminate: true,
            });
            return;
        };
        let _ = session;
    }
    let session = client.session.unwrap_or(SessionId::from_raw(1));
    let seq = SeqNo::from_raw(client.next_seq);
    let acked = SeqNo::from_raw(client.acked);
    let (mut key, is_list) = key_bytes(list_pct, profile.keys, op.key_idx);
    let value = Bytes::from(format!("v{}-{}-{}", op.client, op.seq_in_client, tick).into_bytes());
    let effective_kind = match (op.kind, is_list) {
        (ScheduledKind::Put | ScheduledKind::Delete, true) => ScheduledKind::Append,
        (ScheduledKind::Append, false) => ScheduledKind::Put,
        (kind, _) => kind,
    };
    if effective_kind == ScheduledKind::Range {
        key = Bytes::new();
    }

    match effective_kind {
        ScheduledKind::Put | ScheduledKind::Delete | ScheduledKind::Append => {
            let mut buf = Vec::new();
            match effective_kind {
                ScheduledKind::Put => KvCommand::Put {
                    session,
                    seq,
                    acked,
                    key: key.clone(),
                    value: value.clone(),
                }
                .encode(&mut buf),
                ScheduledKind::Delete => KvCommand::Delete {
                    session,
                    seq,
                    acked,
                    key: key.clone(),
                }
                .encode(&mut buf),
                _ => KvCommand::Append {
                    session,
                    seq,
                    acked,
                    key: key.clone(),
                    value: value.clone(),
                }
                .encode(&mut buf),
            }
            let invoke = tick;
            cluster.propose(leader, Bytes::from(buf));
            let deadline = invoke + profile.timeout_ticks;
            let before = applied.get(&leader).copied().unwrap_or_default();
            let mut done = false;
            let mut t = invoke;
            while t < deadline {
                cluster.tick();
                t += 1;
                apply_all(cluster, stores, applied, applied_terms, harness);
                snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
                let visible = match effective_kind {
                    ScheduledKind::Delete => delete_committed(stores, &leader, &key, before),
                    _ => write_visible_on_leader(stores, &leader, &key, &value, effective_kind),
                };
                if visible {
                    done = true;
                    break;
                }
            }
            client.next_seq += 1;
            client.acked = client.next_seq - 1;
            let client_op = match effective_kind {
                ScheduledKind::Put => ClientOp::Put {
                    value: value.clone(),
                },
                ScheduledKind::Delete => ClientOp::Delete,
                _ => ClientOp::Append {
                    value: value.clone(),
                },
            };
            if done {
                let (result, version) = observe_write_on_leader(stores, &leader, &key, &client_op);
                if let Some(v) = version {
                    client.key_versions.insert(key.to_vec(), v);
                }
                if !is_list {
                    let observed = match &result {
                        OpResult::Value { value, version } => (value.clone(), *version),
                        _ => (None, 0),
                    };
                    if let Some((prev_value, prev_version, _)) =
                        canary_last.get(&key.to_vec()).cloned()
                    {
                        canary_prev.insert(key.to_vec(), (prev_value, prev_version));
                    }
                    canary_last.insert(key.to_vec(), (observed.0, observed.1, t));
                }
                history.push(HistoryOp {
                    id: 0,
                    client: op.client as u64,
                    key,
                    op: client_op,
                    invoke,
                    ret: Some(t),
                    result: Some(result),
                    indeterminate: false,
                });
            } else {
                history.push(HistoryOp {
                    id: 0,
                    client: op.client as u64,
                    key,
                    op: client_op,
                    invoke,
                    ret: Some(deadline),
                    result: None,
                    indeterminate: true,
                });
            }
        }
        ScheduledKind::Get | ScheduledKind::Range => {
            let invoke = tick;
            if stale_canary
                && effective_kind == ScheduledKind::Get
                && !is_list
                && let Some((_, _, write_ret)) = canary_last.get(&key.to_vec())
                && *write_ret < invoke
            {
                let (stale_value, stale_version) =
                    canary_prev.get(&key.to_vec()).cloned().unwrap_or((None, 0));
                history.push(HistoryOp {
                    id: 0,
                    client: op.client as u64,
                    key,
                    op: ClientOp::Get,
                    invoke,
                    ret: Some(invoke + 1),
                    result: Some(OpResult::Value {
                        value: stale_value,
                        version: stale_version,
                    }),
                    indeterminate: false,
                });
                return;
            }
            if stale_canary {
                let target = most_lagging_live(cluster, applied, leader);
                let result = observe_read(stores, &target, &key, effective_kind, is_list);
                history.push(HistoryOp {
                    id: 0,
                    client: op.client as u64,
                    key,
                    op: if effective_kind == ScheduledKind::Get {
                        ClientOp::Get
                    } else {
                        ClientOp::Range { limit: 8 }
                    },
                    invoke,
                    ret: Some(invoke + 1),
                    result: Some(result),
                    indeterminate: false,
                });
                return;
            }
            let use_follower = op.client % 2 == 1;
            if use_follower {
                let followers: Vec<NodeId> = cluster
                    .nodes
                    .keys()
                    .copied()
                    .filter(|id| Some(*id) != cluster.leader())
                    .collect();
                let Some(target) = followers.first().copied().or(cluster.leader()) else {
                    return;
                };
                let deadline = invoke + profile.timeout_ticks;
                let mut t = invoke;
                let mut satisfied = false;
                while t < deadline {
                    cluster.tick();
                    t += 1;
                    apply_all(cluster, stores, applied, applied_terms, harness);
                    snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
                    // Wait on the follower's own commit index, not the
                    // workload's applied map: a commit advance carries a term
                    // match, so the prefix below it is agreed with a leader.
                    // Raw applied indexes can cross leader changes. The
                    // leader's number only counts once it committed in-term;
                    // a fresh leader's stale commit would end the wait early.
                    if let Some(current) = cluster.leader()
                        && let Some(leader_commit) = leader_interm_commit(cluster, current)
                        && cluster
                            .nodes
                            .get(&target)
                            .is_some_and(|core| core.commit_index() >= leader_commit)
                    {
                        satisfied = true;
                        break;
                    }
                }
                let completed = satisfied;
                if completed {
                    let result = observe_read(stores, &target, &key, effective_kind, is_list);
                    if let OpResult::Value { version, .. } | OpResult::List { version, .. } =
                        &result
                    {
                        client.key_versions.insert(key.to_vec(), *version);
                    }
                    history.push(HistoryOp {
                        id: 0,
                        client: op.client as u64,
                        key,
                        op: if effective_kind == ScheduledKind::Get {
                            ClientOp::Get
                        } else {
                            ClientOp::Range { limit: 8 }
                        },
                        invoke,
                        ret: Some(t),
                        result: Some(result),
                        indeterminate: false,
                    });
                } else {
                    history.push(HistoryOp {
                        id: 0,
                        client: op.client as u64,
                        key,
                        op: if effective_kind == ScheduledKind::Get {
                            ClientOp::Get
                        } else {
                            ClientOp::Range { limit: 8 }
                        },
                        invoke,
                        ret: Some(deadline),
                        result: None,
                        indeterminate: true,
                    });
                }
            } else {
                let deadline = invoke + profile.timeout_ticks;
                let mut t = invoke;
                // A fresh leader reports a meaningless commit until its no-op
                // replicates. Issuing the read before that risks serving a
                // prefix that misses committed writes.
                let mut ready = leader_interm_commit(cluster, leader).is_some();
                while !ready && t < deadline {
                    cluster.tick();
                    t += 1;
                    apply_all(cluster, stores, applied, applied_terms, harness);
                    snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
                    ready = cluster.leader() == Some(leader)
                        && leader_interm_commit(cluster, leader).is_some();
                }
                if !ready {
                    history.push(HistoryOp {
                        id: 0,
                        client: op.client as u64,
                        key,
                        op: if effective_kind == ScheduledKind::Get {
                            ClientOp::Get
                        } else {
                            ClientOp::Range { limit: 8 }
                        },
                        invoke,
                        ret: Some(deadline),
                        result: None,
                        indeterminate: true,
                    });
                    return;
                }
                let context =
                    Bytes::from(format!("r{}-{}", op.client, op.seq_in_client).into_bytes());
                cluster.step_node(
                    leader,
                    Input::ReadIndex {
                        context: context.clone(),
                    },
                );
                let mut confirmed: Option<LogIndex> = None;
                while t < deadline {
                    cluster.tick();
                    t += 1;
                    apply_all(cluster, stores, applied, applied_terms, harness);
                    snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
                    let reads = cluster.take_completed_reads(leader);
                    for read in reads {
                        if read.context == context {
                            confirmed = Some(read.read_index);
                            break;
                        }
                    }
                    if confirmed.is_some() {
                        break;
                    }
                }
                if let Some(read_index) = confirmed {
                    let mut waited = t;
                    while waited < deadline {
                        apply_all(cluster, stores, applied, applied_terms, harness);
                        snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
                        if applied.get(&leader).copied().unwrap_or_default() >= read_index {
                            break;
                        }
                        cluster.tick();
                        waited += 1;
                    }
                    // The confirmation is only valid while the same leader
                    // still holds a quorum. Reading a deposed leader's store
                    // can serve uncommitted suffix entries.
                    if cluster.leader() == Some(leader) {
                        let result = observe_read(stores, &leader, &key, effective_kind, is_list);
                        if let OpResult::Value { version, .. } | OpResult::List { version, .. } =
                            &result
                        {
                            client.key_versions.insert(key.to_vec(), *version);
                        }
                        history.push(HistoryOp {
                            id: 0,
                            client: op.client as u64,
                            key,
                            op: if effective_kind == ScheduledKind::Get {
                                ClientOp::Get
                            } else {
                                ClientOp::Range { limit: 8 }
                            },
                            invoke,
                            ret: Some(waited),
                            result: Some(result),
                            indeterminate: false,
                        });
                    } else {
                        history.push(HistoryOp {
                            id: 0,
                            client: op.client as u64,
                            key,
                            op: if effective_kind == ScheduledKind::Get {
                                ClientOp::Get
                            } else {
                                ClientOp::Range { limit: 8 }
                            },
                            invoke,
                            ret: Some(waited),
                            result: None,
                            indeterminate: true,
                        });
                    }
                } else {
                    history.push(HistoryOp {
                        id: 0,
                        client: op.client as u64,
                        key,
                        op: if effective_kind == ScheduledKind::Get {
                            ClientOp::Get
                        } else {
                            ClientOp::Range { limit: 8 }
                        },
                        invoke,
                        ret: Some(deadline),
                        result: None,
                        indeterminate: true,
                    });
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_all(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
) {
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    for id in ids {
        let Some(core) = cluster.nodes.get(&id) else {
            continue;
        };
        let commit = core.commit_index();
        let from = applied.get(&id).copied().unwrap_or_default();
        // A truncated suffix (conflict repair or torn-tail recovery) leaves
        // store effects the log no longer has. Detect the divergence and
        // replay from scratch: small sim logs make the replay cheap and
        // exact. A prefix dropped by snapshot compaction is not a
        // divergence: an install converges the store instead of a replay.
        let diverged = if from.is_zero() || from < core.log().base_index() {
            false
        } else {
            !matches!(
                (core.log().term_at(from), applied_terms.get(&id).copied()),
                (Some(term), Some((at, recorded))) if at == from && recorded == term
            )
        };
        if diverged {
            let base = core.log().base_index();
            let usable = harness
                .snapshots
                .get(&id)
                .filter(|(at, _)| *at >= base && !at.is_zero())
                .cloned();
            if base.is_zero() || usable.is_none() {
                if base.is_zero() {
                    stores.insert(id, KvStore::new(KvConfig::default()));
                    applied.insert(id, LogIndex::from_raw(0));
                    applied_terms.remove(&id);
                }
            } else if let Some((at, bytes)) = usable {
                if let Ok(restored) = KvStore::decode_snapshot(KvConfig::default(), &bytes) {
                    let term = core.log().term_at(at).unwrap_or_default();
                    stores.insert(id, restored);
                    applied.insert(id, at);
                    applied_terms.insert(id, (at, term));
                }
            }
        }
        let mut idx = applied.get(&id).copied().unwrap_or_default().as_u64() + 1;
        while idx <= commit.as_u64() {
            // A compacted prefix reports no entry. Advancing past it would
            // certify state the machine never applied, so stop and wait for
            // the snapshot install that carries the missing effects.
            let Some(entry) = core.log().entry_at(LogIndex::from_raw(idx)) else {
                break;
            };
            let (index, kind, term, payload) =
                (entry.index, entry.kind, entry.term, entry.payload.clone());
            if kind == anvil_types::EntryKind::NoOp {
                harness.archive.insert(index, (term, None));
                applied.insert(id, index);
                applied_terms.insert(id, (index, term));
            } else {
                if let Some(store) = stores.get_mut(&id) {
                    let _ = store.apply(index, &payload);
                }
                harness.archive.insert(index, (term, Some(payload.clone())));
                harness
                    .since_entries
                    .entry(id)
                    .and_modify(|n| *n = n.saturating_add(1))
                    .or_insert(1);
                harness
                    .since_bytes
                    .entry(id)
                    .and_modify(|n| *n = n.saturating_add(payload.len() as u64))
                    .or_insert(payload.len() as u64);
                applied.insert(id, index);
                applied_terms.insert(id, (index, term));
            }
            idx += 1;
        }
        // A restarted core resumes with commit zero while the workload's
        // applied marker survives the crash. Capping the notice at commit
        // keeps the core's applied-at-most-commit bound exact; the marker
        // converges as the node re-commits.
        let advanced = applied.get(&id).copied().unwrap_or_default();
        if let Some(core) = cluster.nodes.get(&id) {
            let capped = advanced.min(core.commit_index());
            if !capped.is_zero() {
                cluster.notify_applied(id, capped);
            }
        }
    }
}

/// Installs core-verified snapshots, supplies snapshot bytes for pending
/// requests, and takes policy-triggered snapshots with compaction. Runs
/// after every apply pass so installs land before client reads execute.
#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
fn snapshot_maintenance(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
) {
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    for id in &ids {
        let ready = cluster.ready_snapshots.remove(id).unwrap_or_default();
        for snap in &ready {
            install_snapshot_state(cluster, stores, applied, applied_terms, harness, *id, snap);
        }
    }
    for id in &ids {
        let requests = cluster.snapshot_requests.remove(id).unwrap_or_default();
        for request in &requests {
            supply_snapshot_for(cluster, stores, applied, harness, *id, request.least_index);
        }
    }
    for id in &ids {
        if cluster.is_node_crashed(*id) {
            continue;
        }
        let entries = harness.since_entries.get(id).copied().unwrap_or(0);
        let bytes = harness.since_bytes.get(id).copied().unwrap_or(0);
        if !harness.policy.should_snapshot(entries, bytes) {
            continue;
        }
        let Some(at) = applied.get(id).copied() else {
            continue;
        };
        if at.is_zero() {
            continue;
        }
        take_snapshot_for(cluster, stores, harness, *id, at);
    }
}

/// Encodes a node's state machine, checks `SnapshotEqualsReplay` against the
/// committed archive, supplies the chunks to the core, and compacts the
/// covered log prefix.
fn take_snapshot_for(
    cluster: &mut SimCluster,
    stores: &BTreeMap<NodeId, KvStore>,
    harness: &mut SnapshotHarness,
    id: NodeId,
    at: LogIndex,
) {
    let Some(store) = stores.get(&id) else {
        return;
    };
    let handle = store.snapshot_handle();
    assert_snapshot_equals_replay(&handle, &harness.archive);
    let full = handle.encode_snapshot();
    let digest = anvil_kv::snapshot_digest(&full);
    let chunks = anvil_kv::split_into_chunks(&full, harness.chunk_bytes);
    let mut total: u64 = 0;
    let mut data = Vec::with_capacity(chunks.len());
    for chunk in &chunks {
        total = total.saturating_add(chunk.payload.len() as u64);
        data.push(SnapshotChunkData {
            chunk: chunk.payload.clone(),
            chunk_crc: chunk.crc,
        });
    }
    let snapped_term = applied_term_for(cluster, id, at);
    cluster.supply_snapshot(id, at, snapped_term, total, digest, data);
    harness.snapshots_taken = harness.snapshots_taken.saturating_add(1);
    harness.snapshots.insert(id, (at, full.clone()));
    harness.since_entries.insert(id, 0);
    harness.since_bytes.insert(id, 0);
    harness.last_snapshot.insert(id, at);
    if let Some(up_to) = harness.policy.compaction_index(at, at, true) {
        cluster.compact_log(id, up_to);
    }
}

/// Replays the committed archive into the engine up to the installed
/// snapshot, closing the gap a snapshot install leaves between the wiped
/// core log and the disk. Archive entries are committed, hence stable, so
/// the backfill matches what the leader holds.
fn backfill_engine_from_archive(
    cluster: &mut SimCluster,
    harness: &SnapshotHarness,
    id: NodeId,
    up_to: LogIndex,
) {
    let Some(tip) = cluster.engine_last_index(id) else {
        return;
    };
    if tip >= up_to {
        return;
    }
    let mut entries = Vec::new();
    let mut idx = tip.as_u64() + 1;
    while idx <= up_to.as_u64() {
        let at = LogIndex::from_raw(idx);
        let Some((term, payload)) = harness.archive.get(&at) else {
            return;
        };
        let (kind, bytes) = match payload {
            Some(bytes) => (anvil_types::EntryKind::Normal, bytes.clone()),
            None => (anvil_types::EntryKind::NoOp, Bytes::new()),
        };
        entries.push(anvil_types::Entry {
            index: at,
            term: *term,
            kind,
            payload: bytes,
        });
        idx += 1;
    }
    let _ = cluster.backfill_log_prefix(id, entries);
}

/// Returns the term covering `at`: the applied entry's term, or the
/// compacted base term when the prefix is already snapshotted away.
fn applied_term_for(cluster: &SimCluster, id: NodeId, at: LogIndex) -> anvil_types::Term {
    cluster
        .nodes
        .get(&id)
        .and_then(|core| core.log().term_at(at))
        .unwrap_or(anvil_types::Term::from_raw(0))
}

/// Supplies one pending snapshot request from the node's applied state.
#[allow(clippy::too_many_arguments)]
fn supply_snapshot_for(
    cluster: &mut SimCluster,
    stores: &BTreeMap<NodeId, KvStore>,
    applied: &BTreeMap<NodeId, LogIndex>,
    harness: &mut SnapshotHarness,
    id: NodeId,
    least: LogIndex,
) {
    let Some(at) = applied.get(&id).copied() else {
        return;
    };
    assert!(
        at.as_u64() + 1 >= least.as_u64(),
        "snapshot need beyond applied state"
    );
    if at.is_zero() {
        return;
    }
    take_snapshot_for(cluster, stores, harness, id, at);
}

/// Installs a verified snapshot into the state machine and advances the
/// applied marker past the compacted prefix.
fn install_snapshot_state(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
    id: NodeId,
    snap: &ReceivedSnapshot,
) {
    let current = applied.get(&id).copied().unwrap_or_default();
    if snap.last_included_index <= current {
        return;
    }

    if anvil_kv::snapshot_digest(&snap.data) != snap.snapshot_digest {
        return;
    }
    let config = KvConfig::default();
    let Ok(restored) = KvStore::decode_snapshot(config, &snap.data) else {
        return;
    };
    if restored.applied_index() != snap.last_included_index {
        return;
    }
    if let Some(storage) = cluster.storage.get_mut(&id) {
        storage.engine.discard_gap_buffer();
    }
    stores.insert(id, restored);
    applied.insert(id, snap.last_included_index);
    applied_terms.insert(id, (snap.last_included_index, snap.last_included_term));
    harness.last_snapshot.insert(id, snap.last_included_index);
    harness.since_entries.insert(id, 0);
    harness.since_bytes.insert(id, 0);
    harness.installs = harness.installs.saturating_add(1);
    harness
        .snapshots
        .insert(id, (snap.last_included_index, snap.data.clone()));
    backfill_engine_from_archive(cluster, harness, id, snap.last_included_index);
    if let Some(core) = cluster.nodes.get(&id) {
        let capped = snap.last_included_index.min(core.commit_index());
        if !capped.is_zero() {
            cluster.notify_applied(id, capped);
        }
    }
}

/// Finds the first `RegisterSession` entry the leader appended after
/// `before_idx`. Its index is the deterministic session id on every replica.
fn find_register_session(
    cluster: &SimCluster,
    leader: NodeId,
    before_idx: u64,
) -> Option<SessionId> {
    let core = cluster.nodes.get(&leader)?;
    let last = core.last_log_index().as_u64();
    let mut idx = before_idx + 1;
    while idx <= last {
        if let Some(entry) = core.log().entry_at(LogIndex::from_raw(idx))
            && entry.kind == anvil_types::EntryKind::Normal
        {
            let config = KvConfig::default();
            if let Ok(KvCommand::RegisterSession) =
                KvCommand::decode(&entry.payload, config.max_key_bytes, config.max_value_bytes)
            {
                return Some(SessionId::from_raw(idx));
            }
        }
        idx += 1;
    }
    None
}

fn write_visible_on_leader(
    stores: &BTreeMap<NodeId, KvStore>,
    leader: &NodeId,
    key: &Bytes,
    value: &Bytes,
    kind: ScheduledKind,
) -> bool {
    let Some(store) = stores.get(leader) else {
        return false;
    };
    match kind {
        ScheduledKind::Put => match store.read_register(key) {
            KvResponse::Value { value: Some(v), .. } => v == *value,
            _ => false,
        },
        ScheduledKind::Append => match store.read_list(key) {
            KvResponse::List { values, .. } => values.last() == Some(value),
            _ => false,
        },
        ScheduledKind::Delete => {
            matches!(
                store.read_register(key),
                KvResponse::Value { value: None, .. }
            )
        }
        _ => false,
    }
}

fn delete_committed(
    stores: &BTreeMap<NodeId, KvStore>,
    leader: &NodeId,
    key: &Bytes,
    before: LogIndex,
) -> bool {
    let Some(store) = stores.get(leader) else {
        return false;
    };
    match store.read_register(key) {
        KvResponse::Value {
            value: None,
            version,
        } => version > before,
        _ => false,
    }
}

fn observe_write_on_leader(
    stores: &BTreeMap<NodeId, KvStore>,
    leader: &NodeId,
    key: &Bytes,
    op: &ClientOp,
) -> (OpResult, Option<u64>) {
    let Some(store) = stores.get(leader) else {
        return (
            OpResult::Value {
                value: None,
                version: 0,
            },
            Some(0),
        );
    };
    match op {
        ClientOp::Put { .. } => match store.read_register(key) {
            KvResponse::Value { value, version } => (
                OpResult::Value {
                    value,
                    version: version.as_u64(),
                },
                Some(version.as_u64()),
            ),
            _ => (
                OpResult::Value {
                    value: None,
                    version: 0,
                },
                Some(0),
            ),
        },
        ClientOp::Delete => match store.read_register(key) {
            KvResponse::Value { version, .. } => (
                OpResult::Value {
                    value: None,
                    version: version.as_u64(),
                },
                Some(version.as_u64()),
            ),
            _ => (OpResult::Ok, None),
        },
        ClientOp::Append { .. } => match store.read_list(key) {
            KvResponse::List { values, version } => (
                OpResult::List {
                    values,
                    version: version.as_u64(),
                },
                Some(version.as_u64()),
            ),
            _ => (
                OpResult::List {
                    values: Vec::new(),
                    version: 0,
                },
                Some(0),
            ),
        },
        _ => (
            OpResult::Value {
                value: None,
                version: 0,
            },
            Some(0),
        ),
    }
}

fn observe_read(
    stores: &BTreeMap<NodeId, KvStore>,
    node: &NodeId,
    key: &Bytes,
    kind: ScheduledKind,
    is_list: bool,
) -> OpResult {
    let Some(store) = stores.get(node) else {
        return OpResult::Value {
            value: None,
            version: 0,
        };
    };
    match kind {
        ScheduledKind::Range => match store.read_range(&[], &[], 8) {
            KvResponse::Range { pairs } => OpResult::Range { pairs },
            _ => OpResult::Range { pairs: Vec::new() },
        },
        ScheduledKind::Get => {
            if is_list {
                match store.read_list(key) {
                    KvResponse::List { values, version } => OpResult::List {
                        values,
                        version: version.as_u64(),
                    },
                    _ => OpResult::List {
                        values: Vec::new(),
                        version: 0,
                    },
                }
            } else {
                match store.read_register(key) {
                    KvResponse::Value { value, version } => OpResult::Value {
                        value,
                        version: version.as_u64(),
                    },
                    _ => OpResult::Value {
                        value: None,
                        version: 0,
                    },
                }
            }
        }
        _ => OpResult::Ok,
    }
}

/// Liveness checks after the heal phase. A leader must exist within ten
/// times the maximum election timeout, and a fresh client write must commit
/// within a bounded virtual time. Anything else is a violation with
/// diagnostics: the assertion message carries the seed and the stuck state.
#[allow(clippy::too_many_arguments)]
fn liveness_check(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    harness: &mut SnapshotHarness,
    node_ids: &[NodeId],
    base_skew: &BTreeMap<NodeId, u64>,
    seed: u64,
) {
    let mut waited = 0u64;
    while cluster.leader().is_none() && waited < 100 {
        heal_step(cluster, node_ids, base_skew);
        cluster.tick();
        waited += 1;
        apply_all(cluster, stores, applied, applied_terms, harness);
        snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
    }
    assert!(
        cluster.leader().is_some(),
        "liveness: no leader within 100 ticks after heal (seed {seed})"
    );
    if let Some(leader) = cluster.leader() {
        let tick_now = stores
            .get(&leader)
            .map_or(0, anvil_kv::KvStore::virtual_tick);
        let mut probe = Vec::new();
        KvCommand::ClockTick {
            tick: tick_now.saturating_add(1),
        }
        .encode(&mut probe);
        // Re-propose on leader change: a probe committed anywhere is durable
        // and every future leader carries it, while an uncommitted probe needs
        // a fresh proposal on the current leader, exactly like a client retry.
        // The tick value is constant so retries never run virtual time forward.
        // The probe index, not the pre-probe commit, is the bar: followers
        // that merely re-reach the old commit would pass a stale comparison
        // while still missing the fresh write. Convergence is re-checked with
        // the drain afterwards so applied indexes even out before the audit.
        cluster.propose(leader, Bytes::copy_from_slice(&probe));
        let mut need = cluster
            .nodes
            .get(&leader)
            .map_or(LogIndex::from_raw(0), |core| core.last_log_index());
        let mut advanced = false;
        waited = 0;
        while waited < 200 {
            heal_step(cluster, node_ids, base_skew);
            cluster.tick();
            waited += 1;
            apply_all(cluster, stores, applied, applied_terms, harness);
            snapshot_maintenance(cluster, stores, applied, applied_terms, harness);
            let converged = cluster.nodes.keys().copied().all(|id| {
                cluster.is_node_crashed(id)
                    || cluster
                        .nodes
                        .get(&id)
                        .is_some_and(|core| core.commit_index() >= need)
            });
            if converged && cluster.leader().is_some() {
                advanced = true;
                break;
            }
            if let Some(current) = cluster.leader() {
                // Re-propose only when the current leader lost the probe
                // (leadership changed and the uncommitted tail went with it):
                // as long as some leader holds it, waiting is correct and
                // re-proposing would only chase the bar upward.
                let lost = cluster
                    .nodes
                    .get(&current)
                    .is_none_or(|core| core.last_log_index() < need);
                if lost {
                    cluster.propose(current, Bytes::copy_from_slice(&probe));
                    need = cluster
                        .nodes
                        .get(&current)
                        .map_or(need, |core| core.last_log_index());
                }
            }
        }
        assert!(
            advanced,
            "liveness: fresh write did not commit within 200 ticks after heal (seed {seed})"
        );
        drain_to_convergence(
            cluster,
            stores,
            applied,
            applied_terms,
            harness,
            &cluster.nodes.keys().copied().collect::<Vec<_>>(),
            &cluster
                .nodes
                .keys()
                .copied()
                .map(|id| (id, cluster.tick_rate(id)))
                .collect::<BTreeMap<_, _>>(),
        );
    }
}

pub fn convergence_audit(
    cluster: &SimCluster,
    stores: &BTreeMap<NodeId, KvStore>,
    applied: &BTreeMap<NodeId, LogIndex>,
) -> bool {
    let live: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|id| !cluster.is_node_crashed(*id))
        .collect();
    if live.is_empty() {
        return true;
    }
    let first_applied = applied.get(&live[0]).copied().unwrap_or_default();
    let first_hash = stores.get(&live[0]).map(|s| s.state_hash());
    for id in live {
        if applied.get(&id).copied().unwrap_or_default() != first_applied {
            return false;
        }
        if stores.get(&id).map(|s| s.state_hash()) != first_hash {
            return false;
        }
    }
    true
}
