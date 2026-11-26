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

use crate::cluster::SimCluster;
use crate::device::SimDeviceConfig;
use crate::rng::DetRng;
use anvil_check::{ClientOp, History, HistoryOp, OpResult};
use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore};
use anvil_raft::Input;
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

/// Live client session state.
struct WorkloadClient {
    session: Option<SessionId>,
    next_seq: u64,
    acked: u64,
    crashed_until: u64,
    key_versions: BTreeMap<Vec<u8>, u64>,
}

/// A pre-generated cluster fault: partitions and crashes with heals.
#[derive(Debug, Clone, Copy)]
enum KvFault {
    Isolate { tick: u64, node: usize },
    Heal { tick: u64 },
    Crash { tick: u64, node: usize },
    Restart { tick: u64, node: usize },
}

fn kv_fault_schedule(seed: u64, size: usize) -> Vec<KvFault> {
    let mut rng = DetRng::new(seed, "sim/kv-faults");
    let mut faults = Vec::new();
    if rng.bernoulli(3, 4) {
        let node = rng.gen_range(0..size as u64) as usize;
        let start = rng.gen_range(30..280);
        let end = start + rng.gen_range(10..40);
        faults.push(KvFault::Isolate { tick: start, node });
        faults.push(KvFault::Heal { tick: end });
    }
    if rng.bernoulli(1, 2) {
        let node = rng.gen_range(0..size as u64) as usize;
        let crash = rng.gen_range(40..260);
        let restart = crash + rng.gen_range(5..25);
        faults.push(KvFault::Crash { tick: crash, node });
        faults.push(KvFault::Restart {
            tick: restart,
            node,
        });
    }
    faults.sort_by_key(|fault| match *fault {
        KvFault::Isolate { tick, .. }
        | KvFault::Heal { tick }
        | KvFault::Crash { tick, .. }
        | KvFault::Restart { tick, .. } => tick,
    });
    faults
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
    run_kv_seed_inner(seed, false, stale_canary)
}

/// Runs one KV seed with storage faults enabled.
pub fn run_kv_seed_with_storage(seed: u64, stale_canary: bool) -> (History, bool) {
    run_kv_seed_inner(seed, true, stale_canary)
}

#[allow(clippy::too_many_lines)]
fn run_kv_seed_inner(seed: u64, storage_faults: bool, stale_canary: bool) -> (History, bool) {
    let profile = swarm_profile(seed);
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
    cluster.plan_net = Some(crate::cluster::PlanNet {
        salt: seed,
        drop_pct: 8,
        dup_pct: 4,
        max_delay: 2,
    });

    let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    for (i, id) in node_ids.iter().enumerate() {
        let rate = match (seed >> (i * 2)) % 4 {
            0 => 500,
            1 => 1000,
            2 => 1000,
            _ => 2000,
        };
        cluster.set_tick_skew(*id, rate);
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
    let faults = kv_fault_schedule(seed, node_ids.len());
    let mut fault_cursor = 0;
    // Canary state: the last two observed register values per key, with the
    // completion tick of the latest write. A canary read returns the older
    // pair once its successor completed, which is exactly the stale read a
    // ReadIndex without heartbeat confirmation would serve.
    let mut canary_last: BTreeMap<Vec<u8>, (Option<Bytes>, u64, u64)> = BTreeMap::new();
    let mut canary_prev: BTreeMap<Vec<u8>, (Option<Bytes>, u64)> = BTreeMap::new();
    cluster.tick_n(15);
    tick += 15;
    apply_all(&mut cluster, &mut stores, &mut applied, &mut applied_terms);

    while tick < end_tick && op_cursor < schedule.len() {
        let op = &schedule[op_cursor];
        while tick < op.invoke_tick && tick < end_tick {
            tick = advance_with_faults(
                &mut cluster,
                &mut stores,
                &mut applied,
                &mut applied_terms,
                &node_ids,
                &faults,
                &mut fault_cursor,
                tick,
            );
        }
        if tick >= end_tick {
            break;
        }
        tick = advance_with_faults(
            &mut cluster,
            &mut stores,
            &mut applied,
            &mut applied_terms,
            &node_ids,
            &faults,
            &mut fault_cursor,
            tick,
        );
        execute_scheduled(
            &mut cluster,
            &mut stores,
            &mut applied,
            &mut applied_terms,
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
        apply_all(&mut cluster, &mut stores, &mut applied, &mut applied_terms);
    }

    cluster.plan_net = None;
    cluster.heal_all();
    drain_to_convergence(&mut cluster, &mut stores, &mut applied, &mut applied_terms);

    let audit = convergence_audit(&cluster, &stores, &applied);
    (history, audit)
}

/// Ticks a healed cluster until every live replica shares the same commit
/// and applied index, or a fixed bound lapses. The bound keeps replay
/// deterministic; the early exit only shortens idle ticks.
fn drain_to_convergence(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
) {
    for _ in 0..500 {
        cluster.tick();
        apply_all(cluster, stores, applied, applied_terms);
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

/// Advances one outer tick, firing any due pre-generated faults first.
fn advance_with_faults(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
    node_ids: &[NodeId],
    faults: &[KvFault],
    fault_cursor: &mut usize,
    tick: u64,
) -> u64 {
    while *fault_cursor < faults.len() {
        let due = match faults[*fault_cursor] {
            KvFault::Isolate { tick: ft, .. }
            | KvFault::Heal { tick: ft }
            | KvFault::Crash { tick: ft, .. }
            | KvFault::Restart { tick: ft, .. } => ft <= tick,
        };
        if !due {
            break;
        }
        match faults[*fault_cursor] {
            KvFault::Isolate { node, .. } => {
                if let Some(id) = node_ids.get(node) {
                    let peers: Vec<NodeId> = node_ids.to_vec();
                    cluster.network.isolate(*id, &peers);
                }
            }
            KvFault::Heal { .. } => cluster.network.heal_all(),
            KvFault::Crash { node, .. } => {
                if let Some(id) = node_ids.get(node) {
                    cluster.crash_node(*id);
                }
            }
            KvFault::Restart { node, .. } => {
                if let Some(id) = node_ids.get(node)
                    && !cluster.is_node_needs_repair(*id)
                    && !cluster.is_node_refused_start(*id)
                {
                    let _ = cluster.restart_node(*id);
                }
            }
        }
        *fault_cursor += 1;
    }
    cluster.tick();
    apply_all(cluster, stores, applied, applied_terms);
    tick + 1
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
            apply_all(cluster, stores, applied, applied_terms);
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
                apply_all(cluster, stores, applied, applied_terms);
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
                    apply_all(cluster, stores, applied, applied_terms);
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
                    apply_all(cluster, stores, applied, applied_terms);
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
                    apply_all(cluster, stores, applied, applied_terms);
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
                        apply_all(cluster, stores, applied, applied_terms);
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

fn apply_all(
    cluster: &mut SimCluster,
    stores: &mut BTreeMap<NodeId, KvStore>,
    applied: &mut BTreeMap<NodeId, LogIndex>,
    applied_terms: &mut BTreeMap<NodeId, (LogIndex, anvil_types::Term)>,
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
        // exact. Snapshots replace this in Week 5.
        let diverged = if from.is_zero() {
            false
        } else {
            !matches!(
                (core.log().term_at(from), applied_terms.get(&id).copied()),
                (Some(term), Some((at, recorded))) if at == from && recorded == term
            )
        };
        if diverged {
            stores.insert(id, KvStore::new(KvConfig::default()));
            applied.insert(id, LogIndex::from_raw(0));
            applied_terms.remove(&id);
        }
        let mut idx = applied.get(&id).copied().unwrap_or_default().as_u64() + 1;
        while idx <= commit.as_u64() {
            if let Some(entry) = core.log().entry_at(LogIndex::from_raw(idx)) {
                let (index, kind, term, payload) =
                    (entry.index, entry.kind, entry.term, entry.payload.clone());
                if kind == anvil_types::EntryKind::NoOp {
                    applied.insert(id, index);
                    applied_terms.insert(id, (index, term));
                } else {
                    if let Some(store) = stores.get_mut(&id) {
                        let _ = store.apply(index, &payload);
                    }
                    applied.insert(id, index);
                    applied_terms.insert(id, (index, term));
                }
            }
            idx += 1;
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

/// After the heal phase, every live replica must agree on applied index and
/// state hash.
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
