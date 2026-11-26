#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_check::check_history;
use anvil_sim::{SimCluster, run_kv_seed, run_kv_seed_with_storage};

#[test]
fn test_kv_seed_is_linearizable_and_convergent() {
    for seed in [0u64, 1, 2, 42, 636, 777] {
        let (history, audit) = run_kv_seed(seed, false);
        assert!(!history.ops.is_empty(), "seed {seed} produced no ops");
        assert!(audit, "seed {seed} failed convergence audit");
        assert!(
            check_history(&history).is_ok(),
            "seed {seed} history failed linearizability"
        );
    }
}

#[test]
fn test_kv_seed_with_storage_faults() {
    let (history, audit) = run_kv_seed_with_storage(99, false);
    assert!(audit, "storage seed failed convergence audit");
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_canary_stale_read_is_caught() {
    let mut caught_at: Option<u64> = None;
    for seed in 0..1000u64 {
        let (history, _) = run_kv_seed(seed, true);
        if check_history(&history).is_err() {
            caught_at = Some(seed);
            break;
        }
    }
    assert!(
        caught_at.is_some(),
        "canary stale read was not caught within 1000 seeds"
    );
}

#[test]
fn test_kv_many_seeds_linearizable_and_convergent() {
    for seed in 0..150u64 {
        let (history, audit) = run_kv_seed(seed, false);
        assert!(!history.ops.is_empty(), "seed {seed} produced no ops");
        assert!(audit, "seed {seed} failed convergence audit");
        assert!(
            check_history(&history).is_ok(),
            "seed {seed} history failed linearizability"
        );
    }
}

#[test]
fn test_kv_canary_sweep_exercises_stale_paths() {
    for seed in 0..25u64 {
        let (history, _) = run_kv_seed(seed, true);
        let _ = check_history(&history);
    }
}

#[test]
fn test_kv_storage_seeds_linearizable_and_convergent() {
    for seed in [0u64, 1, 7, 42, 100, 333, 777, 999] {
        let (history, audit) = run_kv_seed_with_storage(seed, false);
        assert!(audit, "storage seed {seed} failed convergence audit");
        assert!(
            check_history(&history).is_ok(),
            "storage seed {seed} history failed linearizability"
        );
    }
}

#[test]
fn test_kv_farm_entry_points_are_clean() {
    let farm = anvil_sim::run_kv_farm(8, 2, false);
    assert!(farm.is_clean());
    assert!(farm.seeds_per_second() >= 0.0);
    assert_eq!(farm.total_seeds, 8);
    assert_eq!(farm.passed_seeds, 8);
    assert!(farm.failed_seed.is_none());
    let storage_farm = anvil_sim::run_kv_farm(4, 2, true);
    assert!(storage_farm.is_clean());
    let dirty = anvil_sim::FarmResult {
        total_seeds: 8,
        passed_seeds: 7,
        failed_seed: Some(3),
        duration: std::time::Duration::from_secs(1),
    };
    assert!(!dirty.is_clean());
    assert_eq!(dirty.seeds_per_second(), 8.0);
    let instant = anvil_sim::FarmResult {
        total_seeds: 0,
        passed_seeds: 0,
        failed_seed: None,
        duration: std::time::Duration::ZERO,
    };
    assert_eq!(instant.seeds_per_second(), 0.0);
}

#[test]
fn test_swarm_profile_is_deterministic_and_bounded() {
    let first = anvil_sim::swarm_profile(2026);
    let second = anvil_sim::swarm_profile(2026);
    assert_eq!(first.clients, second.clients);
    assert_eq!(first.keys, second.keys);
    assert_eq!(first.ops_per_client, second.ops_per_client);
    assert!((1..=8).contains(&first.clients));
    assert!((1..=16).contains(&first.keys));
    assert!(first.think_ticks >= 1);
    let unordered_seen = (0..16u64).any(|seed| anvil_sim::swarm_profile(seed).unordered);
    let ordered_seen = (0..16u64).any(|seed| !anvil_sim::swarm_profile(seed).unordered);
    assert!(unordered_seen && ordered_seen);
}

#[test]
fn test_convergence_audit_detects_divergence() {
    use anvil_kv::{KvCommand, KvConfig, KvStore};
    use anvil_types::{LogIndex, SeqNo, SessionId};
    use bytes::Bytes;
    use std::collections::BTreeMap;

    let mut cluster = SimCluster::new(3, 11);
    cluster.tick_n(20);
    let node_ids: Vec<anvil_types::NodeId> = cluster.nodes.keys().copied().collect();
    let mut stores: BTreeMap<_, _> = node_ids
        .iter()
        .map(|id| (*id, KvStore::new(KvConfig::default())))
        .collect();
    let applied: BTreeMap<_, _> = node_ids
        .iter()
        .map(|id| (*id, LogIndex::from_raw(0)))
        .collect();
    assert!(anvil_sim::convergence_audit(&cluster, &stores, &applied));

    let session = SessionId::from_raw(1);
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    stores
        .get_mut(&node_ids[0])
        .expect("store")
        .apply(LogIndex::from_raw(1), &reg)
        .expect("register");
    let mut put = Vec::new();
    KvCommand::Put {
        session,
        seq: SeqNo::from_raw(1),
        acked: SeqNo::from_raw(0),
        key: Bytes::from_static(b"diverged"),
        value: Bytes::from_static(b"v"),
    }
    .encode(&mut put);
    stores
        .get_mut(&node_ids[0])
        .expect("store")
        .apply(LogIndex::from_raw(2), &put)
        .expect("put");
    let mut applied_diverged = applied.clone();
    applied_diverged.insert(node_ids[0], LogIndex::from_raw(2));
    for id in &node_ids {
        if *id != node_ids[0] {
            applied_diverged.insert(*id, LogIndex::from_raw(2));
        }
    }
    assert!(!anvil_sim::convergence_audit(
        &cluster,
        &stores,
        &applied_diverged
    ));

    for id in &node_ids {
        cluster.crash_node(*id);
    }
    assert!(anvil_sim::convergence_audit(&cluster, &stores, &applied));
}
