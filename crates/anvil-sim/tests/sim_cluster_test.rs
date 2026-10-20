#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use anvil_raft::{RaftConfig, RaftCore, RecoveredState, Role};
use anvil_sim::{InvariantViolation, RaftInvariantChecker, SimCluster};
use anvil_types::{LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, Ordering};

#[test]
fn test_three_node_cluster_election_and_replication() {
    let mut cluster = SimCluster::new(3, 42);

    // Initial state: no leader
    assert!(cluster.leader().is_none());

    // Advance ticks until leader is elected
    let leader_id = cluster.elect_leader(100);
    assert_eq!(cluster.nodes[&leader_id].role(), Role::Leader);

    // Let initial leader no-op commit across cluster via heartbeats
    cluster.tick_n(10);
    let initial_commit = cluster.highest_commit_index();
    assert!(initial_commit >= LogIndex::from_raw(1));

    // Propose client payload
    let payload = Bytes::from_static(b"cmd_key_val_1");
    cluster.propose(leader_id, payload);

    // Tick cluster for replication and heartbeat commit propagation
    cluster.tick_n(10);

    let final_commit = cluster.highest_commit_index();
    assert!(final_commit > initial_commit);

    // Verify all 3 nodes committed the new entry
    for core in cluster.nodes.values() {
        assert_eq!(core.commit_index(), final_commit);
    }
}

#[test]
fn test_five_and_seven_node_clusters() {
    for size in [5, 7] {
        let mut cluster = SimCluster::new(size, 100 + size as u64);
        let leader_id = cluster.elect_leader(150);
        cluster.tick_n(10);

        cluster.propose(leader_id, Bytes::from_static(b"cluster_proposal"));
        cluster.tick_n(10);

        let highest = cluster.highest_commit_index();
        assert!(highest >= LogIndex::from_raw(1));
        assert!(cluster.checker.committed_entries_count() >= 1);
    }
}

#[test]
fn test_minority_partition_progress() {
    let mut cluster = SimCluster::new(5, 777);
    let leader_id = cluster.elect_leader(100);
    cluster.tick_n(10);

    // Partition 2 minority follower nodes away from the 3 majority nodes
    let non_leaders: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|&id| id != leader_id)
        .collect();

    let minority = &non_leaders[0..2];
    let mut majority = vec![leader_id];
    majority.extend_from_slice(&non_leaders[2..]);

    cluster.partition(minority, &majority);

    // Propose command on leader; majority should commit even with minority isolated
    cluster.propose(leader_id, Bytes::from_static(b"isolated_minority_cmd"));
    cluster.tick_n(15);

    for &maj_id in &majority {
        assert!(cluster.nodes[&maj_id].commit_index() >= LogIndex::from_raw(2));
    }

    // Heal partition; minority should catch up
    cluster.heal_all();
    cluster.tick_n(15);

    for core in cluster.nodes.values() {
        assert!(core.commit_index() >= LogIndex::from_raw(2));
    }
}

#[test]
fn test_majority_partition_and_recovery() {
    let mut cluster = SimCluster::new(5, 888);
    let leader_id = cluster.elect_leader(100);
    cluster.tick_n(10);

    let initial_commit = cluster.nodes[&leader_id].commit_index();

    // Isolate the leader from all 4 followers (forming a 1 vs 4 partition)
    cluster.isolate_node(leader_id);

    // Leader proposes without quorum: cannot commit
    cluster.propose(leader_id, Bytes::from_static(b"uncommittable_cmd"));
    cluster.tick_n(30);
    assert_eq!(cluster.nodes[&leader_id].commit_index(), initial_commit);

    // The remaining 4 nodes form a quorum and must elect a new leader
    let mut new_leader = None;
    for _ in 0..150 {
        cluster.tick();
        for (&id, core) in &cluster.nodes {
            if id != leader_id && core.role() == Role::Leader {
                new_leader = Some(id);
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
    }
    let new_leader_id = new_leader.expect("new leader must be elected by majority partition");
    assert_ne!(new_leader_id, leader_id);

    // Heal cluster: former isolated leader steps down, updates term and synchronizes log
    cluster.heal_all();
    cluster.tick_n(20);

    assert_eq!(cluster.nodes[&leader_id].role(), Role::Follower);
    assert!(cluster.nodes[&leader_id].term() >= cluster.nodes[&new_leader_id].term());
}

#[test]
fn test_unreliable_network_drops_duplicates_delays() {
    let mut cluster = SimCluster::new(3, 999);

    let drop_counter = std::sync::Arc::new(AtomicU64::new(0));
    let drop_clone = drop_counter.clone();
    cluster.drop_hook = Some(Box::new(move |_from, _to, _msg| {
        let count = drop_clone.fetch_add(1, Ordering::Relaxed);
        // Drop every 5th packet
        count.is_multiple_of(5)
    }));

    let dup_counter = std::sync::Arc::new(AtomicU64::new(0));
    let dup_clone = dup_counter.clone();
    cluster.duplicate_hook = Some(Box::new(move |_from, _to, _msg| {
        let count = dup_clone.fetch_add(1, Ordering::Relaxed);
        // Duplicate every 7th packet
        count.is_multiple_of(7)
    }));

    cluster.delay_hook = Some(Box::new(|_from, _to, _msg| {
        // Delay delivery by 2 ticks
        2
    }));

    // Even with network unreliability, election and commitment succeed and invariants hold
    let leader_id = cluster.elect_leader(250);
    cluster.drain_network(60);

    cluster.propose(leader_id, Bytes::from_static(b"unreliable_network_entry"));
    cluster.drain_network(100);

    assert!(cluster.highest_commit_index() >= LogIndex::from_raw(1));
}

#[test]
fn test_invariant_checker_catches_violations() {
    let mut checker = RaftInvariantChecker::new();

    let node1 = NodeId::new(1).expect("valid id");
    let node2 = NodeId::new(2).expect("valid id");

    let mut peers1 = BTreeSet::new();
    peers1.insert(node2);
    let mut peers2 = BTreeSet::new();
    peers2.insert(node1);

    let config1 = RaftConfig {
        node_id: node1,
        peers: peers1,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };
    let config2 = RaftConfig {
        node_id: node2,
        peers: peers2,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };

    let rng1 = ChaCha8Rng::from_seed([1u8; 32]);
    let rng2 = ChaCha8Rng::from_seed([2u8; 32]);

    let core1 = RaftCore::new(config1, RecoveredState::default(), rng1);
    let core2 = RaftCore::new(config2, RecoveredState::default(), rng2);

    // Initial state is valid
    let mut cluster_nodes = BTreeMap::new();
    cluster_nodes.insert(node1, core1);
    cluster_nodes.insert(node2, core2);
    assert!(checker.try_check_invariants(&cluster_nodes).is_ok());

    // 1. Term monotonicity violation test: simulate term regression
    cluster_nodes
        .get_mut(&node1)
        .unwrap()
        .testing_set_term(Term::from_raw(5));
    assert!(checker.try_check_invariants(&cluster_nodes).is_ok());

    cluster_nodes
        .get_mut(&node1)
        .unwrap()
        .testing_set_term(Term::from_raw(4));
    let err = checker.try_check_invariants(&cluster_nodes).unwrap_err();
    assert!(matches!(err, InvariantViolation::TermMonotonic { .. }));

    // Reset back to term 5
    cluster_nodes
        .get_mut(&node1)
        .unwrap()
        .testing_set_term(Term::from_raw(5));

    // 2. Election Safety violation test: two leaders in term 5
    cluster_nodes
        .get_mut(&node1)
        .unwrap()
        .testing_set_role(Role::Leader);
    cluster_nodes
        .get_mut(&node2)
        .unwrap()
        .testing_set_term(Term::from_raw(5));
    cluster_nodes
        .get_mut(&node2)
        .unwrap()
        .testing_set_role(Role::Leader);

    let err2 = checker.try_check_invariants(&cluster_nodes).unwrap_err();
    assert!(matches!(err2, InvariantViolation::ElectionSafety { .. }));
}
