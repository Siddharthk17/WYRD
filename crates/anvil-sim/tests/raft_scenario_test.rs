#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use anvil_raft::{Input, Message, Output, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_sim::SimCluster;
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn create_node(
    node_id: u64,
    peers: &[u64],
    term: u64,
    commit_idx: u64,
    log_terms: &[u64],
) -> RaftCore {
    let mut peer_set = BTreeSet::new();
    for &peer in peers {
        peer_set.insert(NodeId::new(peer).expect("valid peer id"));
    }
    let config = RaftConfig {
        node_id: NodeId::new(node_id).expect("valid node id"),
        peers: peer_set,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };

    let mut entries = Vec::with_capacity(log_terms.len());
    for (position, &term) in log_terms.iter().enumerate() {
        let index = LogIndex::from_raw((position + 1) as u64);
        entries.push(Entry::normal(
            index,
            Term::from_raw(term),
            Bytes::from(format!("payload_{position}_{term}")),
        ));
    }

    let recovered = RecoveredState {
        current_term: Term::from_raw(term),
        voted_for: None,
        commit_index: LogIndex::from_raw(commit_idx),
        entries,
    };

    let rng = ChaCha8Rng::seed_from_u64(node_id);
    RaftCore::new(config, recovered, rng)
}

/// Reproduces Raft paper Figure 7 (Ongaro & Ousterhout 2014, Page 7):
/// Top leader: term 8, log entries at terms [1, 1, 1, 4, 4, 5, 5, 6, 6, 6]
/// Tests convergence against all follower scenarios (a) through (f).
#[test]
fn test_raft_paper_figure_7_all_scenarios() {
    let leader_log_terms = [1, 1, 1, 4, 4, 5, 5, 6, 6, 6];

    let follower_scenarios: [(&str, Vec<u64>); 6] = [
        // (a) Missing entries: fewer entries than leader
        ("case_a", vec![1, 1, 1, 4, 4, 5, 5, 6, 6]),
        // (b) Missing more entries
        ("case_b", vec![1, 1, 1, 4]),
        // (c) Uncommitted extra entries from term 6
        ("case_c", vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6]),
        // (d) Uncommitted extra entries from term 7
        ("case_d", vec![1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7]),
        // (e) Divergent uncommitted entries from term 4
        ("case_e", vec![1, 1, 1, 4, 4, 4, 4]),
        // (f) Divergent uncommitted entries from terms 2 and 3
        ("case_f", vec![1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3]),
    ];

    for (case_name, follower_terms) in follower_scenarios {
        let leader_id = NodeId::new(1).expect("id 1");
        let follower_id = NodeId::new(2).expect("id 2");

        let mut leader = create_node(1, &[2], 8, 10, &leader_log_terms);
        let mut output = Output::new();
        leader.testing_become_leader(&mut output);
        let tokens: Vec<_> = output
            .persist_ops
            .iter()
            .map(anvil_raft::PersistOp::token)
            .collect();
        for token in tokens {
            output.clear();
            leader.step(Input::Persisted { token }, &mut output);
        }

        let follower = create_node(2, &[1], 8, 0, &follower_terms);

        let mut cluster = SimCluster::new(2, 42);
        cluster.nodes.insert(leader_id, leader);
        cluster.nodes.insert(follower_id, follower);

        cluster.tick_n(30);

        let follower_core = &cluster.nodes[&follower_id];
        let leader_core = &cluster.nodes[&leader_id];
        assert_eq!(
            follower_core.last_log_index(),
            leader_core.last_log_index(),
            "mismatch in {case_name}"
        );
        for idx in 1..=leader_core.last_log_index().as_u64() {
            let log_idx = LogIndex::from_raw(idx);
            assert_eq!(
                follower_core.log().term_at(log_idx),
                leader_core.log().term_at(log_idx),
                "term mismatch at index {idx} in {case_name}"
            );
        }
    }
}

/// Reproduces Figure 8 of the Raft paper:
/// An entry from an earlier term replicated to a majority CANNOT be committed
/// by counting replicas alone. Commitment only occurs once an entry from the
/// current term is committed on a majority.
#[test]
fn test_raft_paper_figure_8_commit_rule() {
    let node1 = NodeId::new(1).expect("id 1");
    let node2 = NodeId::new(2).expect("id 2");
    let node3 = NodeId::new(3).expect("id 3");

    let mut leader = create_node(1, &[2, 3], 4, 1, &[1, 2]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    let tokens: Vec<_> = output
        .persist_ops
        .iter()
        .map(anvil_raft::PersistOp::token)
        .collect();
    for token in tokens {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }

    let follower2 = create_node(2, &[1, 3], 4, 1, &[1]);
    let follower3 = create_node(3, &[1, 2], 4, 1, &[1]);

    let mut cluster = SimCluster::new(3, 888);
    // Isolate node 3 so replication only reaches follower 2
    cluster.isolate_node(node3);
    cluster.nodes.insert(node1, leader);
    cluster.nodes.insert(node2, follower2);
    cluster.nodes.insert(node3, follower3);

    assert_eq!(cluster.nodes[&node1].commit_index(), LogIndex::from_raw(1));

    cluster.tick_n(25);

    assert_eq!(
        cluster.nodes[&node1].commit_index(),
        LogIndex::from_raw(3),
        "commit_index must advance to 3 once current-term entry is acknowledged"
    );
    assert_eq!(
        cluster.nodes[&node2].commit_index(),
        LogIndex::from_raw(3),
        "follower 2 commits up to index 3"
    );
}

#[test]
fn test_split_vote_and_timeout_resolution() {
    let mut cluster = SimCluster::new(3, 12345);

    let drop_votes = true;
    cluster.drop_hook = Some(Box::new(move |_from, _to, msg| {
        if drop_votes
            && matches!(
                msg,
                Message::RequestVoteRequest { .. } | Message::PreVoteRequest { .. }
            )
        {
            return true;
        }
        false
    }));

    cluster.tick_n(15);
    assert!(
        cluster.leader().is_none(),
        "no leader should win while votes are dropped"
    );

    cluster.drop_hook = None;
    let leader = cluster.elect_leader(100);
    assert!(cluster.nodes.contains_key(&leader));
    assert_eq!(cluster.nodes[&leader].role(), Role::Leader);
}

#[test]
fn test_stale_leader_steps_down() {
    let mut cluster = SimCluster::new(5, 54321);
    let old_leader = cluster.elect_leader(100);
    cluster.tick_n(10);

    let old_term = cluster.nodes[&old_leader].term();

    cluster.isolate_node(old_leader);

    cluster.propose(old_leader, Bytes::from_static(b"stale_entry"));
    cluster.tick_n(20);
    assert_eq!(
        cluster.nodes[&old_leader].commit_index(),
        LogIndex::from_raw(1)
    );

    let mut new_leader = None;
    for _ in 0..150 {
        cluster.tick();
        for (&node_id, core) in &cluster.nodes {
            if node_id != old_leader && core.role() == Role::Leader {
                new_leader = Some(node_id);
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
    }
    let new_leader_id = new_leader.expect("new leader elected by majority");
    assert!(cluster.nodes[&new_leader_id].term() > old_term);

    cluster.propose(new_leader_id, Bytes::from_static(b"valid_new_term_entry"));
    cluster.tick_n(10);
    assert!(cluster.nodes[&new_leader_id].commit_index() >= LogIndex::from_raw(2));

    cluster.heal_all();
    cluster.tick_n(20);

    assert_eq!(cluster.nodes[&old_leader].role(), Role::Follower);
    assert_eq!(
        cluster.nodes[&old_leader].term(),
        cluster.nodes[&new_leader_id].term()
    );
    assert_eq!(
        cluster.nodes[&old_leader].commit_index(),
        cluster.nodes[&new_leader_id].commit_index()
    );
}

#[test]
fn test_log_divergence_and_truncation() {
    let node1 = NodeId::new(1).expect("id 1");
    let node2 = NodeId::new(2).expect("id 2");

    let mut leader = create_node(1, &[2], 2, 2, &[1, 1, 2]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    let tokens: Vec<_> = output
        .persist_ops
        .iter()
        .map(anvil_raft::PersistOp::token)
        .collect();
    for token in tokens {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }

    let follower = create_node(2, &[1], 2, 1, &[1, 1, 1, 1]);

    let mut cluster = SimCluster::new(2, 42);
    cluster.nodes.insert(node1, leader);
    cluster.nodes.insert(node2, follower);

    let leader_len = cluster.nodes[&node1].last_log_index();
    assert_eq!(leader_len, LogIndex::from_raw(4));

    cluster.tick_n(25);

    let follower_log = cluster.nodes[&node2].log();
    assert_eq!(follower_log.last_index(), LogIndex::from_raw(4));
    assert_eq!(
        follower_log.term_at(LogIndex::from_raw(3)),
        Some(Term::from_raw(2))
    );
    assert_eq!(
        follower_log.term_at(LogIndex::from_raw(4)),
        Some(Term::from_raw(2))
    );
}

#[test]
fn test_leader_crash_during_replication() {
    let mut cluster = SimCluster::new(5, 7777);
    let leader1 = cluster.elect_leader(100);
    cluster.tick_n(10);

    let non_leaders: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|&id| id != leader1)
        .collect();

    let other_followers = &non_leaders[1..];

    cluster.partition(&[leader1], other_followers);

    cluster.propose(leader1, Bytes::from_static(b"partial_entry"));
    cluster.tick_n(5);

    cluster.isolate_node(leader1);

    let mut new_leader = None;
    for _ in 0..150 {
        cluster.tick();
        for &node_id in &non_leaders {
            if cluster.nodes[&node_id].role() == Role::Leader {
                new_leader = Some(node_id);
                break;
            }
        }
        if new_leader.is_some() {
            break;
        }
    }
    let leader2 = new_leader.expect("new leader elected after crash");
    assert_ne!(leader2, leader1);

    cluster.propose(leader2, Bytes::from_static(b"committed_by_leader2"));
    cluster.tick_n(15);

    let commit_idx = cluster.nodes[&leader2].commit_index();
    assert!(commit_idx >= LogIndex::from_raw(2));

    cluster.heal_all();
    cluster.tick_n(20);
    assert_eq!(cluster.nodes[&leader1].role(), Role::Follower);
}

/// When a follower is partitioned, its election timer expires repeatedly.
/// With `PreVote` enabled, the partitioned node does NOT increment its term during `PreVote`,
/// and when it reconnects to the cluster, it does NOT disrupt the legitimate leader.
#[test]
fn test_prevote_prevents_disruptive_server() {
    let mut cluster = SimCluster::new(3, 9999);
    let leader = cluster.elect_leader(100);
    cluster.tick_n(10);

    let initial_term = cluster.nodes[&leader].term();

    let partitioned_follower = cluster
        .nodes
        .keys()
        .copied()
        .find(|&node_id| node_id != leader)
        .expect("follower");

    cluster.isolate_node(partitioned_follower);

    cluster.tick_n(50);

    let follower_term = cluster.nodes[&partitioned_follower].term();
    assert_eq!(
        follower_term, initial_term,
        "PreVote must prevent partitioned node from incrementing its term"
    );

    cluster.heal_all();
    cluster.tick_n(20);

    assert_eq!(cluster.nodes[&leader].role(), Role::Leader);
    assert_eq!(cluster.nodes[&leader].term(), initial_term);
}
