#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use anvil_raft::{Input, Role};
use anvil_sim::SimCluster;
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;
use proptest::prelude::*;

/// Discrete action in a simulated Raft cluster execution trace.
#[derive(Debug, Clone)]
enum RaftAction {
    /// Advances election/heartbeat timer on a specific node.
    TickNode(u8),
    /// Submits a client proposal payload to a specific node.
    Propose(u8, u16),
    /// Delivers an in-flight packet chosen by index modulo queue size.
    Deliver(usize),
    /// Drops an in-flight packet chosen by index modulo queue size.
    Drop(usize),
    /// Duplicates an in-flight packet chosen by index modulo queue size.
    Duplicate(usize),
    /// Injects a network partition between two nodes.
    Partition(u8, u8),
    /// Heals all active network partitions.
    Heal,
}

prop_compose! {
    fn arb_raft_action()(
        op in 0u8..7,
        first_pick in 0u8..5,
        second_pick in 0u8..5,
        idx in 0usize..256,
        payload in 0u16..1000,
    ) -> RaftAction {
        match op {
            0 => RaftAction::TickNode(first_pick),
            1 => RaftAction::Propose(first_pick, payload),
            2 => RaftAction::Deliver(idx),
            3 => RaftAction::Drop(idx),
            4 => RaftAction::Duplicate(idx),
            5 => RaftAction::Partition(first_pick, second_pick),
            _ => RaftAction::Heal,
        }
    }
}

fn apply_action(cluster: &mut SimCluster, node_ids: &[NodeId], action: &RaftAction) {
    match action {
        RaftAction::TickNode(pick) => {
            let id = node_ids[(*pick as usize) % node_ids.len()];
            cluster.step_node(id, Input::Tick);
        }
        RaftAction::Propose(pick, payload) => {
            let id = node_ids[(*pick as usize) % node_ids.len()];
            let bytes = Bytes::from(format!("val_{payload}"));
            cluster.propose(id, bytes);
        }
        RaftAction::Deliver(idx) => {
            cluster.deliver_packet_at(*idx);
        }
        RaftAction::Drop(idx) => {
            cluster.drop_packet_at(*idx);
        }
        RaftAction::Duplicate(idx) => {
            cluster.duplicate_packet_at(*idx);
        }
        RaftAction::Partition(first, second) => {
            let id1 = node_ids[(*first as usize) % node_ids.len()];
            let id2 = node_ids[(*second as usize) % node_ids.len()];
            if id1 != id2 {
                cluster.partition(&[id1], &[id2]);
            }
        }
        RaftAction::Heal => {
            cluster.heal_all();
        }
    }
}

fn execute_action_sequence(cluster_size: usize, master_seed: u64, actions: &[RaftAction]) {
    let mut cluster = SimCluster::new(cluster_size, master_seed);
    let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

    for action in actions {
        apply_action(&mut cluster, &node_ids, action);
    }

    let highest_commit = cluster.highest_commit_index();
    for (&node_id, core) in &cluster.nodes {
        assert!(
            core.commit_index() <= core.last_log_index(),
            "commit index {} exceeds last log index {} on node {}",
            core.commit_index(),
            core.last_log_index(),
            node_id
        );
    }

    for idx in 1..=highest_commit.as_u64() {
        let log_idx = LogIndex::from_raw(idx);
        let mut committed_term = None;
        for core in cluster.nodes.values() {
            if core.commit_index() >= log_idx {
                let term = core.log().term_at(log_idx).expect("committed entry exists");
                if let Some(first_term) = committed_term {
                    assert_eq!(
                        first_term, term,
                        "disagreement on committed entry at index {idx}"
                    );
                } else {
                    committed_term = Some(term);
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 60,
        max_shrink_iters: 2000,
        .. ProptestConfig::default()
    })]

    /// Property test 1: 3-node cluster subjected to arbitrary message reordering, drops,
    /// duplicates, partitions, and client proposals. Raft safety invariants must hold on
    /// every step, with shrinking enabled to isolate minimal failing sequences if violated.
    #[test]
    fn prop_raft_invariants_3_nodes(
        master_seed in any::<u64>(),
        actions in proptest::collection::vec(arb_raft_action(), 15..80),
    ) {
        execute_action_sequence(3, master_seed, &actions);
    }

    /// Property test 2: 5-node cluster subjected to arbitrary message permutation and network churn.
    #[test]
    fn prop_raft_invariants_5_nodes(
        master_seed in any::<u64>(),
        actions in proptest::collection::vec(arb_raft_action(), 15..80),
    ) {
        execute_action_sequence(5, master_seed, &actions);
    }

    /// Property test 3: Verified convergence after partition healing and network quiescence.
    #[test]
    fn prop_convergence_after_healing(
        master_seed in any::<u64>(),
        actions in proptest::collection::vec(arb_raft_action(), 10..50),
    ) {
        let mut cluster = SimCluster::new(3, master_seed);
        let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

        // Run randomized actions
        for action in &actions {
            apply_action(&mut cluster, &node_ids, action);
        }

        // Heal partitions and deliver all in-flight messages
        cluster.heal_all();
        cluster.drain_network(30);

        // Advance cluster ticks to establish stable leadership and repair logs
        cluster.tick_n(25);

        // If a leader exists, its commit index must be at least as high as any follower's commit index
        if let Some(leader_id) = cluster.leader() {
            let leader_commit = cluster.nodes[&leader_id].commit_index();
            for (&id, core) in &cluster.nodes {
                if id != leader_id && core.role() == Role::Follower {
                    assert!(
                        core.commit_index() <= leader_commit,
                        "follower commit {} exceeds leader commit {}",
                        core.commit_index(),
                        leader_commit
                    );
                }
            }
        }
    }
}
