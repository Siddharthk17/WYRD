#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::SimCluster;
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;

fn follower_of(cluster: &SimCluster, leader: NodeId) -> NodeId {
    cluster
        .nodes
        .keys()
        .copied()
        .find(|id| *id != leader)
        .expect("follower")
}

#[test]
fn test_repair_mark_syncs_core_and_suppresses_votes() {
    let mut cluster = SimCluster::new(3, 20001);
    let leader = cluster.elect_leader(60);
    for i in 1..=3u64 {
        cluster.propose(leader, Bytes::from(format!("w-{i}")));
    }
    cluster.tick_n(30);
    let follower = follower_of(&cluster, leader);
    let last = cluster.nodes[&follower].last_log_index();

    cluster.mark_needs_repair(follower, last);
    assert!(cluster.is_node_needs_repair(follower));
    assert_eq!(
        cluster.nodes[&follower].repair_from(),
        Some(last),
        "core flag follows the storage flag"
    );

    // The repair node never campaigns, even after many timeouts.
    for _ in 0..5 {
        cluster.tick_n(20);
        assert_ne!(
            cluster.nodes[&follower].role(),
            anvil_raft::Role::Candidate,
            "repair node must never campaign"
        );
    }

    // The cluster keeps committing through the healthy majority.
    let before = cluster.nodes[&leader].commit_index();
    cluster.propose(leader, Bytes::from("after-repair-mark"));
    cluster.tick_n(40);
    assert!(
        cluster.nodes[&leader].commit_index() > before,
        "quorum without the repair node still commits"
    );
    // The repair node still follows the log through appends.
    assert!(
        cluster.nodes[&follower].commit_index() >= before,
        "repair node answers appends while flagged"
    );

    cluster.clear_repair(follower);
    assert!(!cluster.is_node_needs_repair(follower));
    assert_eq!(cluster.nodes[&follower].repair_from(), None);
}

#[test]
fn test_repair_node_excluded_from_commit_quorum() {
    let mut cluster = SimCluster::new(3, 20002);
    let leader = cluster.elect_leader(60);
    for i in 1..=4u64 {
        cluster.propose(leader, Bytes::from(format!("v-{i}")));
    }
    cluster.tick_n(40);
    let follower = follower_of(&cluster, leader);
    let other = cluster
        .nodes
        .keys()
        .copied()
        .find(|id| *id != leader && *id != follower)
        .expect("third node");
    let last = cluster.nodes[&follower].last_log_index();
    let committed = cluster.nodes[&leader].commit_index();

    // Flag the follower, then isolate the only other voter: the leader must
    // not commit through the repair node's clamped acks.
    cluster.mark_needs_repair(follower, last);
    cluster.isolate_node(other);
    cluster.propose(leader, Bytes::from("post-flag"));
    cluster.tick_n(40);
    assert_eq!(
        cluster.nodes[&leader].commit_index(),
        committed,
        "clamped repair acks must not form a commit quorum"
    );

    // Clearing the repair restores the quorum and the entry commits.
    cluster.clear_repair(follower);
    cluster.tick_n(40);
    assert!(
        cluster.nodes[&leader].commit_index() > committed,
        "commit resumes after the repair clears"
    );
}

#[test]
fn test_repair_zero_index_ignored() {
    let mut cluster = SimCluster::new(3, 20003);
    let leader = cluster.elect_leader(60);
    let follower = follower_of(&cluster, leader);
    cluster.mark_needs_repair(follower, LogIndex::from_raw(0));
    assert!(!cluster.is_node_needs_repair(follower));
    assert_eq!(cluster.nodes[&follower].repair_from(), None);
}
