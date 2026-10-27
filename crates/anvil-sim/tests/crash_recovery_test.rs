#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::cast_possible_truncation
)]

//! Comprehensive crash and restart recovery integration tests for `SimCluster`.
//!
//! Validates:
//! - Clean crash recovery and log replication catch-up
//! - Leader crash, failover, higher-term stepdown, and log reconciliation
//! - Torn-tail truncation without data loss
//! - Mid-log corruption detection and `NeedsRepair` classification
//! - Corrupted hard-state refusal to start (enforcing `VoteNeverForgotten`)
//! - Rolling minority crashes upholding `CommittedNeverLostAcrossCrashes`

use anvil_raft::Role;
use anvil_sim::SimCluster;
use anvil_storage::{
    Device, DeviceOp, FileId, LogRecoveryClassification, SectorSize, StorageError,
};
use anvil_types::{LogIndex, NodeId, Term};
use bytes::Bytes;

#[test]
fn test_crash_and_restart_clean_recovery() {
    let mut cluster = SimCluster::new(3, 1001);
    let leader_id = cluster.elect_leader(30);

    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("follower");

    // Propose entries 1..=3
    for i in 1..=3 {
        cluster.propose(leader_id, Bytes::from(format!("entry-{i}")));
    }
    cluster.tick_n(20);

    let initial_commit = cluster.highest_commit_index();
    assert!(initial_commit >= LogIndex::from_raw(3));

    // Crash the follower
    cluster.crash_node(follower_id);
    assert!(cluster.is_node_crashed(follower_id));
    assert!(!cluster.nodes.contains_key(&follower_id));

    // Propose more entries while follower is down (replicated to 2/3 nodes)
    for i in 4..=6 {
        cluster.propose(leader_id, Bytes::from(format!("entry-{i}")));
    }
    cluster.tick_n(20);

    let post_crash_commit = cluster.highest_commit_index();
    assert!(post_crash_commit >= LogIndex::from_raw(6));

    // Restart the crashed follower
    let recovered = cluster
        .restart_node(follower_id)
        .expect("restart should succeed");
    assert!(!cluster.is_node_crashed(follower_id));
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::Clean
    ));

    // Initial recovered state should have at least the pre-crash entries
    assert!(recovered.log.last_index >= initial_commit);

    // Follower should reconnect and catch up with remaining entries
    cluster.tick_n(30);

    let follower_core = &cluster.nodes[&follower_id];
    let leader_core = &cluster.nodes[&leader_id];
    assert_eq!(
        follower_core.last_log_index(),
        leader_core.last_log_index(),
        "follower failed to catch up after restart"
    );
    assert_eq!(follower_core.commit_index(), leader_core.commit_index());
}

#[test]
fn test_crash_leader_and_recover_with_higher_term() {
    let mut cluster = SimCluster::new(3, 2002);
    let leader_id = cluster.elect_leader(30);
    let initial_term = cluster.nodes[&leader_id].term();

    cluster.propose(leader_id, Bytes::from_static(b"committed-cmd"));
    cluster.tick_n(20);
    assert!(cluster.highest_commit_index() >= LogIndex::from_raw(1));

    // Crash the leader
    cluster.crash_node(leader_id);
    assert!(cluster.is_node_crashed(leader_id));

    // Remaining nodes elect a new leader in a higher term
    let new_leader_id = cluster.elect_leader(50);
    assert_ne!(new_leader_id, leader_id);
    assert!(cluster.nodes[&new_leader_id].term() > initial_term);

    // Propose new entries on the new leader
    cluster.propose(new_leader_id, Bytes::from_static(b"new-term-cmd"));
    cluster.tick_n(20);

    // Restart old leader
    let recovered = cluster.restart_node(leader_id).expect("old leader restart");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::Clean
    ));

    // Step cluster: old leader discovers modern leader, steps down, and synchronizes
    cluster.tick_n(30);

    let old_leader_core = &cluster.nodes[&leader_id];
    let modern_leader_core = &cluster.nodes[&new_leader_id];

    assert_eq!(old_leader_core.role(), Role::Follower);
    assert_eq!(old_leader_core.term(), modern_leader_core.term());
    assert_eq!(
        old_leader_core.last_log_index(),
        modern_leader_core.last_log_index()
    );
}

#[test]
fn test_crash_with_torn_tail_recovery() {
    let mut cluster = SimCluster::new(3, 3003);
    let leader_id = cluster.elect_leader(30);
    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("follower");

    for i in 1..=3 {
        cluster.propose(leader_id, Bytes::from(format!("entry-{i}")));
    }
    cluster.tick_n(20);

    let ack_entries = cluster.storage[&follower_id].acknowledged_entries.len();
    assert!(ack_entries >= 3);

    // Crash follower
    cluster.crash_node(follower_id);

    // Inject a torn tail: append a corrupted non-zero sector at the end of segment 1
    let storage = cluster.storage.get_mut(&follower_id).unwrap();
    let seg1_id = FileId::wal_segment(1);
    let cur_bytes = storage
        .device
        .get_file_durable_bytes(seg1_id)
        .unwrap()
        .to_vec();

    // Corrupt the sector immediately following valid data
    let sector_size = SectorSize::Sector512.as_usize();
    let corrupt_offset = cur_bytes.len().saturating_sub(sector_size * 2);

    storage.device.submit(DeviceOp::Write {
        op_id: 9991,
        file_id: seg1_id,
        offset: corrupt_offset as u64,
        data: Bytes::from(vec![0xDE, 0xAD, 0xBE, 0xEF]),
    });
    storage.device.submit(DeviceOp::Fdatasync {
        op_id: 9992,
        file_id: seg1_id,
    });
    while storage.device.poll_completion().is_some() {}

    // Restart follower: scanner must detect TornTail or Clean and cleanly truncate
    let recovered = cluster
        .restart_node(follower_id)
        .expect("restart with torn tail");

    match recovered.log.classification {
        LogRecoveryClassification::TornTail { .. } | LogRecoveryClassification::Clean => {}
        LogRecoveryClassification::MidLogCorruption { .. } => {
            panic!("expected TornTail or Clean, got MidLogCorruption");
        }
    }

    // Node must be able to resume stepping and catch up
    cluster.tick_n(30);
    assert_eq!(
        cluster.nodes[&follower_id].commit_index(),
        cluster.nodes[&leader_id].commit_index()
    );
}

#[test]
fn test_crash_with_mid_log_corruption_needs_repair() {
    let mut cluster = SimCluster::new(3, 4004);
    let leader_id = cluster.elect_leader(30);
    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("follower");

    // Propose 8 entries
    for i in 1..=8 {
        cluster.propose(leader_id, Bytes::from(format!("entry-{i}")));
    }
    cluster.tick_n(25);

    // Crash follower
    cluster.crash_node(follower_id);

    // Inject corruption in the middle of segment 1 (e.g. sector 2 where valid sector 3..8 follow)
    let storage = cluster.storage.get_mut(&follower_id).unwrap();
    let seg1_id = FileId::wal_segment(1);
    let sector_size = SectorSize::Sector512.as_u64();
    let mid_offset = sector_size * 2; // sector 2 is middle

    storage.device.submit(DeviceOp::Write {
        op_id: 8881,
        file_id: seg1_id,
        offset: mid_offset,
        data: Bytes::from(vec![0xFF; 64]), // corrupt record header/CRC
    });
    storage.device.submit(DeviceOp::Fdatasync {
        op_id: 8882,
        file_id: seg1_id,
    });
    while storage.device.poll_completion().is_some() {}

    // Restart node
    let recovered = cluster
        .restart_node(follower_id)
        .expect("restart completes scan");

    assert!(
        matches!(
            recovered.log.classification,
            LogRecoveryClassification::MidLogCorruption { .. }
        ),
        "expected MidLogCorruption, got {:?}",
        recovered.log.classification
    );

    // Node is marked NeedsRepair
    assert!(cluster.is_node_needs_repair(follower_id));
    assert!(!cluster.nodes.contains_key(&follower_id));

    // Stepping cluster does not activate or elect this damaged node
    cluster.tick_n(20);
    assert!(!cluster.nodes.contains_key(&follower_id));
}

#[test]
fn test_crash_with_both_hard_state_slots_corrupted_refuses_start() {
    let mut cluster = SimCluster::new(3, 5005);
    let leader_id = cluster.elect_leader(30);
    let follower_id = *cluster
        .nodes
        .keys()
        .find(|&&id| id != leader_id)
        .expect("follower");

    // Follower has cast vote for leader in term 1
    assert_eq!(
        cluster.storage[&follower_id].persisted_votes[&Term::from_raw(1)],
        Some(leader_id)
    );

    // Crash follower
    cluster.crash_node(follower_id);

    // Corrupt BOTH slot 0 and slot 1 in FileId::HARD_STATE
    let storage = cluster.storage.get_mut(&follower_id).unwrap();
    let sector_size = SectorSize::Sector512.as_u64();

    storage.device.submit(DeviceOp::Write {
        op_id: 7771,
        file_id: FileId::HARD_STATE,
        offset: 0,
        data: Bytes::from(vec![0xBA, 0xAD, 0xF0, 0x0D]),
    });
    storage.device.submit(DeviceOp::Write {
        op_id: 7772,
        file_id: FileId::HARD_STATE,
        offset: sector_size,
        data: Bytes::from(vec![0xBA, 0xAD, 0xF0, 0x0D]),
    });
    storage.device.submit(DeviceOp::Fdatasync {
        op_id: 7773,
        file_id: FileId::HARD_STATE,
    });
    while storage.device.poll_completion().is_some() {}

    // Attempt restart: MUST fail and refuse to start to protect VoteNeverForgotten
    let restart_res = cluster.restart_node(follower_id);
    assert!(restart_res.is_err());
    assert!(matches!(
        restart_res.unwrap_err(),
        StorageError::CorruptHardStateBothSlots
    ));

    assert!(cluster.is_node_refused_start(follower_id));
    assert!(!cluster.nodes.contains_key(&follower_id));
}

#[test]
fn test_rolling_crashes_and_restarts_quorum_survivability() {
    let mut cluster = SimCluster::new(5, 6006);
    let leader_id = cluster.elect_leader(40);

    // 1. Commit batch 1
    for i in 1..=5 {
        cluster.propose(leader_id, Bytes::from(format!("batch1-{i}")));
    }
    cluster.tick_n(25);
    let commit1 = cluster.highest_commit_index();
    assert!(commit1 >= LogIndex::from_raw(5));

    // 2. Crash 2 followers (minority of 5)
    let followers: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|&id| id != leader_id)
        .collect();
    let crashed1 = followers[0];
    let crashed2 = followers[1];

    cluster.crash_node(crashed1);
    cluster.crash_node(crashed2);

    // 3. Propose batch 2 with 3 live nodes
    for i in 6..=10 {
        cluster.propose(leader_id, Bytes::from(format!("batch2-{i}")));
    }
    cluster.tick_n(25);
    let commit2 = cluster.highest_commit_index();
    assert!(commit2 >= LogIndex::from_raw(10));

    // 4. Restart crashed1
    cluster.restart_node(crashed1).expect("restart crashed1");

    // 5. Crash remaining 2 followers from initial live set
    let crashed3 = followers[2];
    cluster.crash_node(crashed3);

    // 6. Cluster still has majority (leader, crashed1, followers[3])
    for i in 11..=15 {
        cluster.propose(leader_id, Bytes::from(format!("batch3-{i}")));
    }
    cluster.tick_n(30);

    // 7. Restart all crashed nodes
    cluster.restart_node(crashed2).expect("restart crashed2");
    cluster.restart_node(crashed3).expect("restart crashed3");

    cluster.tick_n(40);

    // Verify all 5 nodes agree on committed entries
    let final_leader = cluster.leader().expect("active leader");
    let final_commit = cluster.nodes[&final_leader].commit_index();

    for &id in cluster.nodes.keys() {
        assert_eq!(
            cluster.nodes[&id].commit_index(),
            final_commit,
            "commit mismatch on node {id}"
        );
        for idx in 1..=final_commit.as_u64() {
            let log_idx = LogIndex::from_raw(idx);
            assert_eq!(
                cluster.nodes[&id].log().term_at(log_idx),
                cluster.nodes[&final_leader].log().term_at(log_idx),
                "term mismatch at index {idx} on node {id}"
            );
        }
    }
}
