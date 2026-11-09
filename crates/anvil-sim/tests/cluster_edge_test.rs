#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_raft::{Input, Output, Role};
use anvil_sim::SimCluster;
use anvil_storage::{Device, DeviceOp, FileId, SectorSize};
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("id")
}

#[test]
fn test_partition_splits_and_heals_groups() {
    let mut cluster = SimCluster::new(3, 11);
    let leader = cluster.elect_leader(30);
    let others: Vec<NodeId> = [node(1), node(2), node(3)]
        .into_iter()
        .filter(|&id| id != leader)
        .collect();
    cluster.partition(&[leader], &others);
    assert!(cluster.network.is_cut(leader, others[0]));
    assert!(!cluster.network.is_cut(others[0], others[1]));
    cluster.heal_all();
    assert!(!cluster.network.is_cut(leader, others[0]));
    cluster.tick_n(10);
    assert!(cluster.leader().is_some());
}

#[test]
fn test_leader_returns_none_on_split_brain_roles() {
    let mut cluster = SimCluster::new(3, 12);
    cluster.elect_leader(30);
    for core in cluster.nodes.values_mut() {
        core.testing_set_role(Role::Leader);
    }
    assert_eq!(cluster.leader(), None);
}

#[test]
#[should_panic(expected = "failed to elect leader")]
fn test_elect_leader_fails_when_no_quorum_possible() {
    let mut cluster = SimCluster::new(1, 13);
    let only = node(1);
    cluster.isolate_node(only);
    cluster.elect_leader(3);
}

#[test]
fn test_standalone_node_without_storage_still_routes() {
    let mut cluster = SimCluster::new(3, 14);
    let leader = cluster.elect_leader(30);
    let follower = if leader == node(1) { node(2) } else { node(1) };
    cluster.storage.remove(&follower);
    let mut output = Output::new();
    if let Some(core) = cluster.nodes.get_mut(&follower) {
        core.step(Input::Tick, &mut output);
    }
    cluster.step_node(follower, Input::Tick);
    assert!(!cluster.nodes.is_empty());
}

#[test]
fn test_fsync_failure_crashes_node_through_drive_storage() {
    let mut cluster = SimCluster::new(3, 15);
    let leader = cluster.elect_leader(40);
    cluster.propose(leader, Bytes::from_static(b"x"));
    cluster.tick_n(5);
    assert!(cluster.highest_commit_index() >= LogIndex::from_raw(1));

    for storage in cluster.storage.values_mut() {
        storage.device.config_mut().fsync_failure_rate_ppm = 1_000_000;
    }
    cluster.propose(leader, Bytes::from_static(b"y"));
    cluster.tick_n(5);
    assert!(
        cluster.nodes.len() < 3,
        "fsync failures should crash at least one node"
    );
}

#[test]
fn test_restart_refused_start_when_both_slots_corrupt() {
    let mut cluster = SimCluster::new(3, 16);
    let leader = cluster.elect_leader(30);
    let follower = if leader == node(1) { node(2) } else { node(1) };
    cluster.crash_node(follower);
    let storage = cluster.storage.get_mut(&follower).expect("storage");
    let sector = SectorSize::Sector512.as_usize();
    let file = FileId::HARD_STATE;
    storage.device.submit(DeviceOp::Write {
        op_id: 9001,
        file_id: file,
        offset: 0,
        payload: Bytes::from(vec![0x5Au8; 64]),
    });
    while storage.device.poll_completion().is_some() {}
    storage.device.submit(DeviceOp::Write {
        op_id: 9002,
        file_id: file,
        offset: sector as u64,
        payload: Bytes::from(vec![0x5Au8; 64]),
    });
    while storage.device.poll_completion().is_some() {}
    storage.device.submit(DeviceOp::Fdatasync {
        op_id: 9003,
        file_id: file,
    });
    while storage.device.poll_completion().is_some() {}
    let result = cluster.restart_node(follower);
    assert!(result.is_err());
    assert!(cluster.is_node_refused_start(follower));
}
