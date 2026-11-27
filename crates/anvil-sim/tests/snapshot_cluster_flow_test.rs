#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_raft::SnapshotChunkData;
use anvil_sim::SimCluster;
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("node")
}

fn drive(cluster: &mut SimCluster, ticks: usize) {
    for _ in 0..ticks {
        cluster.tick();
    }
}

fn commit_all(cluster: &mut SimCluster) -> LogIndex {
    let leader = cluster.leader().expect("leader");
    let before = cluster
        .nodes
        .get(&leader)
        .map(anvil_raft::RaftCore::commit_index)
        .unwrap_or_default();
    for _ in 0..200 {
        cluster.tick();
        let reached = cluster.nodes.values().all(|core| {
            core.commit_index() > before || core.commit_index() == core.last_log_index()
        });
        if reached && cluster.leader().is_some() {
            break;
        }
    }
    cluster
        .nodes
        .get(&leader)
        .map(anvil_raft::RaftCore::commit_index)
        .unwrap_or_default()
}

#[test]
fn test_cluster_snapshot_install_after_long_partition() {
    let mut cluster = SimCluster::new(3, 4242);
    drive(&mut cluster, 20);
    let leader = cluster.leader().expect("leader elected during warmup");

    for i in 0..10u64 {
        cluster.propose(leader, Bytes::from(format!("pre-{i}")));
        drive(&mut cluster, 3);
    }
    let leader = cluster.leader().expect("leader");
    assert!(cluster.nodes.get(&leader).expect("core").commit_index() > LogIndex::from_raw(0));
    for id in cluster.nodes.keys().copied().collect::<Vec<_>>() {
        let commit = cluster.nodes.get(&id).expect("core").commit_index();
        cluster.notify_applied(id, commit);
    }

    // Isolate node 3 for the rest of chaos while the quorum keeps writing.
    let follower = node_id(3);
    let peers: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    cluster.network.isolate(follower, &peers);
    for i in 0..30u64 {
        let leader = cluster.leader().expect("leader");
        cluster.propose(leader, Bytes::from(format!("chaos-{i}")));
        drive(&mut cluster, 3);
    }
    let leader = cluster.leader().expect("leader");
    for id in cluster.nodes.keys().copied().collect::<Vec<_>>() {
        let commit = cluster.nodes.get(&id).expect("core").commit_index();
        cluster.notify_applied(id, commit);
    }
    let applied = cluster.nodes.get(&leader).expect("core").commit_index();

    // Snapshot the leader's state machine equivalent: fake bytes stand in
    // for the KvStore encoding; the core only moves opaque chunks.
    let full = Bytes::from(vec![7u8; 1024]);
    let digest = *blake3::hash(&full).as_bytes();
    let mut chunks = Vec::new();
    let mut offset = 0;
    let mut index = 0u32;
    while offset < full.len() {
        let end = (offset + 128).min(full.len());
        let payload = full.slice(offset..end);
        chunks.push(SnapshotChunkData {
            chunk: payload.clone(),
            chunk_crc: crc32c::crc32c(&payload),
        });
        offset = end;
        index += 1;
    }
    let total = full.len() as u64;
    let term = cluster.nodes.get(&leader).expect("core").term();
    cluster.supply_snapshot(leader, applied, term, total, digest, chunks);
    cluster.compact_log(
        leader,
        LogIndex::from_raw(applied.as_u64().saturating_sub(2)),
    );
    let _ = index;

    cluster.heal_all();
    drive(&mut cluster, 100);

    let installed = cluster
        .ready_snapshots
        .remove(&follower)
        .unwrap_or_default();
    assert!(!installed.is_empty(), "follower must install the snapshot");
    assert_eq!(installed[0].last_included_index, applied);
    let follower_base = cluster
        .nodes
        .get(&follower)
        .expect("core")
        .log()
        .base_index();
    assert_eq!(follower_base, applied);

    let _ = commit_all(&mut cluster);
    let commits: Vec<LogIndex> = cluster
        .nodes
        .values()
        .map(anvil_raft::RaftCore::commit_index)
        .collect();
    assert!(
        commits.windows(2).all(|pair| pair[0] == pair[1]),
        "cluster must reconverge after install: {commits:?}"
    );
}
