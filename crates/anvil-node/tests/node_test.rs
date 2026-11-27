#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_node::{Node, NodeConfig, NodeRead, NodeWrite, ReadConsistency};
use anvil_raft::{RaftConfig, RecoveredState};
use anvil_types::{ClusterId, NodeId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn node() -> Node {
    let id = NodeId::new(1).expect("id");
    let mut peers = BTreeSet::new();
    peers.insert(NodeId::new(2).expect("id"));
    peers.insert(NodeId::new(3).expect("id"));
    let config = RaftConfig {
        cluster_id: ClusterId::from_raw(1),
        learners: BTreeSet::new(),
        node_id: id,
        peers,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };
    let rng = ChaCha8Rng::seed_from_u64(7);
    Node::new(
        id,
        config,
        RecoveredState::default(),
        NodeConfig::default(),
        rng,
    )
}

#[test]
fn test_node_stale_read_never_blocks() {
    let mut node = node();
    let mut out = anvil_node::NodeOutput::default();
    node.tick(&mut out);
    let response = node
        .client_read(
            NodeRead::Get {
                key: Bytes::from_static(b"k"),
            },
            ReadConsistency::Stale,
            &mut out,
        )
        .expect("stale always serves");
    assert!(matches!(
        response.response,
        anvil_kv::KvResponse::Value { .. }
    ));
}

#[test]
fn test_node_lease_off_by_default() {
    let mut node = node();
    let mut out = anvil_node::NodeOutput::default();
    let response = node.client_read(
        NodeRead::Get {
            key: Bytes::from_static(b"k"),
        },
        ReadConsistency::Lease,
        &mut out,
    );
    assert!(response.is_none());
}

#[test]
fn test_node_write_rejected_when_not_leader() {
    let mut node = node();
    let mut out = anvil_node::NodeOutput::default();
    let accepted = node.client_write(NodeWrite::RegisterSession, &mut out);
    assert!(!accepted);
}

#[test]
fn test_node_canary_serves_stale_as_linearizable() {
    let config = NodeConfig {
        skip_read_confirmation: true,
        ..NodeConfig::default()
    };
    let id = NodeId::new(1).expect("id");
    let peers = BTreeSet::new();
    let config_raft = RaftConfig {
        cluster_id: ClusterId::from_raw(1),
        learners: BTreeSet::new(),
        node_id: id,
        peers,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let rng = ChaCha8Rng::seed_from_u64(9);
    let mut node = Node::new(id, config_raft, RecoveredState::default(), config, rng);
    let mut out = anvil_node::NodeOutput::default();
    node.tick(&mut out);
    let _ = (
        NodeWrite::RegisterSession,
        NodeRead::Get {
            key: Bytes::from_static(b"k"),
        },
    );
}
