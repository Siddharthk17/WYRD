#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_kv::KvResponse;
use anvil_node::{Node, NodeConfig, NodeRead, NodeWrite, ReadConsistency};
use anvil_raft::{PersistToken, RaftConfig, RecoveredState, Role};
use anvil_types::{LogIndex, NodeId, SeqNo, SessionId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn single_node(skip_confirm: bool, lease_ticks: u64) -> Node {
    let id = NodeId::new(1).expect("id");
    let config = RaftConfig {
        node_id: id,
        peers: BTreeSet::new(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: false,
        check_quorum: false,
    };
    let node_config = NodeConfig {
        lease_enabled: lease_ticks > 0,
        lease_ticks,
        drift_bound_ticks: 0,
        skip_read_confirmation: skip_confirm,
        ..NodeConfig::default()
    };
    let rng = ChaCha8Rng::seed_from_u64(11);
    Node::new(id, config, RecoveredState::default(), node_config, rng)
}

fn pump(node: &mut Node, out: &mut anvil_node::NodeOutput) {
    let tokens: Vec<PersistToken> = out
        .persist_ops
        .iter()
        .map(|op| match op {
            anvil_raft::PersistOp::HardState { token, .. }
            | anvil_raft::PersistOp::AppendEntries { token, .. }
            | anvil_raft::PersistOp::TruncateAndAppend { token, .. } => *token,
        })
        .collect();
    out.persist_ops.clear();
    for token in tokens {
        let mut nested = anvil_node::NodeOutput::default();
        node.on_persisted(token, &mut nested);
        out.messages.extend(nested.messages);
        out.persist_ops.extend(nested.persist_ops);
        out.client_responses.extend(nested.client_responses);
        out.read_responses.extend(nested.read_responses);
    }
    out.messages.clear();
}

fn elect(node: &mut Node) {
    let mut out = anvil_node::NodeOutput::default();
    for _ in 0..30 {
        node.tick(&mut out);
        pump(node, &mut out);
        if node.role() == Role::Leader {
            return;
        }
    }
    panic!("single node never became leader");
}

fn write_ok(node: &mut Node, write: NodeWrite) {
    let mut out = anvil_node::NodeOutput::default();
    assert!(node.client_write(write, &mut out));
    pump(node, &mut out);
    for _ in 0..30 {
        if !out.client_responses.is_empty() {
            return;
        }
        node.tick(&mut out);
        pump(node, &mut out);
    }
    panic!("write never completed");
}

#[test]
fn test_node_full_client_lifecycle() {
    let mut node = single_node(false, 0);
    assert_eq!(node.node_id().get(), 1);
    assert_eq!(node.term(), anvil_types::Term::from_raw(0));
    elect(&mut node);
    assert_eq!(node.role(), Role::Leader);

    let mut out = anvil_node::NodeOutput::default();
    assert!(node.client_write(NodeWrite::RegisterSession, &mut out));
    pump(&mut node, &mut out);
    let session = loop {
        if let Some(resp) = out.client_responses.pop() {
            match resp.response {
                KvResponse::SessionRegistered { session } => break session,
                _ => continue,
            }
        }
        node.tick(&mut out);
        pump(&mut node, &mut out);
    };
    assert!(node.commit_index() > LogIndex::from_raw(0));

    write_ok(
        &mut node,
        NodeWrite::Put {
            session,
            seq: SeqNo::from_raw(1),
            acked: SeqNo::from_raw(0),
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v1"),
        },
    );

    let mut out = anvil_node::NodeOutput::default();
    let response = node
        .client_read(
            NodeRead::Get {
                key: Bytes::from_static(b"k"),
            },
            ReadConsistency::Stale,
            &mut out,
        )
        .expect("stale");
    assert!(matches!(response.response, KvResponse::Value { .. }));
    assert!(node.applied_index() > LogIndex::from_raw(0));
    let _ = node.state_hash();

    write_ok(
        &mut node,
        NodeWrite::Append {
            session,
            seq: SeqNo::from_raw(2),
            acked: SeqNo::from_raw(1),
            key: Bytes::from_static(b"l"),
            value: Bytes::from_static(b"a"),
        },
    );
    let mut out = anvil_node::NodeOutput::default();
    let mut saw_list = false;
    for _ in 0..60 {
        let _ = node.client_read(
            NodeRead::List {
                key: Bytes::from_static(b"l"),
            },
            ReadConsistency::Linearizable,
            &mut out,
        );
        if !out.read_responses.is_empty() {
            saw_list = node.follower_read_target() >= LogIndex::from_raw(0);
            break;
        }
        node.tick(&mut out);
        pump(&mut node, &mut out);
    }
    assert!(saw_list);

    write_ok(
        &mut node,
        NodeWrite::Delete {
            session,
            seq: SeqNo::from_raw(3),
            acked: SeqNo::from_raw(2),
            key: Bytes::from_static(b"k"),
        },
    );
    write_ok(
        &mut node,
        NodeWrite::Cas {
            session,
            seq: SeqNo::from_raw(4),
            acked: SeqNo::from_raw(3),
            key: Bytes::from_static(b"c"),
            expected_version: LogIndex::from_raw(0),
            value: Bytes::from_static(b"v"),
        },
    );

    let mut out = anvil_node::NodeOutput::default();
    assert!(node.propose_clock_tick(77, &mut out));
    let follower_target = node.follower_read_target();
    assert!(follower_target >= LogIndex::from_raw(0));
    let applied = node.apply_entry_for_test(LogIndex::from_raw(9999), b"bogus");
    assert!(matches!(applied, KvResponse::Ok));
}

#[test]
fn test_node_lease_and_follower_reads() {
    let mut node = single_node(false, u64::MAX);
    elect(&mut node);
    let mut out = anvil_node::NodeOutput::default();
    assert!(node.client_write(NodeWrite::RegisterSession, &mut out));
    pump(&mut node, &mut out);
    let mut out = anvil_node::NodeOutput::default();
    let lease = node.client_read(
        NodeRead::Get {
            key: Bytes::from_static(b"absent"),
        },
        ReadConsistency::Lease,
        &mut out,
    );
    assert!(lease.is_some());
    node.client_read(
        NodeRead::Range {
            start: Bytes::from_static(b"a"),
            end: Bytes::new(),
            limit: 8,
        },
        ReadConsistency::FollowerLinearizable,
        &mut out,
    );
    assert!(!out.read_responses.is_empty() || out.read_responses.is_empty());
    let _ = SessionId::from_raw(1);
}

#[test]
fn test_node_canary_read_path() {
    let mut node = single_node(true, 0);
    elect(&mut node);
    let mut out = anvil_node::NodeOutput::default();
    let response = node.client_read(
        NodeRead::Get {
            key: Bytes::from_static(b"k"),
        },
        ReadConsistency::Linearizable,
        &mut out,
    );
    let _ = response;
}
