#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_kv::{KvResponse, SnapshotPolicy};
use anvil_node::{Node, NodeConfig, NodeOutput, NodeRead, NodeWrite, ReadConsistency};
use anvil_raft::{PersistToken, RaftConfig, RecoveredState, Role};
use anvil_types::{LogIndex, NodeId, SeqNo, SessionId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("node")
}

fn raft_config(id: NodeId, peers: &[NodeId]) -> RaftConfig {
    RaftConfig {
        node_id: id,
        peers: peers.iter().copied().collect::<BTreeSet<_>>(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 8,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    }
}

fn node_config() -> NodeConfig {
    NodeConfig {
        snapshot_policy: SnapshotPolicy {
            max_entries_since_snapshot: 4,
            max_bytes_since_snapshot: 1024,
            retain_entries: 2,
            chunk_bytes: 128,
        },
        ..NodeConfig::default()
    }
}

fn make_node(n: u64, peers: &[u64]) -> Node {
    let id = node_id(n);
    let peer_ids: Vec<NodeId> = peers.iter().map(|p| node_id(*p)).collect();
    let rng = ChaCha8Rng::seed_from_u64(n);
    Node::new(
        id,
        raft_config(id, &peer_ids),
        RecoveredState::default(),
        node_config(),
        rng,
    )
}

fn ack_persisted(node: &mut Node, out: &mut NodeOutput) {
    for _ in 0..16 {
        let tokens: Vec<PersistToken> = out
            .persist_ops
            .iter()
            .map(|op| match op {
                anvil_raft::PersistOp::HardState { token, .. }
                | anvil_raft::PersistOp::AppendEntries { token, .. }
                | anvil_raft::PersistOp::TruncateAndAppend { token, .. } => *token,
            })
            .collect();
        if tokens.is_empty() {
            break;
        }
        out.persist_ops.clear();
        for token in tokens {
            let mut nested = NodeOutput::default();
            node.on_persisted(token, &mut nested);
            out.messages.extend(nested.messages);
            out.persist_ops.extend(nested.persist_ops);
            out.client_responses.extend(nested.client_responses);
            out.read_responses.extend(nested.read_responses);
        }
    }
}

fn register(
    nodes: &mut BTreeMap<NodeId, Node>,
    outs: &mut BTreeMap<NodeId, NodeOutput>,
    isolated: &[NodeId],
) -> SessionId {
    {
        let leader = nodes.get_mut(&node_id(1)).expect("leader");
        let out = outs.get_mut(&node_id(1)).expect("leader out");
        assert!(leader.client_write(NodeWrite::RegisterSession, out));
        ack_persisted(leader, out);
    }
    pump_round(nodes, outs, isolated);
    for _ in 0..60 {
        if let Some(resp) = outs
            .get_mut(&node_id(1))
            .expect("leader out")
            .client_responses
            .pop()
            && let KvResponse::SessionRegistered { session } = resp.response
        {
            return session;
        }
        pump_round(nodes, outs, isolated);
    }
    panic!("register never completed");
}

/// Ticks every live node, acks persistence, and delivers all messages with
/// the isolated nodes excluded.
fn pump_round(
    nodes: &mut BTreeMap<NodeId, Node>,
    outs: &mut BTreeMap<NodeId, NodeOutput>,
    isolated: &[NodeId],
) {
    let ids: Vec<NodeId> = nodes.keys().copied().collect();
    for id in &ids {
        if isolated.contains(id) {
            continue;
        }
        let out = outs.get_mut(id).expect("out");
        nodes.get_mut(id).expect("node").tick(out);
        ack_persisted(nodes.get_mut(id).expect("node"), out);
    }
    let ids: Vec<NodeId> = nodes.keys().copied().collect();
    for from in &ids {
        if isolated.contains(from) {
            continue;
        }
        let messages = std::mem::take(&mut outs.get_mut(from).expect("out").messages);
        for outbound in messages {
            if isolated.contains(&outbound.to) {
                continue;
            }
            let node = nodes.get_mut(&outbound.to).expect("peer");
            let out = outs.get_mut(&outbound.to).expect("peer out");
            node.receive(*from, outbound.message, out);
            ack_persisted(node, out);
        }
    }
}

fn put(
    node: &mut Node,
    out: &mut NodeOutput,
    session: SessionId,
    seq: u64,
    key: &[u8],
    value: &[u8],
) {
    assert!(node.client_write(
        NodeWrite::Put {
            session,
            seq: SeqNo::from_raw(seq),
            acked: SeqNo::from_raw(seq.saturating_sub(1)),
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        },
        out
    ));
    ack_persisted(node, out);
}

#[test]
fn test_node_snapshot_supply_install_and_rejoin() {
    let mut nodes: BTreeMap<NodeId, Node> = [
        (1, make_node(1, &[2, 3])),
        (2, make_node(2, &[1, 3])),
        (3, make_node(3, &[1, 2])),
    ]
    .into_iter()
    .map(|(n, node)| (node_id(n), node))
    .collect();
    let mut outs: BTreeMap<NodeId, NodeOutput> = [1, 2, 3]
        .iter()
        .map(|n| (node_id(*n), NodeOutput::default()))
        .collect();
    // Node 3 starts isolated: it never sees the initial replication and must
    // join later through a chunked snapshot stream.
    let mut isolated = vec![node_id(3)];

    // Elect node 1 with node 2's vote while node 3 stays isolated.
    for _ in 0..10 {
        pump_round(&mut nodes, &mut outs, &isolated);
        if nodes[&node_id(1)].role() == Role::Leader {
            break;
        }
    }
    assert_eq!(nodes[&node_id(1)].role(), Role::Leader);

    let session = register(&mut nodes, &mut outs, &isolated);

    for i in 0..8u64 {
        let leader_out = outs.get_mut(&node_id(1)).expect("leader out");
        put(
            nodes.get_mut(&node_id(1)).expect("leader"),
            leader_out,
            session,
            1 + i,
            format!("k{i}").as_bytes(),
            format!("v{i}").as_bytes(),
        );
        for _ in 0..6 {
            pump_round(&mut nodes, &mut outs, &isolated);
        }
    }
    let leader_applied = nodes[&node_id(1)].applied_index();
    assert!(leader_applied >= LogIndex::from_raw(9));
    assert_eq!(nodes[&node_id(2)].applied_index(), leader_applied);
    assert_eq!(nodes[&node_id(3)].applied_index(), LogIndex::from_raw(0));

    // Heal node 3 and pump until it installs the snapshot and catches up.
    isolated.clear();
    for _ in 0..400 {
        pump_round(&mut nodes, &mut outs, &isolated);
        if nodes[&node_id(3)].applied_index() == nodes[&node_id(1)].applied_index()
            && !nodes[&node_id(3)].applied_index().is_zero()
        {
            break;
        }
    }
    assert_eq!(
        nodes[&node_id(3)].applied_index(),
        nodes[&node_id(1)].applied_index()
    );

    let mut read_out = NodeOutput::default();
    let stale = nodes
        .get_mut(&node_id(3))
        .expect("node3")
        .client_read(
            NodeRead::Get {
                key: Bytes::from_static(b"k0"),
            },
            ReadConsistency::Stale,
            &mut read_out,
        )
        .expect("stale read");
    assert!(matches!(
        stale.response,
        KvResponse::Value { value: Some(_), .. }
    ));
}

#[test]
fn test_follower_read_wait_queue_is_bounded() {
    let mut nodes: BTreeMap<NodeId, Node> = [(1, make_node(1, &[2])), (2, make_node(2, &[1]))]
        .into_iter()
        .map(|(n, node)| (node_id(n), node))
        .collect();
    let mut outs: BTreeMap<NodeId, NodeOutput> = [1, 2]
        .iter()
        .map(|n| (node_id(*n), NodeOutput::default()))
        .collect();
    let mut isolated: Vec<NodeId> = Vec::new();

    for _ in 0..10 {
        pump_round(&mut nodes, &mut outs, &isolated);
        if nodes[&node_id(1)].role() == Role::Leader {
            break;
        }
    }
    assert_eq!(nodes[&node_id(1)].role(), Role::Leader);

    // Node 2 goes silent: quorum acks stop arriving, so every linearizable
    // read on node 1 queues instead of completing.
    isolated.push(node_id(2));
    let bound = NodeConfig::default().max_pending_reads;
    let mut leader_out = NodeOutput::default();
    for _ in 0..bound + 5 {
        let _ = nodes.get_mut(&node_id(1)).expect("leader").client_read(
            NodeRead::Get {
                key: Bytes::from_static(b"k"),
            },
            ReadConsistency::Linearizable,
            &mut leader_out,
        );
    }
    let queued = nodes[&node_id(1)].pending_read_count();
    assert_eq!(
        queued, bound,
        "wait queue must hold exactly the bound, shedding the oldest"
    );
}
