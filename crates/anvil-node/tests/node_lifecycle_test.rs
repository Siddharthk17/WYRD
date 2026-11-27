#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty
)]

use anvil_node::{Node, NodeConfig, NodeOutput};
use anvil_raft::{PersistOp, PersistToken, RaftConfig, RecoveredState, Role};
use anvil_types::{ClusterId, EntryKind, LogIndex, NodeId};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("node")
}

fn cluster() -> ClusterId {
    ClusterId::from_raw(7)
}

fn raft_config(id: NodeId, peers: &[NodeId], learners: &[NodeId]) -> RaftConfig {
    RaftConfig {
        cluster_id: cluster(),
        learners: learners.iter().copied().collect(),
        node_id: id,
        peers: peers.iter().copied().collect(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 8,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    }
}

fn joining_config(id: NodeId) -> RaftConfig {
    RaftConfig {
        cluster_id: cluster(),
        learners: BTreeSet::from([id]),
        node_id: id,
        peers: BTreeSet::new(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 8,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    }
}

fn make_node(n: u64, peers: &[u64]) -> Node {
    let id = node_id(n);
    let peer_ids: Vec<NodeId> = peers.iter().map(|p| node_id(*p)).collect();
    Node::new(
        id,
        raft_config(id, &peer_ids, &[]),
        RecoveredState::default(),
        NodeConfig::default(),
        ChaCha8Rng::seed_from_u64(n),
    )
}

fn ack_persisted(node: &mut Node, out: &mut NodeOutput) {
    for _ in 0..16 {
        let tokens: Vec<PersistToken> = out
            .persist_ops
            .iter()
            .map(|op| match op {
                PersistOp::HardState { token, .. }
                | PersistOp::AppendEntries { token, .. }
                | PersistOp::TruncateAndAppend { token, .. } => *token,
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

/// Ticks every node, acks persistence, and delivers all messages.
fn pump_round(nodes: &mut BTreeMap<NodeId, Node>, outs: &mut BTreeMap<NodeId, NodeOutput>) {
    let ids: Vec<NodeId> = nodes.keys().copied().collect();
    for id in &ids {
        let out = outs.get_mut(id).expect("out");
        nodes.get_mut(id).expect("node").tick(out);
        ack_persisted(nodes.get_mut(id).expect("node"), out);
    }
    let ids: Vec<NodeId> = nodes.keys().copied().collect();
    for from in &ids {
        let messages = std::mem::take(&mut outs.get_mut(from).expect("out").messages);
        for outbound in messages {
            if !nodes.contains_key(&outbound.to) {
                // Not yet started (or already removed): the network drops it.
                continue;
            }
            let cluster_id = nodes.get(&outbound.to).map_or(cluster(), Node::cluster_id);
            let node = nodes.get_mut(&outbound.to).expect("peer");
            let out = outs.get_mut(&outbound.to).expect("peer out");
            node.receive(*from, cluster_id, outbound.message, out);
            ack_persisted(node, out);
        }
    }
}

fn elect(nodes: &mut BTreeMap<NodeId, Node>, outs: &mut BTreeMap<NodeId, NodeOutput>) {
    for _ in 0..60 {
        pump_round(nodes, outs);
        if nodes.get(&node_id(1)).expect("n1").role() == Role::Leader {
            break;
        }
    }
    assert_eq!(
        nodes.get(&node_id(1)).expect("n1").role(),
        Role::Leader,
        "node 1 never elected"
    );
    // Wait for the inaugural no-op to commit: only then does the leader
    // accept membership changes.
    for _ in 0..120 {
        pump_round(nodes, outs);
        if nodes.get(&node_id(1)).expect("n1").commit_index() >= LogIndex::from_raw(1) {
            return;
        }
    }
    panic!("inaugural no-op never committed");
}

/// Pumps until every node applied at least `index`, or panics.
fn wait_applied(
    nodes: &mut BTreeMap<NodeId, Node>,
    outs: &mut BTreeMap<NodeId, NodeOutput>,
    index: LogIndex,
) {
    for _ in 0..300 {
        pump_round(nodes, outs);
        if nodes.values().all(|node| node.applied_index() >= index) {
            return;
        }
    }
    panic!("applied never reached {index:?}");
}

#[test]
fn test_bootstrap_writes_identical_entry() {
    let peers: Vec<NodeId> = vec![node_id(1), node_id(2), node_id(3)];
    let mut entries = Vec::new();
    for n in 1..=3u64 {
        let id = node_id(n);
        let others: Vec<NodeId> = peers.iter().copied().filter(|p| *p != id).collect();
        let node = Node::bootstrap(
            raft_config(id, &others, &[]),
            NodeConfig::default(),
            ChaCha8Rng::seed_from_u64(n),
        );
        entries.push(node.log_entry(LogIndex::from_raw(1)).expect("index 1"));
    }
    assert_eq!(entries[0], entries[1]);
    assert_eq!(entries[1], entries[2]);
    assert_eq!(entries[0].kind, EntryKind::LeaveJoint);
}

#[test]
fn test_joining_node_waits_without_campaigning() {
    let mut joiner = Node::joining(
        joining_config(node_id(4)),
        NodeConfig::default(),
        ChaCha8Rng::seed_from_u64(4),
    );
    let mut out = NodeOutput::default();
    for _ in 0..40 {
        joiner.tick(&mut out);
    }
    assert_eq!(joiner.role(), Role::Follower);
    assert!(out.messages.is_empty());
    assert!(joiner.membership_voters().is_empty());
}

#[test]
fn test_foreign_cluster_messages_rejected() {
    let mut node = make_node(1, &[2, 3]);
    let mut out = NodeOutput::default();
    let before = node.term();
    node.receive(
        node_id(2),
        ClusterId::from_raw(999),
        anvil_raft::Message::HeartbeatRequest {
            term: before,
            leader_id: node_id(2),
            commit_index: LogIndex::from_raw(0),
            read_context: None,
        },
        &mut out,
    );
    assert_eq!(node.cluster_rejects(), 1);
    assert_eq!(node.term(), before);
    assert!(out.messages.is_empty());
}

#[test]
fn test_config_entries_skip_state_machine_and_promote() {
    let mut nodes = BTreeMap::new();
    let mut outs = BTreeMap::new();
    for n in 1..=3u64 {
        let peers: Vec<u64> = [1, 2, 3].iter().filter(|p| **p != n).copied().collect();
        nodes.insert(node_id(n), make_node(n, &peers));
        outs.insert(node_id(n), NodeOutput::default());
    }
    elect(&mut nodes, &mut outs);

    // Promoting a non-learner is refused.
    {
        let leader = nodes.get_mut(&node_id(1)).expect("leader");
        let out = outs.get_mut(&node_id(1)).expect("out");
        assert!(!leader.propose_promote(node_id(2), out));
    }

    // Add node 4 as learner.
    {
        let leader = nodes.get_mut(&node_id(1)).expect("leader");
        let out = outs.get_mut(&node_id(1)).expect("out");
        assert!(leader.propose_config(
            vec![node_id(1), node_id(2), node_id(3)],
            vec![node_id(4)],
            out
        ));
    }
    for _ in 0..120 {
        pump_round(&mut nodes, &mut outs);
        if !nodes.get(&node_id(1)).expect("leader").in_joint()
            && nodes.get(&node_id(1)).expect("leader").commit_index() >= LogIndex::from_raw(2)
        {
            break;
        }
    }
    let leader = nodes.get(&node_id(1)).expect("leader");
    assert!(!leader.in_joint());
    assert!(leader.membership_voters().contains(&node_id(3)));
    let committed = leader.commit_index();

    // Config entries advanced applied without client responses.
    wait_applied(&mut nodes, &mut outs, committed);
    let responses: usize = outs.values().map(|out| out.client_responses.len()).sum();
    assert_eq!(responses, 0, "config entries must not answer clients");
}

#[test]
fn test_joiner_catches_up_by_log() {
    let mut nodes = BTreeMap::new();
    let mut outs = BTreeMap::new();
    for n in 1..=3u64 {
        let peers: Vec<u64> = [1, 2, 3].iter().filter(|p| **p != n).copied().collect();
        nodes.insert(node_id(n), make_node(n, &peers));
        outs.insert(node_id(n), NodeOutput::default());
    }
    elect(&mut nodes, &mut outs);

    // Register the joiner as a learner on the leader, then start it.
    {
        let leader = nodes.get_mut(&node_id(1)).expect("leader");
        let out = outs.get_mut(&node_id(1)).expect("out");
        assert!(leader.propose_config(
            vec![node_id(1), node_id(2), node_id(3)],
            vec![node_id(4)],
            out
        ));
    }
    nodes.insert(
        node_id(4),
        Node::joining(
            joining_config(node_id(4)),
            NodeConfig::default(),
            ChaCha8Rng::seed_from_u64(4),
        ),
    );
    outs.insert(node_id(4), NodeOutput::default());

    for _ in 0..400 {
        pump_round(&mut nodes, &mut outs);
        let joiner = nodes.get(&node_id(4)).expect("joiner");
        if !joiner.membership_voters().is_empty() && !joiner.in_joint() {
            break;
        }
    }
    let joiner = nodes.get(&node_id(4)).expect("joiner");
    assert!(joiner.membership_voters().contains(&node_id(1)));
    assert!(!joiner.in_joint());
    assert_eq!(joiner.role(), Role::Follower);
}
