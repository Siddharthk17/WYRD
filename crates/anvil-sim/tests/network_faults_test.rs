#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::{LatencyModel, SimCluster, SimNetwork, TransportMode};
use anvil_types::NodeId;

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("node")
}

fn heartbeat() -> anvil_raft::Message {
    anvil_raft::Message::HeartbeatRequest {
        term: anvil_types::Term::from_raw(1),
        leader_id: node(1),
        commit_index: anvil_types::LogIndex::from_raw(0),
        read_context: None,
    }
}

#[test]
fn test_latency_models_are_deterministic_and_bounded() {
    let models = [
        LatencyModel::Constant(7),
        LatencyModel::Uniform { min: 2, max: 9 },
        LatencyModel::Exponential { mean: 10 },
        LatencyModel::Bimodal {
            fast: 2,
            fast_spread: 1,
            slow: 50,
            slow_spread: 5,
            fast_ppm: 900_000,
        },
    ];
    for model in models {
        let first: Vec<u64> = (1..=32)
            .map(|seq| model.sample_hash(99, 1, 2, seq))
            .collect();
        let second: Vec<u64> = (1..=32)
            .map(|seq| model.sample_hash(99, 1, 2, seq))
            .collect();
        assert_eq!(first, second);
    }
    for seq in 1..=64 {
        let delay = LatencyModel::Uniform { min: 2, max: 9 }.sample_hash(7, 1, 2, seq);
        assert!((2..=9).contains(&delay));
        assert_eq!(LatencyModel::Constant(7).sample_hash(7, 1, 2, seq), 7);
        let exp = LatencyModel::Exponential { mean: 10 }.sample_hash(7, 1, 2, seq);
        assert!(exp <= 80);
    }
}

#[test]
fn test_bandwidth_cap_queues_without_dropping() {
    let mut net = SimNetwork::new();
    net.set_bandwidth_per_tick(2);
    assert!(net.schedule(node(1), node(2), heartbeat(), 10));
    assert!(net.schedule(node(1), node(2), heartbeat(), 10));
    assert!(net.schedule(node(1), node(2), heartbeat(), 10));
    assert_eq!(net.len(), 3);
    let ticks: Vec<u64> = net.in_flight.iter().map(|p| p.deliver_at_tick).collect();
    assert_eq!(ticks[0], 10);
    assert_eq!(ticks[1], 10);
    assert_eq!(ticks[2], 11);
}

#[test]
fn test_ordered_fifo_vs_unordered_reorder() {
    let mut ordered = SimNetwork::new();
    ordered.set_transport(TransportMode::OrderedLossy);
    for _ in 0..8 {
        assert!(ordered.schedule(node(1), node(2), heartbeat(), 5));
    }
    let due = ordered.take_due(100);
    assert_eq!(due.len(), 8);

    let mut unordered = SimNetwork::new();
    unordered.set_transport(TransportMode::Unordered);
    unordered.set_fault_salt(0xDEAD_BEEF);
    unordered.set_fault_rates(100, 0, 0, 0);
    for _ in 0..8 {
        assert!(unordered.schedule(node(1), node(2), heartbeat(), 5));
    }
    let due_unordered = unordered.take_due(100);
    assert_eq!(due_unordered.len(), 8);

    let mut second = SimNetwork::new();
    second.set_transport(TransportMode::Unordered);
    second.set_fault_salt(0xDEAD_BEEF);
    second.set_fault_rates(100, 0, 0, 0);
    for _ in 0..8 {
        assert!(second.schedule(node(1), node(2), heartbeat(), 5));
    }
    let replay = second.take_due(100);
    let first_order: Vec<u64> = due_unordered.iter().map(|p| p.deliver_at_tick).collect();
    let replay_order: Vec<u64> = replay.iter().map(|p| p.deliver_at_tick).collect();
    assert_eq!(first_order, replay_order);
}

#[test]
fn test_partitions_bisection_bridge_and_one_way() {
    let mut net = SimNetwork::new();
    let a = node(1);
    let b = node(2);
    let c = node(3);

    net.bisect(&[a], &[b, c]);
    assert!(net.is_cut(a, b));
    assert!(net.is_cut(b, a));
    assert!(!net.is_cut(b, c));
    net.heal_all();

    net.cut_one_way(a, b);
    assert!(net.is_cut(a, b));
    assert!(!net.is_cut(b, a));
    assert!(net.schedule(b, a, heartbeat(), 0));
    assert!(!net.schedule(a, b, heartbeat(), 0));
    net.heal_directed(a, b);
    assert!(!net.is_cut(a, b));

    net.partition(&[a], &[b]);
    net.bridge_hub(c, &[a], &[b]);
    assert!(net.is_cut(a, b));
    assert!(!net.is_cut(a, c));
    assert!(!net.is_cut(c, b));
}

#[test]
fn test_reset_drops_in_flight_and_backs_off() {
    let mut net = SimNetwork::new();
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert_eq!(net.len(), 2);
    net.reset_connection(node(1), node(2), 0, 8);
    assert!(net.is_empty());
    assert!(net.is_backing_off(node(1), node(2), 4));
    assert!(!net.schedule(node(1), node(2), heartbeat(), 4));
    assert!(!net.is_backing_off(node(1), node(2), 9));
    assert!(net.schedule(node(1), node(2), heartbeat(), 9));
}

#[test]
fn test_tick_skew_and_pause_with_stale_timers() {
    let mut cluster = SimCluster::new(3, 1234);
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    let slow = ids[0];
    let fast = ids[1];

    cluster.set_tick_skew(slow, 500);
    cluster.set_tick_skew(fast, 2000);
    assert_eq!(cluster.tick_rate(slow), 500);
    assert_eq!(cluster.tick_rate(fast), 2000);

    let paused = ids[2];
    cluster.pause_node(paused);
    assert!(cluster.is_paused(paused));
    let tick_before = cluster.current_tick;
    cluster.tick_n(6);
    assert_eq!(cluster.current_tick, tick_before + 6);
    cluster.resume_node(paused);
    assert!(!cluster.is_paused(paused));

    cluster.tick_n(30);
    assert!(cluster.current_tick > 0);
}
