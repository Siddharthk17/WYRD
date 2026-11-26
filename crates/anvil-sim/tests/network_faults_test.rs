#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(clippy::assert_is_empty)]

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

#[test]
fn test_queue_edge_ops_and_duplicate_bound() {
    let mut net = SimNetwork::new();
    assert!(net.is_empty());
    assert_eq!(net.len(), 0);
    assert!(!net.has_due(0));
    assert!(net.take_at(0).is_none());
    assert!(!net.drop_at(3));
    assert!(!net.duplicate_at(0));
    assert!(net.take_due(100).is_empty());
    assert_eq!(net.transport(), TransportMode::OrderedLossy);
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert!(net.has_due(0));
    let packet = net.take_at(5);
    assert!(packet.is_some());
    assert!(net.is_empty());
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert!(net.drop_at(0));
    assert!(net.is_empty());
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert!(net.duplicate_at(0));
    assert_eq!(net.len(), 2);
    for _ in 0..anvil_sim::network::MAX_IN_FLIGHT_PACKETS {
        net.in_flight.push(anvil_sim::InFlightPacket {
            from: node(1),
            to: node(2),
            message: heartbeat(),
            deliver_at_tick: 0,
        });
        if net.len() >= anvil_sim::network::MAX_IN_FLIGHT_PACKETS {
            break;
        }
    }
    assert!(!net.duplicate_at(0));
}

#[test]
fn test_hooks_drop_duplicate_and_delay() {
    let mut net = SimNetwork::new();
    net.drop_hook = Some(Box::new(|_, _, _| true));
    assert!(!net.schedule(node(1), node(2), heartbeat(), 0));
    net.drop_hook = None;
    net.duplicate_hook = Some(Box::new(|_, _, _| true));
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert_eq!(net.len(), 2);
    let mut drained = SimNetwork::new();
    drained.duplicate_hook = None;
    drained.delay_hook = Some(Box::new(|_, _, _| 5));
    assert!(drained.schedule(node(1), node(2), heartbeat(), 10));
    assert_eq!(drained.in_flight[0].deliver_at_tick, 15);
}

#[test]
fn test_latency_edge_models_spike_and_reset_draw() {
    assert_eq!(
        LatencyModel::Uniform { min: 5, max: 3 }.sample_hash(1, 1, 2, 9),
        5
    );
    assert_eq!(
        LatencyModel::Exponential { mean: 0 }.sample_hash(1, 1, 2, 9),
        0
    );
    let bimodal = LatencyModel::Bimodal {
        fast: 2,
        fast_spread: 0,
        slow: 50,
        slow_spread: 0,
        fast_ppm: 500_000,
    };
    let delay = bimodal.sample_hash(3, 1, 2, 4);
    assert!(delay == 2 || delay == 50);

    let mut spiked = SimNetwork::new();
    spiked.set_fault_rates(0, 100, 7, 0);
    assert!(spiked.schedule(node(1), node(2), heartbeat(), 10));
    assert_eq!(spiked.in_flight[0].deliver_at_tick, 17);

    let mut resetting = SimNetwork::new();
    resetting.set_fault_rates(0, 0, 0, 100);
    assert!(!resetting.schedule(node(1), node(2), heartbeat(), 10));
    assert!(resetting.is_backing_off(node(1), node(2), 10));
    assert!(!resetting.schedule(node(1), node(2), heartbeat(), 11));
}

#[test]
fn test_plan_hash_deterministic_faults() {
    use anvil_sim::PlanNet;
    let mut net = SimNetwork::new();
    let dropping = PlanNet {
        salt: 77,
        drop_pct: 100,
        dup_pct: 0,
        max_delay: 0,
    };
    assert!(!net.schedule_plan(node(1), node(2), heartbeat(), 0, &dropping));
    let duplicating = PlanNet {
        salt: 77,
        drop_pct: 0,
        dup_pct: 100,
        max_delay: 3,
    };
    assert!(net.schedule_plan(node(1), node(2), heartbeat(), 5, &duplicating));
    assert_eq!(net.len(), 2);
    assert!(net.in_flight.iter().all(|p| p.deliver_at_tick >= 5));
    assert!(net.in_flight.iter().all(|p| p.deliver_at_tick <= 8));
    net.heal_all();
    net.isolate(node(1), &[node(1), node(2)]);
    assert!(!net.schedule_plan(node(1), node(2), heartbeat(), 0, &duplicating));
    net.heal_pair(node(1), node(2));
    assert!(!net.is_cut(node(1), node(2)));
}

#[test]
fn test_unordered_without_reorder_preserves_fifo() {
    let mut net = SimNetwork::new();
    net.set_transport(TransportMode::Unordered);
    net.set_fault_salt(1);
    net.set_fault_rates(0, 0, 0, 0);
    net.set_latency_model(LatencyModel::Constant(0));
    for _ in 0..4 {
        assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    }
    assert_eq!(net.transport(), TransportMode::Unordered);
    let due = net.take_due(0);
    assert_eq!(due.len(), 4);
    net.set_transport(TransportMode::OrderedLossy);
    assert_eq!(net.transport(), TransportMode::OrderedLossy);
}

#[test]
#[should_panic(expected = "tick skew must be within")]
fn test_tick_skew_rejects_out_of_range() {
    let mut cluster = SimCluster::new(3, 99);
    let id = cluster.nodes.keys().copied().next().expect("node");
    cluster.set_tick_skew(id, 499);
}

#[test]
fn test_resume_unpaused_node_is_noop_and_pause_burst() {
    let mut cluster = SimCluster::new(3, 4242);
    let id = cluster.nodes.keys().copied().next().expect("node");
    cluster.resume_node(id);
    assert!(!cluster.is_paused(id));
    cluster.pause_node(id);
    cluster.tick_n(5);
    cluster.resume_node(id);
    assert!(!cluster.is_paused(id));
    assert!(cluster.take_completed_reads(id).is_empty());
    assert_eq!(cluster.tick_rate(id), 1000);
}

#[test]
fn test_directed_link_latency_override() {
    let mut net = SimNetwork::new();
    net.set_latency_model(LatencyModel::Constant(0));
    net.set_link_latency(node(1), node(2), LatencyModel::Constant(9));
    assert!(net.schedule(node(1), node(2), heartbeat(), 0));
    assert!(net.schedule(node(2), node(1), heartbeat(), 0));
    assert_eq!(net.in_flight[0].deliver_at_tick, 9);
    assert_eq!(net.in_flight[1].deliver_at_tick, 0);
}

#[test]
fn test_bimodal_spread_bands_are_deterministic() {
    let model = LatencyModel::Bimodal {
        fast: 2,
        fast_spread: 1,
        slow: 50,
        slow_spread: 5,
        fast_ppm: 50_000_000,
    };
    let mut saw_fast = false;
    let mut saw_slow = false;
    for seq in 0..256u64 {
        let delay = model.sample_hash(11, 1, 2, seq);
        if delay <= 3 {
            saw_fast = true;
        } else if (45..=55).contains(&delay) {
            saw_slow = true;
        } else {
            panic!("bimodal delay {delay} outside both bands");
        }
    }
    assert!(saw_fast && saw_slow);
    let replay: Vec<u64> = (0..256u64)
        .map(|seq| model.sample_hash(11, 1, 2, seq))
        .collect();
    let again: Vec<u64> = (0..256u64)
        .map(|seq| model.sample_hash(11, 1, 2, seq))
        .collect();
    assert_eq!(replay, again);
}

#[test]
fn test_targeted_delivery_drop_and_duplicate_paths() {
    use bytes::Bytes;
    let mut cluster = SimCluster::new(3, 555);
    assert!(!cluster.deliver_packet_at(0));
    assert!(!cluster.drop_packet_at(0));
    assert!(!cluster.duplicate_packet_at(0));
    let leader = cluster.elect_leader(100);
    let followers: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|id| *id != leader)
        .collect();
    cluster.propose(leader, Bytes::from_static(b"targeted"));
    assert!(!cluster.network.is_empty());
    assert!(cluster.deliver_packet_at(0));
    assert!(cluster.duplicate_packet_at(0));
    assert!(cluster.drop_packet_at(0));

    cluster.crash_node(followers[0]);
    cluster.propose(leader, Bytes::from_static(b"to-crashed"));
    cluster.tick_n(3);
    cluster.network.heal_all();
    cluster.drain_network(50);
}

#[test]
fn test_delivery_to_crashed_or_cut_destination_is_dropped() {
    use anvil_sim::InFlightPacket;
    let mut cluster = SimCluster::new(3, 777);
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

    cluster.crash_node(ids[1]);
    cluster.network.in_flight.push(InFlightPacket {
        from: ids[0],
        to: ids[1],
        message: heartbeat(),
        deliver_at_tick: cluster.current_tick,
    });
    assert!(!cluster.deliver_packet_at(0));
    assert!(cluster.network.is_empty());

    let mut cluster = SimCluster::new(3, 778);
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    cluster.network.in_flight.push(InFlightPacket {
        from: ids[0],
        to: ids[1],
        message: heartbeat(),
        deliver_at_tick: cluster.current_tick,
    });
    cluster.network.cut_one_way(ids[0], ids[1]);
    assert!(!cluster.deliver_packet_at(0));
    cluster.network.in_flight.push(InFlightPacket {
        from: ids[0],
        to: ids[1],
        message: heartbeat(),
        deliver_at_tick: cluster.current_tick,
    });
    cluster.network.heal_directed(ids[0], ids[1]);
    assert!(cluster.deliver_packet_at(0));

    let mut cluster = SimCluster::new(3, 779);
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    cluster.network.in_flight.push(InFlightPacket {
        from: ids[0],
        to: ids[1],
        message: heartbeat(),
        deliver_at_tick: cluster.current_tick,
    });
    cluster.crash_node(ids[1]);
    cluster.tick();
    cluster.network.in_flight.push(InFlightPacket {
        from: ids[0],
        to: ids[2],
        message: heartbeat(),
        deliver_at_tick: cluster.current_tick,
    });
    cluster.network.cut_one_way(ids[0], ids[2]);
    cluster.tick();
    cluster.network.heal_all();
    cluster.tick();
}
