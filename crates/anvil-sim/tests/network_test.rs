#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_raft::Message;
use anvil_sim::{InFlightPacket, PlanNet, SimNetwork};
use anvil_types::{LogIndex, NodeId, Term};

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("node")
}

fn heartbeat(term: u64) -> Message {
    Message::HeartbeatRequest {
        term: Term::from_raw(term),
        leader_id: node(1),
        commit_index: LogIndex::from_raw(0),
        read_context: None,
    }
}

fn packet(from: u64, to: u64) -> InFlightPacket {
    InFlightPacket {
        from: node(from),
        to: node(to),
        message: heartbeat(1),
        deliver_at_tick: 0,
    }
}

#[test]
fn test_network_schedule_hooks_and_partitions() {
    let mut net = SimNetwork::new();
    assert!(net.is_empty());
    assert_eq!(net.len(), 0);

    net.drop_hook = Some(Box::new(|from, _, _| from == node(9)));
    net.duplicate_hook = Some(Box::new(|_, to, _| to == node(3)));
    net.delay_hook = Some(Box::new(|_, _, _| 2));

    assert!(!net.schedule(node(9), node(2), heartbeat(1), 10));
    assert!(net.is_empty());

    assert!(net.schedule(node(1), node(2), heartbeat(1), 10));
    assert!(net.schedule(node(1), node(3), heartbeat(1), 10));
    assert_eq!(net.len(), 3);

    net.isolate(node(2), &[node(1), node(2)]);
    assert!(!net.schedule(node(1), node(2), heartbeat(1), 10));
    net.heal_all();
    assert!(net.schedule(node(1), node(2), heartbeat(1), 10));

    net.partition(&[node(1)], &[node(2)]);
    assert!(net.is_cut(node(1), node(2)));
    assert!(!net.schedule(node(1), node(2), heartbeat(1), 10));
    net.heal_all();
    assert!(!net.is_cut(node(1), node(2)));
}

#[test]
fn test_network_queue_ops() {
    let mut net = SimNetwork::new();
    assert!(net.take_at(0).is_none());
    assert!(!net.drop_at(0));
    assert!(!net.duplicate_at(0));

    net.schedule(node(1), node(2), heartbeat(1), 0);
    assert!(net.duplicate_at(0));
    assert_eq!(net.len(), 2);
    assert!(net.drop_at(5));
    assert_eq!(net.len(), 1);

    let due = net.take_due(0);
    assert_eq!(due.len(), 1);
    assert!(net.is_empty());
    assert!(!net.has_due(0));

    for _ in 0..600 {
        net.in_flight.push(packet(1, 2));
    }
    assert!(!net.duplicate_at(0));
}

#[test]
fn test_network_plan_faults_are_deterministic() {
    let salt = 0x1234_5678_9ABC_DEF0;
    let net = PlanNet {
        salt,
        drop_pct: 30,
        dup_pct: 20,
        max_delay: 3,
    };
    let mut first = SimNetwork::new();
    let mut second = SimNetwork::new();
    for _ in 0..50 {
        first.schedule_plan(node(1), node(2), heartbeat(1), 7, &net);
        second.schedule_plan(node(1), node(2), heartbeat(1), 7, &net);
    }
    let a: Vec<u64> = first.in_flight.iter().map(|p| p.deliver_at_tick).collect();
    let b: Vec<u64> = second.in_flight.iter().map(|p| p.deliver_at_tick).collect();
    assert_eq!(a, b);

    let quiet = PlanNet {
        salt,
        drop_pct: 0,
        dup_pct: 0,
        max_delay: 0,
    };
    let mut none = SimNetwork::new();
    assert!(none.schedule_plan(node(1), node(2), heartbeat(1), 0, &quiet));
    assert_eq!(none.len(), 1);

    let dropping = PlanNet {
        salt,
        drop_pct: 100,
        dup_pct: 0,
        max_delay: 0,
    };
    let mut all_drop = SimNetwork::new();
    assert!(!all_drop.schedule_plan(node(1), node(2), heartbeat(1), 0, &dropping));

    let duplicating = PlanNet {
        salt,
        drop_pct: 0,
        dup_pct: 100,
        max_delay: 0,
    };
    let mut all_dup = SimNetwork::new();
    assert!(all_dup.schedule_plan(node(1), node(2), heartbeat(1), 5, &duplicating));
    assert_eq!(all_dup.len(), 2);
    assert_eq!(all_dup.in_flight[1].deliver_at_tick, 6);

    let lagging = PlanNet {
        salt,
        drop_pct: 0,
        dup_pct: 0,
        max_delay: 4,
    };
    let mut delayed = SimNetwork::new();
    assert!(delayed.schedule_plan(node(1), node(2), heartbeat(1), 5, &lagging));
    assert!(delayed.in_flight[0].deliver_at_tick >= 5);
    assert!(delayed.in_flight[0].deliver_at_tick <= 9);
    assert!(!delayed.has_due(4));
}

#[test]
fn test_network_take_due_keeps_fifo_order() {
    let mut net = SimNetwork::new();
    net.in_flight.push(packet(1, 2));
    net.in_flight.push(packet(3, 4));
    net.in_flight[1].deliver_at_tick = 99;
    let due = net.take_due(0);
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].from, node(1));
    assert_eq!(net.len(), 1);
    assert_eq!(net.in_flight[0].from, node(3));
    assert!(net.has_due(99));
}
