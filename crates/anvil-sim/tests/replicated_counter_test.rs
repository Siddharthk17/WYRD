#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_lossless,
    clippy::cast_possible_truncation,
    clippy::items_after_statements
)]

//! Comprehensive test suite validating simulation kernel semantics using a toy
//! replicated-counter consensus protocol:
//! 1. Determinism across runs (byte-identical trace hashes given identical seeds).
//! 2. Deterministic tie ordering (insertion-order tie-breaking on identical timestamps).
//! 3. Time monotonicity (virtual time never goes backward).
//! 4. Buggify points evaluation and activation during simulation.
//! 5. Watchdog trips on deliberately stuck protocol (stagnation and early depletion).

use anvil_sim::buggify::{
    list_registered_points, reset_buggify_registry, set_buggify_enabled, set_buggify_seed,
};
use anvil_sim::kernel::{Actor, ActorContext, SimError, SimKernel};
use anvil_sim::rng::DetRng;
use anvil_sim::time::SimTime;
use anvil_sim::watchdog::{Watchdog, WatchdogError};
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};

// ---------------------------------------------------------------------------
// Toy Replicated-Counter Protocol Model
// ---------------------------------------------------------------------------

const MSG_CLIENT_PROPOSE: u8 = 1;
const MSG_REPLICATE: u8 = 2;
const MSG_REPLICATE_ACK: u8 = 3;
const MSG_COMMIT: u8 = 4;
const MSG_CLIENT_RESPONSE: u8 = 5;
const MSG_STUCK_PING: u8 = 6;
const MSG_STUCK_PONG: u8 = 7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtocolMsg {
    kind: u8,
    sender: u8,
    id_or_idx: u16,
    value: u32,
}

impl ProtocolMsg {
    fn pack(self) -> u64 {
        ((self.kind as u64) << 56)
            | ((self.sender as u64) << 48)
            | ((self.id_or_idx as u64) << 32)
            | (self.value as u64)
    }

    fn unpack(raw: u64) -> Self {
        Self {
            kind: (raw >> 56) as u8,
            sender: (raw >> 48) as u8,
            id_or_idx: (raw >> 32) as u16,
            value: raw as u32,
        }
    }
}

/// Node actor replicating counter updates via simple majority acknowledgement.
struct NodeActor {
    id: u8,
    peers: Vec<u64>,
    is_leader: bool,
    counter: u32,
    last_log_index: u16,
    commit_index: u16,
    proposal_values: BTreeMap<u16, u32>,
    acks: BTreeMap<u16, BTreeSet<u8>>,
    rng: DetRng,
    last_seen_time: SimTime,
    time_travel_detected: bool,
    enable_buggify_drop: bool,
}

impl NodeActor {
    fn new(id: u8, peers: Vec<u64>, is_leader: bool, seed: u64) -> Self {
        Self {
            id,
            peers,
            is_leader,
            counter: 0,
            last_log_index: 0,
            commit_index: 0,
            proposal_values: BTreeMap::new(),
            acks: BTreeMap::new(),
            rng: DetRng::new(seed, &format!("node_{id}")),
            last_seen_time: SimTime::ZERO,
            time_travel_detected: false,
            enable_buggify_drop: false,
        }
    }
}

impl Actor for NodeActor {
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64) {
        let now = ctx.now();
        if now < self.last_seen_time {
            self.time_travel_detected = true;
        }
        self.last_seen_time = now;

        let msg = ProtocolMsg::unpack(payload);

        match msg.kind {
            MSG_CLIENT_PROPOSE => {
                if !self.is_leader {
                    return;
                }
                self.last_log_index = self.last_log_index.saturating_add(1);
                let idx = self.last_log_index;
                self.proposal_values.insert(idx, msg.value);

                let mut self_acks = BTreeSet::new();
                self_acks.insert(self.id);
                self.acks.insert(idx, self_acks);

                // Broadcast Replicate to peers
                for &peer in &self.peers {
                    let delay_nanos = self.rng.gen_range(5_000_000..15_000_000);
                    let replicate_msg = ProtocolMsg {
                        kind: MSG_REPLICATE,
                        sender: self.id,
                        id_or_idx: idx,
                        value: msg.value,
                    };
                    ctx.schedule(
                        now.saturating_add_nanos(delay_nanos),
                        peer,
                        replicate_msg.pack(),
                    );
                }
            }
            MSG_REPLICATE => {
                if self.is_leader {
                    return;
                }

                // Check buggify fault injection
                if self.enable_buggify_drop
                    && anvil_sim::buggify!("counter_drop_replicate", 250_000)
                {
                    // Simulated message drop
                    return;
                }

                let reply_delay = self.rng.gen_range(2_000_000..8_000_000);
                let ack_msg = ProtocolMsg {
                    kind: MSG_REPLICATE_ACK,
                    sender: self.id,
                    id_or_idx: msg.id_or_idx,
                    value: msg.value,
                };
                ctx.schedule(
                    now.saturating_add_nanos(reply_delay),
                    msg.sender as u64,
                    ack_msg.pack(),
                );
            }
            MSG_REPLICATE_ACK => {
                if !self.is_leader {
                    return;
                }
                let idx = msg.id_or_idx;
                let acks = self.acks.entry(idx).or_default();
                acks.insert(msg.sender);

                // Majority of 3 nodes is 2
                if acks.len() >= 2 && idx > self.commit_index {
                    self.commit_index = idx;
                    if let Some(val) = self.proposal_values.get(&idx) {
                        self.counter = self.counter.saturating_add(*val);
                    }
                    ctx.record_progress();

                    // Commit to peers
                    for &peer in &self.peers {
                        let delay = self.rng.gen_range(2_000_000..5_000_000);
                        let commit_msg = ProtocolMsg {
                            kind: MSG_COMMIT,
                            sender: self.id,
                            id_or_idx: idx,
                            value: self.counter,
                        };
                        ctx.schedule(now.saturating_add_nanos(delay), peer, commit_msg.pack());
                    }

                    // Respond to client (actor 100)
                    let client_delay = self.rng.gen_range(1_000_000..3_000_000);
                    let resp = ProtocolMsg {
                        kind: MSG_CLIENT_RESPONSE,
                        sender: self.id,
                        id_or_idx: idx,
                        value: self.counter,
                    };
                    ctx.schedule(now.saturating_add_nanos(client_delay), 100, resp.pack());
                }
            }
            MSG_COMMIT => {
                if self.is_leader {
                    return;
                }
                self.commit_index = msg.id_or_idx;
                self.counter = msg.value;
                ctx.record_progress();
            }
            _ => {}
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Client actor proposing sequential counter increments and asserting replies.
struct ClientActor {
    leader_id: u64,
    target_proposals: u32,
    confirmed_proposals: u32,
    final_counter: u32,
    rng: DetRng,
    last_seen_time: SimTime,
    time_travel_detected: bool,
}

impl ClientActor {
    fn new(leader_id: u64, target_proposals: u32, seed: u64) -> Self {
        Self {
            leader_id,
            target_proposals,
            confirmed_proposals: 0,
            final_counter: 0,
            rng: DetRng::new(seed, "client"),
            last_seen_time: SimTime::ZERO,
            time_travel_detected: false,
        }
    }
}

impl Actor for ClientActor {
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64) {
        let now = ctx.now();
        if now < self.last_seen_time {
            self.time_travel_detected = true;
        }
        self.last_seen_time = now;

        let msg = ProtocolMsg::unpack(payload);
        if msg.kind == MSG_CLIENT_RESPONSE {
            self.confirmed_proposals = self.confirmed_proposals.saturating_add(1);
            self.final_counter = msg.value;
            ctx.record_progress();

            if self.confirmed_proposals < self.target_proposals {
                let think_time = self.rng.gen_range(1_000_000..5_000_000);
                let next_req = ProtocolMsg {
                    kind: MSG_CLIENT_PROPOSE,
                    sender: 100,
                    id_or_idx: self.confirmed_proposals as u16,
                    value: 1,
                };
                ctx.schedule(
                    now.saturating_add_nanos(think_time),
                    self.leader_id,
                    next_req.pack(),
                );
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Helper building a standard 3-node cluster and 1 client.
fn build_replicated_counter_sim(seed: u64, proposals: u32, enable_buggify: bool) -> SimKernel {
    let mut kernel = SimKernel::new(seed);

    let mut node1 = NodeActor::new(1, vec![2, 3], true, seed);
    let mut node2 = NodeActor::new(2, vec![1, 3], false, seed);
    let mut node3 = NodeActor::new(3, vec![1, 2], false, seed);
    node1.enable_buggify_drop = enable_buggify;
    node2.enable_buggify_drop = enable_buggify;
    node3.enable_buggify_drop = enable_buggify;

    let client = ClientActor::new(1, proposals, seed);

    kernel.register_actor(1, Box::new(node1));
    kernel.register_actor(2, Box::new(node2));
    kernel.register_actor(3, Box::new(node3));
    kernel.register_actor(100, Box::new(client));

    // Seed first client proposal at T = 1 ms
    let first_req = ProtocolMsg {
        kind: MSG_CLIENT_PROPOSE,
        sender: 100,
        id_or_idx: 0,
        value: 1,
    };
    kernel.schedule(SimTime::from_millis(1), 1, first_req.pack());

    kernel
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn test_replicated_counter_determinism_across_runs() {
    const MASTER_SEED: u64 = 0xDEAD_BEEF_CAFE_BABE;
    const NUM_PROPOSALS: u32 = 20;

    let mut kernel1 = build_replicated_counter_sim(MASTER_SEED, NUM_PROPOSALS, false);
    let report1 = kernel1.run().expect("run 1 should succeed");

    let mut kernel2 = build_replicated_counter_sim(MASTER_SEED, NUM_PROPOSALS, false);
    let report2 = kernel2.run().expect("run 2 should succeed");

    let mut kernel3 = build_replicated_counter_sim(MASTER_SEED, NUM_PROPOSALS, false);
    let report3 = kernel3.run().expect("run 3 should succeed");

    // All metrics and hashes must match byte-for-byte
    assert_eq!(report1.final_time, report2.final_time);
    assert_eq!(report2.final_time, report3.final_time);
    assert_eq!(report1.events_processed, report2.events_processed);
    assert_eq!(report2.events_processed, report3.events_processed);
    assert_eq!(report1.trace_hash, report2.trace_hash);
    assert_eq!(report2.trace_hash, report3.trace_hash);

    // Final state of nodes must match exactly
    let client1 = kernel1.actor::<ClientActor>(100).expect("client 1");
    let client2 = kernel2.actor::<ClientActor>(100).expect("client 2");
    let client3 = kernel3.actor::<ClientActor>(100).expect("client 3");
    assert_eq!(client1.confirmed_proposals, NUM_PROPOSALS);
    assert_eq!(client2.confirmed_proposals, NUM_PROPOSALS);
    assert_eq!(client3.confirmed_proposals, NUM_PROPOSALS);
    assert_eq!(client1.final_counter, NUM_PROPOSALS);
    assert_eq!(client2.final_counter, NUM_PROPOSALS);
    assert_eq!(client3.final_counter, NUM_PROPOSALS);

    // Different seed must produce a distinct trace hash
    let mut kernel_diff = build_replicated_counter_sim(MASTER_SEED + 1, NUM_PROPOSALS, false);
    let report_diff = kernel_diff.run().expect("diff run should succeed");
    assert_ne!(
        report1.trace_hash, report_diff.trace_hash,
        "different seed must produce distinct trace hash"
    );
}

#[test]
fn test_replicated_counter_tie_ordering() {
    // Test that events with identical timestamps are popped in strictly monotonic sequence order
    struct CollectorActor {
        order: Vec<u64>,
    }

    impl Actor for CollectorActor {
        fn on_event(&mut self, _ctx: &mut ActorContext<'_>, payload: u64) {
            self.order.push(payload);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    let mut kernel = SimKernel::new(999);
    kernel.register_actor(10, Box::new(CollectorActor { order: Vec::new() }));

    let tie_time = SimTime::from_nanos(50_000);
    kernel.schedule(tie_time, 10, 101);
    kernel.schedule(tie_time, 10, 102);
    kernel.schedule(tie_time, 10, 103);
    kernel.schedule(tie_time, 10, 104);

    kernel.run().expect("kernel run");

    let collector = kernel.actor::<CollectorActor>(10).expect("collector");
    assert_eq!(collector.order, vec![101, 102, 103, 104]);

    // Reverse schedule order
    let mut kernel_rev = SimKernel::new(999);
    kernel_rev.register_actor(10, Box::new(CollectorActor { order: Vec::new() }));
    kernel_rev.schedule(tie_time, 10, 104);
    kernel_rev.schedule(tie_time, 10, 103);
    kernel_rev.schedule(tie_time, 10, 102);
    kernel_rev.schedule(tie_time, 10, 101);

    kernel_rev.run().expect("kernel rev run");

    let collector_rev = kernel_rev
        .actor::<CollectorActor>(10)
        .expect("collector rev");
    assert_eq!(collector_rev.order, vec![104, 103, 102, 101]);
}

#[test]
fn test_replicated_counter_time_never_goes_backward() {
    let mut kernel = build_replicated_counter_sim(777, 30, false);
    kernel.run().expect("run should complete cleanly");

    for id in [1, 2, 3] {
        let node = kernel.actor::<NodeActor>(id).expect("node");
        assert!(
            !node.time_travel_detected,
            "node {id} detected backward time movement"
        );
    }
    let client = kernel.actor::<ClientActor>(100).expect("client");
    assert!(
        !client.time_travel_detected,
        "client detected backward time movement"
    );

    // Negative verification: if an event is processed with a past timestamp, kernel rejects it loudly
    let mut regressed_kernel = SimKernel::new(123);
    struct Dummy;
    impl Actor for Dummy {
        fn on_event(&mut self, _ctx: &mut ActorContext<'_>, _payload: u64) {}
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }
    regressed_kernel.register_actor(1, Box::new(Dummy));
    // Advance kernel time to 100ns
    regressed_kernel.schedule(SimTime::from_nanos(100), 1, 0);
    let _ = regressed_kernel.step().expect("step 1");
    assert_eq!(regressed_kernel.now(), SimTime::from_nanos(100));

    // Force an event at 50ns (in the past relative to current now)
    // Note: EventQueue itself does not prevent inserting past events, but kernel.step() checks time monotonicity
    regressed_kernel.schedule(SimTime::from_nanos(50), 1, 0);
    let err = regressed_kernel.step().unwrap_err();
    match err {
        SimError::TimeRegression { current, attempted } => {
            assert_eq!(current, SimTime::from_nanos(100));
            assert_eq!(attempted, SimTime::from_nanos(50));
        }
        other => panic!("expected TimeRegression, got {other:?}"),
    }
}

#[test]
fn test_replicated_counter_buggify_points_fire() {
    reset_buggify_registry();
    set_buggify_enabled(true);
    set_buggify_seed(4242);

    // Run simulation with buggify enabled
    let mut kernel = build_replicated_counter_sim(4242, 25, true);
    let _ = kernel.run();

    let points = list_registered_points();
    assert_ne!(points.len(), 0, "buggify points must be registered");

    let drop_point = points
        .iter()
        .find(|p| p.name == "counter_drop_replicate")
        .expect("counter_drop_replicate point must exist");

    assert!(
        drop_point.evaluations > 0,
        "counter_drop_replicate must be evaluated"
    );
    assert!(
        drop_point.activations > 0,
        "counter_drop_replicate must fire at least once"
    );

    // Disable buggify and verify activations freeze
    set_buggify_enabled(false);
    let mut hits = 0;
    for _ in 0..50 {
        if anvil_sim::buggify!("counter_drop_replicate", 1_000_000) {
            hits += 1;
        }
    }
    assert_eq!(hits, 0, "disabled buggify must never activate");
}

#[test]
fn test_watchdog_trips_on_deliberately_stuck_protocol_stagnation() {
    // Protocol stuck in ping-pong loop without ever calling record_progress()
    struct StuckNode {
        peer: u64,
    }

    impl Actor for StuckNode {
        fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64) {
            let next_kind = if payload == MSG_STUCK_PING as u64 {
                MSG_STUCK_PONG as u64
            } else {
                MSG_STUCK_PING as u64
            };
            // Never call ctx.record_progress()!
            ctx.schedule(ctx.now().saturating_add_nanos(1_000), self.peer, next_kind);
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    let stall_limit = 25;
    let mut kernel = SimKernel::new(555).with_watchdog(Watchdog::new(None, stall_limit));

    kernel.register_actor(1, Box::new(StuckNode { peer: 2 }));
    kernel.register_actor(2, Box::new(StuckNode { peer: 1 }));

    kernel.schedule(SimTime::from_nanos(1_000), 1, MSG_STUCK_PING as u64);

    let err = kernel.run().unwrap_err();
    match err {
        SimError::Watchdog(WatchdogError::StagnationDetected {
            events_without_progress,
            limit,
        }) => {
            assert_eq!(events_without_progress, stall_limit);
            assert_eq!(limit, stall_limit);
        }
        other => panic!("expected StagnationDetected, got {other:?}"),
    }
}

#[test]
fn test_watchdog_trips_on_early_queue_depletion() {
    // Queue empties at T = 10ms, but watchdog expects simulation to run for at least 100ms
    let mut kernel =
        SimKernel::new(666).with_watchdog(Watchdog::new(Some(SimTime::from_millis(100)), 10_000));

    struct TerminatingActor;
    impl Actor for TerminatingActor {
        fn on_event(&mut self, _ctx: &mut ActorContext<'_>, _payload: u64) {
            // Empties queue immediately
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn as_any_mut(&mut self) -> &mut dyn Any {
            self
        }
    }

    kernel.register_actor(1, Box::new(TerminatingActor));
    kernel.schedule(SimTime::from_millis(10), 1, 0);

    let err = kernel.run().unwrap_err();
    match err {
        SimError::Watchdog(WatchdogError::PrematureDepletion {
            current_time,
            expected_min_time,
        }) => {
            assert_eq!(current_time, SimTime::from_millis(10));
            assert_eq!(expected_min_time, SimTime::from_millis(100));
        }
        other => panic!("expected PrematureDepletion, got {other:?}"),
    }
}
