#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::buggify::{list_registered_points, reset_buggify_registry, set_buggify_seed};
use anvil_sim::kernel::{Actor, SimKernel};
use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue, HierarchicalTimingWheel};
use anvil_sim::rng::{DetRng, derive_component_seed};
use anvil_sim::time::SimTime;
use anvil_sim::trace::{TraceEvent, TraceRecorder};
use anvil_sim::watchdog::{Watchdog, WatchdogError};

#[test]
fn test_sim_time_operations() {
    let t0 = SimTime::ZERO;
    assert_eq!(t0.as_nanos(), 0);

    let t1 = SimTime::from_nanos(500);
    let t2 = SimTime::from_nanos(1_000);
    assert!(t1 < t2);
    assert_eq!(t1.checked_add_nanos(500), Some(t2));
    assert_eq!(t2.checked_sub_nanos(500), Some(t1));

    assert_eq!(t2.saturating_duration_since(t1), 500);
    assert_eq!(t1.saturating_duration_since(t2), 0);

    let t_sec = SimTime::from_secs(2);
    assert_eq!(t_sec.as_nanos(), 2_000_000_000);
    assert_eq!(format!("{t_sec}"), "2.000s");
}

#[test]
fn test_det_rng_stream_derivation_isolation() {
    let master_seed = 0x1234_5678_9ABC_DEF0;

    let seed1 = derive_component_seed(master_seed, "node/1/election");
    let seed2 = derive_component_seed(master_seed, "node/2/election");
    let seed1_repeat = derive_component_seed(master_seed, "node/1/election");

    assert_eq!(seed1, seed1_repeat, "derivation must be deterministic");
    assert_ne!(seed1, seed2, "different paths must derive different seeds");

    let mut rng1 = DetRng::new(master_seed, "node/1/election");
    let mut rng2 = DetRng::new(master_seed, "node/2/election");

    let draw1 = [rng1.next_u64(), rng1.next_u64(), rng1.next_u64()];
    let draw2 = [rng2.next_u64(), rng2.next_u64(), rng2.next_u64()];

    assert_ne!(draw1, draw2);
}

#[test]
fn test_det_rng_samplers() {
    let mut rng = DetRng::new(42, "test");

    // Unbiased range
    for _ in 0..10_000 {
        let v = rng.gen_range(10..20);
        assert!((10..20).contains(&v));
    }

    // Bernoulli
    assert!(rng.bernoulli(10, 10));
    assert!(!rng.bernoulli(0, 10));

    // Fisher-Yates shuffle
    let mut items = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
    rng.shuffle(&mut items);
    let mut sorted = items;
    sorted.sort_unstable();
    assert_eq!(sorted, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);

    // Bimodal
    for _ in 0..100 {
        let v = rng.bimodal(100, 10, 10_000, 50, 500_000);
        assert!(v > 0);
    }

    // Exponential
    for _ in 0..100 {
        let v = rng.exponential(1_000);
        assert!(v < 100_000); // reasonable bound for mean 1,000
    }
}

#[test]
fn test_event_queue_tie_breaking_deterministic() {
    let mut heap = BinaryHeapEventQueue::new();
    let mut wheel = HierarchicalTimingWheel::new();

    let t = SimTime::from_nanos(100);
    // Push multiple events with the same timestamp
    heap.push(t, 1, 10); // seq 0
    heap.push(t, 2, 20); // seq 1
    heap.push(t, 3, 30); // seq 2

    wheel.push(t, 1, 10);
    wheel.push(t, 2, 20);
    wheel.push(t, 3, 30);

    // Heap pop must preserve insertion sequence exactly
    let ev0 = heap.pop().expect("ev0");
    let ev1 = heap.pop().expect("ev1");
    let ev2 = heap.pop().expect("ev2");

    assert_eq!(ev0.insertion_seq, 0);
    assert_eq!(ev0.actor_id, 1);
    assert_eq!(ev1.insertion_seq, 1);
    assert_eq!(ev1.actor_id, 2);
    assert_eq!(ev2.insertion_seq, 2);
    assert_eq!(ev2.actor_id, 3);

    let wev0 = wheel.pop().expect("wev0");
    let wev1 = wheel.pop().expect("wev1");
    let wev2 = wheel.pop().expect("wev2");

    assert_eq!(wev0.insertion_seq, 0);
    assert_eq!(wev1.insertion_seq, 1);
    assert_eq!(wev2.insertion_seq, 2);
}

#[test]
fn test_trace_recorder_ring_buffer_and_blake3() {
    let mut tracer = TraceRecorder::with_capacity(3);

    for i in 1..=5 {
        tracer.record(TraceEvent {
            time: SimTime::from_nanos(i * 10),
            actor_id: i,
            event_kind: 1,
            payload: [0u8; 14],
        });
    }

    assert_eq!(tracer.total_events(), 5);
    let recent = tracer.recent_events();
    assert_eq!(recent.len(), 3);
    assert_eq!(recent[0].actor_id, 3);
    assert_eq!(recent[1].actor_id, 4);
    assert_eq!(recent[2].actor_id, 5);

    let hash1 = tracer.finalize_hash();

    // Replay identical sequence and verify hash matches bit-for-bit
    let mut tracer_replay = TraceRecorder::with_capacity(3);
    for i in 1..=5 {
        tracer_replay.record(TraceEvent {
            time: SimTime::from_nanos(i * 10),
            actor_id: i,
            event_kind: 1,
            payload: [0u8; 14],
        });
    }
    let hash2 = tracer_replay.finalize_hash();
    assert_eq!(hash1, hash2, "blake3 hash must match byte-for-byte");
}

#[test]
fn test_watchdog_stall_and_early_depletion() {
    let mut watchdog = Watchdog::new(Some(SimTime::from_nanos(1000)), 5);

    for _ in 0..4 {
        assert!(watchdog.on_event().is_ok());
    }
    // 5th event without progress trips stall
    let err = watchdog.on_event().unwrap_err();
    assert_eq!(
        err,
        WatchdogError::StagnationDetected {
            events_without_progress: 5,
            limit: 5
        }
    );

    // Reset progress
    watchdog.record_progress();
    assert_eq!(watchdog.events_without_progress(), 0);

    // Premature queue empty check
    let empty_err = watchdog
        .on_queue_empty(SimTime::from_nanos(500))
        .unwrap_err();
    assert_eq!(
        empty_err,
        WatchdogError::PrematureDepletion {
            current_time: SimTime::from_nanos(500),
            expected_min_time: SimTime::from_nanos(1000),
        }
    );
}

#[test]
fn test_buggify_registry_and_macro() {
    reset_buggify_registry();
    set_buggify_seed(12345);

    let mut hits: u64 = 0;
    for _ in 0..100 {
        if anvil_sim::buggify!("test_point", 500_000) {
            hits += 1;
        }
    }

    assert!(hits > 0, "buggify with 50% probability should fire");
    let points = list_registered_points();
    assert_ne!(points.len(), 0);
    assert_eq!(points[0].name, "test_point");
    assert_eq!(points[0].evaluations, 100);
    assert_eq!(points[0].activations, hits);
}

struct EchoActor;

impl Actor for EchoActor {
    fn on_event(&mut self, ctx: &mut anvil_sim::ActorContext<'_>, payload: u64) {
        if payload < 3 {
            ctx.schedule(ctx.now().saturating_add_nanos(10), 1, payload + 1);
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

#[test]
fn test_sim_kernel_run_loop() {
    let mut kernel = SimKernel::new(100)
        .with_max_events(10)
        .with_max_virtual_time(SimTime::from_nanos(1_000));

    kernel.register_actor(1, Box::new(EchoActor));
    kernel.schedule(SimTime::from_nanos(0), 1, 0);

    let report = kernel.run().expect("kernel run");
    assert_eq!(report.events_processed, 4);
    assert_eq!(report.final_time, SimTime::from_nanos(30));
    assert_ne!(report.trace_hash, blake3::Hash::from([0u8; 32]));
}
