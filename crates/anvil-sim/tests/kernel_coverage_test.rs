#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::buggify::BuggifyRegistry;
use anvil_sim::kernel::{Actor, ActorContext, SimError, SimKernel};
use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue, HierarchicalTimingWheel};
use anvil_sim::rng::DetRng;
use anvil_sim::time::SimTime;
use anvil_sim::trace::{TraceEvent, TraceRecorder};
use std::any::Any;

#[test]
fn test_sim_time_arithmetic_edges() {
    assert_eq!(
        SimTime::from_nanos(u64::MAX).checked_add_nanos(1),
        None,
        "nanos add overflow"
    );
    assert_eq!(
        SimTime::from_nanos(u64::MAX).saturating_add_nanos(1),
        SimTime::from_nanos(u64::MAX),
        "saturating add clamps"
    );
    assert_eq!(SimTime::ZERO.checked_sub_nanos(1), None, "sub underflow");
    assert_eq!(
        SimTime::from_nanos(3).saturating_sub_nanos(9),
        SimTime::ZERO,
        "saturating sub clamps"
    );
    assert_eq!(SimTime::from_millis(7).as_millis(), 7, "millis roundtrip");
}

#[test]
fn test_sim_time_display_branches() {
    assert_eq!(format!("{}", SimTime::from_nanos(5)), "5ns");
    assert_eq!(format!("{}", SimTime::from_nanos(1_500)), "1.500µs");
    assert_eq!(format!("{}", SimTime::from_nanos(2_500_000)), "2.500ms");
    assert_eq!(format!("{}", SimTime::from_nanos(1_500_000_000)), "1.500s");
}

#[test]
fn test_trace_event_bytes_roundtrip() {
    let event = TraceEvent {
        time: SimTime::from_nanos(123),
        actor_id: 9,
        event_kind: 4,
        payload: [7u8; 14],
    };
    let bytes = event.to_bytes();
    assert_eq!(TraceEvent::from_bytes(&bytes), event);
}

#[test]
fn test_trace_recorder_partial_and_default() {
    let mut recorder = TraceRecorder::with_capacity(5);
    for actor in [1u64, 2] {
        recorder.record(TraceEvent {
            time: SimTime::from_nanos(actor),
            actor_id: actor,
            event_kind: 1,
            payload: [0u8; 14],
        });
    }
    assert_eq!(recorder.total_events(), 2);
    let recent = recorder.recent_events();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].actor_id, 1);
    assert_eq!(recent[1].actor_id, 2);

    let fallback = TraceRecorder::default();
    assert_eq!(fallback.total_events(), 0);
}

#[test]
fn test_heap_capacity_clear_and_peek() {
    let mut queue = BinaryHeapEventQueue::with_capacity(16);
    assert!(queue.is_empty());
    assert_eq!(queue.len(), 0);

    queue.push(SimTime::from_nanos(30), 1, 1);
    queue.push(SimTime::from_nanos(10), 2, 2);
    assert_eq!(queue.len(), 2);
    let head = queue.peek().expect("peek");
    assert_eq!(head.actor_id, 2);

    queue.clear();
    assert!(queue.is_empty());

    queue.push(SimTime::from_nanos(5), 3, 3);
    let only = queue.pop().expect("pop");
    assert_eq!(only.actor_id, 3);
    assert!(queue.is_empty());
}

#[test]
fn test_wheel_far_future_and_peek() {
    let mut wheel = HierarchicalTimingWheel::default();
    assert!(wheel.is_empty());

    let far = SimTime::from_nanos(1u64 << 30);
    wheel.push(far, 7, 70);
    wheel.push(SimTime::from_nanos(10), 8, 80);
    assert_eq!(wheel.len(), 2);

    let head = wheel.peek().expect("peek");
    assert_eq!(head.actor_id, 8);

    let first = wheel.pop().expect("first");
    assert_eq!(first.actor_id, 8);
    let second = wheel.pop().expect("second");
    assert_eq!(second.actor_id, 7);
    assert!(wheel.is_empty());
    assert!(wheel.pop().is_none());
    assert!(wheel.peek().is_none());
}

struct NoopActor;

impl Actor for NoopActor {
    fn on_event(&mut self, _ctx: &mut ActorContext<'_>, _payload: u64) {}

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct CountingActor {
    seen: u64,
}

impl Actor for CountingActor {
    fn on_event(&mut self, _ctx: &mut ActorContext<'_>, payload: u64) {
        self.seen = self.seen.saturating_add(payload);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct TickingActor;

impl Actor for TickingActor {
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, _payload: u64) {
        ctx.record_progress();
        ctx.schedule(ctx.now().saturating_add_nanos(10), 1, 0);
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[test]
fn test_kernel_virtual_time_cap() {
    let mut kernel = SimKernel::new(11).with_max_virtual_time(SimTime::from_nanos(100));
    kernel.register_actor(1, Box::new(TickingActor));
    kernel.schedule(SimTime::from_nanos(10), 1, 0);

    let report = kernel.run().expect("run stops at cap");
    assert_eq!(report.events_processed, 10);
    assert_eq!(report.final_time, SimTime::from_nanos(100));
}

#[test]
fn test_kernel_event_cap() {
    let mut kernel = SimKernel::new(12).with_max_events(2);
    kernel.register_actor(1, Box::new(NoopActor));
    for i in 0..5 {
        kernel.schedule(SimTime::from_nanos(i + 1), 1, i);
    }

    let report = kernel.run().expect("run stops at cap");
    assert_eq!(report.events_processed, 2);
    assert_eq!(kernel.events_processed(), 2);
}

#[test]
fn test_kernel_rejects_unknown_actor() {
    let mut kernel = SimKernel::new(13);
    kernel.schedule(SimTime::from_nanos(1), 999, 0);
    match kernel.step() {
        Err(SimError::UnknownActor(999)) => {}
        other => panic!("expected UnknownActor, got {other:?}"),
    }
}

#[test]
fn test_kernel_accessors_and_mutation() {
    let mut kernel = SimKernel::new(14);
    kernel.register_actor(1, Box::new(CountingActor { seen: 0 }));
    kernel.record_progress();
    assert_eq!(kernel.now(), SimTime::ZERO);

    let drawn = kernel.rng().next_u64();
    let _ = drawn;
    assert_eq!(kernel.tracer().total_events(), 0);

    let actor = kernel.actor::<CountingActor>(1).expect("actor");
    assert_eq!(actor.seen, 0);
    let actor_mut = kernel.actor_mut::<CountingActor>(1).expect("actor mut");
    actor_mut.seen = 41;
    assert!(kernel.actor::<NoopActor>(1).is_none());
    assert!(kernel.actor_mut::<NoopActor>(2).is_none());

    kernel.schedule(SimTime::from_nanos(1), 1, 1);
    kernel.run().expect("run");
    let actor = kernel.actor::<CountingActor>(1).expect("actor");
    assert_eq!(actor.seen, 42);
}

#[test]
fn test_rng_edge_samplers() {
    let mut rng = DetRng::new(99, "edges");

    assert_eq!(rng.gen_range(5..6), 5, "unit span returns low");
    for _ in 0..100 {
        let drawn = rng.gen_range(0..((1u64 << 63) + 1));
        assert!(drawn < (1u64 << 63) + 1);
    }

    assert_eq!(rng.gen_range_inclusive(8..=8), 8, "equal bounds");
    let _full = rng.gen_range_inclusive(0..=u64::MAX);

    assert_eq!(rng.exponential(0), 0, "zero mean gives zero");
    assert_eq!(rng.bimodal(50, 0, 9_000, 0, 1_000_000), 50);
    assert_eq!(rng.bimodal(50, 0, 9_000, 0, 0), 9_000);

    let mut empty: [u64; 0] = [];
    rng.shuffle(&mut empty);
    let mut single = [1u64];
    rng.shuffle(&mut single);
    assert_eq!(single, [1u64]);
}

#[test]
fn test_buggify_registry_direct() {
    let mut registry = BuggifyRegistry::new();
    assert!(registry.is_enabled());
    registry.set_enabled(false);
    assert!(!registry.is_enabled());
    registry.set_enabled(true);

    registry.set_seed(7);
    assert!(!registry.evaluate("p", "f.rs", 1, 0), "zero ppm stays off");
    assert!(registry.evaluate("p", "f.rs", 1, 1_000_000));

    let fallback = BuggifyRegistry::default();
    assert!(fallback.is_enabled());

    assert_eq!(registry.list_points().len(), 1);
    registry.reset();
    assert_eq!(registry.list_points().len(), 0);
}

#[test]
fn test_wheel_slot_order_and_overflow_peek() {
    let mut wheel = HierarchicalTimingWheel::default();
    wheel.push(SimTime::from_nanos(200), 1, 1);
    wheel.push(SimTime::from_nanos(10), 2, 2);
    wheel.push(SimTime::from_nanos(1u64 << 20), 5, 5);
    let far_small = SimTime::from_nanos(1u64 << 30);
    let far_big = SimTime::from_nanos((1u64 << 30) + 100);
    wheel.push(far_big, 3, 3);
    wheel.push(far_small, 4, 4);

    let head = wheel.peek().expect("peek");
    assert_eq!(head.actor_id, 2);

    let mut order = Vec::new();
    while let Some(event) = wheel.pop() {
        order.push(event.actor_id);
    }
    assert_eq!(order, vec![2, 1, 5, 4, 3]);

    let mut overflow_only = HierarchicalTimingWheel::default();
    overflow_only.push(SimTime::from_nanos(1u64 << 30), 9, 9);
    let head = overflow_only.peek().expect("overflow peek");
    assert_eq!(head.actor_id, 9);
}
