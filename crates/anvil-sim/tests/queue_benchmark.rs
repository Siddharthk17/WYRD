#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::cast_precision_loss,
    clippy::print_stdout
)]

use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue, HierarchicalTimingWheel};
use anvil_sim::time::SimTime;
use std::time::Instant;

#[test]
fn test_queue_empirical_benchmark() {
    const OPS: usize = 100_000;

    // Benchmark BinaryHeap
    let mut heap = BinaryHeapEventQueue::new();
    let start_heap = Instant::now();
    for i in 0..OPS {
        let t = SimTime::from_nanos(((i * 37) % 50_000) as u64);
        heap.push(t, 1, i as u64);
    }
    for _ in 0..OPS {
        let _ = heap.pop().expect("heap non-empty");
    }
    let heap_elapsed = start_heap.elapsed();

    // Benchmark TimingWheel
    let mut wheel = HierarchicalTimingWheel::new();
    let start_wheel = Instant::now();
    for i in 0..OPS {
        let t = SimTime::from_nanos(((i * 37) % 50_000) as u64);
        wheel.push(t, 1, i as u64);
    }
    for _ in 0..OPS {
        let _ = wheel.pop().expect("wheel non-empty");
    }
    let wheel_elapsed = start_wheel.elapsed();

    let heap_ops_per_sec = (OPS as f64 * 2.0) / heap_elapsed.as_secs_f64();
    let wheel_ops_per_sec = (OPS as f64 * 2.0) / wheel_elapsed.as_secs_f64();

    println!(
        "QUEUE BENCHMARK: BinaryHeap = {:.2} M ops/sec ({:?}), TimingWheel = {:.2} M ops/sec ({:?})",
        heap_ops_per_sec / 1_000_000.0,
        heap_elapsed,
        wheel_ops_per_sec / 1_000_000.0,
        wheel_elapsed
    );

    assert!(heap.is_empty());
    assert!(wheel.is_empty());
}
