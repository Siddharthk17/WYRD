#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_precision_loss
)]

//! Performance and allocation counting tests for `anvil-raft`.
//!
//! Validates the Charter and Week 2 doctrine:
//! - Follower Tick: 0 allocations in steady state
//! - Leader Heartbeat: 0 allocations in steady state
//! - Replication: at most 1 allocation per entry amortized

use anvil_raft::{Input, Output, PersistOp, RaftConfig, RaftCore, RecoveredState};
use anvil_types::NodeId;
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeSet;

struct CountingAlloc;

thread_local! {
    static TRACKING_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static THREAD_ALLOC_COUNT: Cell<usize> = const { Cell::new(0) };
    static THREAD_ALLOC_BYTES: Cell<usize> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = TRACKING_ACTIVE.try_with(|active| {
            if active.get() {
                let _ = THREAD_ALLOC_COUNT.try_with(|c| c.set(c.get() + 1));
                let _ = THREAD_ALLOC_BYTES.try_with(|b| b.set(b.get() + layout.size()));
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAlloc = CountingAlloc;

static ALLOC_TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn enable_tracking() {
    let _ = THREAD_ALLOC_COUNT.try_with(|c| c.set(0));
    let _ = THREAD_ALLOC_BYTES.try_with(|b| b.set(0));
    let _ = TRACKING_ACTIVE.try_with(|a| a.set(true));
}

fn disable_tracking() -> usize {
    let _ = TRACKING_ACTIVE.try_with(|a| a.set(false));
    THREAD_ALLOC_COUNT.try_with(Cell::get).unwrap_or(0)
}

fn current_alloc_count() -> usize {
    THREAD_ALLOC_COUNT.try_with(Cell::get).unwrap_or(0)
}

fn create_test_node(
    id: u64,
    peers: &[u64],
    election_timeout: u64,
    heartbeat_interval: u64,
) -> RaftCore {
    let mut peer_set = BTreeSet::new();
    for &p in peers {
        peer_set.insert(NodeId::new(p).expect("valid peer id"));
    }
    let config = RaftConfig {
        node_id: NodeId::new(id).expect("valid node id"),
        peers: peer_set,
        election_timeout_ticks: election_timeout,
        heartbeat_interval_ticks: heartbeat_interval,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let rng = ChaCha8Rng::seed_from_u64(id);
    RaftCore::new(config, RecoveredState::default(), rng)
}

#[test]
fn test_allocator_tracks_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    enable_tracking();
    assert_eq!(current_alloc_count(), 0);

    let b = Box::new(12345u64);
    assert_eq!(*b, 12345);
    let count = disable_tracking();
    assert!(count > 0);
}

#[test]
fn test_follower_tick_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    // High election timeout to prevent timer expiry during steady-state ticks
    let mut follower = create_test_node(1, &[2, 3], 1_000_000, 10);
    let mut output = Output::new();

    // Warm-up step
    output.clear();
    follower.step(Input::Tick, &mut output);

    enable_tracking();

    // Hot path steady-state: 10,000 consecutive follower ticks
    for _ in 0..10_000 {
        output.clear();
        follower.step(Input::Tick, &mut output);
    }

    let allocs = disable_tracking();
    assert_eq!(
        allocs, 0,
        "follower Tick in steady-state must incur exactly 0 allocations! Observed: {allocs}"
    );
}

#[test]
fn test_leader_heartbeat_zero_allocations() {
    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    let mut leader = create_test_node(1, &[2, 3], 1_000_000, 5);
    let mut output = Output::new();

    // Transition to leader
    leader.testing_become_leader(&mut output);
    let tokens: Vec<_> = output.persist_ops.iter().map(PersistOp::token).collect();
    for token in tokens {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }

    // Warm-up heartbeat cycles so output.messages capacity is retained
    for _ in 0..10 {
        output.clear();
        for _ in 0..5 {
            leader.step(Input::Tick, &mut output);
        }
    }

    enable_tracking();

    // Hot path steady-state: 1,000 heartbeat broadcasts (5,000 ticks)
    for _ in 0..1_000 {
        output.clear();
        for _ in 0..5 {
            leader.step(Input::Tick, &mut output);
        }
    }

    let allocs = disable_tracking();
    assert_eq!(
        allocs, 0,
        "leader Heartbeat in steady-state must incur exactly 0 allocations! Observed: {allocs}"
    );
}

#[test]
fn test_replication_amortized_allocations_per_entry() {
    const ENTRIES_COUNT: usize = 10_000;

    let _guard = ALLOC_TEST_LOCK.lock().unwrap();
    let mut leader = create_test_node(1, &[2], 1_000_000, 100);
    let mut output = Output::new();

    // Transition to leader
    leader.testing_become_leader(&mut output);
    let tokens: Vec<_> = output.persist_ops.iter().map(PersistOp::token).collect();
    for token in tokens {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }

    let payload =
        Bytes::from_static(b"benchmark_replication_payload_64_bytes_fixed_slice_reference");

    // Warm up
    for _ in 0..100 {
        output.clear();
        leader.step(
            Input::Propose {
                payload: payload.clone(),
            },
            &mut output,
        );
        if let Some(op) = output.persist_ops.first() {
            let token = op.token();
            output.clear();
            leader.step(Input::Persisted { token }, &mut output);
        }
    }

    enable_tracking();

    for _ in 0..ENTRIES_COUNT {
        output.clear();
        leader.step(
            Input::Propose {
                payload: payload.clone(),
            },
            &mut output,
        );
        if let Some(op) = output.persist_ops.first() {
            let token = op.token();
            output.clear();
            leader.step(Input::Persisted { token }, &mut output);
        }
    }

    let total_allocs = disable_tracking();
    let allocs_per_entry = (total_allocs as f64) / (ENTRIES_COUNT as f64);

    println!(
        "Replication allocations: total={total_allocs} over {ENTRIES_COUNT} entries = {allocs_per_entry:.3} allocs/entry"
    );

    // Week 2 Performance Charter: at most one allocation per entry amortized during replication.
    // The single allocation per entry is the transient Vec<Entry> created for PersistOp::AppendEntries.
    assert!(
        allocs_per_entry <= 1.05,
        "replication must incur at most 1 allocation per entry amortized! Observed: {allocs_per_entry:.3}"
    );
}
