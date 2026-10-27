#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

//! Deterministic simulation seed farm for ANVIL consensus testing.
//!
//! Executes thousands of parallel simulation runs against lossy, duplicating,
//! and reordering networks on 3, 5, and 7-node clusters with online invariant checking.

use crate::cluster::SimCluster;
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Aggregated outcome of a simulator farm execution.
#[derive(Debug, Clone)]
pub struct FarmResult {
    /// Total number of random seeds evaluated.
    pub total_seeds: usize,
    /// Number of seeds that passed all invariant checks.
    pub passed_seeds: usize,
    /// First seed that produced an invariant violation, if any.
    pub failed_seed: Option<u64>,
    /// Total execution duration.
    pub duration: Duration,
}

impl FarmResult {
    /// Returns true if all seeds completed with zero invariant violations.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.failed_seed.is_none() && self.passed_seeds == self.total_seeds
    }

    /// Evaluates execution throughput in simulated seeds per second.
    #[must_use]
    pub fn seeds_per_second(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.total_seeds as f64 / secs
        } else {
            0.0
        }
    }
}

/// Runs a single deterministic simulation seed.
///
/// Evaluates a 3, 5, or 7-node cluster under network drop, duplicate, and reordering faults.
/// The online `RaftInvariantChecker` asserts safety after every transition of every node.
pub fn run_farm_seed(seed: u64) {
    let sizes = [3, 5, 7];
    let cluster_size = sizes[(seed % 3) as usize];
    let mut cluster = SimCluster::new(cluster_size, seed);

    let drop_seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    // 1. Lossy network: 8% drop rate
    cluster.drop_hook = Some(Box::new(move |from, to, _msg| {
        let h = (from.get() ^ to.get() ^ drop_seed).wrapping_mul(0x517c_c1b7_2722_0a95);
        (h % 100) < 8
    }));

    // 2. Duplicating network: 5% packet duplication
    cluster.duplicate_hook = Some(Box::new(move |from, to, _msg| {
        let h = (from.get().wrapping_add(to.get()) ^ drop_seed).wrapping_mul(0x4b66_f4d9_e213_5a2d);
        (h % 100) < 5
    }));

    // 3. Reordering / Delay network: 0 to 3 ticks delay
    cluster.delay_hook = Some(Box::new(move |from, to, _msg| {
        let h =
            (from.get() ^ to.get().rotate_left(16) ^ drop_seed).wrapping_mul(0x6c62_272e_07bb_0142);
        h % 4
    }));

    let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

    // Advance 30 logical ticks with client proposals and temporary partitions
    for tick in 1..=30 {
        cluster.tick();

        // Submit client proposals periodically if a leader exists
        if tick % 6 == 0
            && let Some(leader) = cluster.leader()
        {
            let payload = Bytes::from(format!("seed_{seed}_tick_{tick}"));
            cluster.propose(leader, payload);
        }

        // Inject temporary minority partition at tick 12
        if tick == 12 && cluster_size > 3 {
            let isolated = node_ids[cluster_size - 1];
            cluster.isolate_node(isolated);
        }

        // Inject node crash and storage recovery at tick 15
        if tick == 15 {
            let victim = node_ids[(seed as usize) % cluster_size];
            if Some(victim) != cluster.leader() {
                cluster.crash_node(victim);
                let _ = cluster.restart_node(victim);
            }
        }

        // Heal network at tick 20
        if tick == 20 {
            cluster.heal_all();
        }
    }

    // Quiesce network: heal all partitions and drain in-flight messages
    cluster.heal_all();
    cluster.drain_network(20);

    // Final cluster-wide consistency validation
    let highest_commit = cluster.highest_commit_index();
    for (&id, core) in &cluster.nodes {
        assert!(
            core.commit_index() <= core.last_log_index(),
            "commit index exceeds last log index on node {id}"
        );
    }

    // Verify all nodes agreeing on committed entries have identical terms
    for idx in 1..=highest_commit.as_u64() {
        let log_idx = LogIndex::from_raw(idx);
        let mut term = None;
        for core in cluster.nodes.values() {
            if core.commit_index() >= log_idx
                && let Some(t) = core.log().term_at(log_idx)
            {
                if let Some(prev) = term {
                    assert_eq!(
                        prev, t,
                        "seed {seed}: conflicting terms at committed index {idx}"
                    );
                } else {
                    term = Some(t);
                }
            }
        }
    }
}

/// Executes `num_seeds` simulation seeds in parallel across worker threads.
#[must_use]
pub fn run_seed_farm(num_seeds: usize, num_threads: usize) -> FarmResult {
    let start = Instant::now();
    let passed = Arc::new(AtomicUsize::new(0));
    let failed_seed = Arc::new(std::sync::Mutex::new(None));
    let aborted = Arc::new(AtomicBool::new(false));

    let threads_count = num_threads.max(1);
    let chunk_size = num_seeds.div_ceil(threads_count);

    let mut handles = Vec::with_capacity(threads_count);

    for t in 0..threads_count {
        let start_seed = t * chunk_size;
        let end_seed = (start_seed + chunk_size).min(num_seeds);
        let passed_clone = Arc::clone(&passed);
        let failed_clone = Arc::clone(&failed_seed);
        let aborted_clone = Arc::clone(&aborted);

        let handle = thread::spawn(move || {
            for seed in start_seed..end_seed {
                if aborted_clone.load(Ordering::Relaxed) {
                    break;
                }
                let seed_u64 = seed as u64;
                // Run seed under catch_unwind to capture exact failing seed
                let res = std::panic::catch_unwind(|| {
                    run_farm_seed(seed_u64);
                });

                if res.is_ok() {
                    passed_clone.fetch_add(1, Ordering::Relaxed);
                } else {
                    let mut lock = failed_clone
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if lock.is_none() {
                        *lock = Some(seed_u64);
                    }
                    aborted_clone.store(true, Ordering::Relaxed);
                    break;
                }
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.join();
    }

    let duration = start.elapsed();
    let passed_count = passed.load(Ordering::SeqCst);
    let failed = *failed_seed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    FarmResult {
        total_seeds: num_seeds,
        passed_seeds: passed_count,
        failed_seed: failed,
        duration,
    }
}
