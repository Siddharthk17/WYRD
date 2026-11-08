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
use crate::device::SimDeviceConfig;
use anvil_storage::SectorSize;
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
    run_farm_seed_inner(seed, false);
}

/// Runs a single seed with storage fault injection enabled on every node.
///
/// Extends [`run_farm_seed`] with write-path bit rot, read-path bit rot on
/// recovery, fsync failure crashes, torn writes and unsynced-data loss,
/// all gated by the shared storage fault atlas.
pub fn run_farm_seed_with_storage_faults(seed: u64) {
    run_farm_seed_inner(seed, true);
}

/// Runs a single deterministic simulation seed.
fn run_farm_seed_inner(seed: u64, storage_faults: bool) {
    let sizes = [3, 5, 7];
    let cluster_size = sizes[(seed % 3) as usize];

    let mut cluster = if storage_faults {
        let dev_config = SimDeviceConfig::chaotic(SectorSize::Sector512, 10_000, 5_000, 200);
        SimCluster::new_with_storage_faults(cluster_size, seed, &dev_config)
    } else {
        SimCluster::new(cluster_size, seed)
    };

    let drop_seed = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15);

    cluster.drop_hook = Some(Box::new(move |from, to, _msg| {
        let draw = (from.get() ^ to.get() ^ drop_seed).wrapping_mul(0x517c_c1b7_2722_0a95);
        (draw % 100) < 8
    }));

    cluster.duplicate_hook = Some(Box::new(move |from, to, _msg| {
        let draw =
            (from.get().wrapping_add(to.get()) ^ drop_seed).wrapping_mul(0x4b66_f4d9_e213_5a2d);
        (draw % 100) < 5
    }));

    cluster.delay_hook = Some(Box::new(move |from, to, _msg| {
        let draw =
            (from.get() ^ to.get().rotate_left(16) ^ drop_seed).wrapping_mul(0x6c62_272e_07bb_0142);
        draw % 4
    }));

    let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

    for tick in 1..=30 {
        cluster.tick();

        if tick % 6 == 0
            && let Some(leader) = cluster.leader()
        {
            let payload = Bytes::from(format!("seed_{seed}_tick_{tick}"));
            cluster.propose(leader, payload);
        }

        if tick == 12 && cluster_size > 3 {
            let isolated = node_ids[cluster_size - 1];
            cluster.isolate_node(isolated);
        }

        if tick == 15 {
            let victim = node_ids[(seed as usize) % cluster_size];
            if Some(victim) != cluster.leader() {
                cluster.crash_node(victim);
                let _ = cluster.restart_node(victim);
            }
        }

        if tick == 20 {
            cluster.heal_all();
        }
    }

    cluster.heal_all();
    cluster.drain_network(20);

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
        let mut committed_term = None;
        for core in cluster.nodes.values() {
            if core.commit_index() >= log_idx
                && let Some(term) = core.log().term_at(log_idx)
            {
                if let Some(first_term) = committed_term {
                    assert_eq!(
                        first_term, term,
                        "seed {seed}: conflicting terms at committed index {idx}"
                    );
                } else {
                    committed_term = Some(term);
                }
            }
        }
    }
}

/// Executes `num_seeds` simulation seeds in parallel across worker threads.
#[must_use]
pub fn run_seed_farm(num_seeds: usize, num_threads: usize) -> FarmResult {
    run_seed_farm_inner(num_seeds, num_threads, false)
}

/// Executes `num_seeds` seeds with storage fault injection enabled on every node.
#[must_use]
pub fn run_seed_farm_with_storage_faults(num_seeds: usize, num_threads: usize) -> FarmResult {
    run_seed_farm_inner(num_seeds, num_threads, true)
}

fn run_seed_farm_inner(num_seeds: usize, num_threads: usize, storage_faults: bool) -> FarmResult {
    let start = Instant::now();
    let passed = Arc::new(AtomicUsize::new(0));
    let failed_seed = Arc::new(std::sync::Mutex::new(None));
    let aborted = Arc::new(AtomicBool::new(false));

    let threads_count = num_threads.max(1);
    let chunk_size = num_seeds.div_ceil(threads_count);

    let mut handles = Vec::with_capacity(threads_count);

    for worker in 0..threads_count {
        let start_seed = worker * chunk_size;
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
                let outcome = std::panic::catch_unwind(|| {
                    if storage_faults {
                        run_farm_seed_with_storage_faults(seed_u64);
                    } else {
                        run_farm_seed(seed_u64);
                    }
                });

                if outcome.is_ok() {
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

    for handle in handles {
        let _ = handle.join();
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
