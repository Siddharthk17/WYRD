#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

//! Deterministic simulation seed farm for ANVIL consensus testing.
//!
//! Each seed materializes a serializable [`Plan`] before its run starts and
//! executes it with no generator draws mid-run. Online invariant checking
//! asserts safety after every transition of every node.

use crate::plan::{Plan, run_plan};
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
/// Generates the seed's [`Plan`] and executes it.
pub fn run_farm_seed(seed: u64) {
    run_plan(&Plan::generate(seed, false));
}

/// Runs a single seed with storage fault injection enabled on every node.
///
/// Generates the seed's storage-fault [`Plan`] and executes it.
pub fn run_farm_seed_with_storage_faults(seed: u64) {
    run_plan(&Plan::generate(seed, true));
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
