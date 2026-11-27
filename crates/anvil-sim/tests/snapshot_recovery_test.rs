#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Snapshot-over-divergence crash recovery regression tests.
//!
//! A follower that replicated a divergent fork, then installed a snapshot
//! covering that prefix, kept the stale fork frames on disk: the install
//! wiped the core log but the engine retained the full WAL. Crashing before
//! the fork was overwritten replayed the stale terms at recovery and
//! contradicted the committed oracle (`CommittedNeverLostAcrossCrashes` at
//! index 33: expected term 2, recovered term 1). Installing now reconciles
//! the engine prefix against the committed archive, so these pinned seeds
//! must recover cleanly.

use anvil_sim::run_kv_seed_with_stats;

#[test]
fn test_snapshot_over_divergent_prefix_recovers_cleanly() {
    for seed in [89_738u64, 132_635, 136_637, 144_041, 163_047, 178_942] {
        let (_, audit, _) = run_kv_seed_with_stats(seed, false);
        assert!(audit, "seed {seed} must converge with a clean audit");
    }
}

/// Storage-fault profile pins: repair cascade (88700, 118313), torn
/// committed write sideline (31604), durable acknowledgments plus
/// directory sync (100260, 123509), and the hard-state survivor budget
/// (20400, 48019, 101519, 140919). Each failed a 200k-seed farm leg
/// before its fix and passes after; the liveness-class pins exercise
/// the full heal-phase convergence the farm asserts.
#[test]
fn test_storage_fault_pins_recover_cleanly() {
    for seed in [
        31_604u64, 88_700, 118_313, 100_260, 123_509, 20_400, 48_019, 101_519, 140_919, 59_349,
        74_788, 79_738, 105_094, 159_614, 176_149,
    ] {
        let (_, audit, _) = run_kv_seed_with_stats(seed, true);
        assert!(audit, "seed {seed} must converge with a clean audit");
    }
}
