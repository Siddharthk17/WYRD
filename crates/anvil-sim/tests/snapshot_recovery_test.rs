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
    for seed in [89738u64, 132635, 136637, 144041, 163047, 178942] {
        let (_, audit, _) = run_kv_seed_with_stats(seed, false);
        assert!(audit, "seed {seed} must converge with a clean audit");
    }
}
