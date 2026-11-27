#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_check::check_history;
use anvil_sim::workload::run_kv_seed_with_stats;

#[test]
fn test_snapshot_flow_takes_and_installs() {
    let mut saw_snapshot = false;
    let mut saw_install = false;
    for seed in 0..60u64 {
        let (history, audit, stats) = run_kv_seed_with_stats(seed, false);
        assert!(!history.ops.is_empty(), "seed {seed} produced no ops");
        assert!(audit, "seed {seed} failed convergence audit");
        assert!(
            check_history(&history).is_ok(),
            "seed {seed} history failed linearizability"
        );
        saw_snapshot |= stats.snapshots_taken > 0;
        saw_install |= stats.installs > 0;
    }
    assert!(saw_snapshot, "no seed took a snapshot in 60 seeds");
    assert!(saw_install, "no seed installed a snapshot in 60 seeds");
}

#[test]
fn test_snapshot_flow_with_storage_faults() {
    let mut saw_snapshot = false;
    for seed in 0..20u64 {
        let (history, audit, stats) = run_kv_seed_with_stats(seed, true);
        assert!(
            audit,
            "seed {seed} failed convergence audit under storage faults"
        );
        assert!(
            check_history(&history).is_ok(),
            "seed {seed} history failed linearizability under storage faults"
        );
        saw_snapshot |= stats.snapshots_taken > 0;
    }
    assert!(saw_snapshot, "no seed took a snapshot under storage faults");
}
