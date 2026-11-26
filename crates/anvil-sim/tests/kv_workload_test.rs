#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_check::check_history;
use anvil_sim::{run_kv_seed, run_kv_seed_with_storage};

#[test]
fn test_kv_seed_is_linearizable_and_convergent() {
    for seed in [0u64, 1, 2, 42, 636, 777] {
        let (history, audit) = run_kv_seed(seed, false);
        assert!(!history.ops.is_empty(), "seed {seed} produced no ops");
        assert!(audit, "seed {seed} failed convergence audit");
        assert!(
            check_history(&history).is_ok(),
            "seed {seed} history failed linearizability"
        );
    }
}

#[test]
fn test_kv_seed_with_storage_faults() {
    let (history, audit) = run_kv_seed_with_storage(99, false);
    assert!(audit, "storage seed failed convergence audit");
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_canary_stale_read_is_caught() {
    let mut caught_at: Option<u64> = None;
    for seed in 0..1000u64 {
        let (history, _) = run_kv_seed(seed, true);
        if check_history(&history).is_err() {
            caught_at = Some(seed);
            break;
        }
    }
    assert!(
        caught_at.is_some(),
        "canary stale read was not caught within 1000 seeds"
    );
}
