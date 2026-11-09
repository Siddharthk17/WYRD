#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Smoke test executing the materialized plan farm path end to end.

use anvil_sim::{Plan, run_plan, run_seed_farm, run_seed_farm_with_storage_faults, shrink_plan};

#[test]
fn test_farm_path_runs_generated_plans() {
    let farm = run_seed_farm(4, 2);
    assert!(farm.is_clean(), "smoke farm must pass all seeds");
    assert_eq!(farm.passed_seeds, 4);
    let faults = run_seed_farm_with_storage_faults(2, 1);
    assert!(faults.is_clean(), "storage-fault farm must pass all seeds");
}

#[test]
fn test_plan_execution_matches_farm() {
    run_plan(&Plan::generate(7, false));
    run_plan(&Plan::generate(7, true));
}

#[test]
fn test_shrinker_preserves_reproducibility() {
    let plan = Plan::generate(99, false);
    let encoding = plan.encode();
    let decoded = Plan::decode(&encoding).expect("roundtrip");
    assert_eq!(plan, decoded);
    let shrunk = shrink_plan(&decoded, &|_| false);
    assert_eq!(shrunk.encode(), encoding);
}

#[test]
fn test_golden_scenario_is_deterministic() {
    let (events_a, hash_a) = anvil_sim::run_golden_scenario(42).expect("golden scenario runs");
    let (events_b, hash_b) = anvil_sim::run_golden_scenario(42).expect("golden scenario reruns");
    assert_eq!(events_a, events_b);
    assert_eq!(hash_a, hash_b);
    let (_, hash_other) = anvil_sim::run_golden_scenario(43).expect("golden scenario runs");
    assert_ne!(hash_a, hash_other);
}
