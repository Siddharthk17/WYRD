#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty
)]

use anvil_sim::{Plan, run_plan, shrink_plan};

#[test]
fn test_plan_encode_decode_roundtrip() {
    for seed in [1, 2, 42, 100, 1337, 2024, 54321, 999_999] {
        for storage in [false, true] {
            let plan = Plan::generate(seed, storage);
            let encoded = plan.encode();
            let decoded = Plan::decode(&encoded).expect("roundtrip");
            assert_eq!(plan, decoded, "seed {seed} storage {storage}");
        }
    }
}

#[test]
fn test_plan_decode_rejects_garbage() {
    assert!(Plan::decode("").is_err());
    assert!(Plan::decode("anvil-plan/99;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/1;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/1;seed=1;size=10;ticks=30;salt=7").is_err());
    assert!(Plan::decode("anvil-plan/1;seed=1;size=3;ticks=30;net=101,0,0;salt=7").is_err());
    assert!(
        Plan::decode("anvil-plan/1;seed=1;size=3;ticks=30;net=8,5,3;salt=7;part=20-12:1").is_err()
    );
    assert!(
        Plan::decode("anvil-plan/1;seed=1;size=3;ticks=30;net=8,5,3;salt=7;crash=5-3:0").is_err()
    );
    assert!(
        Plan::decode("anvil-plan/1;seed=1;size=3;ticks=30;net=8,5,3;salt=7;part=1-2:9").is_err()
    );
    assert!(Plan::decode("anvil-plan/1;seed=1;size=3;ticks=30;net=8,5,3;salt=7;bogus=1").is_err());
}

#[test]
fn test_plan_runs_are_deterministic() {
    let first = Plan::generate(777, false).encode();
    let second = Plan::generate(777, false).encode();
    assert_eq!(first, second);
    run_plan(&Plan::generate(777, false));
}

#[test]
fn test_shrinker_keeps_failure_minimal() {
    let mut plan = Plan::generate(4242, false);
    plan.crashes.push(anvil_sim::PlanCrash {
        crash_tick: 5,
        restart_tick: 5,
        node_idx: 0,
    });
    plan.crashes.push(anvil_sim::PlanCrash {
        crash_tick: 6,
        restart_tick: 6,
        node_idx: 1,
    });
    let fails = |candidate: &Plan| candidate.crashes.len() >= 2 && !candidate.proposals.is_empty();
    let shrunk = shrink_plan(&plan, &fails);
    assert_eq!(shrunk.crashes.len(), 2);
    assert_eq!(shrunk.partitions.len(), 0);
    assert_eq!(shrunk.proposals.len(), 1);
    assert_eq!(shrunk.drop_pct, 0);
}
