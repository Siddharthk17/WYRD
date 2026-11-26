#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty
)]

use anvil_sim::{FaultEvent, FaultKind, Plan, WorkloadSpec, run_plan, shrink_plan};

fn workload_spec() -> WorkloadSpec {
    WorkloadSpec {
        clients: 4,
        keys: 8,
        ops_per_client: 3,
        list_pct: 20,
        think_ticks: 2,
        timeout_ticks: 25,
        client_crashes: true,
        unordered: false,
    }
}

#[test]
fn test_plan_encode_decode_roundtrip() {
    for seed in [1, 2, 42, 100, 1337, 2024, 54321, 999_999] {
        for storage in [false, true] {
            let plan = Plan::generate(seed, storage);
            let encoded = plan.encode();
            let decoded = Plan::decode(&encoded).expect("roundtrip");
            assert_eq!(plan, decoded, "seed {seed} storage {storage}");
            let workload = Plan::generate_for_workload(seed, storage, workload_spec());
            let encoded = workload.encode();
            let decoded = Plan::decode(&encoded).expect("roundtrip");
            assert_eq!(workload, decoded, "seed {seed} storage {storage}");
        }
    }
}

#[test]
fn test_plan_decode_rejects_garbage() {
    assert!(Plan::decode("").is_err());
    assert!(Plan::decode("anvil-plan/99;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/1;seed=1").is_err());
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=10;ticks=30;warm=3;heal=27;net=8,5,3;salt=7;storage=0"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=3;heal=27;net=101,0,0;salt=7;storage=0"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=3;heal=27;net=8,5,3;salt=7;storage=0;ev=0:20-12:part:1"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=3;heal=27;net=8,5,3;salt=7;storage=0;ev=0:12-5:crash:0"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=3;heal=27;net=8,5,3;salt=7;storage=0;ev=0:1-2:part:9"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=3;heal=27;net=8,5,3;salt=7;storage=0;bogus=1"
        )
        .is_err()
    );
    assert!(
        Plan::decode(
            "anvil-plan/2;seed=1;size=3;ticks=30;warm=27;heal=3;net=8,5,3;salt=7;storage=0"
        )
        .is_err()
    );
}

#[test]
fn test_plan_runs_are_deterministic() {
    let first = Plan::generate(777, false).encode();
    let second = Plan::generate(777, false).encode();
    assert_eq!(first, second);
    run_plan(&Plan::generate(777, false));
}

#[test]
fn test_plan_events_cover_fault_vocabulary() {
    let mut seen = std::collections::BTreeSet::new();
    let mut single_or_dual = 0u32;
    for seed in 0..200u64 {
        let plan = Plan::generate(seed, false);
        assert!(plan.warmup_end < plan.heal_start);
        assert!(plan.heal_start <= plan.ticks);
        let mut classes = std::collections::BTreeSet::new();
        for event in &plan.events {
            assert!(event.start >= plan.warmup_end);
            assert!(event.end <= plan.heal_start);
            assert!(event.end > event.start);
            let kind = match event.fault {
                FaultKind::Partition { .. } => "partition",
                FaultKind::Asymmetric { .. } => "asymmetric",
                FaultKind::Crash { .. } => "crash",
                FaultKind::Pause { .. } => "pause",
                FaultKind::SlowClock { .. } => "clock",
                FaultKind::DelaySpike { .. } => "delay",
            };
            seen.insert(kind);
            classes.insert(kind);
        }
        // Loss-only seeds carry no timed events; everything else counts.
        // Empty event lists count as single-class: the seed enabled at most
        // the loss class.
        if classes.len() <= 2 {
            single_or_dual += 1;
        }
    }
    for kind in [
        "partition",
        "asymmetric",
        "crash",
        "pause",
        "clock",
        "delay",
    ] {
        assert!(seen.contains(kind), "fault class {kind} never generated");
    }
    assert!(
        single_or_dual >= 60,
        "swarm must run one or two classes on at least 30% of seeds, saw {single_or_dual}/200"
    );
}

#[test]
fn test_plan_removing_event_keeps_others_stable() {
    for seed in [11u64, 22, 33] {
        let plan = Plan::generate(seed, false);
        if plan.events.len() < 2 {
            continue;
        }
        let mut removed = plan.clone();
        removed.events.remove(0);
        for (kept, original) in removed.events.iter().zip(plan.events[1..].iter()) {
            assert_eq!(kept, original);
        }
    }
}

#[test]
fn test_shrinker_keeps_failure_minimal() {
    let mut plan = Plan::generate(4242, false);
    plan.events.push(FaultEvent {
        id: 1000,
        start: 5,
        end: 6,
        fault: FaultKind::Crash { node: 0 },
    });
    plan.events.push(FaultEvent {
        id: 1001,
        start: 6,
        end: 7,
        fault: FaultKind::Crash { node: 1 },
    });
    let fails = |candidate: &Plan| candidate.events.len() >= 2 && !candidate.proposals.is_empty();
    let shrunk = shrink_plan(&plan, &fails);
    assert_eq!(shrunk.events.len(), 2);
    assert_eq!(shrunk.proposals.len(), 1);
    assert_eq!(shrunk.drop_pct, 0);
}
