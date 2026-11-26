#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::{
    FaultEvent, FaultKind, Plan, WorkloadSpec, consensus_failure_class, kv_plan_failure_class,
    normalize_failure_class, panic_failure_class, shrink_plan_same_class, swarm_profile,
};

fn workload_spec_for(seed: u64) -> WorkloadSpec {
    let profile = swarm_profile(seed);
    WorkloadSpec {
        clients: profile.clients,
        keys: profile.keys,
        ops_per_client: profile.ops_per_client,
        list_pct: profile.list_pct,
        think_ticks: profile.think_ticks,
        timeout_ticks: profile.timeout_ticks,
        client_crashes: profile.client_crashes,
        unordered: profile.unordered,
    }
}

#[test]
fn test_normalize_failure_class_collapses_digits() {
    assert_eq!(
        normalize_failure_class("liveness: no leader within 100 ticks after heal (seed 7)"),
        "liveness: no leader within # ticks after heal (seed #)"
    );
    assert_eq!(
        normalize_failure_class("FATAL RAFT INVARIANT VIOLATION: TermMonotonic at NodeId(2)"),
        "FATAL RAFT INVARIANT VIOLATION: TermMonotonic at NodeId(#)"
    );
    assert_eq!(normalize_failure_class("panic: boom"), "panic: boom");
}

#[test]
fn test_panic_failure_class_names_payload() {
    let class = panic_failure_class(&String::from("invariant X at index 42"));
    assert_eq!(class, "panic:invariant X at index #");
    let class = panic_failure_class(&"static at tick 9");
    assert_eq!(class, "panic:static at tick #");
}

#[test]
fn test_consensus_run_passes_on_quiet_plan() {
    let plan = Plan::generate(4242, false);
    assert!(consensus_failure_class(&plan).is_none());
}

#[test]
fn test_shrinker_returns_clone_when_passing() {
    let plan = Plan::generate(99, false);
    let shrunk = shrink_plan_same_class(&plan, &|_| None);
    assert_eq!(shrunk, plan);
}

#[test]
fn test_shrinker_preserves_failure_class() {
    let mut plan = Plan::generate(4242, false);
    plan.drop_pct = 5;
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
    let run = |candidate: &Plan| {
        if candidate.events.iter().any(|event| event.id == 1001) && candidate.drop_pct > 0 {
            Some(String::from("boom:1001"))
        } else if candidate.events.is_empty() {
            Some(String::from("empty"))
        } else {
            None
        }
    };
    let shrunk = shrink_plan_same_class(&plan, &run);
    assert_eq!(run(&shrunk), Some(String::from("boom:1001")));
    assert!(shrunk.events.iter().any(|event| event.id == 1001));
    assert!(shrunk.drop_pct > 0);
    assert!(shrunk.events.len() < plan.events.len());
}

#[test]
fn test_shrinker_minimizes_canary_to_30_events() {
    let mut plan = Plan::generate_for_workload(0, false, workload_spec_for(0));
    let original_events = plan.events.len();
    for i in 0..30u32 {
        plan.events.push(FaultEvent {
            id: 1000 + i,
            start: 50,
            end: 51,
            fault: FaultKind::DelaySpike { extra_ticks: 0 },
        });
    }
    plan.events.sort_by_key(|event| (event.start, event.id));
    let expected =
        kv_plan_failure_class(&plan, true).expect("seed-0 canary must fail with padding");
    assert!(
        expected.starts_with("linearizability:"),
        "seed-0 canary must fail in the checker, got {expected}"
    );
    let run = |candidate: &Plan| kv_plan_failure_class(candidate, true);
    let shrunk = shrink_plan_same_class(&plan, &run);
    assert!(
        shrunk.events.len() <= 30,
        "shrunk canary has {} events, expected at most 30",
        shrunk.events.len()
    );
    assert!(
        shrunk.events.iter().all(|event| event.id < 1000),
        "inert padding must shrink away entirely"
    );
    assert!(
        shrunk.proposals.is_empty(),
        "proposals a workload run never consumes must shrink away"
    );
    assert_eq!(run(&shrunk).as_deref(), Some(expected.as_str()));
    let round = Plan::decode(&shrunk.encode()).expect("shrunk plan re-decodes");
    assert_eq!(round, shrunk);
    assert!(shrunk.events.len() <= original_events);
}
