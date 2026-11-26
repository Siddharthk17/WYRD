#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(clippy::assert_is_empty)]

use anvil_sim::{Plan, run_kv_plan_traced, run_plan_traced, swarm_profile};

#[test]
fn test_trace_is_deterministic() {
    let plan = Plan::generate(7, false);
    let (stats_a, lines_a) = run_plan_traced(&plan, true);
    let (stats_b, lines_b) = run_plan_traced(&plan, true);
    assert_eq!(stats_a, stats_b);
    assert_eq!(lines_a, lines_b);
    assert!(!lines_a.is_empty());
}

#[test]
fn test_trace_marks_transitions_and_faults() {
    let plan = Plan::generate(7, false);
    assert!(!plan.events.is_empty());
    let (_, lines) = run_plan_traced(&plan, true);
    assert!(lines.iter().any(|line| line.contains("fault begin")));
    assert!(lines.iter().any(|line| line.contains("role")));
}

#[test]
fn test_trace_without_flag_stays_quiet() {
    let plan = Plan::generate(7, false);
    let (_, lines) = run_plan_traced(&plan, false);
    assert!(lines.is_empty());
}

#[test]
fn test_kv_trace_has_snapshots_and_history() {
    let profile = swarm_profile(0);
    let workload = anvil_sim::WorkloadSpec {
        clients: profile.clients,
        keys: profile.keys,
        ops_per_client: profile.ops_per_client,
        list_pct: profile.list_pct,
        think_ticks: profile.think_ticks,
        timeout_ticks: profile.timeout_ticks,
        client_crashes: profile.client_crashes,
        unordered: profile.unordered,
    };
    let plan = Plan::generate_for_workload(0, false, workload);
    let (history, audit, _, lines) = run_kv_plan_traced(&plan, false);
    assert!(audit);
    assert!(lines.iter().any(|line| line.contains("snapshot")));
    assert!(!history.ops.is_empty());
    let text = lines.join("\n");
    assert!(text.contains("fault begin"));
}

#[test]
fn test_trace_binary_dumps_seed() {
    let bin = env!("CARGO_BIN_EXE_anvil-sim");
    let out = std::process::Command::new(bin)
        .args([
            "trace",
            "--seed",
            "7",
            "--profile",
            "smoke",
            "--limit",
            "50",
        ])
        .output()
        .expect("run trace binary");
    assert!(out.status.success());
    let text = String::from_utf8(out.stdout).expect("utf8 trace");
    assert!(text.contains("anvil trace seed=7"));
    assert!(text.contains("fault begin"));
}
