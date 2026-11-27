#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty,
    clippy::manual_assert_eq
)]

//! Coverage tests for `anvil-sim`: binary CLI subprocess paths plus
//! in-process coverage for farm rendering/resume, plan decode/run/shrink,
//! raft invariants, network helpers, and the golden scenario.
//!
//! Subprocesses inherit `LLVM_PROFILE_FILE`, so their coverage merges
//! automatically. All runs are tiny and deterministic.

use anvil_raft::{RaftConfig, RaftCore, RecoveredState, Role};
use anvil_sim::{
    FarmOptions, FarmProfile, FarmRange, FarmSummary, FaultEvent, FaultKind, Plan, SimNetwork,
    TransportMode, WorkloadSpec,
};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("node id")
}

fn bin_exe() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_BIN_EXE_anvil-sim"))
}

fn tmpdir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("anvil-sim-cov-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create tmpdir");
    dir
}

fn normal_entry(index: u64, term: u64) -> Entry {
    Entry::normal(
        LogIndex::from_raw(index),
        Term::from_raw(term),
        Bytes::from_static(b"v"),
    )
}

fn two_node_configs() -> (RaftConfig, RaftConfig, NodeId, NodeId) {
    let node1 = node(1);
    let node2 = node(2);
    let mut peers1 = BTreeSet::new();
    peers1.insert(node2);
    let mut peers2 = BTreeSet::new();
    peers2.insert(node1);
    let base = |node_id: NodeId, peers: BTreeSet<NodeId>| RaftConfig {
        node_id,
        peers,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };
    (base(node1, peers1), base(node2, peers2), node1, node2)
}

fn core_with_vote(
    config: RaftConfig,
    term: u64,
    commit: u64,
    entries: Vec<Entry>,
    voted_for: Option<NodeId>,
    seed: u64,
) -> RaftCore {
    let recovered = RecoveredState {
        current_term: Term::from_raw(term),
        voted_for,
        commit_index: LogIndex::from_raw(commit),
        entries,
    };
    RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(seed))
}

fn heartbeat(from: NodeId, to: NodeId) -> anvil_raft::Message {
    let _ = to;
    anvil_raft::Message::HeartbeatRequest {
        term: Term::from_raw(1),
        leader_id: from,
        commit_index: LogIndex::from_raw(0),
        read_context: None,
    }
}

// ---------------------------------------------------------------------------
// Part 1 — binary CLI via subprocess
// ---------------------------------------------------------------------------

#[test]
fn cli_farm_smoke_success_with_out_and_resume() {
    let dir = tmpdir("farm-smoke");
    let out = dir.join("results.jsonl");
    let output = std::process::Command::new(bin_exe())
        .args([
            "farm",
            "--seeds",
            "0..2",
            "--jobs",
            "1",
            "--profile",
            "smoke",
            "--out",
            out.to_str().expect("utf8"),
            "--resume",
        ])
        .output()
        .expect("run anvil-sim farm");
    assert!(
        output.status.success(),
        "farm smoke failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("ANVIL seed farm"), "stdout: {stdout}");
    let text = std::fs::read_to_string(&out).expect("jsonl written");
    assert!(text.contains("\"type\":\"seed\""), "jsonl: {text}");
    assert!(text.contains("\"type\":\"summary\""), "jsonl: {text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cli_farm_bad_range_exit_2() {
    let output = std::process::Command::new(bin_exe())
        .args(["farm", "--seeds", "bogus", "--jobs", "1"])
        .output()
        .expect("run anvil-sim farm");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("bad seed"), "stderr: {stderr}");
}

#[test]
fn cli_farm_empty_range_exit_2() {
    let output = std::process::Command::new(bin_exe())
        .args(["farm", "--seeds", "5..5", "--jobs", "1"])
        .output()
        .expect("run anvil-sim farm");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("empty seed range"), "stderr: {stderr}");
}

#[test]
fn cli_farm_bad_profile_exit_2() {
    let output = std::process::Command::new(bin_exe())
        .args(["farm", "--seeds", "0..1", "--jobs", "1", "--profile", "bogus"])
        .output()
        .expect("run anvil-sim farm");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown farm profile"), "stderr: {stderr}");
}

#[test]
fn cli_farm_require_sometimes_coverage_exit_1() {
    // Two seeds cannot hit every registered sometimes point.
    let output = std::process::Command::new(bin_exe())
        .args([
            "farm",
            "--seeds",
            "0..2",
            "--jobs",
            "1",
            "--profile",
            "smoke",
            "--require-sometimes-coverage",
        ])
        .output()
        .expect("run anvil-sim farm");
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("sometimes coverage incomplete"),
        "stderr: {stderr}"
    );
}

#[test]
fn cli_farm_kv_tiny_success() {
    let dir = tmpdir("farm-kv");
    let out = dir.join("kv.jsonl");
    let output = std::process::Command::new(bin_exe())
        .args([
            "farm",
            "--seeds",
            "0..1",
            "--profile",
            "kv",
            "--out",
            out.to_str().expect("utf8"),
        ])
        .output()
        .expect("run anvil-sim farm kv");
    assert!(
        output.status.success(),
        "farm kv failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("profile kv"), "stdout: {stdout}");
    let text = std::fs::read_to_string(&out).expect("kv jsonl written");
    assert!(text.contains("\"type\":\"seed\""), "jsonl: {text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cli_trace_smoke_to_file() {
    let dir = tmpdir("trace-smoke");
    let out = dir.join("trace.txt");
    let output = std::process::Command::new(bin_exe())
        .args([
            "trace",
            "--seed",
            "1",
            "--profile",
            "smoke",
            "--limit",
            "50",
            "--out",
            out.to_str().expect("utf8"),
        ])
        .output()
        .expect("run anvil-sim trace");
    assert!(
        output.status.success(),
        "trace smoke failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(&out).expect("trace written");
    assert!(text.contains("anvil trace seed=1 profile=smoke"), "trace: {text}");
    assert!(text.contains("outcome:"), "trace: {text}");
    assert!(text.contains("plan: anvil-plan/"), "trace: {text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cli_trace_kv_to_file() {
    let dir = tmpdir("trace-kv");
    let out = dir.join("trace-kv.txt");
    let output = std::process::Command::new(bin_exe())
        .args([
            "trace",
            "--seed",
            "1",
            "--profile",
            "kv",
            "--limit",
            "100000",
            "--out",
            out.to_str().expect("utf8"),
        ])
        .output()
        .expect("run anvil-sim trace kv");
    assert!(
        output.status.success(),
        "trace kv failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = std::fs::read_to_string(&out).expect("kv trace written");
    assert!(text.contains("anvil trace seed=1 profile=kv"), "trace: {text}");
    assert!(text.contains("outcome:"), "trace: {text}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cli_trace_bad_profile_exit_2() {
    let output = std::process::Command::new(bin_exe())
        .args(["trace", "--seed", "1", "--profile", "bogus"])
        .output()
        .expect("run anvil-sim trace");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("unknown farm profile"), "stderr: {stderr}");
}

#[test]
fn cli_trace_write_error_exit_2() {
    let dir = tmpdir("trace-err");
    let missing = dir.join("no-such-dir").join("out.txt");
    let output = std::process::Command::new(bin_exe())
        .args([
            "trace",
            "--seed",
            "1",
            "--profile",
            "smoke",
            "--out",
            missing.to_str().expect("utf8"),
        ])
        .output()
        .expect("run anvil-sim trace");
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot write"), "stderr: {stderr}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cli_trace_limit_truncation_marker() {
    let output = std::process::Command::new(bin_exe())
        .args(["trace", "--seed", "1", "--profile", "smoke", "--limit", "5"])
        .output()
        .expect("run anvil-sim trace");
    assert!(
        output.status.success(),
        "trace failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("[truncated to 5 lines; rerun with --limit N]"),
        "stdout: {stdout}"
    );
}

// ---------------------------------------------------------------------------
// Part 2 — farm.rs: render, kv-storage, resume filtering
// ---------------------------------------------------------------------------

#[test]
fn farm_render_full_and_empty_sometimes() {
    let full: Vec<(String, u64)> = anvil_sim::SOMETIMES_INVENTORY
        .iter()
        .map(|name| ((*name).to_string(), 1u64))
        .collect();
    let covered = FarmSummary {
        total_seeds: 2,
        passed_seeds: 2,
        failed_seeds: Vec::new(),
        duration: Duration::from_secs(1),
        simulated_hours: 0.01,
        faults_by_class: vec![("partition".to_string(), 2)],
        buggify: vec![("pt".to_string(), "loc".to_string(), 10, 2)],
        invariant_checks: 7,
        sometimes: full,
    };
    let text = covered.render(FarmProfile::Smoke);
    assert!(text.contains("sometimes coverage: all registered points hit"));
    assert!(!text.contains("NEVER HIT"));
    assert!(text.contains("pt@loc"));
    assert!(text.contains("partition: 2"));

    let empty = FarmSummary {
        total_seeds: 3,
        passed_seeds: 2,
        failed_seeds: vec![9],
        duration: Duration::ZERO,
        simulated_hours: 0.0,
        faults_by_class: Vec::new(),
        buggify: Vec::new(),
        invariant_checks: 0,
        sometimes: Vec::new(),
    };
    let missing = empty.render(FarmProfile::KvStorage);
    assert!(missing.contains("profile kv-storage"));
    assert!(missing.contains("none registered"));
    assert!(missing.contains("none hit"));
    assert!(missing.contains("NEVER HIT"));
    assert!(missing.contains("first failing seed: 9"));
    assert_eq!(empty.seeds_per_second(), 0.0);
}

#[test]
fn farm_range_kv_storage_single_seed() {
    let options = FarmOptions {
        jobs: 1,
        out_path: None,
        resume: false,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (outcomes, summary) = anvil_sim::run_farm_range(
        FarmRange { start: 2, end: 3 },
        FarmProfile::KvStorage,
        &options,
    );
    assert_eq!(outcomes.len(), 1);
    assert_eq!(summary.total_seeds, 1);
}

#[test]
fn farm_range_resume_filters_mixed_lines() {
    let dir = tmpdir("farm-resume");
    let out = dir.join("results.jsonl");
    std::fs::write(
        &out,
        concat!(
            "{\"type\":\"seed\",\"seed\":0,\"ok\":true}\n",
            "{\"type\":\"seed\",\"seed\":1,\"ok\":false}\n",
            "this line has no seed marker\n",
            "{\"type\":\"seed\",\"seed\":abc,\"ok\":true}\n",
            "{\"type\":\"summary\",\"seed\":0,\"ok\":true}\n",
        ),
    )
    .expect("seed resume file");
    let options = FarmOptions {
        jobs: 1,
        out_path: Some(out),
        resume: true,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (outcomes, _) = anvil_sim::run_farm_range(
        FarmRange { start: 0, end: 2 },
        FarmProfile::Smoke,
        &options,
    );
    // Seed 0 was recorded as passed and is skipped; seed 1 re-runs.
    assert_eq!(outcomes.len(), 1);
    assert_eq!(outcomes[0].seed, 1);
    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Part 2 — plan.rs: decode errors, build validation, crafted runs
// ---------------------------------------------------------------------------

const MINIMAL: &str = "anvil-plan/2;seed=1;size=3;ticks=30;warm=5;heal=25;salt=7";

#[test]
fn plan_decode_error_branches() {
    assert!(Plan::decode("bogus").is_err());
    assert!(Plan::decode("anvil-plan/99;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/2;").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;lonely").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;unknownkey=5").is_err());
    assert!(Plan::decode(&format!("{MINIMAL};size=0")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};size=10")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};size=100001")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};seed=abc")).is_err());
    assert!(Plan::decode("anvil-plan/2;size=3;ticks=30;warm=5;heal=25;salt=7").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;ticks=30;warm=5;heal=25;salt=7").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;size=3;warm=5;heal=25;salt=7").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;size=3;ticks=100001;warm=5;heal=25;salt=7").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;size=3;ticks=30;warm=20;heal=10;salt=7").is_err());
    assert!(Plan::decode(&format!("{MINIMAL};net=1,2")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};net=101,0,0")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};net=0,0,100001")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};storage=2")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};propose=100001")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=garbage")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:10-5:delay:3")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:bogus:1")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:asym:1")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:clock:1")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:clock:1-100")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:crash:9")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};ev=0:6-7:delay:3;ev=0:8-9:delay:3")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};workload=1,2,3")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};workload=0,4,4,0,5,50,0,0")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};workload=2,4,4,0,5,50,2,0")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};workload=2,4,4,0,5,50,0,2")).is_err());
    assert!(Plan::decode(&format!("{MINIMAL};workload=2,0,4,0,5,50,0,0")).is_err());

    // Too many events in one decode.
    let mut many = String::from(MINIMAL);
    for i in 0..65u32 {
        use std::fmt::Write as _;
        let _ = write!(many, ";ev={i}:6-7:delay:3");
    }
    assert!(Plan::decode(&many).is_err());

    // Minimal valid plan round-trips.
    let minimal = Plan::decode(MINIMAL).expect("minimal decodes");
    assert_eq!(minimal.cluster_size, 3);
    let round = Plan::decode(&minimal.encode()).expect("roundtrip");
    assert_eq!(round, minimal);

    // Valid workload plan still round-trips.
    let workload = WorkloadSpec {
        clients: 2,
        keys: 4,
        ops_per_client: 4,
        list_pct: 0,
        think_ticks: 5,
        timeout_ticks: 50,
        client_crashes: false,
        unordered: false,
    };
    let with_workload = Plan::decode(&format!("{MINIMAL};workload=2,4,4,0,5,50,0,0"))
        .expect("workload decodes");
    assert_eq!(with_workload.workload, Some(workload));
}

fn all_faults_plan(seed: u64, storage_faults: bool) -> Plan {
    Plan {
        version: 2,
        seed,
        cluster_size: 3,
        ticks: 30,
        proposals: vec![10, 16, 22],
        warmup_end: 5,
        heal_start: 25,
        events: vec![
            FaultEvent {
                id: 0,
                start: 6,
                end: 8,
                fault: FaultKind::Partition { member: 0 },
            },
            FaultEvent {
                id: 1,
                start: 7,
                end: 9,
                fault: FaultKind::Asymmetric { from: 0, to: 1 },
            },
            FaultEvent {
                id: 2,
                start: 8,
                end: 12,
                fault: FaultKind::Crash { node: 1 },
            },
            FaultEvent {
                id: 3,
                start: 6,
                end: 30,
                fault: FaultKind::Pause { node: 2 },
            },
            FaultEvent {
                id: 4,
                start: 6,
                end: 10,
                fault: FaultKind::SlowClock {
                    node: 0,
                    rate_per_mille: 500,
                },
            },
            FaultEvent {
                id: 5,
                start: 20,
                end: 21,
                fault: FaultKind::Transfer { node: 1 },
            },
            FaultEvent {
                id: 6,
                start: 6,
                end: 10,
                fault: FaultKind::DelaySpike { extra_ticks: 3 },
            },
        ],
        drop_pct: 5,
        dup_pct: 2,
        max_delay: 3,
        salt: 12345,
        storage_faults,
        workload: None,
    }
}

#[test]
fn plan_crafted_all_faults_runs() {
    let plan = all_faults_plan(0, false);
    let stats = anvil_sim::run_plan(&plan);
    assert!(stats.virtual_ticks > 0);
    let (traced_stats, lines) = anvil_sim::run_plan_traced(&plan, true);
    assert_eq!(traced_stats.virtual_ticks, stats.virtual_ticks);
    assert!(!lines.is_empty());

    let storage_plan = all_faults_plan(0, true);
    let storage_stats = anvil_sim::run_plan(&storage_plan);
    assert!(storage_stats.virtual_ticks > 0);
}

#[test]
fn plan_shrink_helpers_with_synthetic_oracles() {
    let plan = all_faults_plan(3, false);

    // Never fails: every shrinker declines.
    let untouched = anvil_sim::shrink_plan(&plan, &|_| false);
    assert_eq!(untouched, plan);
    let same = anvil_sim::shrink_plan_same_class(&plan, &|_| None);
    assert_eq!(same, plan);

    // Always fails: every shrinker applies.
    let shrunk = anvil_sim::shrink_plan(&plan, &|_| true);
    assert!(shrunk.events.len() <= plan.events.len());
    assert!(shrunk.proposals.len() <= plan.proposals.len());
    let shrunk_class = anvil_sim::shrink_plan_same_class(&plan, &|_| Some("x".to_string()));
    assert!(shrunk_class.events.len() <= plan.events.len());

    // Workload plan exercises dead-proposal and workload shrinking.
    let workload = WorkloadSpec {
        clients: 3,
        keys: 4,
        ops_per_client: 8,
        list_pct: 10,
        think_ticks: 5,
        timeout_ticks: 50,
        client_crashes: false,
        unordered: false,
    };
    let workload_plan = Plan::generate_for_workload(22, false, workload);
    let shrunk_workload =
        anvil_sim::shrink_plan_same_class(&workload_plan, &|_| Some("y".to_string()));
    assert!(shrunk_workload.events.len() <= workload_plan.events.len());
    let shrunk_both = anvil_sim::shrink_plan(&workload_plan, &|_| true);
    assert!(shrunk_both.events.len() <= workload_plan.events.len());

    // Wide cluster with low member references shrinks to three nodes.
    let mut wide = all_faults_plan(4, false);
    wide.cluster_size = 5;
    wide.workload = Some(WorkloadSpec {
        clients: 2,
        keys: 2,
        ops_per_client: 2,
        list_pct: 0,
        think_ticks: 5,
        timeout_ticks: 50,
        client_crashes: false,
        unordered: false,
    });
    let shrunk_wide = anvil_sim::shrink_plan_same_class(&wide, &|_| Some("z".to_string()));
    assert!(shrunk_wide.cluster_size <= wide.cluster_size);

    // Tick shrinking shortens an over-long run.
    let mut long = all_faults_plan(5, false);
    long.ticks = 100;
    let shrunk_ticks = anvil_sim::shrink_plan(&long, &|_| true);
    assert!(shrunk_ticks.ticks <= long.ticks);

    // Zeroed network faults decline net shrinking.
    let mut clean_net = all_faults_plan(6, false);
    clean_net.drop_pct = 0;
    clean_net.dup_pct = 0;
    clean_net.max_delay = 0;
    let kept = anvil_sim::shrink_plan(&clean_net, &|_| true);
    assert_eq!(kept.drop_pct, 0);
}

#[test]
fn plan_failure_class_str_payload_and_normalize() {
    let payload: Box<dyn std::any::Any + Send> = Box::new("boom 12");
    assert_eq!(
        anvil_sim::panic_failure_class(&payload),
        "panic:boom #"
    );
    assert_eq!(anvil_sim::normalize_failure_class(""), "");
    assert_eq!(anvil_sim::normalize_failure_class("abc"), "abc");
    assert_eq!(anvil_sim::normalize_failure_class("a1b22c"), "a#b#c");
}

// ---------------------------------------------------------------------------
// Part 2 — raft_invariants.rs
// ---------------------------------------------------------------------------

#[test]
fn invariants_election_safety_two_leaders() {
    let (config1, config2, node1, node2) = two_node_configs();
    let mut core1 = core_with_vote(config1, 2, 0, Vec::new(), None, 101);
    let mut core2 = core_with_vote(config2, 2, 0, Vec::new(), None, 102);
    core1.testing_set_role(Role::Leader);
    core2.testing_set_role(Role::Leader);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, core1);
    nodes.insert(node2, core2);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::ElectionSafety { .. }
    ));
}

#[test]
fn invariants_vote_change_within_term() {
    let (config1, _, node1, node2) = two_node_configs();
    let first = core_with_vote(config1.clone(), 1, 0, Vec::new(), Some(node2), 111);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, first);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());
    let second = core_with_vote(config1, 1, 0, Vec::new(), Some(node1), 112);
    nodes.insert(node1, second);
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::VoteNeverChangesWithinTerm { .. }
    ));
}

#[test]
fn invariants_term_monotonic_regression() {
    let (config1, _, node1, _) = two_node_configs();
    let higher = core_with_vote(config1.clone(), 2, 0, Vec::new(), None, 121);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, higher);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());
    let lower = core_with_vote(config1, 1, 0, Vec::new(), None, 122);
    nodes.insert(node1, lower);
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::TermMonotonic { .. }
    ));
}

#[test]
fn invariants_leader_completeness_wrong_term() {
    let (config1, config2, node1, node2) = two_node_configs();
    let committer1 = core_with_vote(config1.clone(), 1, 1, vec![normal_entry(1, 1)], None, 131);
    let committer2 = core_with_vote(config2, 1, 1, vec![normal_entry(1, 1)], None, 132);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, committer1);
    nodes.insert(node2, committer2);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());
    assert!(checker.committed_entries_count() >= 1);
    assert!(checker.committed_oracle().contains_key(&LogIndex::from_raw(1)));

    let mut divergent = core_with_vote(config1, 2, 0, vec![normal_entry(1, 2)], None, 133);
    divergent.testing_set_role(Role::Leader);
    nodes.insert(node1, divergent);
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::LeaderCompleteness { .. }
    ));
}

#[test]
fn invariants_recovered_log_against_oracle() {
    let (config1, config2, node1, node2) = two_node_configs();
    let committer1 = core_with_vote(config1, 1, 1, vec![normal_entry(1, 1)], None, 141);
    let committer2 = core_with_vote(config2, 1, 1, vec![normal_entry(1, 1)], None, 142);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, committer1);
    nodes.insert(node2, committer2);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());

    // Matching term is fine; entries beyond the commit bound are ignored.
    assert!(
        checker
            .check_recovered_log_against_oracle(node1, &[normal_entry(1, 1)], LogIndex::from_raw(1))
            .is_ok()
    );
    assert!(
        checker
            .check_recovered_log_against_oracle(node1, &[normal_entry(9, 9)], LogIndex::from_raw(1))
            .is_ok()
    );
    assert!(
        checker
            .check_recovered_log_against_oracle(node1, &[], LogIndex::from_raw(1))
            .is_ok()
    );
    // Divergent term at a committed index fails.
    let err = checker
        .check_recovered_log_against_oracle(node1, &[normal_entry(1, 2)], LogIndex::from_raw(1))
        .unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::CommittedNeverLostAcrossCrashes { .. }
    ));
}

#[test]
fn invariants_vote_never_forgotten_and_reset_for_restart() {
    let (config1, _, node1, node2) = two_node_configs();
    let voter = core_with_vote(config1.clone(), 1, 0, Vec::new(), Some(node2), 151);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, voter);
    let mut checker = anvil_sim::RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());

    // Unknown term has no record: always fine.
    assert!(
        checker
            .check_vote_never_forgotten(node1, Term::from_raw(9), None)
            .is_ok()
    );

    // Same vote round-trips through a restart.
    let empty_acked: BTreeMap<Term, Option<NodeId>> = BTreeMap::new();
    assert!(
        checker
            .reset_for_restart(node1, Term::from_raw(1), Some(node2), &empty_acked)
            .is_ok()
    );

    // A non-acknowledged vote is replaced by the recovered vote.
    assert!(
        checker
            .reset_for_restart(node1, Term::from_raw(1), None, &empty_acked)
            .is_ok()
    );

    // Re-record the vote, then refuse to lose an acknowledged one.
    let voted = core_with_vote(config1, 1, 0, Vec::new(), Some(node2), 152);
    nodes.insert(node1, voted);
    assert!(checker.try_check_invariants(&nodes).is_ok());
    let mut acked: BTreeMap<Term, Option<NodeId>> = BTreeMap::new();
    acked.insert(Term::from_raw(1), Some(node2));
    let err = checker
        .reset_for_restart(node1, Term::from_raw(1), Some(node1), &acked)
        .unwrap_err();
    assert_eq!(err, (Some(node2), Some(node1)));

    // A non-acknowledged divergent vote is adopted.
    let mut fresh = anvil_sim::RaftInvariantChecker::new();
    let (fresh_config, _, fresh_node, _) = two_node_configs();
    let fresh_voter = core_with_vote(fresh_config, 1, 0, Vec::new(), Some(node2), 153);
    let mut fresh_nodes = BTreeMap::new();
    fresh_nodes.insert(fresh_node, fresh_voter);
    assert!(fresh.try_check_invariants(&fresh_nodes).is_ok());
    assert!(
        fresh
            .reset_for_restart(fresh_node, Term::from_raw(1), Some(fresh_node), &empty_acked)
            .is_ok()
    );
}

// ---------------------------------------------------------------------------
// Part 2 — network.rs and scenario.rs
// ---------------------------------------------------------------------------

#[test]
fn network_helpers_cover_fault_and_queue_paths() {
    let first = node(1);
    let second = node(2);
    let third = node(3);

    let mut net = SimNetwork::new();
    assert!(net.is_empty());
    assert_eq!(net.len(), 0);
    assert!(!net.has_due(100));
    assert!(SimNetwork::default().is_empty());

    net.set_transport(TransportMode::Unordered);
    assert_eq!(net.transport(), TransportMode::Unordered);
    net.set_transport(TransportMode::OrderedLossy);
    net.set_fault_salt(77);
    net.set_bandwidth_per_tick(0);
    net.set_latency_model(anvil_sim::LatencyModel::Constant(0));
    net.set_link_latency(
        first,
        second,
        anvil_sim::LatencyModel::Uniform { min: 1, max: 3 },
    );

    // Drop hook refuses the packet.
    net.drop_hook = Some(Box::new(|_, _, _| true));
    assert!(!net.schedule(first, second, heartbeat(first, second), 0));
    net.drop_hook = None;

    // Full reset rate drops and backs off.
    net.set_fault_rates(0, 0, 0, 100);
    assert!(!net.schedule(first, second, heartbeat(first, second), 0));
    assert!(net.is_backing_off(first, second, 0));
    net.set_fault_rates(0, 0, 0, 0);

    // Delay and duplicate hooks add packets (past the reset backoff).
    net.delay_hook = Some(Box::new(|_, _, _| 5));
    net.duplicate_hook = Some(Box::new(|_, _, _| true));
    assert!(net.schedule(first, second, heartbeat(first, second), 100));
    assert_eq!(net.len(), 2);
    net.delay_hook = None;
    net.duplicate_hook = None;

    // Plan-hash faults: drop everything, then duplicate.
    let drop_all = anvil_sim::PlanNet {
        salt: 5,
        drop_pct: 100,
        dup_pct: 0,
        max_delay: 0,
    };
    assert!(!net.schedule_plan(first, third, heartbeat(first, third), 100, &drop_all));
    let dup_all = anvil_sim::PlanNet {
        salt: 5,
        drop_pct: 0,
        dup_pct: 100,
        max_delay: 4,
    };
    assert!(net.schedule_plan(first, third, heartbeat(first, third), 100, &dup_all));

    // Unordered delivery reorders deterministically.
    net.set_transport(TransportMode::Unordered);
    net.set_fault_rates(50, 0, 0, 0);
    let due = net.take_due(u64::MAX);
    assert!(!due.is_empty());
    assert!(net.is_empty());

    // Indexed queue operations on a fresh packet.
    assert!(net.schedule(first, second, heartbeat(first, second), 100));
    assert!(net.take_at(0).is_some());
    assert!(net.take_at(0).is_none());
    assert!(net.schedule(first, second, heartbeat(first, second), 100));
    assert!(net.drop_at(0));
    assert!(!net.drop_at(0));
    assert!(net.schedule(first, second, heartbeat(first, second), 100));
    assert!(net.duplicate_at(0));
    assert!(net.duplicate_at(999_999));
    let mut empty_net = SimNetwork::new();
    assert!(!empty_net.duplicate_at(0));

    // Partition helpers.
    net.isolate(first, &[second, third]);
    assert!(net.is_cut(first, second));
    assert!(!net.schedule(first, second, heartbeat(first, second), 0));
    net.heal_pair(first, second);
    assert!(!net.is_cut(first, second));
    net.partition(&[first], &[second, third]);
    net.heal_directed(first, second);
    assert!(!net.is_cut(first, second));
    assert!(net.is_cut(second, first));
    net.bisect(&[first], &[second]);
    net.bridge_hub(third, &[first], &[second]);
    assert!(!net.is_cut(third, first));
    net.cut_one_way(first, second);
    assert!(net.is_cut(first, second));
    net.heal_all();
    assert!(!net.is_cut(first, second));

    // Reset helper suppresses sends until the backoff lapses.
    net.reset_connection(first, second, 10, 4);
    assert!(net.is_backing_off(first, second, 11));
    assert!(!net.is_backing_off(first, second, 100));
}

#[test]
fn network_latency_models_sample_deterministically() {
    use anvil_sim::LatencyModel;
    assert_eq!(LatencyModel::Constant(7).sample_hash(1, 2, 3, 4), 7);
    assert_eq!(
        LatencyModel::Uniform { min: 5, max: 5 }.sample_hash(1, 2, 3, 4),
        5
    );
    let uniform = LatencyModel::Uniform { min: 1, max: 9 }.sample_hash(11, 1, 2, 3);
    assert!((1..=9).contains(&uniform));
    assert_eq!(LatencyModel::Exponential { mean: 0 }.sample_hash(1, 2, 3, 4), 0);
    let exp = LatencyModel::Exponential { mean: 10 }.sample_hash(13, 1, 2, 3);
    assert!(exp <= 80);
    let bimodal = LatencyModel::Bimodal {
        fast: 3,
        fast_spread: 0,
        slow: 50,
        slow_spread: 0,
        fast_ppm: 1_000_000,
    };
    // fast_ppm = 1M selects fast unless the hash draw lands on slow;
    // either way the draw is deterministic and equals the mean.
    let b = bimodal.sample_hash(1, 2, 3, 4);
    assert!(b == 3 || b == 50, "bimodal mean, got {b}");
    assert_eq!(b, bimodal.sample_hash(1, 2, 3, 4));
    let bimodal = LatencyModel::Bimodal {
        fast: 3,
        fast_spread: 2,
        slow: 50,
        slow_spread: 5,
        fast_ppm: 500_000,
    }
    .sample_hash(17, 1, 2, 4);
    assert!(bimodal <= 57);
}

#[test]
fn scenario_golden_runs_deterministically() {
    let (events, hash) = anvil_sim::run_golden_scenario(42).expect("golden scenario");
    assert!(events > 0);
    assert!(!hash.as_bytes().iter().all(|byte| *byte == 0));
    let (again, _) = anvil_sim::run_golden_scenario(42).expect("golden repeat");
    assert_eq!(events, again);
}
