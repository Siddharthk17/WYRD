#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

//! Deterministic simulation seed farm for ANVIL consensus testing.
//!
//! Each seed materializes a serializable [`Plan`] before its run starts and
//! executes it with no generator draws mid-run. Online invariant checking
//! asserts safety after every transition of every node.

use crate::plan::{Plan, run_plan};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

/// Aggregated outcome of a simulator farm execution.
#[derive(Debug, Clone)]
pub struct FarmResult {
    /// Total number of random seeds evaluated.
    pub total_seeds: usize,
    /// Number of seeds that passed all invariant checks.
    pub passed_seeds: usize,
    /// First seed that produced an invariant violation, if any.
    pub failed_seed: Option<u64>,
    /// Total execution duration.
    pub duration: Duration,
}

impl FarmResult {
    /// Returns true if all seeds completed with zero invariant violations.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.failed_seed.is_none() && self.passed_seeds == self.total_seeds
    }

    /// Evaluates execution throughput in simulated seeds per second.
    #[must_use]
    pub fn seeds_per_second(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.total_seeds as f64 / secs
        } else {
            0.0
        }
    }
}

/// Runs a single deterministic simulation seed.
///
/// Generates the seed's [`Plan`] and executes it.
pub fn run_farm_seed(seed: u64) {
    run_plan(&Plan::generate(seed, false));
}

/// Runs a single seed with storage fault injection enabled on every node.
///
/// Generates the seed's storage-fault [`Plan`] and executes it.
pub fn run_farm_seed_with_storage_faults(seed: u64) {
    run_plan(&Plan::generate(seed, true));
}

/// Runs a single KV seed: full fault model, workload, checker, audit.
pub fn run_kv_farm_seed(seed: u64) {
    let (history, audit) = crate::workload::run_kv_seed(seed, false);
    assert!(audit, "seed {seed}: convergence audit failed");
    let failure = match anvil_check::check_history(&history) {
        Ok(()) => None,
        Err(failure) => Some(failure.json),
    };
    assert!(
        failure.is_none(),
        "seed {seed}: linearizability violation: {}",
        failure.unwrap_or_default()
    );
}

/// Runs a single KV seed with storage faults on.
pub fn run_kv_farm_seed_with_storage(seed: u64) {
    let (history, audit) = crate::workload::run_kv_seed_with_storage(seed, false);
    assert!(audit, "seed {seed}: convergence audit failed");
    let failure = match anvil_check::check_history(&history) {
        Ok(()) => None,
        Err(failure) => Some(failure.json),
    };
    assert!(
        failure.is_none(),
        "seed {seed}: linearizability violation: {}",
        failure.unwrap_or_default()
    );
}

/// Executes `num_seeds` KV seeds in parallel across worker threads.
#[must_use]
pub fn run_kv_farm(num_seeds: usize, num_threads: usize, storage_faults: bool) -> FarmResult {
    let start = Instant::now();
    let passed = Arc::new(AtomicUsize::new(0));
    let failed_seed = Arc::new(std::sync::Mutex::new(None));

    let threads_count = num_threads.max(1);
    let chunk_size = num_seeds.div_ceil(threads_count);
    let mut handles = Vec::with_capacity(threads_count);

    for worker in 0..threads_count {
        let start_seed = worker * chunk_size;
        let end_seed = (start_seed + chunk_size).min(num_seeds);
        let passed_clone = Arc::clone(&passed);
        let failed_clone = Arc::clone(&failed_seed);
        let handle = thread::spawn(move || {
            let mut local_passed = 0usize;
            let mut local_failed: Option<u64> = None;
            for seed in start_seed..end_seed {
                let outcome = std::panic::catch_unwind(|| {
                    if storage_faults {
                        run_kv_farm_seed_with_storage(seed as u64);
                    } else {
                        run_kv_farm_seed(seed as u64);
                    }
                });
                if outcome.is_ok() {
                    local_passed += 1;
                } else if local_failed.is_none() {
                    local_failed = Some(seed as u64);
                }
            }
            passed_clone.fetch_add(local_passed, Ordering::Relaxed);
            if let Some(failed) = local_failed {
                let mut lock = failed_clone
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if lock.is_none_or(|current| failed < current) {
                    *lock = Some(failed);
                }
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        let _ = handle.join();
    }
    let duration = start.elapsed();
    let passed_count = passed.load(Ordering::SeqCst);
    let failed = *failed_seed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    FarmResult {
        total_seeds: num_seeds,
        passed_seeds: passed_count,
        failed_seed: failed,
        duration,
    }
}

/// Executes `num_seeds` simulation seeds in parallel across worker threads.
#[must_use]
pub fn run_seed_farm(num_seeds: usize, num_threads: usize) -> FarmResult {
    run_seed_farm_inner(num_seeds, num_threads, false)
}

/// Executes `num_seeds` seeds with storage fault injection enabled on every node.
#[must_use]
pub fn run_seed_farm_with_storage_faults(num_seeds: usize, num_threads: usize) -> FarmResult {
    run_seed_farm_inner(num_seeds, num_threads, true)
}

fn run_seed_farm_inner(num_seeds: usize, num_threads: usize, storage_faults: bool) -> FarmResult {
    let start = Instant::now();
    let passed = Arc::new(AtomicUsize::new(0));
    let failed_seed = Arc::new(std::sync::Mutex::new(None));

    let threads_count = num_threads.max(1);
    let chunk_size = num_seeds.div_ceil(threads_count);

    let mut handles = Vec::with_capacity(threads_count);

    for worker in 0..threads_count {
        let start_seed = worker * chunk_size;
        let end_seed = (start_seed + chunk_size).min(num_seeds);
        let passed_clone = Arc::clone(&passed);
        let failed_clone = Arc::clone(&failed_seed);

        let handle = thread::spawn(move || {
            let mut local_passed = 0usize;
            let mut local_failed: Option<u64> = None;
            for seed in start_seed..end_seed {
                let seed_u64 = seed as u64;
                let outcome = std::panic::catch_unwind(|| {
                    if storage_faults {
                        run_farm_seed_with_storage_faults(seed_u64);
                    } else {
                        run_farm_seed(seed_u64);
                    }
                });

                if outcome.is_ok() {
                    local_passed += 1;
                } else if local_failed.is_none() {
                    local_failed = Some(seed_u64);
                }
            }
            passed_clone.fetch_add(local_passed, Ordering::Relaxed);
            if let Some(failed) = local_failed {
                let mut lock = failed_clone
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if lock.is_none_or(|current| failed < current) {
                    *lock = Some(failed);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.join();
    }

    let duration = start.elapsed();
    let passed_count = passed.load(Ordering::SeqCst);
    let failed = *failed_seed
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);

    FarmResult {
        total_seeds: num_seeds,
        passed_seeds: passed_count,
        failed_seed: failed,
        duration,
    }
}

/// Farm profiles: which harness runs each seed and with what fault mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FarmProfile {
    /// Consensus plans without storage faults (fastest).
    Smoke,
    /// KV workload without storage faults.
    Kv,
    /// KV workload with chaotic storage faults.
    KvStorage,
}

impl FarmProfile {
    /// Parses a profile name from the CLI.
    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "smoke" => Ok(Self::Smoke),
            "kv" => Ok(Self::Kv),
            "kv-storage" => Ok(Self::KvStorage),
            _ => Err(format!(
                "unknown farm profile '{name}': expected smoke, kv, or kv-storage"
            )),
        }
    }

    /// Whether runs in this profile enable storage fault injection.
    #[must_use]
    pub const fn storage_faults(self) -> bool {
        matches!(self, Self::KvStorage)
    }

    /// Whether runs in this profile execute the KV workload.
    #[must_use]
    pub const fn is_kv(self) -> bool {
        matches!(self, Self::Kv | Self::KvStorage)
    }

    /// Profile name as used on the CLI.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Kv => "kv",
            Self::KvStorage => "kv-storage",
        }
    }
}

/// Half-open seed range `[start, end)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FarmRange {
    /// First seed, inclusive.
    pub start: u64,
    /// End seed, exclusive.
    pub end: u64,
}

impl FarmRange {
    /// Parses `A..B` (inclusive start, exclusive end) or a bare count `N`
    /// meaning `0..N`.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if let Some((a, b)) = raw.split_once("..") {
            let start: u64 = a
                .parse()
                .map_err(|_| format!("bad seed range '{raw}': expected A..B"))?;
            let end: u64 = b
                .parse()
                .map_err(|_| format!("bad seed range '{raw}': expected A..B"))?;
            if end < start {
                return Err(format!("bad seed range '{raw}': end before start"));
            }
            Ok(Self { start, end })
        } else {
            let count: u64 = raw
                .parse()
                .map_err(|_| format!("bad seed count '{raw}': expected N or A..B"))?;
            Ok(Self {
                start: 0,
                end: count,
            })
        }
    }

    /// Number of seeds in the range.
    #[must_use]
    pub fn len(self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// Whether the range holds no seeds.
    #[must_use]
    pub const fn is_empty(self) -> bool {
        self.end <= self.start
    }
}

/// Outcome of a single seed.
#[derive(Debug, Clone)]
pub struct SeedOutcome {
    /// Master seed.
    pub seed: u64,
    /// True when every check passed.
    pub ok: bool,
    /// Human-readable failure class on failure.
    pub failure: Option<String>,
    /// Virtual ticks advanced.
    pub virtual_ticks: u64,
    /// Snapshots taken.
    pub snapshots_taken: u64,
    /// Snapshots installed.
    pub installs: u64,
    /// Invariant evaluation rounds.
    pub invariant_checks: u64,
    /// Fault events injected, by class name.
    pub fault_counts: Vec<(String, u64)>,
    /// Rare-path hits tallied by point name.
    pub sometimes: Vec<(String, u64)>,
}

/// Registered `sometimes!` coverage points. Every name marked anywhere in
/// the deterministic crates or the sim must appear here exactly once; a
/// source-scan test enforces it. Session expiry, joint config, and repair
/// completion join in Week 6 with the code that triggers them.
pub const SOMETIMES_INVENTORY: &[&str] = &[
    "raft.snapshot.install.complete",
    "raft.snapshot.resend",
    "raft.snapshot.send.start",
    "raft.term.bump.append",
    "raft.transfer.aborted",
    "raft.transfer.timeoutnow.sent",
    "sim.client.crash",
    "sim.repair.cleared",
    "sim.transfer.begin",
    "storage.midlog.needsrepair",
    "storage.torn.tail",
];

/// Points in the inventory with zero hits in a summary.
#[must_use]
pub fn sometimes_missed(sometimes: &[(String, u64)]) -> Vec<String> {
    SOMETIMES_INVENTORY
        .iter()
        .filter(|point| {
            !sometimes
                .iter()
                .any(|(name, hits)| name == **point && *hits > 0)
        })
        .map(|point| (*point).to_string())
        .collect()
}

/// Aggregated farm summary.
#[derive(Debug, Clone, Default)]
pub struct FarmSummary {
    /// Seeds run (excluding resumed skips).
    pub total_seeds: u64,
    /// Seeds that passed.
    pub passed_seeds: u64,
    /// Seeds that failed, in order.
    pub failed_seeds: Vec<u64>,
    /// Wall-clock duration.
    pub duration: std::time::Duration,
    /// Simulated virtual hours (1 tick is 1 millisecond).
    pub simulated_hours: f64,
    /// Fault events injected, by class name.
    pub faults_by_class: Vec<(String, u64)>,
    /// Buggify points observed, with evaluation and activation counts.
    pub buggify: Vec<(String, String, u64, u64)>,
    /// Total invariant evaluation rounds.
    pub invariant_checks: u64,
    /// Rare-path hits tallied by point name.
    pub sometimes: Vec<(String, u64)>,
}

impl FarmSummary {
    /// Inventory points with zero hits in this summary.
    #[must_use]
    pub fn sometimes_missed(&self) -> Vec<String> {
        sometimes_missed(&self.sometimes)
    }
}

impl FarmSummary {
    /// Seeds per wall-clock second.
    #[must_use]
    pub fn seeds_per_second(&self) -> f64 {
        let secs = self.duration.as_secs_f64();
        if secs > 0.0 {
            self.total_seeds as f64 / secs
        } else {
            0.0
        }
    }

    /// Renders the human-readable summary printed after a farm run.
    #[must_use]
    pub fn render(&self, profile: FarmProfile) -> String {
        use std::fmt::Write as _;
        let mut out = format!(
            "ANVIL seed farm (profile {})\nseeds run: {} ({} passed, {} failed)\n",
            profile.name(),
            self.total_seeds,
            self.passed_seeds,
            self.failed_seeds.len(),
        );
        let _ = write!(
            out,
            "wall time: {:.2?} ({:.1} seeds/s)\nsimulated hours: {:.2}\n",
            self.duration,
            self.seeds_per_second(),
            self.simulated_hours
        );
        out.push_str("faults injected by class:\n");
        for (class, count) in &self.faults_by_class {
            let _ = writeln!(out, "  {class}: {count}");
        }
        out.push_str("buggify points (evaluations / activations):\n");
        if self.buggify.is_empty() {
            out.push_str("  none registered\n");
        }
        for (name, location, evals, hits) in &self.buggify {
            let _ = writeln!(out, "  {name}@{location}: {evals} / {hits}");
        }
        out.push_str("sometimes points (hits):\n");
        if self.sometimes.is_empty() {
            out.push_str("  none hit\n");
        }
        for (name, hits) in &self.sometimes {
            let _ = writeln!(out, "  {name}: {hits}");
        }
        let missed = self.sometimes_missed();
        if missed.is_empty() {
            out.push_str("sometimes coverage: all registered points hit\n");
        } else {
            let _ = writeln!(out, "sometimes coverage: NEVER HIT: {}", missed.join(", "));
        }
        let _ = writeln!(out, "invariants evaluated: {}", self.invariant_checks);
        if !self.failed_seeds.is_empty() {
            let _ = writeln!(out, "first failing seed: {}", self.failed_seeds[0]);
        }
        out
    }
}

/// Options for a farm run.
#[derive(Debug, Clone, Default)]
pub struct FarmOptions {
    /// Worker thread count.
    pub jobs: usize,
    /// Append JSONL results here when set.
    pub out_path: Option<std::path::PathBuf>,
    /// Skip seeds already recorded as passed in `out_path`.
    pub resume: bool,
    /// Stop taking new seeds after this many wall-clock seconds.
    pub time_budget_secs: Option<u64>,
    /// Directory holding failing-seed artifacts. Defaults to `<out>.failures`.
    pub failures_dir: Option<std::path::PathBuf>,
}

/// Runs one consensus-plan seed, catching violations as failure text.
fn run_consensus_seed(seed: u64, storage_faults: bool) -> SeedOutcome {
    let plan = Plan::generate(seed, storage_faults);
    let outcome = std::panic::catch_unwind(|| run_plan(&plan));
    finish_consensus(seed, &plan, outcome)
}

fn fault_kind_name(kind: &crate::plan::FaultKind) -> &'static str {
    match kind {
        crate::plan::FaultKind::Partition { .. } => "partition",
        crate::plan::FaultKind::Asymmetric { .. } => "asymmetric",
        crate::plan::FaultKind::Crash { .. } => "crash",
        crate::plan::FaultKind::Pause { .. } => "pause",
        crate::plan::FaultKind::SlowClock { .. } => "clock",
        crate::plan::FaultKind::Transfer { .. } => "transfer",
        crate::plan::FaultKind::DelaySpike { .. } => "delay",
    }
}

fn finish_consensus(
    seed: u64,
    plan: &Plan,
    outcome: Result<crate::plan::PlanRunStats, Box<dyn std::any::Any + Send>>,
) -> SeedOutcome {
    let mut faults = BTreeMap::new();
    for event in &plan.events {
        *faults
            .entry(fault_kind_name(&event.fault).to_string())
            .or_insert(0u64) += 1;
    }
    if plan.drop_pct > 0 || plan.dup_pct > 0 {
        faults.insert("lossy_network".to_string(), 1);
    }
    if plan.storage_faults {
        faults.insert("storage".to_string(), 1);
    }
    match outcome {
        Ok(stats) => SeedOutcome {
            seed,
            ok: true,
            failure: None,
            virtual_ticks: stats.virtual_ticks,
            snapshots_taken: 0,
            installs: 0,
            invariant_checks: stats.invariant_checks,
            fault_counts: faults.into_iter().collect(),
            sometimes: stats.sometimes,
        },
        Err(payload) => SeedOutcome {
            seed,
            ok: false,
            failure: Some(format!("invariant violation: {}", panic_message(&payload))),
            virtual_ticks: 0,
            snapshots_taken: 0,
            installs: 0,
            invariant_checks: 0,
            fault_counts: faults.into_iter().collect(),
            sometimes: Vec::new(),
        },
    }
}

/// Runs one KV workload seed, catching violations as failure text.
fn run_kv_seed_outcome(seed: u64, storage_faults: bool) -> SeedOutcome {
    let outcome = std::panic::catch_unwind(|| {
        let (history, audit, stats) = crate::workload::run_kv_seed_with_stats(seed, storage_faults);
        assert!(audit, "convergence audit failed");
        match anvil_check::check_history(&history) {
            Ok(()) => Ok(stats),
            Err(failure) => Err(failure.json),
        }
    });
    let mut faults = BTreeMap::new();
    for (class, count) in kv_plan_fault_counts(seed, storage_faults) {
        faults.insert(class, count);
    }
    match outcome {
        Ok(Ok(stats)) => SeedOutcome {
            seed,
            ok: true,
            failure: None,
            virtual_ticks: stats.virtual_ticks,
            snapshots_taken: stats.snapshots_taken,
            installs: stats.installs,
            invariant_checks: stats.invariant_checks,
            fault_counts: faults.into_iter().collect(),
            sometimes: stats.sometimes,
        },
        Ok(Err(json)) => SeedOutcome {
            seed,
            ok: false,
            failure: Some(format!("linearizability violation: {json}")),
            virtual_ticks: 0,
            snapshots_taken: 0,
            installs: 0,
            invariant_checks: 0,
            fault_counts: faults.into_iter().collect(),
            sometimes: Vec::new(),
        },
        Err(payload) => SeedOutcome {
            seed,
            ok: false,
            failure: Some(panic_message(&payload)),
            virtual_ticks: 0,
            snapshots_taken: 0,
            installs: 0,
            invariant_checks: 0,
            fault_counts: faults.into_iter().collect(),
            sometimes: Vec::new(),
        },
    }
}

/// Tallies fault classes for a KV seed by regenerating its plan.
/// Generation is deterministic, so the counts match the executed run.
fn kv_plan_fault_counts(seed: u64, storage_faults: bool) -> Vec<(String, u64)> {
    let profile = crate::workload::swarm_profile(seed);
    let workload = crate::plan::WorkloadSpec {
        clients: profile.clients,
        keys: profile.keys,
        ops_per_client: profile.ops_per_client,
        list_pct: profile.list_pct,
        think_ticks: profile.think_ticks,
        timeout_ticks: profile.timeout_ticks,
        client_crashes: profile.client_crashes,
        unordered: profile.unordered,
    };
    let plan = Plan::generate_for_workload(seed, storage_faults, workload);
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for event in &plan.events {
        *counts
            .entry(fault_kind_name(&event.fault).to_string())
            .or_insert(0) += 1;
    }
    if plan.drop_pct > 0 || plan.dup_pct > 0 {
        counts.insert("lossy_network".to_string(), 1);
    }
    if storage_faults {
        counts.insert("storage".to_string(), 1);
    }
    counts.into_iter().collect()
}

/// Extracts a human-readable message from a caught panic.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    // See `panic_failure_class`: downcast through an explicitly typed
    // reborrow, never an implicitly coerced `&(dyn Any)` argument.
    let borrowed: &(dyn std::any::Any + Send) = payload.as_ref();
    if let Some(text) = borrowed.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Escapes a string for JSON output.
fn json_escape_into(out: &mut String, text: &str) {
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ if (ch as u32) < 0x20 => {
                use std::fmt::Write as _;
                let _ = write!(out, "\\u{:04x}", ch as u32);
            }
            _ => out.push(ch),
        }
    }
}

/// Serializes one seed outcome as a JSONL line.
fn outcome_json(profile: FarmProfile, outcome: &SeedOutcome) -> String {
    use std::fmt::Write as _;
    let mut out = String::new();
    let _ = write!(
        out,
        "{{\"type\":\"seed\",\"profile\":\"{}\",\"seed\":{},\"ok\":{},\"virtual_ticks\":{},\"snapshots_taken\":{},\"installs\":{},\"invariant_checks\":{}",
        profile.name(),
        outcome.seed,
        outcome.ok,
        outcome.virtual_ticks,
        outcome.snapshots_taken,
        outcome.installs,
        outcome.invariant_checks,
    );
    out.push_str(",\"faults\":{");
    let mut first = true;
    for (class, count) in &outcome.fault_counts {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        json_escape_into(&mut out, class);
        let _ = write!(out, "\":{count}");
    }
    out.push_str("},\"failure\":");
    match &outcome.failure {
        None => out.push_str("null"),
        Some(text) => {
            out.push('"');
            json_escape_into(&mut out, text);
            out.push('"');
        }
    }
    out.push('}');
    out
}

/// Scans existing JSONL output for seeds already recorded as passed.
fn read_completed_ok(path: &std::path::Path) -> std::collections::BTreeSet<u64> {
    let mut done = std::collections::BTreeSet::new();
    let Ok(text) = std::fs::read_to_string(path) else {
        return done;
    };
    for line in text.lines() {
        let Some(seed_pos) = line.find("\"seed\":") else {
            continue;
        };
        if !line[seed_pos..].contains("\"ok\":true") {
            continue;
        }
        let rest = &line[seed_pos + 7..];
        let digits: String = rest.chars().take_while(|c: &char| c.is_numeric()).collect();
        if let Ok(seed) = digits.parse::<u64>() {
            done.insert(seed);
        }
    }
    done
}

/// Serializes the farm summary as a final JSONL object.
fn summary_json(profile: FarmProfile, summary: &FarmSummary) -> String {
    use std::fmt::Write as _;
    let mut out = format!(
        "{{\"type\":\"summary\",\"profile\":\"{}\",\"total_seeds\":{},\"passed_seeds\":{},\"failed_seeds\":[{}],\"duration_secs\":{:.3},\"seeds_per_second\":{:.1},\"simulated_hours\":{:.2},\"invariant_checks\":{}",
        profile.name(),
        summary.total_seeds,
        summary.passed_seeds,
        summary
            .failed_seeds
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(","),
        summary.duration.as_secs_f64(),
        summary.seeds_per_second(),
        summary.simulated_hours,
        summary.invariant_checks,
    );
    out.push_str(",\"faults_by_class\":{");
    let mut first = true;
    for (class, count) in &summary.faults_by_class {
        if !first {
            out.push(',');
        }
        first = false;
        out.push('"');
        json_escape_into(&mut out, class);
        let _ = write!(out, "\":{count}");
    }
    out.push_str("},\"buggify\":[");
    first = true;
    for (name, location, evals, hits) in &summary.buggify {
        if !first {
            out.push(',');
        }
        first = false;
        out.push_str("{\"name\":\"");
        json_escape_into(&mut out, name);
        out.push_str("\",\"location\":\"");
        json_escape_into(&mut out, location);
        let _ = write!(out, "\"evaluations\":{evals},\"activations\":{hits}}}");
    }
    out.push_str("]}");
    out
}

/// Saves a failing seed's plan and failure report next to the JSONL output.
/// The report carries the failing plan encoding plus a file-based repro
/// command, so `cargo xtask repro` reruns it from a clean checkout.
fn save_failure_artifact(dir: &std::path::Path, seed: u64, plan: &Plan, failure: &str) {
    use std::fmt::Write as _;
    let _ = std::fs::create_dir_all(dir);
    let plan_path = dir.join(format!("seed_{seed}.plan"));
    let _ = std::fs::write(&plan_path, plan.encode());
    let mut report = String::from("{\"seed\":");
    report.push_str(&seed.to_string());
    report.push_str(",\"failure\":\"");
    json_escape_into(&mut report, failure);
    report.push_str("\",\"plan\":\"");
    json_escape_into(&mut report, &plan.encode());
    let _ = write!(
        report,
        "\",\"repro\":\"cargo xtask repro {}\",\"plan_file\":\"{}\"}}",
        plan_path.display(),
        plan_path.display()
    );
    let _ = std::fs::write(dir.join(format!("seed_{seed}.json")), report);
}

/// Runs seeds `[range.start, range.end)` across worker threads and returns
/// every outcome plus the aggregate summary.
///
/// Each seed runs isolated and single-threaded; parallelism is across seeds.
/// With `out_path` set, one JSONL line is appended per seed as it completes
/// and failing seeds are saved with their plans. With `resume`, seeds
/// already recorded as passed are skipped. With `time_budget_secs`, workers
/// stop taking new seeds once the wall-clock budget lapses.
#[allow(clippy::too_many_lines)]
pub fn run_farm_range(
    range: FarmRange,
    profile: FarmProfile,
    options: &FarmOptions,
) -> (Vec<SeedOutcome>, FarmSummary) {
    let started = Instant::now();
    let deadline = options
        .time_budget_secs
        .map(|secs| started + Duration::from_secs(secs));
    let completed: std::collections::BTreeSet<u64> = if options.resume {
        options
            .out_path
            .as_ref()
            .map_or_else(std::collections::BTreeSet::new, |path| {
                read_completed_ok(path)
            })
    } else {
        std::collections::BTreeSet::new()
    };
    let pending: Vec<u64> = (range.start..range.end)
        .filter(|seed| !completed.contains(seed))
        .collect();

    let jobs = options.jobs.max(1);
    let next = Arc::new(std::sync::Mutex::new(pending.into_iter().peekable()));
    let outcomes = Arc::new(std::sync::Mutex::new(Vec::<SeedOutcome>::new()));
    let buggify_hits = Arc::new(std::sync::Mutex::new(Vec::<crate::BuggifyPoint>::new()));
    let storage = profile.storage_faults();
    let is_kv = profile.is_kv();

    let mut handles = Vec::with_capacity(jobs);
    for _ in 0..jobs {
        let next = Arc::clone(&next);
        let outcomes = Arc::clone(&outcomes);
        let buggify_hits = Arc::clone(&buggify_hits);
        handles.push(thread::spawn(move || {
            loop {
                if let Some(end) = deadline
                    && Instant::now() >= end
                {
                    break;
                }
                let seed = {
                    let mut guard = next
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    guard.next()
                };
                let Some(seed) = seed else { break };
                let outcome = if is_kv {
                    run_kv_seed_outcome(seed, storage)
                } else {
                    run_consensus_seed(seed, storage)
                };
                outcomes
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(outcome);
            }
            let points = crate::list_registered_points();
            buggify_hits
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(points);
        }));
    }
    for handle in handles {
        let _ = handle.join();
    }

    let mut outcomes = outcomes
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    outcomes.sort_by_key(|outcome| outcome.seed);

    let failures_dir = options.failures_dir.clone().or_else(|| {
        options.out_path.as_ref().map(|path| {
            let mut dir = path.as_os_str().to_owned();
            dir.push(".failures");
            std::path::PathBuf::from(dir)
        })
    });
    let mut writer = options.out_path.as_ref().and_then(|path| {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .ok()
            .map(std::io::BufWriter::new)
    });
    for outcome in &outcomes {
        if let Some(writer) = writer.as_mut() {
            use std::io::Write as _;
            let _ = writeln!(writer, "{}", outcome_json(profile, outcome));
            let _ = writer.flush();
        }
        if !outcome.ok
            && let Some(dir) = &failures_dir
        {
            let plan = regenerated_plan(profile, outcome.seed);
            let failure = outcome.failure.as_deref().unwrap_or("unknown failure");
            save_failure_artifact(dir, outcome.seed, &plan, failure);
        }
    }

    let buggify = buggify_hits
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    let summary = summarize(&outcomes, &buggify, started.elapsed());
    if let Some(writer) = writer.as_mut() {
        use std::io::Write as _;
        let _ = writeln!(writer, "{}", summary_json(profile, &summary));
        let _ = writer.flush();
    }
    (outcomes, summary)
}

/// Regenerates a seed's plan for failure artifacts and fault tallies.
/// Generation is deterministic, so this is the exact plan that ran.
fn regenerated_plan(profile: FarmProfile, seed: u64) -> Plan {
    if profile.is_kv() {
        let swarm = crate::workload::swarm_profile(seed);
        crate::plan::Plan::generate_for_workload(
            seed,
            profile.storage_faults(),
            crate::plan::WorkloadSpec {
                clients: swarm.clients,
                keys: swarm.keys,
                ops_per_client: swarm.ops_per_client,
                list_pct: swarm.list_pct,
                think_ticks: swarm.think_ticks,
                timeout_ticks: swarm.timeout_ticks,
                client_crashes: swarm.client_crashes,
                unordered: swarm.unordered,
            },
        )
    } else {
        Plan::generate(seed, profile.storage_faults())
    }
}

fn summarize(
    outcomes: &[SeedOutcome],
    buggify: &[crate::BuggifyPoint],
    duration: Duration,
) -> FarmSummary {
    let total_seeds = outcomes.len() as u64;
    let passed_seeds = outcomes.iter().filter(|o| o.ok).count() as u64;
    let failed_seeds: Vec<u64> = outcomes.iter().filter(|o| !o.ok).map(|o| o.seed).collect();
    let virtual_ticks: u64 = outcomes.iter().map(|o| o.virtual_ticks).sum();
    let invariant_checks: u64 = outcomes.iter().map(|o| o.invariant_checks).sum();
    let mut faults: BTreeMap<String, u64> = BTreeMap::new();
    for outcome in outcomes {
        for (class, count) in &outcome.fault_counts {
            *faults.entry(class.clone()).or_insert(0) += count;
        }
    }
    let mut buggify_merged: BTreeMap<(String, String), (u64, u64)> = BTreeMap::new();
    for point in buggify {
        let entry = buggify_merged
            .entry((point.name.clone(), point.location.clone()))
            .or_insert((0, 0));
        entry.0 = entry.0.saturating_add(point.evaluations);
        entry.1 = entry.1.saturating_add(point.activations);
    }
    let mut sometimes: BTreeMap<String, u64> = BTreeMap::new();
    for outcome in outcomes {
        for (name, hits) in &outcome.sometimes {
            *sometimes.entry(name.clone()).or_insert(0) += hits;
        }
    }
    FarmSummary {
        total_seeds,
        passed_seeds,
        failed_seeds,
        duration,
        simulated_hours: virtual_ticks as f64 / 3_600_000.0,
        faults_by_class: faults.into_iter().collect(),
        buggify: buggify_merged
            .into_iter()
            .map(|((name, location), (evals, hits))| (name, location, evals, hits))
            .collect(),
        invariant_checks,
        sometimes: sometimes.into_iter().collect(),
    }
}
