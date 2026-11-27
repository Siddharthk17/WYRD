#![forbid(unsafe_code)]

//! `anvil-sim` command line: parallel deterministic seed farms.
//!
//! `anvil-sim farm --seeds A..B --jobs N --profile NAME [--out results.jsonl]
//! [--resume] [--time-budget SECS]`

use anvil_sim::{FarmOptions, FarmProfile, FarmRange, run_farm_range};
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "anvil-sim", about = "ANVIL deterministic simulator")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Run a parallel deterministic seed farm.
    Farm(FarmArgs),
    /// Dump a human-readable text trace of one seed for debugging.
    Trace(TraceArgs),
}

#[derive(Args)]
struct FarmArgs {
    /// Seed range `A..B` (B exclusive) or bare count `N` meaning `0..N`.
    #[arg(long, default_value = "2000")]
    seeds: String,
    /// Parallel worker threads; defaults to available parallelism.
    #[arg(long)]
    jobs: Option<usize>,
    /// Farm profile: smoke, kv, or kv-storage.
    #[arg(long, default_value = "smoke")]
    profile: String,
    /// Append one JSON object per seed plus a final summary object here.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
    /// Skip seeds already recorded as passed in `out`.
    #[arg(long, default_value_t = false)]
    resume: bool,
    /// Stop taking new seeds after this many wall-clock seconds (nightly).
    #[arg(long)]
    time_budget_secs: Option<u64>,
    /// Fail when any registered sometimes-point is never hit. Use on runs
    /// of at least 100,000 seeds; small runs cannot reach every rare path.
    #[arg(long, default_value_t = false)]
    require_sometimes_coverage: bool,
}

/// Trace one seed: consensus and client events in tick order.
#[derive(Args)]
struct TraceArgs {
    /// Master seed to trace.
    #[arg(long)]
    seed: u64,
    /// Farm profile: smoke, kv, or kv-storage.
    #[arg(long, default_value = "smoke")]
    profile: String,
    /// Maximum lines to emit; older lines are kept, the tail truncated.
    #[arg(long, default_value_t = 2000)]
    limit: usize,
    /// Write the trace here instead of stdout.
    #[arg(long)]
    out: Option<std::path::PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Farm(args) => run_farm(args),
        Commands::Trace(args) => run_trace(args),
    }
}

fn run_farm(args: FarmArgs) {
    let range = FarmRange::parse(&args.seeds).unwrap_or_else(|err| {
        eprintln!("anvil-sim farm: {err}");
        std::process::exit(2);
    });
    if range.is_empty() {
        eprintln!("anvil-sim farm: empty seed range");
        std::process::exit(2);
    }
    let profile = FarmProfile::parse(&args.profile).unwrap_or_else(|err| {
        eprintln!("anvil-sim farm: {err}");
        std::process::exit(2);
    });
    let jobs = args.jobs.unwrap_or_else(|| {
        std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get)
    });
    let options = FarmOptions {
        jobs,
        out_path: args.out,
        resume: args.resume,
        time_budget_secs: args.time_budget_secs,
        failures_dir: None,
    };
    let (outcomes, summary) = run_farm_range(range, profile, &options);
    print!("{}", summary.render(profile));
    let mut failed = outcomes.iter().any(|outcome| !outcome.ok);
    if args.require_sometimes_coverage {
        let missed = summary.sometimes_missed();
        if !missed.is_empty() {
            eprintln!(
                "anvil-sim farm: sometimes coverage incomplete: {}",
                missed.join(", ")
            );
            failed = true;
        }
    }
    if failed {
        std::process::exit(1);
    }
}

fn run_trace(args: TraceArgs) {
    let profile = FarmProfile::parse(&args.profile).unwrap_or_else(|err| {
        eprintln!("anvil-sim trace: {err}");
        std::process::exit(2);
    });
    let mut lines = vec![format!(
        "anvil trace seed={} profile={}",
        args.seed,
        profile.name()
    )];
    let failed = match profile {
        FarmProfile::Smoke => {
            let plan = anvil_sim::Plan::generate(args.seed, false);
            lines.push(format!("plan: {}", plan.encode()));
            match std::panic::catch_unwind(|| anvil_sim::run_plan_traced(&plan, true)) {
                Ok((stats, trace)) => {
                    lines.extend(trace);
                    lines.push(format!(
                        "outcome: ok ticks={} invariant_checks={}",
                        stats.virtual_ticks, stats.invariant_checks
                    ));
                    false
                }
                Err(payload) => {
                    lines.push(format!("outcome: FAILED {}", panic_text(&payload)));
                    true
                }
            }
        }
        FarmProfile::Kv | FarmProfile::KvStorage => {
            let storage = profile.storage_faults();
            let swarm = anvil_sim::swarm_profile(args.seed);
            let workload = anvil_sim::WorkloadSpec {
                clients: swarm.clients,
                keys: swarm.keys,
                ops_per_client: swarm.ops_per_client,
                list_pct: swarm.list_pct,
                think_ticks: swarm.think_ticks,
                timeout_ticks: swarm.timeout_ticks,
                client_crashes: swarm.client_crashes,
                unordered: swarm.unordered,
            };
            let plan = anvil_sim::Plan::generate_for_workload(args.seed, storage, workload);
            lines.push(format!("plan: {}", plan.encode()));
            match std::panic::catch_unwind(|| anvil_sim::run_kv_plan_traced(&plan, false)) {
                Ok((history, audit, stats, trace)) => {
                    lines.extend(trace);
                    for op in &history.ops {
                        lines.push(format!(
                            "op id={} client={} key={:?} {:?} invoke={} ret={:?} indet={} result={:?}",
                            op.id,
                            op.client,
                            String::from_utf8_lossy(&op.key),
                            op.op,
                            op.invoke,
                            op.ret,
                            op.indeterminate,
                            op.result,
                        ));
                    }
                    let check = anvil_check::check_history(&history);
                    let check_ok = check.is_ok();
                    lines.push(format!(
                        "outcome: audit={audit} checker={} snapshots_taken={} installs={}",
                        if check_ok { "ok" } else { "VIOLATION" },
                        stats.snapshots_taken,
                        stats.installs,
                    ));
                    if let Err(failure) = check {
                        lines.push(format!("failing_ids={:?}", failure.failing_ids));
                    }
                    !(audit && check_ok)
                }
                Err(payload) => {
                    lines.push(format!("outcome: FAILED {}", panic_text(&payload)));
                    true
                }
            }
        }
    };
    if lines.len() > args.limit {
        lines.truncate(args.limit);
        lines.push(format!(
            "[truncated to {} lines; rerun with --limit N]",
            args.limit
        ));
    }
    let text = lines.join("\n") + "\n";
    if let Some(path) = args.out {
        if let Err(err) = std::fs::write(&path, &text) {
            eprintln!("anvil-sim trace: cannot write {}: {err}", path.display());
            std::process::exit(2);
        }
    } else {
        print!("{text}");
    }
    if failed {
        std::process::exit(1);
    }
}

fn panic_text(payload: &Box<dyn std::any::Any + Send>) -> String {
    // See `anvil_sim::panic_failure_class`: downcast through an explicitly
    // typed reborrow, never an implicitly coerced `&(dyn Any)` argument.
    let borrowed: &(dyn std::any::Any + Send) = payload.as_ref();
    if let Some(text) = borrowed.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else {
        String::from("non-string panic payload")
    }
}
