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
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Commands::Farm(args) => run_farm(args),
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
    if outcomes.iter().any(|outcome| !outcome.ok) {
        std::process::exit(1);
    }
}
