#![forbid(unsafe_code)]

//! `xtask`: Developer automation and quality gates for ANVIL.

use xtask::ci;
use xtask::determinism;

use clap::{Parser, Subcommand};
use std::process::ExitCode;

#[derive(Parser)]
#[command(name = "xtask", about = "ANVIL automation and CI gate runner")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Run all continuous integration gates
    Ci,
    /// Run determinism verification gate against golden seeds
    Determinism {
        /// Run in negative test mode using non-deterministic `HashMap` iteration fixture
        #[arg(long, default_value_t = false)]
        negative_test: bool,
        /// Internal worker mode for fresh process evaluation
        #[arg(long)]
        worker_seed: Option<u64>,
        /// Update golden seeds file with current hashes
        #[arg(long, default_value_t = false)]
        update_golden: bool,
    },
    /// Run simulator seed farm
    Farm {
        #[arg(long, default_value_t = 2000)]
        seeds: usize,
        /// Enable storage fault injection (bit rot, torn writes, fsync failures) on every node
        #[arg(long, default_value_t = false)]
        storage_faults: bool,
    },
    /// Run performance and instruction-count benchmarks
    Bench,
    /// Run cargo-fuzz harness targets
    Fuzz {
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
    },
    /// Run TLC model checker on formal specifications (smoke bounds)
    Tlc,
    /// Run TLC model checker on the full reference bounds (nightly only)
    TlcFull,
    /// Print the materialized fault plan for a seed
    Plan {
        /// Master seed the plan is generated from
        #[arg(long)]
        seed: u64,
        /// Enable storage fault injection in the plan
        #[arg(long, default_value_t = false)]
        storage_faults: bool,
    },
    /// Replay a plan encoding, shrinking it on failure
    Repro {
        /// Plan encoding as printed by `xtask plan`
        plan: String,
    },
    /// Build and verify documentation
    Docs,
    /// Validate release readiness, packaging, and reproducibility
    Release,
}

fn run_farm_command(seeds: usize, storage_faults: bool) -> ExitCode {
    if storage_faults {
        println!("ANVIL Simulator Seed Farm (storage faults on)");
    } else {
        println!("ANVIL Simulator Seed Farm");
    }
    let threads = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    println!("Evaluating {seeds} seeds across {threads} parallel worker threads...");
    let farm = if storage_faults {
        anvil_sim::run_seed_farm_with_storage_faults(seeds, threads)
    } else {
        anvil_sim::run_seed_farm(seeds, threads)
    };
    println!(
        "Farm Result: {}/{} passed in {:.2?} ({:.1} seeds/sec)",
        farm.passed_seeds,
        farm.total_seeds,
        farm.duration,
        farm.seeds_per_second()
    );
    if let Some(failed_seed) = farm.failed_seed {
        eprintln!("FARM FAILURE: Seed {failed_seed} produced an invariant violation!");
        ExitCode::FAILURE
    } else {
        println!("FARM PASSED: All {seeds} seeds completed with zero invariant violations.");
        ExitCode::SUCCESS
    }
}

fn run_repro_command(plan: &str) -> ExitCode {
    let decoded = match anvil_sim::Plan::decode(plan) {
        Err(err) => {
            eprintln!("Repro error: invalid plan encoding: {err}");
            return ExitCode::FAILURE;
        }
        Ok(decoded) => decoded,
    };
    if std::panic::catch_unwind(|| anvil_sim::run_plan(&decoded)).is_ok() {
        println!("REPRO PASSED: plan completed with zero invariant violations.");
        ExitCode::SUCCESS
    } else {
        let shrunk = anvil_sim::shrink_plan(&decoded, &|candidate| {
            std::panic::catch_unwind(|| anvil_sim::run_plan(candidate)).is_err()
        });
        eprintln!("REPRO FAILED: plan reproduced the failure.");
        eprintln!("Shrunk plan: {}", shrunk.encode());
        ExitCode::FAILURE
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ci => match ci::run_ci() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("\nCI FAILED: {err}");
                ExitCode::FAILURE
            }
        },
        Commands::Determinism {
            negative_test,
            worker_seed,
            update_golden,
        } => {
            if let Some(seed) = worker_seed {
                determinism::run_worker(seed, negative_test);
                ExitCode::SUCCESS
            } else {
                match determinism::run_determinism_gate(negative_test, update_golden) {
                    Ok(()) => ExitCode::SUCCESS,
                    Err(err) => {
                        eprintln!("Gate error: {err}");
                        ExitCode::FAILURE
                    }
                }
            }
        }
        Commands::Tlc => match xtask::tlc::run_tlc_check() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("TLC gate error: {err}");
                ExitCode::FAILURE
            }
        },
        Commands::TlcFull => match xtask::tlc::run_tlc_check_full() {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("TLC full gate error: {err}");
                ExitCode::FAILURE
            }
        },
        Commands::Farm {
            seeds,
            storage_faults,
        } => run_farm_command(seeds, storage_faults),
        Commands::Bench => {
            let status = std::process::Command::new("cargo")
                .args([
                    "run",
                    "--release",
                    "-p",
                    "anvil-bench",
                    "--",
                    "--verify-budgets",
                ])
                .status();
            match status {
                Ok(finished) if finished.success() => ExitCode::SUCCESS,
                _ => ExitCode::FAILURE,
            }
        }
        Commands::Plan {
            seed,
            storage_faults,
        } => {
            let plan = anvil_sim::Plan::generate(seed, storage_faults);
            println!("{}", plan.encode());
            ExitCode::SUCCESS
        }
        Commands::Repro { plan } => run_repro_command(&plan),
        Commands::Fuzz { duration_secs } => match xtask::fuzz::run_fuzz(duration_secs) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("Fuzz gate error: {err}");
                ExitCode::FAILURE
            }
        },
        Commands::Docs => {
            eprintln!("ERROR: `xtask docs` is not yet wired. Failing loudly per charter Rule 5.");
            ExitCode::FAILURE
        }
        Commands::Release => {
            eprintln!(
                "ERROR: `xtask release` is not yet wired. Failing loudly per charter Rule 5."
            );
            ExitCode::FAILURE
        }
    }
}
