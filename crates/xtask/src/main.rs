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
    },
    /// Run performance and instruction-count benchmarks
    Bench,
    /// Run cargo-fuzz harness targets
    Fuzz {
        #[arg(long, default_value_t = 60)]
        duration_secs: u64,
    },
    /// Run TLC model checker on formal specifications
    Tlc,
    /// Build and verify documentation
    Docs,
    /// Validate release readiness, packaging, and reproducibility
    Release,
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
        Commands::Farm { seeds } => {
            println!("=== ANVIL Simulator Seed Farm ===");
            let threads =
                std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
            println!("Evaluating {seeds} seeds across {threads} parallel worker threads...");
            let res = anvil_sim::run_seed_farm(seeds, threads);
            println!(
                "Farm Result: {}/{} passed in {:.2?} ({:.1} seeds/sec)",
                res.passed_seeds,
                res.total_seeds,
                res.duration,
                res.seeds_per_second()
            );
            if let Some(f_seed) = res.failed_seed {
                eprintln!("FARM FAILURE: Seed {f_seed} produced an invariant violation!");
                ExitCode::FAILURE
            } else {
                println!(
                    "FARM PASSED: All {seeds} seeds completed with zero invariant violations."
                );
                ExitCode::SUCCESS
            }
        }
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
                Ok(s) if s.success() => ExitCode::SUCCESS,
                _ => ExitCode::FAILURE,
            }
        }
        Commands::Fuzz { .. } => {
            eprintln!("ERROR: `xtask fuzz` is not yet wired. Failing loudly per charter Rule 5.");
            ExitCode::FAILURE
        }
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
