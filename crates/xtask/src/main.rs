#![forbid(unsafe_code)]

//! `xtask`: Developer automation and quality gates for ANVIL.

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
        #[arg(long, default_value_t = false)]
        negative_test: bool,
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
    /// Build and verify documentation
    Docs,
    /// Validate release readiness, packaging, and reproducibility
    Release,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Commands::Ci => {
            eprintln!(
                "ERROR: `xtask ci` is not yet fully wired. Failing loudly per charter Rule 5."
            );
            ExitCode::FAILURE
        }
        Commands::Determinism { .. } => {
            eprintln!(
                "ERROR: `xtask determinism` is not yet wired. Failing loudly per charter Rule 5."
            );
            ExitCode::FAILURE
        }
        Commands::Farm { .. } => {
            eprintln!("ERROR: `xtask farm` is not yet wired. Failing loudly per charter Rule 5.");
            ExitCode::FAILURE
        }
        Commands::Bench => {
            eprintln!("ERROR: `xtask bench` is not yet wired. Failing loudly per charter Rule 5.");
            ExitCode::FAILURE
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
