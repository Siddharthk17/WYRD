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
        /// Run the Week 4 KV workload farm (workload plus checker plus audit)
        #[arg(long, default_value_t = false)]
        kv: bool,
    },
    /// Run performance and instruction-count benchmarks
    Bench,
    /// Run the hand-authored mutant kill matrix (`ID` is M01..M20 or `all`)
    Mutant {
        /// Mutant id, or `all` for the full matrix
        id: String,
    },
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
    /// Replay a plan encoding or plan file, shrinking it on failure.
    /// Accepts a literal `anvil-plan/...` encoding, a path to a file
    /// holding one, or a path to a JSON failure report with a "plan" field
    /// (the farm's `seed_N.json` shape), so `cargo xtask repro plan.json`
    /// works from a clean checkout.
    Repro {
        /// Plan encoding or path to a plan file.
        plan: String,
    },
    /// Build and verify documentation
    Docs,
    /// Validate release readiness, packaging, and reproducibility
    Release,
}

fn run_farm_command(seeds: usize, storage_faults: bool, kv: bool) -> ExitCode {
    if kv {
        println!("ANVIL KV Seed Farm (workload plus checker plus audit)");
    } else if storage_faults {
        println!("ANVIL Simulator Seed Farm (storage faults on)");
    } else {
        println!("ANVIL Simulator Seed Farm");
    }
    let threads = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    println!("Evaluating {seeds} seeds across {threads} parallel worker threads...");
    let farm = if kv {
        anvil_sim::run_kv_farm(seeds, threads, storage_faults)
    } else if storage_faults {
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

fn run_repro_command(arg: &str) -> ExitCode {
    let (plan, source) = match load_repro_plan(arg) {
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::FAILURE;
        }
        Ok(loaded) => loaded,
    };
    // Workload plans replay through the KV runner, consensus plans through
    // the consensus runner; either way the classifier names the failure and
    // the shrinker must preserve it.
    let runner = |candidate: &anvil_sim::Plan| {
        if candidate.workload.is_some() {
            anvil_sim::kv_plan_failure_class(candidate, false)
        } else {
            anvil_sim::consensus_failure_class(candidate)
        }
    };
    match runner(&plan) {
        None => {
            println!("REPRO PASSED: plan completed with zero invariant violations.");
            ExitCode::SUCCESS
        }
        Some(class) => {
            let shrunk = anvil_sim::shrink_plan_same_class(&plan, &runner);
            eprintln!("REPRO FAILED: {class}");
            eprintln!(
                "Shrunk plan ({} events): {}",
                shrunk.events.len(),
                shrunk.encode()
            );
            if let Some(path) = source {
                let minimal = format!("{path}.minimal.plan");
                match std::fs::write(&minimal, shrunk.encode()) {
                    Ok(()) => eprintln!("cargo xtask repro {minimal}"),
                    Err(err) => eprintln!("repro note: cannot write {minimal}: {err}"),
                }
            }
            ExitCode::FAILURE
        }
    }
}

/// Loads a plan from a literal encoding or from a file. Returns the plan
/// and, when the input was a file path, that path for artifact output.
fn load_repro_plan(arg: &str) -> Result<(anvil_sim::Plan, Option<String>), String> {
    let trimmed = arg.trim();
    let (content, source) = if std::path::Path::new(trimmed).exists() {
        let text = std::fs::read_to_string(trimmed)
            .map_err(|err| format!("Repro error: cannot read {trimmed}: {err}"))?;
        (text, Some(trimmed.to_string()))
    } else {
        (trimmed.to_string(), None)
    };
    let content = content.trim();
    let encoding = if content.starts_with('{') {
        extract_plan_field(content)?
    } else {
        content.to_string()
    };
    anvil_sim::Plan::decode(&encoding)
        .map_err(|err| format!("Repro error: invalid plan encoding: {err}"))
        .map(|plan| (plan, source))
}

/// Extracts the `"plan"` string field from a JSON failure report.
fn extract_plan_field(json: &str) -> Result<String, String> {
    let key = json
        .find("\"plan\"")
        .ok_or_else(|| String::from("Repro error: JSON report has no \"plan\" field"))?;
    let rest = &json[key + 6..];
    let colon = rest
        .find(':')
        .ok_or_else(|| String::from("Repro error: malformed \"plan\" field"))?;
    let mut chars = rest[colon + 1..].trim_start().chars();
    if chars.next() != Some('"') {
        return Err(String::from("Repro error: malformed \"plan\" field"));
    }
    let mut out = String::new();
    let mut escaped = false;
    for ch in chars {
        if escaped {
            match ch {
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'u' => return Err(String::from("Repro error: unsupported unicode escape")),
                _ => return Err(String::from("Repro error: bad string escape")),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == '"' {
            return Ok(out);
        } else {
            out.push(ch);
        }
    }
    Err(String::from("Repro error: unterminated \"plan\" string"))
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
            kv,
        } => run_farm_command(seeds, storage_faults, kv),
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
        Commands::Mutant { id } => match xtask::mutant::run_mutant_matrix(
            &std::env::current_dir().unwrap_or_default(),
            &id,
        ) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("Mutant gate error: {err}");
                ExitCode::FAILURE
            }
        },
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod repro_tests {
    use super::{extract_plan_field, load_repro_plan, run_repro_command};
    use std::process::ExitCode;

    #[test]
    fn test_extract_plan_field_reads_report_json() {
        let encoding = anvil_sim::Plan::generate(7, false).encode();
        let json = format!("{{\"seed\":7,\"failure\":\"boom\",\"plan\":\"{encoding}\"}}");
        assert_eq!(extract_plan_field(&json).expect("plan field"), encoding);
    }

    #[test]
    fn test_extract_plan_field_rejects_json_without_plan() {
        assert!(extract_plan_field("{\"seed\":7}").is_err());
        assert!(extract_plan_field("{\"plan\":7}").is_err());
        assert!(extract_plan_field("{\"plan\":\"unterminated").is_err());
    }

    #[test]
    fn test_repro_passes_on_quiet_plan() {
        let encoding = anvil_sim::Plan::generate(4242, false).encode();
        assert_eq!(run_repro_command(&encoding), ExitCode::SUCCESS);
    }

    #[test]
    fn test_repro_reads_plan_file() {
        let dir = std::env::temp_dir().join("anvil-repro-file-test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("seed_4242.plan");
        let encoding = anvil_sim::Plan::generate(4242, false).encode();
        std::fs::write(&path, &encoding).expect("write plan file");
        let (plan, source) = load_repro_plan(&path.to_string_lossy()).expect("file loads");
        assert_eq!(plan.encode(), encoding);
        assert_eq!(source, Some(path.to_string_lossy().into_owned()));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_repro_reads_json_report() {
        let encoding = anvil_sim::Plan::generate(4242, false).encode();
        let dir = std::env::temp_dir().join("anvil-repro-json-test");
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("seed_4242.json");
        std::fs::write(&path, format!("{{\"seed\":4242,\"plan\":\"{encoding}\"}}"))
            .expect("write report");
        assert_eq!(
            run_repro_command(&path.to_string_lossy()),
            ExitCode::SUCCESS
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
