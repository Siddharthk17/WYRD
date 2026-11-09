#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::print_stdout
)]

//! TLC Model Checker automation for ANVIL formal specifications.
//!
//! Validates:
//! 1. `specs/anvil_raft.tla` under the smoke config `specs/anvil_raft_smoke.cfg`
//!    via seeded random simulation (no invariant violations on any trace).
//! 2. `specs/anvil_raft.tla` under `specs/anvil_raft_negative_smoke.cfg`
//!    via seeded random simulation (invariant violation found, confirming
//!    the Figure 8 counterexample).
//!
//! Exhaustive model checking of the reference 3-server, 3-term, 3-entry
//! bounds exceeds small machines (measured: 300M+ states, disk exhaustion),
//! so the per-change gate simulates bounded traces and the reference bounds
//! run via `cargo xtask tlc-full` on larger hosts. Simulation uses fixed
//! seeds, so the gate is deterministic for a given spec and tool version.

use crate::determinism::find_workspace_root;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// Locates `tla2tools.jar` from environment variables, project directories, or system paths.
pub fn find_tla2tools_jar(workspace_root: &Path) -> Option<PathBuf> {
    if let Ok(path_str) = std::env::var("TLA2TOOLS_JAR") {
        let p = PathBuf::from(path_str);
        if p.exists() {
            return Some(p);
        }
    }

    let candidates = [
        workspace_root.join("tools").join("tla2tools.jar"),
        workspace_root.join("specs").join("tla2tools.jar"),
        PathBuf::from("/usr/share/java/tla2tools.jar"),
        PathBuf::from("/usr/local/share/java/tla2tools.jar"),
        PathBuf::from("/opt/tla/tla2tools.jar"),
    ];

    for c in &candidates {
        if c.exists() {
            return Some(c.clone());
        }
    }

    None
}

/// Runs the TLC model checking gate.
///
/// Checks the smoke configs by default. Set `full` to check the reference
/// 3-server, 3-term, 3-entry bounds instead (nightly only: the state space
/// exceeds small machines).
pub fn run_tlc_check() -> Result<(), String> {
    run_tlc_check_inner(false)
}

/// Runs the TLC model checking gate against the reference full-bound configs.
pub fn run_tlc_check_full() -> Result<(), String> {
    run_tlc_check_inner(true)
}

fn run_tlc_check_inner(full: bool) -> Result<(), String> {
    let workspace_root = find_workspace_root();
    let spec_path = workspace_root.join("specs").join("anvil_raft.tla");
    let (cfg_name, neg_cfg_name) = if full {
        ("anvil_raft.cfg", "anvil_raft_negative.cfg")
    } else {
        ("anvil_raft_smoke.cfg", "anvil_raft_negative_smoke.cfg")
    };
    let cfg_path = workspace_root.join("specs").join(cfg_name);
    let neg_cfg_path = workspace_root.join("specs").join(neg_cfg_name);

    if !spec_path.exists() {
        return Err(format!("Spec file not found at {}", spec_path.display()));
    }
    if !cfg_path.exists() {
        return Err(format!("Config file not found at {}", cfg_path.display()));
    }
    if !neg_cfg_path.exists() {
        return Err(format!(
            "Negative config file not found at {}",
            neg_cfg_path.display()
        ));
    }

    // Verify Java runtime is available
    let java_check = Command::new("java").arg("-version").output();
    if java_check.is_err() {
        return Err(
            "Java runtime ('java') not found in PATH. TLC requires OpenJDK 11+.".to_string(),
        );
    }

    let Some(jar) = find_tla2tools_jar(&workspace_root) else {
        return Err(
            "TLC gate requires tla2tools.jar (tools/tla2tools.jar or TLA2TOOLS_JAR) and Java 11+. \
             Download from https://github.com/tlaplus/tlaplus/releases. \
             Gate not run, not passed."
                .to_string(),
        );
    };

    println!("Running TLC model checking on ANVIL Raft formal specification...");
    let start = Instant::now();

    if full {
        run_exhaustive(
            &jar,
            &workspace_root,
            &spec_path,
            &cfg_path,
            &neg_cfg_path,
            cfg_name,
            neg_cfg_name,
        )?;
    } else {
        run_simulation_gate(&jar, &workspace_root, &spec_path, &cfg_path, &neg_cfg_path)?;
    }
    println!(">>> TLC model check completed in {:.2?}", start.elapsed());

    Ok(())
}

/// Seeded simulation seeds and depths. Fixed so the gate is deterministic
/// for a given spec and tool version.
const SIM_SEEDS: [u64; 3] = [1, 7, 42];
const SIM_DEPTH: u64 = 500;

/// Runs one seeded simulation, returning TLC's combined output and exit status.
fn run_simulation(
    jar: &Path,
    workspace_root: &Path,
    spec_path: &Path,
    cfg_path: &Path,
    seed: u64,
) -> Result<(String, bool), String> {
    let jar_text = jar
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 tla2tools jar path: {}", jar.display()))?;
    let spec_text = spec_path
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 spec path: {}", spec_path.display()))?;
    let cfg_text = cfg_path
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 config path: {}", cfg_path.display()))?;
    let output = Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            jar_text,
            "tlc2.TLC",
            "-simulate",
            "-seed",
            &seed.to_string(),
            "-depth",
            &SIM_DEPTH.to_string(),
            "-config",
            cfg_text,
            spec_text,
        ])
        .current_dir(workspace_root)
        .output()
        .map_err(|e| format!("Failed to execute TLC simulation: {e}"))?;
    let success = output.status.success();
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    for stale in std::fs::read_dir(workspace_root.join("specs"))
        .map_err(|e| format!("Failed to list specs dir: {e}"))?
    {
        let stale = stale.map_err(|e| format!("Failed to read specs entry: {e}"))?;
        let name = stale.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("anvil_raft_TTrace_")
            && name.ends_with(".tla")
            && let Err(e) = std::fs::remove_file(stale.path())
        {
            return Err(format!("Failed to remove stale TLC trace: {e}"));
        }
    }
    Ok((combined, success))
}

fn simulation_failed(output: &str) -> bool {
    output.contains("Error: Invariant")
        || output.contains("is violated")
        || output.contains("TLC threw an unexpected exception")
}

fn run_simulation_gate(
    jar: &Path,
    workspace_root: &Path,
    spec_path: &Path,
    cfg_path: &Path,
    neg_cfg_path: &Path,
) -> Result<(), String> {
    println!("\n>>> 1. Simulating positive model (3 seeds x depth {SIM_DEPTH})...");
    for seed in SIM_SEEDS {
        let (output, success) = run_simulation(jar, workspace_root, spec_path, cfg_path, seed)?;
        if !success || simulation_failed(&output) {
            return Err(format!(
                "TLC positive simulation FAILED on seed {seed}!\nOUTPUT:\n{output}"
            ));
        }
    }
    println!(">>> [PASS] Positive simulation: no invariant violations on any trace.");

    println!("\n>>> 2. Simulating negative model counterexample...");
    let mut violation = None;
    for seed in SIM_SEEDS {
        let (output, success) = run_simulation(jar, workspace_root, spec_path, neg_cfg_path, seed)?;
        if !success && (output.contains("Error: Invariant") || output.contains("is violated")) {
            violation = Some(seed);
            break;
        }
        if success
            && simulation_failed(&output)
            && (output.contains("Error: Invariant") || output.contains("is violated"))
        {
            violation = Some(seed);
            break;
        }
    }
    match violation {
        Some(seed) => {
            println!(
                ">>> [PASS] Negative simulation: Figure 8 counterexample detected (seed {seed})."
            );
            Ok(())
        }
        None => Err(
            "TLC negative simulation failed: expected invariant violation (Figure 8) \
             on every seed, but TLC reported none. The counterexample may need deeper traces."
                .to_string(),
        ),
    }
}

fn run_exhaustive(
    jar: &Path,
    workspace_root: &Path,
    spec_path: &Path,
    cfg_path: &Path,
    neg_cfg_path: &Path,
    cfg_name: &str,
    neg_cfg_name: &str,
) -> Result<(), String> {
    let jar_text = jar
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 tla2tools jar path: {}", jar.display()))?;
    let cfg_text = cfg_path
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 config path: {}", cfg_path.display()))?;
    let spec_text = spec_path
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 spec path: {}", spec_path.display()))?;
    let neg_cfg_text = neg_cfg_path
        .to_str()
        .ok_or_else(|| format!("Non-UTF8 negative config path: {}", neg_cfg_path.display()))?;
    println!("\n>>> 1. Verifying positive model (specs/{cfg_name})...");
    let positive_output = Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            jar_text,
            "tlc2.TLC",
            "-workers",
            "auto",
            "-deadlock",
            "-cleanup",
            "-config",
            cfg_text,
            spec_text,
        ])
        .current_dir(workspace_root)
        .output()
        .map_err(|e| format!("Failed to execute TLC: {e}"))?;

    let positive_stdout = String::from_utf8_lossy(&positive_output.stdout);
    let positive_stderr = String::from_utf8_lossy(&positive_output.stderr);

    if !positive_output.status.success()
        || positive_stdout.contains("Error:")
        || positive_stdout.contains("violated")
    {
        return Err(format!(
            "TLC positive verification FAILED!\nSTDOUT:\n{positive_stdout}\nSTDERR:\n{positive_stderr}"
        ));
    }
    println!(">>> [PASS] Positive verification: all invariants held, 0 errors.");

    println!("\n>>> 2. Verifying negative model counterexample (specs/{neg_cfg_name})...");
    let negative_output = Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            jar_text,
            "tlc2.TLC",
            "-workers",
            "auto",
            "-deadlock",
            "-cleanup",
            "-config",
            neg_cfg_text,
            spec_text,
        ])
        .current_dir(workspace_root)
        .output()
        .map_err(|e| format!("Failed to execute negative TLC: {e}"))?;

    let negative_stdout = String::from_utf8_lossy(&negative_output.stdout);

    let violation_found =
        negative_stdout.contains("Error: Invariant") || negative_stdout.contains("is violated");
    if !violation_found {
        return Err(format!(
            "TLC negative verification failed: expected invariant violation (Figure 8), but TLC did not report an error.\nSTDOUT:\n{negative_stdout}"
        ));
    }
    println!(
        ">>> [PASS] Negative verification: Figure 8 counterexample successfully detected by TLC."
    );
    Ok(())
}
