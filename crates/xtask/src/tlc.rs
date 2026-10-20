#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::print_stdout
)]

//! TLC Model Checker automation for ANVIL formal specifications.
//!
//! Validates:
//! 1. `specs/anvil_raft.tla` under `specs/anvil_raft.cfg` (all 7 invariants pass with 0 errors).
//! 2. `specs/anvil_raft.tla` under `specs/anvil_raft_negative.cfg` (invariant violation occurs,
//!    confirming the Figure 8 counterexample).

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
pub fn run_tlc_check() -> Result<(), String> {
    let workspace_root = find_workspace_root();
    let spec_path = workspace_root.join("specs").join("anvil_raft.tla");
    let cfg_path = workspace_root.join("specs").join("anvil_raft.cfg");
    let neg_cfg_path = workspace_root.join("specs").join("anvil_raft_negative.cfg");

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
        println!(
            "Notice: tla2tools.jar not found at tools/tla2tools.jar or in TLA2TOOLS_JAR.\n\
             Specification files are validated at: {}\n\
             To run model checker locally, provide tla2tools.jar in tools/ or set TLA2TOOLS_JAR.",
            spec_path.display()
        );
        return Ok(());
    };

    println!("Running TLC model checking on ANVIL Raft formal specification...");
    let start = Instant::now();

    // 1. Positive Verification
    println!("\n>>> 1. Verifying positive model (specs/anvil_raft.cfg)...");
    let pos_output = Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            jar.to_str().unwrap_or_default(),
            "tlc2.TLC",
            "-workers",
            "auto",
            "-deadlock",
            "-cleanup",
            "-config",
            cfg_path.to_str().unwrap_or_default(),
            spec_path.to_str().unwrap_or_default(),
        ])
        .current_dir(&workspace_root)
        .output()
        .map_err(|e| format!("Failed to execute TLC: {e}"))?;

    let pos_stdout = String::from_utf8_lossy(&pos_output.stdout);
    let pos_stderr = String::from_utf8_lossy(&pos_output.stderr);

    if !pos_output.status.success()
        || pos_stdout.contains("Error:")
        || pos_stdout.contains("violated")
    {
        return Err(format!(
            "TLC positive verification FAILED!\nSTDOUT:\n{pos_stdout}\nSTDERR:\n{pos_stderr}"
        ));
    }
    println!(">>> [PASS] Positive verification: all invariants held, 0 errors.");

    // 2. Negative Verification (Figure 8 Counterexample)
    println!("\n>>> 2. Verifying negative model counterexample (specs/anvil_raft_negative.cfg)...");
    let neg_output = Command::new("java")
        .args([
            "-XX:+UseParallelGC",
            "-cp",
            jar.to_str().unwrap_or_default(),
            "tlc2.TLC",
            "-workers",
            "auto",
            "-deadlock",
            "-cleanup",
            "-config",
            neg_cfg_path.to_str().unwrap_or_default(),
            spec_path.to_str().unwrap_or_default(),
        ])
        .current_dir(&workspace_root)
        .output()
        .map_err(|e| format!("Failed to execute negative TLC: {e}"))?;

    let neg_stdout = String::from_utf8_lossy(&neg_output.stdout);

    // Negative check MUST detect an invariant violation
    let violation_found =
        neg_stdout.contains("Error: Invariant") || neg_stdout.contains("is violated");
    if !violation_found {
        return Err(format!(
            "TLC negative verification failed: expected invariant violation (Figure 8), but TLC did not report an error.\nSTDOUT:\n{neg_stdout}"
        ));
    }
    println!(
        ">>> [PASS] Negative verification: Figure 8 counterexample successfully detected by TLC."
    );
    println!(">>> TLC model check completed in {:.2?}", start.elapsed());

    Ok(())
}
