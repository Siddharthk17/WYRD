#![forbid(unsafe_code)]

//! Determinism verification gate runner for ANVIL.
//!
//! Verifies that simulation executions are byte-for-byte identical:
//! 1. Across consecutive runs within the same process.
//! 2. Across separate fresh child processes under Address Space Layout Randomization (ASLR)
//!    and varying environment variables.
//! 3. Against checked-in golden trace digests in `tests/golden/seed_hashes.txt`.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// Fixed set of golden seeds exercising edge cases, powers of 2, and pseudorandom patterns.
pub const GOLDEN_SEEDS: [u64; 10] = [
    1,
    2,
    42,
    100,
    1337,
    2024,
    54321,
    999_999,
    0xDEAD_BEEF_CAFE_BABE,
    0xFEED_FACE_1234_5678,
];

/// Locates the root of the ANVIL Cargo workspace.
pub fn find_workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    if let Some(parent) = manifest_dir.parent().and_then(|p| p.parent())
        && parent.join("Cargo.toml").exists()
    {
        return parent.to_path_buf();
    }

    let mut current = std::env::current_dir().unwrap_or(manifest_dir);
    loop {
        if current.join("Cargo.toml").exists() && current.join("crates").exists() {
            return current;
        }
        if !current.pop() {
            break;
        }
    }
    PathBuf::from(".")
}

/// Loads expected trace hashes from `tests/golden/seed_hashes.txt`.
pub fn load_golden_hashes(workspace_root: &Path) -> Result<BTreeMap<u64, String>, String> {
    let golden_path = workspace_root.join("tests/golden/seed_hashes.txt");
    if !golden_path.exists() {
        return Err(format!(
            "Golden seed hashes file not found at: {}",
            golden_path.display()
        ));
    }
    let content = std::fs::read_to_string(&golden_path)
        .map_err(|e| format!("Failed to read {}: {e}", golden_path.display()))?;

    let mut map = BTreeMap::new();
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(format!(
                "Malformed line in {}: '{line}'",
                golden_path.display()
            ));
        }
        let seed = parts[0].parse::<u64>().map_err(|e| {
            format!(
                "Invalid seed '{}' in {}: {e}",
                parts[0],
                golden_path.display()
            )
        })?;
        let hash = parts[1].to_string();
        map.insert(seed, hash);
    }
    Ok(map)
}

/// Resolves the path to the xtask executable, compiling it if necessary.
fn resolve_xtask_binary(workspace_root: &Path) -> Result<PathBuf, String> {
    let debug_bin = workspace_root.join("target/debug/xtask");
    if debug_bin.exists() {
        return Ok(debug_bin);
    }
    let release_bin = workspace_root.join("target/release/xtask");
    if release_bin.exists() {
        return Ok(release_bin);
    }

    // Build the xtask binary once
    let status = std::process::Command::new("cargo")
        .current_dir(workspace_root)
        .args(["build", "-p", "xtask"])
        .status()
        .map_err(|e| format!("Failed to invoke cargo build -p xtask: {e}"))?;

    if !status.success() {
        return Err("cargo build -p xtask failed".to_string());
    }

    if debug_bin.exists() {
        Ok(debug_bin)
    } else {
        Err(format!(
            "Compiled xtask binary not found at {}",
            debug_bin.display()
        ))
    }
}

/// Spawns a fresh child process with ASLR and modified environment variables.
fn run_in_child_process(
    workspace_root: &Path,
    seed: u64,
    negative_test: bool,
) -> Result<String, String> {
    let exe = resolve_xtask_binary(workspace_root)?;

    let mut cmd = std::process::Command::new(exe);
    cmd.current_dir(workspace_root);
    cmd.arg("determinism");
    cmd.arg("--worker-seed");
    cmd.arg(seed.to_string());
    if negative_test {
        cmd.arg("--negative-test");
    }

    // Pass perturbed environment variables to verify zero external entropy leakage
    cmd.env("ANVIL_DETERMINISM_SALT", format!("salt_proc_{seed}"));
    cmd.env("RUST_LOG", "debug");
    cmd.env("ANVIL_CHILD_PROC", "1");

    let output = cmd
        .output()
        .map_err(|e| format!("Failed to execute child process: {e}"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("Child process failed for seed {seed}: {stderr}"));
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let hash = stdout.trim();
    if hash.len() != 64 {
        return Err(format!("Invalid hash from child process: '{hash}'"));
    }

    Ok(hash.to_string())
}

/// Executes worker child process mode for a single seed and outputs the BLAKE3 trace digest.
pub fn run_worker(seed: u64, negative_test: bool) {
    let result = if negative_test {
        anvil_sim::run_scenario_with_hashmap(seed)
    } else {
        anvil_sim::run_golden_scenario(seed)
    };
    match result {
        Ok((_, hash)) => println!("{}", hash.to_hex()),
        Err(err) => {
            eprintln!("Scenario failed for seed {seed}: {err}");
            std::process::exit(1);
        }
    }
}

fn update_golden_hashes_file(golden_path: &Path) -> Result<(), String> {
    use std::fmt::Write;
    println!(
        "Generating golden hashes for {} seeds...",
        GOLDEN_SEEDS.len()
    );
    let mut out = String::from("# ANVIL Golden Seed Hashes\n# Format: <seed> <blake3_hash>\n");
    for &seed in &GOLDEN_SEEDS {
        let (_, hash) = anvil_sim::run_golden_scenario(seed)
            .map_err(|e| format!("Failed scenario for seed {seed}: {e}"))?;
        let _ = writeln!(out, "{seed} {}", hash.to_hex());
    }
    std::fs::write(golden_path, out)
        .map_err(|e| format!("Failed to write {}: {e}", golden_path.display()))?;
    println!("Updated golden hashes at {}", golden_path.display());
    Ok(())
}

fn verify_single_seed(
    workspace_root: &Path,
    seed: u64,
    expected_hash: &str,
    negative_test: bool,
) -> Result<String, String> {
    // Run 1 in-process
    let res1 = if negative_test {
        anvil_sim::run_scenario_with_hashmap(seed)
    } else {
        anvil_sim::run_golden_scenario(seed)
    };
    let (_, hash1) = res1.map_err(|e| format!("In-process run 1 failed: {e}"))?;
    let h1_str = hash1.to_hex().to_string();

    // Run 2 in-process
    let res2 = if negative_test {
        anvil_sim::run_scenario_with_hashmap(seed)
    } else {
        anvil_sim::run_golden_scenario(seed)
    };
    let (_, hash2) = res2.map_err(|e| format!("In-process run 2 failed: {e}"))?;
    let h2_str = hash2.to_hex().to_string();

    if h1_str != h2_str {
        return Err(format!("In-process run 1 ({h1_str}) != run 2 ({h2_str})"));
    }

    // Run 3 in fresh child process with ASLR and modified env
    let h3_str = run_in_child_process(workspace_root, seed, negative_test)?;
    if h1_str != h3_str {
        return Err(format!(
            "Parent process ({h1_str}) != child process ({h3_str})"
        ));
    }

    if h1_str != expected_hash {
        return Err(format!(
            "Produced hash ({h1_str}) != golden expected ({expected_hash})"
        ));
    }

    Ok(h1_str)
}

/// Runs the complete determinism gate against all golden seeds.
pub fn run_determinism_gate(negative_test: bool, update_golden: bool) -> Result<(), String> {
    let workspace_root = find_workspace_root();
    let golden_path = workspace_root.join("tests/golden/seed_hashes.txt");

    if update_golden {
        return update_golden_hashes_file(&golden_path);
    }

    let golden_hashes = load_golden_hashes(&workspace_root)?;
    println!("=== ANVIL Determinism Gate ===");
    println!(
        "Evaluating {} golden seeds (negative_test={})...",
        golden_hashes.len(),
        negative_test
    );

    let mut all_passed = true;
    let mut divergences = Vec::new();

    for (&seed, expected_hash) in &golden_hashes {
        match verify_single_seed(&workspace_root, seed, expected_hash, negative_test) {
            Ok(h1_str) => println!("  Seed {seed:<20} -> {h1_str} [PASS]"),
            Err(e) => {
                divergences.push(format!("Seed {seed}: {e}"));
                all_passed = false;
            }
        }
    }

    if negative_test {
        if !all_passed {
            println!(
                "Negative test detected divergence as expected ({} divergences observed).",
                divergences.len()
            );
            return Err("Divergence detected in negative test fixture".to_string());
        }
        return Err(
            "NEGATIVE TEST FAILED: HashMap iteration unexpectedly produced identical hashes!"
                .to_string(),
        );
    }

    if !all_passed {
        eprintln!("\nDETERMINISM GATE FAILED! Divergences found:");
        for div in &divergences {
            eprintln!("  - {div}");
        }
        return Err(format!(
            "Determinism gate failed with {} divergence(s)",
            divergences.len()
        ));
    }

    println!(
        "\nALL {} SEEDS PASSED: in-process runs, out-of-process runs, and golden hashes match.",
        golden_hashes.len()
    );
    Ok(())
}
