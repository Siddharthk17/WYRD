#![forbid(unsafe_code)]
#![allow(
    clippy::disallowed_types,
    clippy::disallowed_methods,
    clippy::print_stdout
)]

//! Continuous Integration (CI) gate runner for ANVIL.
//!
//! Enforces all charter quality gates:
//! 1. Format check (`cargo fmt --all -- --check`)
//! 2. Clippy with `-D warnings` (`cargo clippy --workspace --all-targets -- -D warnings`)
//! 3. Test suite (`cargo nextest run --workspace`)
//! 4. Doc tests (`cargo test --doc --workspace`)
//! 5. Documentation generation (`cargo doc --workspace --no-deps`)
//! 6. Zero-unsafe audit (verifies `#![forbid(unsafe_code)]` in all crates and 0 `unsafe` in production source)
//! 7. Determinism gate (`cargo xtask determinism`)
//! 8. Performance budgets gate (`cargo xtask bench` / `--verify-budgets`)
//! 9. TLC formal specification verification (`cargo xtask tlc`)
//! 10. Simulator smoke farm gate (`cargo xtask farm --seeds 2000`)
//! 11. Fuzz smoke gate (`cargo xtask fuzz --duration-secs 60`)
//! 12. Supply chain gates (`cargo deny check`, `cargo audit`)
//! 13. Unsafe audit (`cargo geiger`) and feature powerset (`cargo hack check`)
//! 14. Coverage floors (`cargo llvm-cov`)

use crate::determinism::{self, find_workspace_root};
use std::path::Path;
use std::process::Command;
use std::time::Instant;

/// Runs a command step and records elapsed time and success status.
fn run_step(name: &str, cmd: &mut Command) -> Result<(), String> {
    println!("\n>>> Running Gate: {name}...");
    let start = Instant::now();
    let status = cmd
        .status()
        .map_err(|e| format!("Failed to launch process for {name}: {e}"))?;
    let elapsed = start.elapsed();

    if status.success() {
        println!(">>> [PASS] {name} ({elapsed:.2?})");
        Ok(())
    } else {
        println!(">>> [FAIL] {name} ({elapsed:.2?})");
        Err(format!("Gate '{name}' failed with status {status}"))
    }
}

/// Verifies that all 14 workspace crates strictly enforce `#![forbid(unsafe_code)]`
/// and contain zero `unsafe` keyword occurrences in production `src/` trees.
fn audit_unsafe_code(workspace_root: &Path) -> Result<(), String> {
    println!("\n>>> Running Gate: Zero-Unsafe Code Audit...");
    let start = Instant::now();
    let crates_dir = workspace_root.join("crates");

    let entries = std::fs::read_dir(&crates_dir)
        .map_err(|e| format!("Failed to read crates directory: {e}"))?;

    let mut audited_crates = 0;
    for entry in entries {
        let entry = entry.map_err(|e| format!("Directory read error: {e}"))?;
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let crate_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        let src_dir = path.join("src");
        if !src_dir.exists() {
            continue;
        }

        // Verify root has #![forbid(unsafe_code)]
        let lib_rs = src_dir.join("lib.rs");
        let main_rs = src_dir.join("main.rs");
        let root_rs = if lib_rs.exists() {
            lib_rs
        } else if main_rs.exists() {
            main_rs
        } else {
            return Err(format!(
                "No lib.rs or main.rs found in {}",
                src_dir.display()
            ));
        };

        let root_content = std::fs::read_to_string(&root_rs)
            .map_err(|e| format!("Failed to read {}: {e}", root_rs.display()))?;

        if !root_content.contains("#![forbid(unsafe_code)]") {
            return Err(format!(
                "Crate {crate_name} root {} is missing `#![forbid(unsafe_code)]`",
                root_rs.display()
            ));
        }

        // Scan all .rs files in src/ for forbidden blocks. The needle is
        // assembled at runtime so this file does not match its own search.
        let needle_space = ["un", "safe "].concat();
        let needle_brace = ["un", "safe{"].concat();
        let allow_marker = ["forbid("].concat();
        for file in walk_rs_files(&src_dir)? {
            let content = std::fs::read_to_string(&file)
                .map_err(|e| format!("Failed to read {}: {e}", file.display()))?;
            for (line_no, line) in content.lines().enumerate() {
                let trimmed = line.trim_start();
                if trimmed.starts_with("//")
                    || trimmed.starts_with("/*")
                    || trimmed.starts_with('*')
                {
                    continue;
                }
                if line.contains(&needle_space) || line.contains(&needle_brace) {
                    if line.contains(&allow_marker) {
                        continue;
                    }
                    return Err(format!(
                        "Forbidden `unsafe` block detected in production code: {}:{}: {}",
                        file.display(),
                        line_no + 1,
                        line
                    ));
                }
            }
        }
        audited_crates += 1;
    }

    let elapsed = start.elapsed();
    println!(
        ">>> [PASS] Zero-Unsafe Code Audit ({audited_crates} crates audited, 0 `unsafe` blocks) ({elapsed:.2?})"
    );
    Ok(())
}

fn walk_rs_files(dir: &Path) -> Result<Vec<std::path::PathBuf>, String> {
    let mut files = Vec::new();
    if dir.is_dir() {
        for entry in std::fs::read_dir(dir).map_err(|e| format!("Read dir error: {e}"))? {
            let entry = entry.map_err(|e| format!("Dir entry error: {e}"))?;
            let path = entry.path();
            if path.is_dir() {
                files.extend(walk_rs_files(&path)?);
            } else if path.extension().and_then(|s| s.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    Ok(files)
}

/// Executes all CI quality gates sequentially.
pub fn run_ci() -> Result<(), String> {
    let workspace_root = find_workspace_root();
    println!("ANVIL Continuous Integration (CI)");

    // 1. Format check
    let mut fmt_cmd = Command::new("cargo");
    fmt_cmd.current_dir(&workspace_root);
    fmt_cmd.args(["fmt", "--all", "--", "--check"]);
    run_step("Format Check (`cargo fmt --check`)", &mut fmt_cmd)?;

    // 2. Clippy check
    let mut clippy_cmd = Command::new("cargo");
    clippy_cmd.current_dir(&workspace_root);
    clippy_cmd.args([
        "clippy",
        "--workspace",
        "--all-targets",
        "--",
        "-D",
        "warnings",
    ]);
    run_step("Clippy Lints (`cargo clippy -D warnings`)", &mut clippy_cmd)?;

    // 3. Workspace unit & integration tests via nextest
    let mut test_cmd = Command::new("cargo");
    test_cmd.current_dir(&workspace_root);
    test_cmd.args(["nextest", "run", "--workspace"]);
    run_step(
        "Workspace Tests (`cargo nextest run --workspace`)",
        &mut test_cmd,
    )?;

    // 4. Documentation tests
    let mut doc_test_cmd = Command::new("cargo");
    doc_test_cmd.current_dir(&workspace_root);
    doc_test_cmd.args(["test", "--doc", "--workspace"]);
    run_step(
        "Documentation Tests (`cargo test --doc`)",
        &mut doc_test_cmd,
    )?;

    // 5. Documentation generation
    let mut doc_cmd = Command::new("cargo");
    doc_cmd.current_dir(&workspace_root);
    doc_cmd.args(["doc", "--workspace", "--no-deps"]);
    run_step("Documentation Build (`cargo doc`)", &mut doc_cmd)?;

    // 6. Zero-Unsafe code audit
    audit_unsafe_code(&workspace_root)?;

    // 7. Determinism gate
    println!("\n>>> Running Gate: Determinism Verification...");
    let start_det = Instant::now();
    determinism::run_determinism_gate(false, false)?;
    println!(
        ">>> [PASS] Determinism Verification ({:.2?})",
        start_det.elapsed()
    );

    // 8. Performance and budget verification
    let mut bench_cmd = Command::new("cargo");
    bench_cmd.current_dir(&workspace_root);
    bench_cmd.args([
        "run",
        "--release",
        "-p",
        "anvil-bench",
        "--",
        "--verify-budgets",
    ]);
    run_step("Performance Budgets (`cargo xtask bench`)", &mut bench_cmd)?;

    // 9. TLC Spec Verification gate
    println!("\n>>> Running Gate: TLC Spec Verification (`cargo xtask tlc`)...");
    let start_tlc = Instant::now();
    crate::tlc::run_tlc_check()?;
    println!(
        ">>> [PASS] TLC Spec Verification ({:.2?})",
        start_tlc.elapsed()
    );

    // 10. Simulator smoke farm gate (2,000 seeds)
    println!("\n>>> Running Gate: Simulator Smoke Farm (2,000 seeds)...");
    let start_farm = Instant::now();
    let num_cpus = std::thread::available_parallelism().map_or(4, std::num::NonZeroUsize::get);
    let farm = anvil_sim::run_seed_farm(2000, num_cpus);
    if !farm.is_clean() {
        return Err(format!(
            "Simulator smoke farm failed! Failed seed: {:?}, passed: {}/{}",
            farm.failed_seed, farm.passed_seeds, farm.total_seeds
        ));
    }
    println!(
        ">>> [PASS] Simulator Smoke Farm (2000 seeds in {:.2?}, {:.0} seeds/sec)",
        start_farm.elapsed(),
        farm.seeds_per_second()
    );

    run_extended_gates(&workspace_root)?;

    println!("ALL CI QUALITY GATES PASSED!");
    Ok(())
}

/// Gates needing external tools: fuzz smoke, supply chain, unsafe scan,
/// feature powerset, and coverage floors.
fn run_extended_gates(workspace_root: &Path) -> Result<(), String> {
    // 11. Fuzz smoke gate (60 seconds per target)
    println!("\n>>> Running Gate: Fuzz Smoke (60s per target)...");
    let start_fuzz = Instant::now();
    crate::fuzz::run_fuzz(60)?;
    println!(
        ">>> [PASS] Fuzz Smoke (60s per target) ({:.2?})",
        start_fuzz.elapsed()
    );

    // 12. Supply chain gates (fail loudly when the tool is not installed)
    let mut deny_cmd = Command::new("cargo");
    deny_cmd.current_dir(workspace_root);
    deny_cmd.args(["deny", "check"]);
    run_step("Supply Chain (`cargo deny check`)", &mut deny_cmd)?;

    let mut audit_cmd = Command::new("cargo");
    audit_cmd.current_dir(workspace_root);
    audit_cmd.args(["audit"]);
    run_step("Advisories (`cargo audit`)", &mut audit_cmd)?;

    // 13. Zero-unsafe expansion and feature powerset. `cargo geiger
    // --forbid-only` marks each crate `:)` when every entry point declares
    // `#![forbid(unsafe_code)]`; the gate fails on any other mark.
    for member in [
        "anvil-types",
        "anvil-raft",
        "anvil-kv",
        "anvil-storage",
        "anvil-node",
        "anvil-check",
        "anvil-sim",
        "anvil-device-fs",
        "anvil-server",
        "anvil-client",
        "anvilctl",
        "anvil-viz",
        "anvil-bench",
        "xtask",
    ] {
        let manifest = workspace_root
            .join("crates")
            .join(member)
            .join("Cargo.toml");
        let probe = Command::new("cargo")
            .current_dir(workspace_root)
            .args([
                "geiger",
                "--manifest-path",
                manifest.to_str().unwrap_or_default(),
                "--forbid-only",
            ])
            .output()
            .map_err(|e| format!("Failed to launch geiger for {member}: {e}"))?;
        let report = String::from_utf8_lossy(&probe.stdout);
        let clean = report
            .lines()
            .any(|line| line.starts_with(":)") && line[2..].trim_start().starts_with(member));
        if !probe.status.success() || !clean {
            return Err(format!(
                "Unsafe Scan (`cargo geiger` {member}) failed: missing forbid marker"
            ));
        }
        println!(">>> [PASS] Unsafe Scan (`cargo geiger` {member})");
    }

    let mut hack_cmd = Command::new("cargo");
    hack_cmd.current_dir(workspace_root);
    hack_cmd.args(["hack", "check", "--feature-powerset", "--no-dev-deps"]);
    run_step("Feature Powerset (`cargo hack check`)", &mut hack_cmd)?;

    // 14. Coverage floors per crate, aggregated from one workspace run so
    // cross-crate exercise (for example sim tests driving the storage
    // engine) counts toward the owning crate.
    run_coverage_gate(workspace_root)?;

    println!("ALL CI QUALITY GATES PASSED!");
    Ok(())
}

/// Charter line-coverage floors per deterministic crate.
fn coverage_floors() -> Vec<(&'static str, f64)> {
    vec![
        ("anvil-types", 95.0),
        ("anvil-raft", 95.0),
        ("anvil-storage", 95.0),
        ("anvil-sim", 90.0),
    ]
}

#[allow(clippy::cast_precision_loss)]
fn coverage_percent(lines: u64, missed: u64) -> f64 {
    100.0 * (lines - missed) as f64 / lines as f64
}

/// Runs workspace coverage once, parses the per-file summary table, and
/// enforces each deterministic crate's floor over its `src/` files.
fn run_coverage_gate(workspace_root: &Path) -> Result<(), String> {
    println!("\n>>> Running Gate: Coverage (`cargo llvm-cov`)...");
    let start = Instant::now();
    let mut cmd = Command::new("cargo");
    cmd.current_dir(workspace_root);
    cmd.args(["llvm-cov", "nextest", "--workspace", "--summary-only"]);
    // Only forward explicit LLVM tool overrides. When unset, cargo-llvm-cov
    // resolves llvm-cov/llvm-profdata from the active toolchain sysroot,
    // which is where the CI toolchain step installs them. Defaulting to
    // bare names here would force a PATH lookup that fails on machines
    // without system LLVM installed.
    if let Ok(llvm_cov) = std::env::var("LLVM_COV") {
        cmd.env("LLVM_COV", llvm_cov);
    }
    if let Ok(llvm_profdata) = std::env::var("LLVM_PROFDATA") {
        cmd.env("LLVM_PROFDATA", llvm_profdata);
    }
    let probe = cmd
        .output()
        .map_err(|e| format!("Failed to launch coverage: {e}"))?;
    if !probe.status.success() {
        let stderr = String::from_utf8_lossy(&probe.stderr);
        let tail: Vec<&str> = stderr.lines().rev().take(20).collect();
        let tail = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
        return Err(format!(
            "Coverage run failed with status {}:\n{tail}",
            probe.status
        ));
    }
    let report = String::from_utf8_lossy(&probe.stdout);
    let totals = aggregate_coverage(&report)?;
    for (krate, floor) in coverage_floors() {
        let (lines, missed) = totals.get(krate).copied().unwrap_or((0, 0));
        if lines == 0 {
            return Err(format!("No coverage rows for crate {krate}"));
        }
        let covered = coverage_percent(lines, missed);
        if covered < floor {
            return Err(format!(
                "Coverage floor missed for {krate}: {covered:.2}% lines covered, floor {floor:.2}%"
            ));
        }
        println!(
            ">>> [PASS] Coverage {krate} ({covered:.2}% lines, floor {floor:.2}%) ({:.2?})",
            start.elapsed()
        );
    }
    Ok(())
}

/// Crates the coverage gate enforces floors for.
fn covered_crates() -> Vec<&'static str> {
    vec![
        "anvil-types",
        "anvil-raft",
        "anvil-storage",
        "anvil-sim",
        "anvil-node",
        "anvil-kv",
        "anvil-check",
        "anvil-device-fs",
    ]
}

/// Aggregates per-file line counts from an llvm-cov summary table, keyed by
/// workspace crate. Accepts absolute paths containing `/crates/<name>/src/`
/// as well as workspace-relative `<name>/src/` paths.
fn aggregate_coverage(
    report: &str,
) -> Result<std::collections::BTreeMap<String, (u64, u64)>, String> {
    let mut totals: std::collections::BTreeMap<String, (u64, u64)> =
        std::collections::BTreeMap::new();
    let mut rows = 0u32;
    for line in report.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let Some(path) = fields.first() else {
            continue;
        };
        let Some(krate) = covered_crates().into_iter().find(|krate| {
            path.contains(&format!("/crates/{krate}/src/"))
                || path.starts_with(&format!("{krate}/src/"))
        }) else {
            continue;
        };
        if fields.len() < 11 {
            return Err(format!("Unparseable coverage row: {line}"));
        }
        let lines: u64 = fields[7]
            .parse()
            .map_err(|_| format!("Unparseable coverage row: {line}"))?;
        let missed: u64 = fields[8]
            .parse()
            .map_err(|_| format!("Unparseable coverage row: {line}"))?;
        let entry = totals.entry(krate.to_string()).or_insert((0, 0));
        entry.0 += lines;
        entry.1 += missed;
        rows += 1;
    }
    if rows == 0 {
        return Err("Coverage report contained no per-file rows".to_string());
    }
    Ok(totals)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod coverage_tests {
    use super::{aggregate_coverage, coverage_percent};

    const FIXTURE: &str = "\
Filename Regions Missed Regions Cover Functions Missed Functions Executed Lines Missed Lines Cover Branches Missed Branches Cover
mnt/data2/WYRD/crates/anvil-types/src/codec.rs 10 1 90.00% 4 0 100.00% 100 10 90.00% 0 0 -
mnt/data2/WYRD/crates/anvil-raft/src/core.rs 20 2 90.00% 6 0 100.00% 200 10 95.00% 0 0 -
anvil-storage/src/engine.rs 5 0 100.00% 2 0 100.00% 50 5 90.00% 0 0 -
/other/path/outside_repo.rs 5 5 0.00% 1 1 0.00% 50 50 0.00% 0 0 -";

    #[test]
    fn test_aggregate_coverage_groups_by_crate() {
        let totals = aggregate_coverage(FIXTURE).expect("fixture parses");
        assert_eq!(totals.get("anvil-types"), Some(&(100, 10)));
        assert_eq!(totals.get("anvil-raft"), Some(&(200, 10)));
        assert_eq!(totals.get("anvil-storage"), Some(&(50, 5)));
        assert!(!totals.contains_key("other"));
        assert_eq!(coverage_percent(100, 10), 90.0);
    }

    #[test]
    fn test_aggregate_coverage_rejects_empty_report() {
        assert!(aggregate_coverage("").is_err());
        assert!(aggregate_coverage("TOTAL 1 1 0%").is_err());
    }
}
