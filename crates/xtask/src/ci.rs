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
//! 3. Test suite (`cargo test --workspace`)
//! 4. Doc tests (`cargo test --doc --workspace`)
//! 5. Documentation generation (`cargo doc --workspace --no-deps`)
//! 6. Zero-unsafe audit (verifies `#![forbid(unsafe_code)]` in all crates and 0 `unsafe` in production source)
//! 7. Determinism gate (`cargo xtask determinism`)
//! 8. Performance budgets gate (`cargo xtask bench` / `--verify-budgets`)

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

        // Scan all .rs files in src/ for 'unsafe ' or 'unsafe{'
        for file in walk_rs_files(&src_dir)? {
            // Skip self-audit of ci.rs to avoid matching the search patterns
            if file.ends_with("ci.rs") {
                continue;
            }
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
                if line.contains("unsafe ") || line.contains("unsafe{") {
                    // Check if it's #![forbid(unsafe_code)]
                    if line.contains("#![forbid(unsafe_code)]") {
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
        ">>> [PASS] Zero-Unsafe Code Audit ({audited_crates} crates audited, 0 unsafe blocks) ({elapsed:.2?})"
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
    println!("===========================================================");
    println!("               ANVIL Continuous Integration (CI)          ");
    println!("===========================================================");

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

    // 3. Workspace unit & integration tests
    let mut test_cmd = Command::new("cargo");
    test_cmd.current_dir(&workspace_root);
    test_cmd.args(["test", "--workspace"]);
    run_step("Workspace Tests (`cargo test --workspace`)", &mut test_cmd)?;

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

    println!("\n===========================================================");
    println!("              ALL CI QUALITY GATES PASSED!                ");
    println!("===========================================================");
    Ok(())
}
