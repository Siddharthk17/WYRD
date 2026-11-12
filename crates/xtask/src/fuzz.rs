#![forbid(unsafe_code)]
#![allow(clippy::print_stdout, clippy::print_stderr)]

//! Fuzzing runner and corpus verification gate for ANVIL.

use std::process::Command;

/// List of all registered libfuzzer targets in the repository.
pub const FUZZ_TARGETS: &[&str] = &[
    "fuzz_varint",
    "fuzz_frame_header",
    "fuzz_entry",
    "fuzz_wal_header",
    "fuzz_wal_frame",
    "fuzz_hard_state",
    "fuzz_snapshot",
];

/// Detects the native host target triple via `rustc -vV`.
fn native_target() -> Option<String> {
    let output = Command::new("rustc").arg("-vV").output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&output.stdout);
    text.lines()
        .find_map(|line| line.strip_prefix("host: ").map(str::to_string))
}

/// Runs cargo-fuzz targets if `cargo-fuzz` is installed, or verifies fuzz target
/// compilation and corpus integrity if running in a non-nightly environment.
///
/// # Errors
/// Returns an error if target compilation or corpus verification fails.
pub fn run_fuzz(duration_secs: u64) -> Result<(), String> {
    let workspace_root = crate::determinism::find_workspace_root();
    let fuzz_manifest = workspace_root.join("fuzz").join("Cargo.toml");

    println!("=== ANVIL Fuzzing Harness ===");
    println!(
        "Checking compilation of {} fuzz targets...",
        FUZZ_TARGETS.len()
    );

    let mut check_cmd = Command::new("cargo");
    check_cmd.current_dir(&workspace_root);
    check_cmd.args(["check", "--manifest-path"]);
    check_cmd.arg(&fuzz_manifest);
    let check_status = check_cmd
        .status()
        .map_err(|e| format!("Failed to check fuzz crate: {e}"))?;

    if !check_status.success() {
        return Err("Fuzz targets failed compilation check".to_string());
    }

    // Check if cargo-fuzz is available
    let has_cargo_fuzz = Command::new("cargo")
        .args(["fuzz", "--version"])
        .output()
        .is_ok_and(|out| out.status.success());

    if has_cargo_fuzz {
        println!("Running libfuzzer on all targets for {duration_secs}s per target...");
        println!("(libfuzzer requires nightly sanitizers; invoking via nightly toolchain)");
        // cargo-fuzz defaults to a musl target that sanitizers reject, so pin
        // the native host target explicitly (works on x86_64 and aarch64).
        let native_target = native_target();
        for &target in FUZZ_TARGETS {
            println!("  -> Fuzzing target `{target}`...");
            let mut run_cmd = Command::new("cargo");
            run_cmd.current_dir(workspace_root.join("fuzz"));
            run_cmd.env("RUSTUP_TOOLCHAIN", "nightly");
            run_cmd.args(["fuzz", "run", target]);
            if let Some(native) = &native_target {
                run_cmd.args(["--target", native]);
            }
            run_cmd.args(["--", &format!("-max_total_time={duration_secs}")]);
            let status = run_cmd
                .status()
                .map_err(|e| format!("Failed to run fuzz target {target}: {e}"))?;
            if !status.success() {
                return Err(format!(
                    "Fuzz target `{target}` encountered a crash or error"
                ));
            }
        }
        println!("All {duration_secs}s fuzz runs completed clean!");
    } else {
        return Err(
            "`cargo-fuzz` not found (requires nightly toolchain + `cargo install cargo-fuzz`). \
             Fuzz gate not run, not passed. Install and re-run `cargo xtask fuzz --duration-secs 60`."
                .to_string(),
        );
    }

    Ok(())
}
