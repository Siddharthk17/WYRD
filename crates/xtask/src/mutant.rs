//! Hand-authored mutant kill matrix gate.
//!
//! Every mutant is planted behind the `mutants` feature. A killer is either
//! a farm seed whose run fails with the mutant and passes without it, or a
//! targeted test that fails with the mutant and passes without it. Both
//! forms are executed here, never merely reported.

use anvil_types::mutants::MutantId;
use std::path::Path;
use std::process::{Command, Stdio};

/// How a mutant was detected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detector {
    /// A Raft safety invariant panicked.
    Invariant,
    /// The linearizability checker rejected the history.
    Checker,
    /// The farm's post-heal liveness probe failed.
    Liveness,
    /// The convergence audit failed.
    Audit,
    /// A targeted unit test failed.
    UnitTest,
}

impl Detector {
    /// Short label for the kill-matrix table.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::Invariant => "invariant",
            Self::Checker => "checker",
            Self::Liveness => "liveness",
            Self::Audit => "audit",
            Self::UnitTest => "unit test",
        }
    }
}

/// A verified killer for one mutant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Killer {
    /// Farm seed, profile, and detector.
    Farm {
        /// Master seed that fails with the mutant.
        seed: u64,
        /// Farm profile.
        profile: &'static str,
        /// Detector that caught the planted bug.
        detector: Detector,
    },
    /// Targeted test invocation.
    Test {
        /// Workspace crate.
        crate_name: &'static str,
        /// Test target file.
        test_file: &'static str,
        /// Test name.
        test_name: &'static str,
    },
}

impl Killer {
    /// Human-readable killer description.
    #[must_use]
    pub fn describe(self) -> String {
        match self {
            Self::Farm {
                seed,
                profile,
                detector,
            } => format!("farm {profile} seed {seed} ({})", detector.label()),
            Self::Test {
                crate_name,
                test_file,
                test_name,
            } => format!("{crate_name}/{test_file}:{test_name}"),
        }
    }
}

/// Verified kill table. `None` means no killer was found within the
/// 10,000-seed bound and the mutant is reported as a survivor.
#[must_use]
pub fn killer_for(id: MutantId) -> Option<Killer> {
    match id {
        MutantId::M01 => Some(Killer::Farm {
            seed: 0,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M02 => Some(Killer::Farm {
            seed: 3524,
            profile: "kv-storage",
            detector: Detector::Liveness,
        }),
        MutantId::M03 => Some(Killer::Farm {
            seed: 927,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M04 => Some(Killer::Test {
            crate_name: "anvil-raft",
            test_file: "core_test",
            test_name: "test_leader_propose_and_parallel_persist_contract",
        }),
        MutantId::M05 | MutantId::M06 => Some(Killer::Farm {
            seed: 0,
            profile: "kv",
            detector: Detector::Checker,
        }),
        MutantId::M07 => Some(Killer::Farm {
            seed: 17,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M08 => Some(Killer::Farm {
            seed: 40,
            profile: "kv",
            detector: Detector::Audit,
        }),
        MutantId::M09 | MutantId::M10 => Some(Killer::Test {
            crate_name: "anvil-node",
            test_file: "node_snapshot_test",
            test_name: "test_node_snapshot_supply_install_and_rejoin",
        }),
        MutantId::M11 => Some(Killer::Farm {
            seed: 1608,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M12 => Some(Killer::Test {
            crate_name: "anvil-raft",
            test_file: "membership_test",
            test_name: "test_config_change_refused_before_current_term_commit",
        }),
        MutantId::M13 => Some(Killer::Farm {
            seed: 13230,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M14 => Some(Killer::Test {
            crate_name: "anvil-kv",
            test_file: "kv_test",
            test_name: "test_kv_session_dedup_returns_cached_response",
        }),
        MutantId::M15 | MutantId::M20 => None,
        MutantId::M16 => Some(Killer::Farm {
            seed: 4166,
            profile: "kv-storage",
            detector: Detector::Invariant,
        }),
        MutantId::M17 => Some(Killer::Farm {
            seed: 14755,
            profile: "kv",
            detector: Detector::Checker,
        }),
        MutantId::M18 => Some(Killer::Farm {
            seed: 16189,
            profile: "kv",
            detector: Detector::Invariant,
        }),
        MutantId::M19 => Some(Killer::Test {
            crate_name: "anvil-raft",
            test_file: "membership_test",
            test_name: "test_removed_node_does_not_campaign",
        }),
    }
}

/// Runs one farm killer. Returns true when the seed fails with the mutant.
fn run_farm(workspace: &Path, binary: &Path, id: MutantId, killer: Killer) -> bool {
    let Killer::Farm {
        seed,
        profile,
        detector: _,
    } = killer
    else {
        return false;
    };
    let end = seed.saturating_add(1);
    let status = Command::new(binary)
        .current_dir(workspace)
        .args([
            "farm",
            "--profile",
            profile,
            "--seeds",
            &format!("{seed}..{end}"),
            "--jobs",
            "1",
            "--mutant",
            id.name(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    status.is_ok_and(|status| !status.success())
}

/// Runs one control farm seed. Returns true when the unmutated seed passes.
fn run_control_farm(workspace: &Path, binary: &Path, profile: &str, seed: u64) -> bool {
    let end = seed.saturating_add(1);
    let status = Command::new(binary)
        .current_dir(workspace)
        .args([
            "farm",
            "--profile",
            profile,
            "--seeds",
            &format!("{seed}..{end}"),
            "--jobs",
            "1",
            "--mutant",
            "none",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    status.is_ok_and(|status| status.success())
}

/// Runs a targeted test with and without the mutant. Returns true when the
/// mutant run fails and the control run passes.
fn run_test_killer(workspace: &Path, id: MutantId, killer: Killer) -> bool {
    let Killer::Test {
        crate_name,
        test_file,
        test_name,
    } = killer
    else {
        return false;
    };
    let run = |mutant: &str| -> bool {
        let status = Command::new("cargo")
            .current_dir(workspace)
            .env("ANVIL_MUTANT", mutant)
            .args([
                "test",
                "-q",
                "-p",
                crate_name,
                "--features",
                "mutants",
                "--test",
                test_file,
                test_name,
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status();
        status.is_ok_and(|status| status.success())
    };
    !run(id.name()) && run("none")
}

/// Runs the kill matrix for one id or for all twenty mutants.
pub fn run_mutant_matrix(workspace: &Path, requested: &str) -> Result<(), String> {
    let selected: Vec<MutantId> = if requested.eq_ignore_ascii_case("all") {
        anvil_types::mutants::ALL.to_vec()
    } else {
        vec![MutantId::parse(requested).ok_or_else(|| format!("unknown mutant id '{requested}'"))?]
    };

    let binary = workspace.join("target/release/anvil-sim");
    if !binary.exists() {
        return Err(format!(
            "missing {}; build with `cargo build --release -p anvil-sim --features mutants`",
            binary.display()
        ));
    }

    println!(">>> Running Gate: Mutant kill matrix ...");
    let mut survived = Vec::new();
    for id in selected {
        let Some(killer) = killer_for(id) else {
            println!("{}  SURVIVED  (no killer within 10,000 seeds)", id.name());
            survived.push(id.name());
            continue;
        };
        let was_killed = match killer {
            Killer::Farm { profile, seed, .. } => {
                run_farm(workspace, &binary, id, killer)
                    && run_control_farm(workspace, &binary, profile, seed)
            }
            Killer::Test { .. } => run_test_killer(workspace, id, killer),
        };
        if was_killed {
            println!("{}  KILLED    {}", id.name(), killer.describe());
        } else {
            println!("{}  SURVIVED  {}", id.name(), killer.describe());
            survived.push(id.name());
        }
    }
    if survived.is_empty() {
        println!(">>> [PASS] Mutant kill matrix");
        Ok(())
    } else {
        Err(format!("surviving mutants: {}", survived.join(", ")))
    }
}
