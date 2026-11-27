#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::{FarmOptions, FarmProfile, FarmRange, run_farm_range};

fn small_options(out: Option<std::path::PathBuf>) -> FarmOptions {
    FarmOptions {
        jobs: 2,
        out_path: out,
        resume: false,
        time_budget_secs: None,
        failures_dir: None,
    }
}

#[test]
fn test_farm_range_parsing() {
    let range = FarmRange::parse("10..20").expect("range");
    assert_eq!((range.start, range.end), (10, 20));
    assert!(!range.is_empty());
    let count = FarmRange::parse("50").expect("count");
    assert_eq!((count.start, count.end), (0, 50));
    assert!(FarmRange::parse("20..10").is_err());
    assert!(FarmRange::parse("nope").is_err());
    assert!(FarmRange::parse("").is_err());
}

#[test]
fn test_farm_profile_parsing() {
    assert_eq!(
        FarmProfile::parse("smoke").expect("smoke"),
        FarmProfile::Smoke
    );
    assert_eq!(FarmProfile::parse("kv").expect("kv"), FarmProfile::Kv);
    assert_eq!(
        FarmProfile::parse("kv-storage").expect("kv-storage"),
        FarmProfile::KvStorage
    );
    assert!(FarmProfile::parse("nope").is_err());
}

#[test]
fn test_farm_smoke_range_reports_summary() {
    let (outcomes, summary) = run_farm_range(
        FarmRange { start: 0, end: 8 },
        FarmProfile::Smoke,
        &small_options(None),
    );
    assert_eq!(outcomes.len(), 8);
    assert!(outcomes.iter().all(|o| o.ok));
    assert_eq!(summary.total_seeds, 8);
    assert_eq!(summary.passed_seeds, 8);
    assert!(summary.invariant_checks > 0);
    assert!(summary.simulated_hours > 0.0);
    assert!(summary.seeds_per_second() > 0.0);
    assert_ne!(summary.faults_by_class.len(), 0);
}

#[test]
fn test_farm_jsonl_and_resume() {
    let dir = std::env::temp_dir().join(format!("anvil-farm-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("farm temp dir");
    let out = dir.join("results.jsonl");

    let options = FarmOptions {
        jobs: 2,
        out_path: Some(out.clone()),
        resume: false,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (first, _) = run_farm_range(FarmRange { start: 0, end: 4 }, FarmProfile::Smoke, &options);
    assert!(first.iter().all(|o| o.ok));
    let lines: Vec<String> = std::fs::read_to_string(&out)
        .expect("jsonl written")
        .lines()
        .map(str::to_string)
        .collect();
    // Four seed lines plus one summary line.
    assert_eq!(lines.len(), 5);
    assert!(
        lines[..4]
            .iter()
            .all(|line| line.contains("\"type\":\"seed\""))
    );
    assert!(lines[4].contains("\"type\":\"summary\""));

    // A resumed run with the same range performs no new seeds.
    let resumed = FarmOptions {
        resume: true,
        ..options
    };
    let (second, second_summary) =
        run_farm_range(FarmRange { start: 0, end: 4 }, FarmProfile::Smoke, &resumed);
    assert!(second.is_empty());
    assert_eq!(second_summary.total_seeds, 0);
    let rerun: Vec<String> = std::fs::read_to_string(&out)
        .expect("jsonl readable")
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(rerun.len(), 6, "only the second summary appends");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_farm_time_box_stops_taking_seeds() {
    let options = FarmOptions {
        jobs: 1,
        out_path: None,
        resume: false,
        time_budget_secs: Some(0),
        failures_dir: None,
    };
    let (outcomes, _) = run_farm_range(
        FarmRange { start: 0, end: 100 },
        FarmProfile::Smoke,
        &options,
    );
    assert!(
        (outcomes.len() as u64) < 100,
        "a zero-second budget must stop the farm early"
    );
}
