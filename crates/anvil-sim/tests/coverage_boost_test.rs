#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty,
    clippy::manual_assert_eq
)]

use anvil_raft::SnapshotChunkData;
use anvil_sim::{
    DetRng, FarmOptions, FarmProfile, FarmRange, FarmSummary, Plan, PlanNet, SimCluster, SimDevice,
    SimDeviceConfig, SimNetwork, SplitMix64, StorageFaultAtlas, SwarmProfile, TraceEvent,
    TraceRecorder, TransportMode, Watchdog, WorkloadSpec, consensus_failure_class, count_sometimes,
    derive_component_seed, kv_plan_failure_class, normalize_failure_class, panic_failure_class,
    run_farm_range, run_farm_seed, run_farm_seed_with_storage_faults, run_kv_farm,
    run_kv_farm_seed, run_kv_farm_seed_with_storage, run_kv_plan, run_kv_plan_traced, run_kv_seed,
    run_kv_seed_with_stats, run_kv_seed_with_storage, run_plan, run_plan_traced, run_seed_farm,
    run_seed_farm_with_storage_faults, shrink_plan, shrink_plan_same_class, swarm_profile,
};
use anvil_storage::{Device, DeviceOp, FileId, SectorSize};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::time::Duration;

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("node id")
}

fn small_options() -> FarmOptions {
    FarmOptions {
        jobs: 1,
        out_path: None,
        resume: false,
        time_budget_secs: None,
        failures_dir: None,
    }
}

fn workload_for(seed: u64) -> WorkloadSpec {
    let profile = swarm_profile(seed);
    WorkloadSpec {
        clients: profile.clients,
        keys: profile.keys,
        ops_per_client: profile.ops_per_client,
        list_pct: profile.list_pct,
        think_ticks: profile.think_ticks,
        timeout_ticks: profile.timeout_ticks,
        client_crashes: profile.client_crashes,
        unordered: profile.unordered,
    }
}

#[test]
fn test_boost_farm_result_throughput() {
    let clean = anvil_sim::FarmResult {
        total_seeds: 4,
        passed_seeds: 4,
        failed_seed: None,
        duration: Duration::from_secs(2),
    };
    assert!(clean.is_clean());
    assert_eq!(clean.seeds_per_second(), 2.0);
    let dirty = anvil_sim::FarmResult {
        total_seeds: 4,
        passed_seeds: 3,
        failed_seed: Some(2),
        duration: Duration::from_secs(1),
    };
    assert!(!dirty.is_clean());
    let instant = anvil_sim::FarmResult {
        total_seeds: 0,
        passed_seeds: 0,
        failed_seed: None,
        duration: Duration::ZERO,
    };
    assert_eq!(instant.seeds_per_second(), 0.0);
    let farm = run_seed_farm(1, 1);
    assert!(farm.is_clean());
    assert_eq!(farm.total_seeds, 1);
}

#[test]
fn test_boost_farm_profile_accessors() {
    assert!(!FarmProfile::Kv.storage_faults());
    assert!(FarmProfile::KvStorage.storage_faults());
    assert!(!FarmProfile::Smoke.storage_faults());
    assert!(FarmProfile::KvStorage.is_kv());
    assert!(FarmProfile::Kv.is_kv());
    assert!(!FarmProfile::Smoke.is_kv());
    assert_eq!(FarmProfile::Smoke.name(), "smoke");
    assert_eq!(FarmProfile::Kv.name(), "kv");
    assert_eq!(FarmProfile::KvStorage.name(), "kv-storage");
    assert_eq!(
        FarmProfile::parse("smoke").expect("smoke"),
        FarmProfile::Smoke
    );
    assert!(FarmProfile::parse("bogus").is_err());
}

#[test]
fn test_boost_farm_range_len_and_empty() {
    let range = FarmRange { start: 3, end: 7 };
    assert_eq!(range.len(), 4);
    assert!(!range.is_empty());
    let empty = FarmRange { start: 5, end: 5 };
    assert_eq!(empty.len(), 0);
    assert!(empty.is_empty());
    let inverted = FarmRange { start: 9, end: 2 };
    assert_eq!(inverted.len(), 0);
    assert!(inverted.is_empty());
}

#[test]
fn test_boost_farm_range_parse_edges() {
    assert!(FarmRange::parse("7..3").is_err());
    assert!(FarmRange::parse("abc").is_err());
    assert!(FarmRange::parse("1..2..3").is_err());
    assert!(FarmRange::parse("").is_err());
    let bare = FarmRange::parse("4").expect("bare count");
    assert_eq!((bare.start, bare.end), (0, 4));
    assert_eq!(bare.len(), 4);
    let span = FarmRange::parse("2..5").expect("span");
    assert_eq!((span.start, span.end), (2, 5));
}

#[test]
fn test_boost_count_sometimes_tallies() {
    let tallied = count_sometimes(vec!["alpha", "beta", "alpha"]);
    assert_eq!(
        tallied,
        vec![("alpha".to_string(), 2), ("beta".to_string(), 1)]
    );
    assert!(count_sometimes(Vec::new()).is_empty());
    let missed = anvil_sim::sometimes_missed(&[]);
    assert_eq!(missed.len(), anvil_sim::SOMETIMES_INVENTORY.len());
    let full: Vec<(String, u64)> = anvil_sim::SOMETIMES_INVENTORY
        .iter()
        .map(|name| ((*name).to_string(), 1u64))
        .collect();
    assert!(anvil_sim::sometimes_missed(&full).is_empty());
}

#[test]
fn test_boost_farm_summary_render_and_rates() {
    let empty_summary = FarmSummary {
        total_seeds: 0,
        passed_seeds: 0,
        failed_seeds: Vec::new(),
        duration: Duration::ZERO,
        simulated_hours: 0.0,
        faults_by_class: Vec::new(),
        buggify: Vec::new(),
        invariant_checks: 0,
        sometimes: Vec::new(),
    };
    assert_eq!(empty_summary.seeds_per_second(), 0.0);
    assert!(!empty_summary.sometimes_missed().is_empty());
    let empty_text = empty_summary.render(FarmProfile::Smoke);
    assert!(empty_text.contains("ANVIL seed farm"));
    assert!(empty_text.contains("none registered"));
    assert!(empty_text.contains("none hit"));

    let full_summary = FarmSummary {
        total_seeds: 2,
        passed_seeds: 1,
        failed_seeds: vec![1],
        duration: Duration::from_secs(2),
        simulated_hours: 1.5,
        faults_by_class: vec![("partition".to_string(), 3)],
        buggify: vec![("point".to_string(), "file.rs:1".to_string(), 10, 2)],
        invariant_checks: 42,
        sometimes: vec![("sim.repair.cleared".to_string(), 1)],
    };
    assert!(full_summary.seeds_per_second() > 0.0);
    let full_text = full_summary.render(FarmProfile::Kv);
    assert!(full_text.contains("profile kv"));
    assert!(full_text.contains("first failing seed: 1"));
    assert!(full_text.contains("partition: 3"));
    assert!(full_text.contains("point@file.rs:1"));
}

#[test]
fn test_boost_single_consensus_seed_entry_points() {
    run_farm_seed(1);
    run_farm_seed_with_storage_faults(1);
    run_plan(&Plan::generate(3, false));
    run_plan(&Plan::generate(3, true));
    let (stats, lines) = run_plan_traced(&Plan::generate(3, true), false);
    assert!(stats.virtual_ticks > 0);
    assert!(lines.is_empty());
}

#[test]
fn test_boost_single_kv_seed_entry_points() {
    run_kv_farm_seed(1);
    run_kv_farm_seed_with_storage(1);
    let (history, audit) = run_kv_seed(1, false);
    assert!(!history.ops.is_empty());
    assert!(audit);
    let (storage_history, storage_audit) = run_kv_seed_with_storage(1, false);
    assert!(!storage_history.ops.is_empty());
    assert!(storage_audit);
}

#[test]
fn test_boost_tiny_parallel_farms() {
    let consensus = run_seed_farm(2, 1);
    assert!(consensus.is_clean());
    assert_eq!(consensus.total_seeds, 2);
    let storage = run_seed_farm_with_storage_faults(1, 1);
    assert_eq!(storage.total_seeds, 1);
    let kv = run_kv_farm(1, 1, false);
    assert!(kv.is_clean());
    assert_eq!(kv.passed_seeds, 1);
}

#[test]
fn test_boost_farm_range_kv_profiles() {
    let (kv_outcomes, kv_summary) = run_farm_range(
        FarmRange { start: 0, end: 1 },
        FarmProfile::Kv,
        &small_options(),
    );
    assert_eq!(kv_outcomes.len(), 1);
    assert!(kv_outcomes.iter().all(|outcome| outcome.ok));
    assert_eq!(kv_summary.total_seeds, 1);

    let (storage_outcomes, storage_summary) = run_farm_range(
        FarmRange { start: 1, end: 2 },
        FarmProfile::KvStorage,
        &small_options(),
    );
    assert_eq!(storage_outcomes.len(), 1);
    assert_eq!(storage_summary.total_seeds, 1);

    let (smoke_outcomes, _) = run_farm_range(
        FarmRange { start: 0, end: 2 },
        FarmProfile::Smoke,
        &FarmOptions {
            jobs: 0,
            out_path: None,
            resume: false,
            time_budget_secs: None,
            failures_dir: None,
        },
    );
    assert_eq!(smoke_outcomes.len(), 2);
}

#[test]
fn test_boost_farm_range_resume_without_out_path() {
    let options = FarmOptions {
        jobs: 1,
        out_path: None,
        resume: true,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (outcomes, summary) =
        run_farm_range(FarmRange { start: 5, end: 5 }, FarmProfile::Smoke, &options);
    assert!(outcomes.is_empty());
    assert_eq!(summary.total_seeds, 0);
}

#[test]
fn test_boost_farm_range_out_path_and_bad_writer() {
    let pid = std::process::id();
    let dir = std::env::temp_dir().join(format!("anvil-boost-farm-{pid}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("boost farm dir");
    let out = dir.join("results.jsonl");
    let failures = dir.join("custom-failures");
    let options = FarmOptions {
        jobs: 1,
        out_path: Some(out.clone()),
        resume: false,
        time_budget_secs: None,
        failures_dir: Some(failures),
    };
    let (outcomes, _) =
        run_farm_range(FarmRange { start: 0, end: 2 }, FarmProfile::Smoke, &options);
    assert_eq!(outcomes.len(), 2);
    let text = std::fs::read_to_string(&out).expect("jsonl written");
    assert!(text.contains("\"type\":\"seed\""));
    assert!(text.contains("\"type\":\"summary\""));

    let resumed = FarmOptions {
        jobs: 1,
        out_path: Some(out.clone()),
        resume: true,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (second, second_summary) =
        run_farm_range(FarmRange { start: 0, end: 2 }, FarmProfile::Smoke, &resumed);
    assert!(second.is_empty());
    assert_eq!(second_summary.total_seeds, 0);

    let garbage = dir.join("garbage.jsonl");
    std::fs::write(&garbage, "not json\n{\"nope\":true}\n").expect("garbage");
    let garbage_options = FarmOptions {
        jobs: 1,
        out_path: Some(garbage),
        resume: true,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (third, _) = run_farm_range(
        FarmRange { start: 0, end: 1 },
        FarmProfile::Smoke,
        &garbage_options,
    );
    assert_eq!(third.len(), 1);

    let bad_options = FarmOptions {
        jobs: 1,
        out_path: Some(std::path::PathBuf::from(
            "/nonexistent-boost-dir-xyz/results.jsonl",
        )),
        resume: false,
        time_budget_secs: None,
        failures_dir: None,
    };
    let (fourth, _) = run_farm_range(
        FarmRange { start: 0, end: 1 },
        FarmProfile::Smoke,
        &bad_options,
    );
    assert_eq!(fourth.len(), 1);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn test_boost_farm_range_time_budget_kv() {
    let options = FarmOptions {
        jobs: 1,
        out_path: None,
        resume: false,
        time_budget_secs: Some(0),
        failures_dir: None,
    };
    let (outcomes, _) = run_farm_range(FarmRange { start: 0, end: 10 }, FarmProfile::Kv, &options);
    assert!((outcomes.len() as u64) < 10);
}

#[test]
fn test_boost_cluster_basic_tick_and_rates() {
    let mut cluster = SimCluster::new(3, 101);
    assert_eq!(cluster.tick_rate(node(1)), 1000);
    cluster.set_tick_skew(node(1), 500);
    cluster.set_tick_skew(node(2), 2000);
    assert_eq!(cluster.tick_rate(node(1)), 500);
    assert_eq!(cluster.tick_rate(node(2)), 2000);
    assert!(!cluster.is_paused(node(1)));
    cluster.tick_n(4);
    assert!(cluster.current_tick >= 4);
    assert!(cluster.highest_commit_index() == LogIndex::from_raw(0));
}

#[test]
fn test_boost_cluster_pause_resume_cycle() {
    let mut cluster = SimCluster::new(3, 202);
    let target = node(1);
    cluster.pause_node(target);
    assert!(cluster.is_paused(target));
    cluster.tick_n(3);
    assert!(cluster.is_paused(target));
    cluster.resume_node(target);
    assert!(!cluster.is_paused(target));
    cluster.resume_node(target);
    assert!(!cluster.is_paused(target));
    assert!(cluster.take_completed_reads(target).is_empty());
}

#[test]
fn test_boost_cluster_crash_restart_cycle() {
    let mut cluster = SimCluster::new(3, 303);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"boost-entry"));
    cluster.tick_n(6);
    let follower = if leader == node(1) { node(2) } else { node(1) };
    assert!(!cluster.is_node_crashed(follower));
    cluster.crash_node(follower);
    assert!(cluster.is_node_crashed(follower));
    let restarted = cluster.restart_node(follower);
    assert!(restarted.is_ok());
    assert!(!cluster.is_node_crashed(follower));
    assert!(!cluster.is_node_needs_repair(follower));
    assert!(!cluster.is_node_refused_start(follower));
    cluster.acknowledge_token(follower, 999_999);
    assert!(cluster.engine_last_index(follower).is_some());
}

#[test]
fn test_boost_cluster_restart_missing_node_errors() {
    let mut cluster = SimCluster::new(3, 404);
    let missing = node(99);
    assert!(cluster.restart_node(missing).is_err());
    assert!(!cluster.is_node_crashed(missing));
    assert!(!cluster.is_node_needs_repair(missing));
    assert!(!cluster.is_node_refused_start(missing));
    assert!(cluster.engine_last_index(missing).is_none());
}

#[test]
fn test_boost_cluster_network_helpers() {
    let mut cluster = SimCluster::new(3, 505);
    assert!(!cluster.deliver_packet_at(0));
    assert!(!cluster.drop_packet_at(0));
    assert!(!cluster.duplicate_packet_at(0));
    cluster.drain_network(5);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"net-helper"));
    cluster.deliver_due_messages();
    cluster.drain_network(10);
    cluster.isolate_node(node(1));
    cluster.partition(&[node(1)], &[node(2), node(3)]);
    cluster.heal_all();
    cluster.trace_line("boost trace off".to_string());
    assert!(cluster.trace_lines.is_empty());
    cluster.trace_enabled = true;
    cluster.trace_line("boost trace on".to_string());
    cluster.tick();
    assert!(!cluster.trace_lines.is_empty());
    assert!(cluster.take_completed_reads(leader).is_empty());
}

#[test]
fn test_boost_cluster_storage_helpers() {
    let mut cluster = SimCluster::new(3, 606);
    let target = node(1);
    cluster.sync_node_storage_with_core(target);
    assert!(cluster.backfill_log_prefix(target, Vec::new()));
    let single = vec![Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"backfill"),
    }];
    let _backfilled = cluster.backfill_log_prefix(target, single);
    let empty_archive: anvil_sim::CommittedArchive = BTreeMap::new();
    cluster.reconcile_snapshot_prefix(target, &empty_archive, LogIndex::from_raw(0));
    cluster.crash_node(node(2));
    cluster.reconcile_snapshot_prefix(node(2), &empty_archive, LogIndex::from_raw(0));
    cluster.repair_step(None);
    cluster.repair_step(Some(&empty_archive));
    cluster.notify_applied(target, LogIndex::from_raw(0));
    cluster.compact_log(target, LogIndex::from_raw(0));
    cluster.supply_snapshot(
        target,
        LogIndex::from_raw(0),
        Term::from_raw(0),
        0,
        [0u8; 32],
        Vec::new(),
    );
    cluster.step_node(target, anvil_raft::Input::Tick);
    assert!(cluster.engine_last_index(target).is_some());
}

#[test]
fn test_boost_cluster_all_crashed_commit_is_zero() {
    let mut cluster = SimCluster::new(3, 707);
    for id in [node(1), node(2), node(3)] {
        cluster.crash_node(id);
    }
    assert_eq!(cluster.highest_commit_index(), LogIndex::from_raw(0));
    assert!(cluster.leader().is_none());
}

#[test]
fn test_boost_cluster_new_with_storage_faults() {
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut cluster = SimCluster::new_with_storage_faults(3, 808, &config);
    cluster.tick_n(6);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"faulty-entry"));
    cluster.tick_n(6);
    assert!(cluster.engine_last_index(leader).is_some());
}

#[test]
fn test_boost_device_config_variants() {
    let chaotic = SimDeviceConfig::chaotic(SectorSize::Sector512, 100, 200, 300);
    assert_eq!(chaotic.read_corruption_rate_ppm, 100);
    let latency = SimDeviceConfig::with_latency(SectorSize::Sector512, 2, 5);
    assert_eq!(latency.min_latency_ticks, 2);
    assert_eq!(latency.max_latency_ticks, 5);
    let mut device = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(1, "cfg"),
    );
    assert_eq!(device.current_tick(), 0);
    device.step_time(7);
    assert_eq!(device.current_tick(), 7);
    device.set_time(42);
    assert_eq!(device.current_tick(), 42);
    assert_eq!(device.config().sector_size, SectorSize::Sector512);
    device.config_mut().fsync_stall_ticks = 3;
    assert_eq!(device.config().fsync_stall_ticks, 3);
    assert!(!device.has_pending_completions());
}

#[test]
fn test_boost_device_atlas_unit() {
    let atlas = StorageFaultAtlas::new(3);
    assert_eq!(atlas.cluster_size(), 3);
    assert_eq!(atlas.quorum_size(), 2);
    assert_eq!(atlas.commit_index(), LogIndex::from_raw(0));
    let first = node(1);
    let second = node(2);
    let third = node(3);
    let committed = LogIndex::from_raw(9);
    atlas.record_persisted(first, committed);
    atlas.record_persisted(second, committed);
    atlas.record_persisted(third, committed);
    assert_eq!(atlas.valid_copies_count(committed), 3);
    atlas.advance_commit_index(committed);
    assert_eq!(atlas.commit_index(), committed);
    assert!(atlas.is_committed(committed));
    assert!(!atlas.is_committed(LogIndex::from_raw(10)));
    atlas.record_corruption(first, committed);
    assert_eq!(atlas.valid_copies_count(committed), 2);
    assert!(atlas.entry_survives_corruption(first, committed));
    assert!(atlas.can_lose_hard_state(first));
    atlas.record_hard_state_dead(first);
    assert!(!atlas.can_lose_hard_state(second));
    let _allowed = atlas.can_corrupt_entry(second, LogIndex::from_raw(500));
    let _vetoed = atlas.can_corrupt_entry(first, committed);
    assert!(atlas.vetoed_corruptions() + atlas.allowed_corruptions() >= 2);

    let permissive = StorageFaultAtlas::permissive();
    assert_eq!(permissive.cluster_size(), 1);
    assert_eq!(permissive.quorum_size(), 1);
    assert!(permissive.can_lose_hard_state(first));
    assert!(permissive.entry_survives_corruption(first, committed));
    assert!(permissive.can_corrupt_entry(first, committed));
}

#[test]
fn test_boost_device_read_durable_eio() {
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    config.read_eio_rate_ppm = 1_000_000;
    let mut device = SimDevice::new(config, DetRng::new(909, "durable-eio"));
    let file = FileId::wal_segment(1);
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().expect("create").result.is_ok());
    device.submit(DeviceOp::Write {
        op_id: 2,
        file_id: file,
        offset: 0,
        payload: Bytes::from(vec![0xABu8; 64]),
    });
    assert!(device.poll_completion().expect("write").result.is_ok());
    device.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: file,
    });
    assert!(device.poll_completion().expect("sync").result.is_ok());
    assert!(device.read_durable(file).is_err());
    assert!(device.stats().read_eios_injected >= 1);
}

#[test]
fn test_boost_device_overwrite_extend_and_lists() {
    let mut device = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(919, "overwrite"),
    );
    device.overwrite_durable(FileId::wal_segment(50), 0, &[1u8; 8]);
    assert!(
        device
            .get_file_durable_bytes(FileId::wal_segment(50))
            .is_none()
    );
    let file = FileId::wal_segment(3);
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().expect("create").result.is_ok());
    device.overwrite_durable(file, 500, &[9u8; 32]);
    let durable = device.get_file_durable_bytes(file).expect("extended");
    assert!(durable.len() >= 532);
    assert!(!device.list_volatile_files().is_empty());
    let _ = device.list_durable_files();
    assert!(device.drain_ready_completions().is_empty());
    assert!(device.drain_all_completions_forcing().is_empty());
    device.crash();
    assert!(device.stats().crash_count >= 1);
}

#[test]
fn test_boost_rng_splitmix_and_derive() {
    let mut first = SplitMix64::new(1234);
    let mut second = SplitMix64::new(1234);
    assert_eq!(first.next_u64(), second.next_u64());
    let mixed = SplitMix64::new(7).mix(99);
    assert_ne!(mixed, SplitMix64::new(7));
    let empty_seed = derive_component_seed(42, "");
    let long_seed = derive_component_seed(42, "a-much-longer/component/path/for/coverage");
    assert_ne!(empty_seed, long_seed);
    let short_a = derive_component_seed(42, "12345678");
    let short_b = derive_component_seed(42, "12345678");
    assert_eq!(short_a, short_b);
    let mut rng = DetRng::from_seed(short_a);
    let _ = rng.next_u64();
    let _first_word = rng.next_u32();
    let _second_word = rng.next_u32();
}

#[test]
fn test_boost_rng_det_samplers() {
    let mut rng = DetRng::new(777, "samplers");
    assert_eq!(rng.gen_range(9..10), 9);
    for _ in 0..50 {
        let drawn = rng.gen_range(0..3);
        assert!(drawn < 3);
    }
    assert_eq!(rng.gen_range_inclusive(4..=4), 4);
    let _full = rng.gen_range_inclusive(0..=u64::MAX);
    let high = rng.gen_range_inclusive(5..=u64::MAX);
    assert!(high >= 5);
    assert!(rng.bernoulli(5, 3));
    assert!(!rng.bernoulli(0, 7));
    let _ = rng.bernoulli(1, 2);
    assert!(rng.bernoulli_ppm(1_000_000));
    assert!(!rng.bernoulli_ppm(0));
    assert_eq!(rng.exponential(0), 0);
    assert!(rng.exponential(100) <= 10_000);
    assert_eq!(rng.bimodal(70, 0, 9000, 0, 1_000_000), 70);
    assert_eq!(rng.bimodal(70, 0, 9000, 0, 0), 9000);
    let spread = rng.bimodal(100, 10, 9000, 10, 500_000);
    assert!(spread <= 9010);
    let mut values = [3u64, 1, 2];
    rng.shuffle(&mut values);
    values.sort_unstable();
    assert_eq!(values, [1, 2, 3]);
}

#[test]
fn test_boost_trace_recorder_wrap() {
    let mut recorder = TraceRecorder::with_capacity(2);
    for actor in 0..5u64 {
        recorder.record(TraceEvent {
            time: anvil_sim::time::SimTime::from_nanos(actor),
            actor_id: actor,
            event_kind: 1,
            payload: [0u8; 14],
        });
    }
    assert_eq!(recorder.total_events(), 5);
    let recent = recorder.recent_events();
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].actor_id, 3);
    assert_eq!(recent[1].actor_id, 4);
    assert!(
        !recorder
            .finalize_hash()
            .as_bytes()
            .iter()
            .all(|byte| *byte == 0)
    );
    let fallback = TraceRecorder::default();
    assert_eq!(fallback.total_events(), 0);
}

#[test]
fn test_boost_trace_event_roundtrip_extra() {
    let event = TraceEvent {
        time: anvil_sim::time::SimTime::from_millis(9),
        actor_id: 77,
        event_kind: 13,
        payload: [0xA5u8; 14],
    };
    let bytes = event.to_bytes();
    assert_eq!(TraceEvent::from_bytes(&bytes), event);
}

#[test]
fn test_boost_plan_decode_extra_errors() {
    assert!(Plan::decode("bogus").is_err());
    assert!(Plan::decode("anvil-plan/99;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/2").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;pair-without-equals").is_err());
    assert!(Plan::decode("anvil-plan/2;seed=1;unknownkey=5").is_err());
    let plan = Plan::generate(11, false);
    let encoded = plan.encode();
    let round = Plan::decode(&encoded).expect("roundtrip");
    assert_eq!(round, plan);
    let workload_plan = Plan::generate_for_workload(11, false, workload_for(11));
    let workload_round = Plan::decode(&workload_plan.encode()).expect("workload roundtrip");
    assert_eq!(workload_round, workload_plan);
}

#[test]
fn test_boost_plan_run_storage_traced() {
    let plan = Plan::generate(5, true);
    let stats = run_plan(&plan);
    assert!(stats.virtual_ticks > 0);
    let (traced_stats, lines) = run_plan_traced(&plan, true);
    assert_eq!(traced_stats.virtual_ticks, stats.virtual_ticks);
    assert!(!lines.is_empty());
}

#[test]
fn test_boost_normalize_truncation_and_panic_nonstring() {
    let long_digits: String = std::iter::repeat_n('7', 300).collect();
    let class = normalize_failure_class(&long_digits);
    assert!(class.len() <= 240);
    assert_eq!(class, "#");
    let boxed: Box<dyn std::any::Any + Send> = Box::new(42u32);
    assert_eq!(
        panic_failure_class(&boxed),
        "panic:non-string panic payload"
    );
    let text: Box<dyn std::any::Any + Send> = Box::new(String::from("boom at 12"));
    assert_eq!(panic_failure_class(&text), "panic:boom at #");
}

#[test]
fn test_boost_consensus_failure_class_panic_branch() {
    let mut plan = Plan::generate(9, false);
    assert!(consensus_failure_class(&plan).is_none());
    plan.cluster_size = 0;
    let class = consensus_failure_class(&plan).expect("invalid size must fail");
    assert!(class.starts_with("panic:"));
}

#[test]
fn test_boost_kv_failure_class_panic_branch() {
    let plan = Plan::generate_for_workload(9, false, workload_for(9));
    assert!(kv_plan_failure_class(&plan, false).is_none());
    // A zero-node cluster degrades gracefully instead of panicking.
    let mut bad = plan.clone();
    bad.cluster_size = 0;
    assert!(kv_plan_failure_class(&bad, false).is_none());
    // The stale canary serves stale reads the checker rejects, covering
    // the audit-failure classification.
    let class = kv_plan_failure_class(&plan, true).expect("canary must fail the audit");
    assert!(class.starts_with("linearizability:"));
}

#[test]
fn test_boost_shrink_trivial_no_workload() {
    let plan = Plan::generate(21, false);
    let same = shrink_plan_same_class(&plan, &|_| None);
    assert_eq!(same, plan);
    let shrunk = shrink_plan_same_class(&plan, &|_| Some("always".to_string()));
    assert!(shrunk.events.len() <= plan.events.len());
    let kept = shrink_plan(&plan, &|_| true);
    assert!(kept.events.len() <= plan.events.len());
    let untouched = shrink_plan(&plan, &|_| false);
    assert_eq!(untouched, plan);
}

#[test]
fn test_boost_shrink_trivial_with_workload() {
    let plan = Plan::generate_for_workload(22, false, workload_for(22));
    let same = shrink_plan_same_class(&plan, &|_| None);
    assert_eq!(same, plan);
    let shrunk = shrink_plan_same_class(&plan, &|_| Some("always".to_string()));
    assert!(shrunk.events.len() <= plan.events.len());
    let no_workload_shrink = shrink_plan(&plan, &|_| true);
    assert!(no_workload_shrink.events.len() <= plan.events.len());
}

#[test]
fn test_boost_run_kv_plan_variants() {
    let plain = Plan::generate(31, false);
    let (history, audit, stats) = run_kv_plan(&plain, false);
    assert!(!history.ops.is_empty());
    assert!(audit);
    assert!(stats.virtual_ticks > 0);
    let workload_plan = Plan::generate_for_workload(31, false, workload_for(31));
    let (traced_history, traced_audit, _, lines) = run_kv_plan_traced(&workload_plan, false);
    assert!(!traced_history.ops.is_empty());
    assert!(traced_audit);
    assert!(!lines.is_empty());
    assert!(kv_plan_failure_class(&plain, false).is_none());
}

#[test]
fn test_boost_workload_swarm_and_stats() {
    let first: SwarmProfile = swarm_profile(555);
    let second = swarm_profile(555);
    assert_eq!(first.clients, second.clients);
    assert!((1..=8).contains(&first.clients));
    let (history, audit, stats) = run_kv_seed_with_stats(2, false);
    assert!(!history.ops.is_empty());
    assert!(audit);
    assert!(stats.virtual_ticks > 0);
    let (storage_history, storage_audit, _) = run_kv_seed_with_stats(1, true);
    assert!(!storage_history.ops.is_empty());
    assert!(storage_audit);
    let (canary_history, _) = run_kv_seed(2, true);
    assert!(!canary_history.ops.is_empty());
}

#[test]
fn test_boost_convergence_audit_empty_stores() {
    let cluster = SimCluster::new(3, 909);
    let stores: BTreeMap<anvil_types::NodeId, anvil_kv::KvStore> = BTreeMap::new();
    let applied: BTreeMap<anvil_types::NodeId, LogIndex> = BTreeMap::new();
    assert!(anvil_sim::convergence_audit(&cluster, &stores, &applied));
}

#[test]
fn test_boost_scenario_hashmap_runs() {
    let (events, hash) = anvil_sim::run_scenario_with_hashmap(42).expect("hashmap scenario");
    assert!(events > 0);
    assert!(!hash.as_bytes().iter().all(|byte| *byte == 0));
}

#[test]
fn test_boost_buggify_registry_helpers() {
    anvil_sim::set_buggify_enabled(true);
    anvil_sim::set_buggify_seed(1234);
    let _ = anvil_sim::list_registered_points();
    anvil_sim::reset_buggify_registry();
    let _ = anvil_sim::list_registered_points();
    anvil_sim::set_buggify_seed(999);
}

#[test]
fn test_boost_watchdog_helpers() {
    let mut watchdog = Watchdog::new(None, 2);
    assert_eq!(watchdog.events_without_progress(), 0);
    assert!(watchdog.on_event().is_ok());
    assert!(watchdog.on_event().is_err());
    watchdog.record_progress();
    assert_eq!(watchdog.events_without_progress(), 0);
    assert!(
        watchdog
            .on_queue_empty(anvil_sim::time::SimTime::from_nanos(5))
            .is_ok()
    );
    let bounded = Watchdog::new(
        Some(anvil_sim::time::SimTime::from_nanos(100)),
        Watchdog::DEFAULT_MAX_STAGNANT_EVENTS,
    );
    assert!(
        bounded
            .on_queue_empty(anvil_sim::time::SimTime::from_nanos(5))
            .is_err()
    );
    assert!(
        bounded
            .on_queue_empty(anvil_sim::time::SimTime::from_nanos(200))
            .is_ok()
    );
    let fallback = Watchdog::default();
    assert_eq!(fallback.events_without_progress(), 0);
}

#[test]
fn test_boost_network_plan_and_transport_helpers() {
    let mut net = SimNetwork::new();
    assert_eq!(net.transport(), TransportMode::OrderedLossy);
    net.set_transport(TransportMode::Unordered);
    assert_eq!(net.transport(), TransportMode::Unordered);
    net.set_transport(TransportMode::OrderedLossy);
    net.set_fault_salt(77);
    net.set_fault_rates(0, 0, 0, 0);
    net.set_bandwidth_per_tick(100);
    let plan_net = PlanNet {
        salt: 5,
        drop_pct: 0,
        dup_pct: 0,
        max_delay: 0,
    };
    let from = node(1);
    let to = node(2);
    let message = anvil_raft::Message::HeartbeatRequest {
        term: Term::from_raw(1),
        leader_id: from,
        commit_index: LogIndex::from_raw(0),
        read_context: None,
    };
    assert!(net.schedule_plan(from, to, message, 0, &plan_net));
    assert!(!net.is_empty());
    let _ = net.has_due(100);
    let _ = net.len();
}

#[test]
fn test_boost_queue_and_chunk_helpers() {
    use anvil_sim::queue::{BinaryHeapEventQueue, EventQueue};
    let mut heap = BinaryHeapEventQueue::new();
    assert!(heap.is_empty());
    heap.push(anvil_sim::time::SimTime::from_nanos(9), 1, 7);
    assert_eq!(heap.len(), 1);
    let popped = heap.pop().expect("pop");
    assert_eq!(popped.actor_id, 1);
    let chunk = SnapshotChunkData {
        chunk: Bytes::from_static(b"boost"),
        chunk_crc: crc32c::crc32c(b"boost"),
    };
    assert_eq!(chunk.chunk.len(), 5);
}
