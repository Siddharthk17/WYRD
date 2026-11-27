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

//! Second-wave coverage tests for `anvil-sim`.
//!
//! Targets the regions left uncovered by the first coverage pass:
//! - `cluster.rs`: tick-skew capping, no-storage nodes, backfill/reconcile
//!   branches, header-gap/torn-tail restarts, stale and refused restarts,
//!   peer and archive repair flows, packet helper edges, trace draining.
//! - `workload.rs`: multi-seed sweeps (plain, storage, canary, traced),
//!   hand-built chaos plans covering every `FaultKind` begin/end branch,
//!   lingering faults that exercise the heal path, and direct
//!   `convergence_audit` branch tests.
//! - `device/sim_device.rs`: namespace op errors, read/write edge cases,
//!   injected EIO/corruption/fsync faults, latency drains, crash fates
//!   (torn/lost/persisted), and atlas veto/protect/immune paths.

use anvil_kv::{KvCommand, KvConfig, KvStore};
use anvil_raft::Input;
use anvil_sim::{
    DetRng, FaultEvent, FaultKind, Plan, PlanNet, SimCluster, SimDevice, SimDeviceConfig,
    StorageFaultAtlas, WorkloadSpec, convergence_audit, run_kv_plan, run_kv_plan_traced,
    run_kv_seed, run_kv_seed_with_stats, swarm_profile,
};
use anvil_storage::{
    CompletionPayload, Device, DeviceOp, FileId, LogRecoveryClassification, SectorSize,
    StorageError, WalRecordFrame, WalSegmentHeader,
};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, SeqNo, SessionId, Term};
use bytes::Bytes;
use std::collections::BTreeMap;

fn node(id: u64) -> NodeId {
    NodeId::new(id).expect("node id")
}

fn follower_of(cluster: &SimCluster, leader: NodeId) -> NodeId {
    cluster
        .nodes
        .keys()
        .copied()
        .find(|id| *id != leader)
        .expect("follower")
}

// ---------------------------------------------------------------------------
// Small test helpers.
// ---------------------------------------------------------------------------

struct OpCounter(u64);

impl OpCounter {
    fn next(&mut self) -> u64 {
        self.0 += 1;
        self.0
    }
}

fn drain_one(device: &mut SimDevice) -> Result<CompletionPayload, StorageError> {
    device.poll_completion().expect("completion due").result
}

fn drain_device(device: &mut SimDevice) {
    while device.poll_completion().is_some() {}
}

/// Creates a WAL segment with `count` single-sector frames starting at
/// `first`, then syncs it so the bytes are durable.
fn seed_wal_frames(device: &mut SimDevice, ops: &mut OpCounter, seg: u64, first: u64, count: u64) {
    let sector = SectorSize::Sector512;
    let file = FileId::wal_segment(seg);
    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512 * (count + 1),
    });
    assert_eq!(
        drain_one(device).expect("create"),
        CompletionPayload::Created
    );
    let header = WalSegmentHeader::new(sector, seg, LogIndex::from_raw(first), Term::from_raw(1));
    let mut header_buf = vec![0u8; sector.as_usize()];
    header.encode(&mut header_buf).expect("header encodes");
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::from(header_buf),
    });
    assert!(drain_one(device).is_ok());
    let mut crc = header.header_crc;
    for i in 0..count {
        let entry = Entry {
            index: LogIndex::from_raw(first + i),
            term: Term::from_raw(1),
            kind: EntryKind::Normal,
            payload: Bytes::from(vec![0xABu8; 16]),
        };
        let frame = WalRecordFrame::from_entry(&entry, crc, seg, 512 * (i + 1));
        crc = frame.crc32c;
        let mut frame_buf = Vec::new();
        frame.encode_frame(sector, &mut frame_buf);
        assert_eq!(frame_buf.len(), sector.as_usize());
        device.submit(DeviceOp::Write {
            op_id: ops.next(),
            file_id: file,
            offset: 512 * (i + 1),
            payload: Bytes::from(frame_buf),
        });
        assert!(drain_one(device).is_ok());
    }
    device.submit(DeviceOp::Fdatasync {
        op_id: ops.next(),
        file_id: file,
    });
    assert!(drain_one(device).is_ok());
}

fn workload_spec_small() -> WorkloadSpec {
    WorkloadSpec {
        clients: 2,
        keys: 2,
        ops_per_client: 2,
        list_pct: 0,
        think_ticks: 1,
        timeout_ticks: 25,
        client_crashes: false,
        unordered: false,
    }
}

fn workload_spec_spread() -> WorkloadSpec {
    WorkloadSpec {
        clients: 3,
        keys: 3,
        ops_per_client: 6,
        list_pct: 20,
        think_ticks: 30,
        timeout_ticks: 25,
        client_crashes: true,
        unordered: true,
    }
}

// ---------------------------------------------------------------------------
// SimDevice: namespace operations and error paths.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_device_namespace_ops() {
    let mut device = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(11, "core2-ns"),
    );
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);

    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512,
    });
    assert_eq!(
        drain_one(&mut device).expect("create"),
        CompletionPayload::Created
    );
    // Duplicate create is rejected.
    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut device).is_err());

    // Open succeeds for a known file and fails for a missing one.
    device.submit(DeviceOp::Open {
        op_id: ops.next(),
        file_id: file,
    });
    assert_eq!(
        drain_one(&mut device).expect("open"),
        CompletionPayload::Opened
    );
    device.submit(DeviceOp::Open {
        op_id: ops.next(),
        file_id: FileId::wal_segment(9),
    });
    assert!(drain_one(&mut device).is_err());

    // Rename moves the namespace entry and fails when the source is missing.
    let target = FileId::wal_segment(2);
    device.submit(DeviceOp::Rename {
        op_id: ops.next(),
        from: file,
        to: target,
    });
    assert_eq!(
        drain_one(&mut device).expect("rename"),
        CompletionPayload::Renamed
    );
    assert!(device.file_exists(target));
    assert!(!device.file_exists(file));
    device.submit(DeviceOp::Rename {
        op_id: ops.next(),
        from: file,
        to: FileId::wal_segment(3),
    });
    assert!(drain_one(&mut device).is_err());

    // Truncate succeeds on a live file and fails on a missing one.
    device.submit(DeviceOp::Truncate {
        op_id: ops.next(),
        file_id: target,
        len: 100,
    });
    assert_eq!(
        drain_one(&mut device).expect("truncate"),
        CompletionPayload::Truncated
    );
    device.submit(DeviceOp::Truncate {
        op_id: ops.next(),
        file_id: file,
        len: 10,
    });
    assert!(drain_one(&mut device).is_err());

    // Unlink drops the volatile entry (and its cached bytes) but keeps the
    // durable image, so durable reads fail with file-not-found.
    device.submit(DeviceOp::Unlink {
        op_id: ops.next(),
        file_id: target,
    });
    assert_eq!(
        drain_one(&mut device).expect("unlink"),
        CompletionPayload::Unlinked
    );
    assert!(!device.file_exists(target));
    assert!(device.get_file_volatile_bytes(target).is_none());
    assert!(
        device
            .get_file_volatile_bytes(FileId::wal_segment(77))
            .is_none()
    );
    assert!(device.read_durable(target).is_err());

    // Fdatasync on a missing file errors.
    device.submit(DeviceOp::Fdatasync {
        op_id: ops.next(),
        file_id: file,
    });
    assert!(drain_one(&mut device).is_err());

    // A live file exposes its volatile bytes.
    let live = FileId::wal_segment(4);
    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: live,
        preallocate_bytes: 64,
    });
    assert!(drain_one(&mut device).is_ok());
    assert!(device.get_file_volatile_bytes(live).is_some());
    assert!(device.get_file_durable_bytes(live).is_some());

    // ENOSPC when the preallocation exceeds capacity.
    let mut tight = SimDeviceConfig::for_test(SectorSize::Sector512);
    tight.disk_capacity_bytes = 100;
    let mut small = SimDevice::new(tight, DetRng::new(12, "core2-enospc"));
    small.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::wal_segment(1),
        preallocate_bytes: 512,
    });
    assert!(
        small
            .poll_completion()
            .expect("enospc completion")
            .result
            .is_err()
    );
}

#[test]
fn test_core2_device_read_write_edges() {
    let mut device = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(13, "core2-rw"),
    );
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);

    // Write to a missing file errors.
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::from_static(b"nope"),
    });
    assert!(drain_one(&mut device).is_err());

    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut device).is_ok());

    // Empty writes succeed without marking sectors dirty.
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::new(),
    });
    assert_eq!(
        drain_one(&mut device).expect("empty write"),
        CompletionPayload::Written(0)
    );

    let payload = Bytes::from_static(b"hello durable world");
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: payload.clone(),
    });
    assert_eq!(
        drain_one(&mut device).expect("write"),
        CompletionPayload::Written(payload.len())
    );
    device.submit(DeviceOp::Fdatasync {
        op_id: ops.next(),
        file_id: file,
    });
    assert_eq!(
        drain_one(&mut device).expect("sync"),
        CompletionPayload::Synced
    );

    // Normal read, past-end read (empty), and missing-file read.
    device.submit(DeviceOp::Read {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        len: payload.len(),
    });
    assert_eq!(
        drain_one(&mut device).expect("read"),
        CompletionPayload::Read(payload)
    );
    device.submit(DeviceOp::Read {
        op_id: ops.next(),
        file_id: file,
        offset: 100_000,
        len: 16,
    });
    assert_eq!(
        drain_one(&mut device).expect("past-end read"),
        CompletionPayload::Read(Bytes::new())
    );
    device.submit(DeviceOp::Read {
        op_id: ops.next(),
        file_id: FileId::wal_segment(9),
        offset: 0,
        len: 16,
    });
    assert!(drain_one(&mut device).is_err());

    // Durable read of a missing file errors.
    assert!(device.read_durable(FileId::wal_segment(9)).is_err());

    // SyncDir marks namespace entries durable.
    device.submit(DeviceOp::SyncDir { op_id: ops.next() });
    assert_eq!(
        drain_one(&mut device).expect("syncdir"),
        CompletionPayload::DirSynced
    );
    assert!(device.is_file_durable(file));
    assert_eq!(device.stats().sync_dirs_completed, 1);
}

// ---------------------------------------------------------------------------
// SimDevice: injected faults on every path.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_device_injected_faults() {
    // Latent read errors surface on both the volatile and durable paths.
    let mut eio_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    eio_config.read_eio_rate_ppm = 1_000_000;
    let mut eio = SimDevice::new(eio_config, DetRng::new(14, "core2-eio"));
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);
    eio.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut eio).is_ok());
    eio.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::from(vec![7u8; 64]),
    });
    assert!(drain_one(&mut eio).is_ok());
    eio.submit(DeviceOp::Read {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        len: 64,
    });
    assert!(drain_one(&mut eio).is_err());
    eio.submit(DeviceOp::Fdatasync {
        op_id: ops.next(),
        file_id: file,
    });
    assert!(drain_one(&mut eio).is_ok());
    assert!(eio.read_durable(file).is_err());
    assert!(eio.stats().read_eios_injected >= 2);

    // Fsync and SyncDir failures report `FsyncFailed`.
    let mut fsync_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    fsync_config.fsync_failure_rate_ppm = 1_000_000;
    let mut fsync = SimDevice::new(fsync_config, DetRng::new(15, "core2-fsync"));
    fsync.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut fsync).is_ok());
    fsync.submit(DeviceOp::Fdatasync {
        op_id: 2,
        file_id: file,
    });
    assert!(matches!(
        drain_one(&mut fsync),
        Err(StorageError::FsyncFailed { .. })
    ));
    fsync.submit(DeviceOp::SyncDir { op_id: 3 });
    assert!(matches!(
        drain_one(&mut fsync),
        Err(StorageError::FsyncFailed { .. })
    ));
    assert!(fsync.stats().fsync_failures_injected >= 2);
}

#[test]
fn test_core2_device_read_corruption_paths() {
    // Multi-sector file: the corruption offset is drawn from the RNG and a
    // valid frame index is recorded against the (absent) atlas.
    let mut device = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(16, "core2-rot"),
    );
    let mut ops = OpCounter(0);
    seed_wal_frames(&mut device, &mut ops, 1, 1, 2);
    device.config_mut().read_corruption_rate_ppm = 1_000_000;
    let before = device
        .get_file_durable_bytes(FileId::wal_segment(1))
        .expect("durable")
        .to_vec();
    assert!(device.read_durable(FileId::wal_segment(1)).is_ok());
    assert!(device.stats().read_corruptions_injected >= 1);
    let after = device
        .get_file_durable_bytes(FileId::wal_segment(1))
        .expect("durable");
    assert_ne!(before, after);

    // Single-sector file: the corruption lands at offset zero.
    let mut tiny = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(17, "core2-tiny"),
    );
    let small_file = FileId::wal_segment(5);
    tiny.submit(DeviceOp::Create {
        op_id: 1,
        file_id: small_file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut tiny).is_ok());
    tiny.submit(DeviceOp::Write {
        op_id: 2,
        file_id: small_file,
        offset: 0,
        payload: Bytes::from(vec![0x11u8; 4]),
    });
    assert!(drain_one(&mut tiny).is_ok());
    tiny.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: small_file,
    });
    assert!(drain_one(&mut tiny).is_ok());
    tiny.config_mut().read_corruption_rate_ppm = 1_000_000;
    // The sub-frame buffer has no decodable index, so nothing is recorded.
    assert!(tiny.read_durable(small_file).is_ok());
    assert_eq!(tiny.stats().read_corruptions_injected, 1);

    // Hard-state reads without an atlas still corrupt (allowed arm).
    let mut hard = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(18, "core2-hard"),
    );
    hard.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut hard).is_ok());
    hard.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        payload: Bytes::from(vec![0x22u8; 1024]),
    });
    assert!(drain_one(&mut hard).is_ok());
    hard.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: FileId::HARD_STATE,
    });
    assert!(drain_one(&mut hard).is_ok());
    hard.config_mut().read_corruption_rate_ppm = 1_000_000;
    assert!(hard.read_durable(FileId::HARD_STATE).is_ok());
    assert_eq!(hard.stats().read_corruptions_injected, 1);
}

#[test]
fn test_core2_device_read_corruption_atlas_veto_and_allow() {
    // A committed entry with no other copies vetoes read-path corruption.
    let atlas = StorageFaultAtlas::new(3);
    let me = node(1);
    let committed = LogIndex::from_raw(1);
    atlas.record_persisted(me, committed);
    atlas.advance_commit_index(committed);
    let mut veto_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    veto_config.read_corruption_rate_ppm = 1_000_000;
    let mut vetoed =
        SimDevice::new(veto_config, DetRng::new(19, "core2-veto")).with_atlas(atlas, me);
    let mut ops = OpCounter(0);
    seed_wal_frames(&mut vetoed, &mut ops, 1, 1, 1);
    assert!(vetoed.read_durable(FileId::wal_segment(1)).is_ok());
    // The single draw hits the header sector here: headers carry no entry
    // index, so the entry veto does not apply, and read-rot lands on it.
    // The entry frame itself stays veto-protected (see the atlas unit
    // test asserting can_corrupt_entry is false with no other copies).
    assert_eq!(vetoed.stats().read_corruptions_injected, 1);
    let raw = vetoed
        .get_file_durable_bytes(FileId::wal_segment(1))
        .expect("bytes")
        .to_vec();
    assert!(
        anvil_storage::WalSegmentHeader::decode(
            &raw[..SectorSize::Sector512.as_usize()],
            SectorSize::Sector512
        )
        .is_err(),
        "header sector took the rot"
    );

    // A permissive atlas allows hard-state damage.
    let mut allow_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    allow_config.read_corruption_rate_ppm = 1_000_000;
    let mut allowed = SimDevice::new(allow_config, DetRng::new(20, "core2-allow"))
        .with_atlas(StorageFaultAtlas::permissive(), me);
    allowed.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut allowed).is_ok());
    allowed.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        payload: Bytes::from(vec![0x33u8; 1024]),
    });
    assert!(drain_one(&mut allowed).is_ok());
    allowed.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: FileId::HARD_STATE,
    });
    assert!(drain_one(&mut allowed).is_ok());
    assert!(allowed.read_durable(FileId::HARD_STATE).is_ok());
    assert_eq!(allowed.stats().read_corruptions_injected, 1);
}

// ---------------------------------------------------------------------------
// SimDevice: latency, drains, and crash fates.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_device_latency_and_drains() {
    let mut config = SimDeviceConfig::with_latency(SectorSize::Sector512, 2, 5);
    config.fsync_stall_ticks = 4;
    let mut device = SimDevice::new(config, DetRng::new(21, "core2-lat"));
    let file = FileId::wal_segment(1);

    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(device.has_pending_completions());
    // Nothing is due yet: poll misses and the ready drain breaks early.
    assert!(device.poll_completion().is_none());
    assert!(device.drain_ready_completions().is_empty());

    device.step_time(30);
    let ready = device.drain_ready_completions();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].op_id, 1);

    // A sync op exercises the fsync-stall latency branch.
    device.submit(DeviceOp::Fdatasync {
        op_id: 2,
        file_id: file,
    });
    device.step_time(30);
    let forced = device.drain_all_completions_forcing();
    assert_eq!(forced.len(), 1);
    assert!(device.drain_all_completions_forcing().is_empty());
    assert!(!device.has_pending_completions());
}

#[test]
fn test_core2_device_crash_fates() {
    // Torn writes persist a resized durable image with a flipped torn half.
    let mut torn_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    torn_config.torn_write_rate_ppm = 1_000_000;
    torn_config.unsynced_sector_loss_rate_ppm = 0;
    let mut torn = SimDevice::new(torn_config, DetRng::new(22, "core2-torn"));
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);
    torn.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 2048,
    });
    assert!(drain_one(&mut torn).is_ok());
    torn.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::from(vec![0xAAu8; 512]),
    });
    assert!(drain_one(&mut torn).is_ok());
    torn.submit(DeviceOp::Fdatasync {
        op_id: ops.next(),
        file_id: file,
    });
    assert!(drain_one(&mut torn).is_ok());
    // An unsynced sector past the durable prefix forces the durable
    // resize path on crash. SyncDir first so directory loss cannot drop
    // the file before fate resolution runs.
    torn.submit(DeviceOp::SyncDir { op_id: ops.next() });
    assert!(drain_one(&mut torn).is_ok());
    torn.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 1536,
        payload: Bytes::from(vec![0xBBu8; 512]),
    });
    assert!(drain_one(&mut torn).is_ok());
    torn.crash();
    assert!(torn.stats().torn_sectors_persisted >= 1);
    assert!(torn.get_file_durable_bytes(file).expect("durable").len() >= 2048);

    // Lost sectors revert to the old durable image.
    let mut loss_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    loss_config.torn_write_rate_ppm = 0;
    loss_config.unsynced_sector_loss_rate_ppm = 1_000_000;
    let mut loss = SimDevice::new(loss_config, DetRng::new(23, "core2-loss"));
    let mut loss_ops = OpCounter(0);
    seed_wal_frames(&mut loss, &mut loss_ops, 1, 1, 1);
    loss.submit(DeviceOp::SyncDir {
        op_id: loss_ops.next(),
    });
    assert!(drain_one(&mut loss).is_ok());
    loss.submit(DeviceOp::Write {
        op_id: loss_ops.next(),
        file_id: FileId::wal_segment(1),
        offset: 512,
        payload: Bytes::from(vec![0xCCu8; 64]),
    });
    assert!(drain_one(&mut loss).is_ok());
    loss.crash();
    assert!(loss.stats().lost_sectors_discarded >= 1);

    // With both rates at zero every dirty sector persists whole.
    let mut calm_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    calm_config.torn_write_rate_ppm = 0;
    calm_config.unsynced_sector_loss_rate_ppm = 0;
    let mut calm = SimDevice::new(calm_config, DetRng::new(24, "core2-calm"));
    let mut calm_ops = OpCounter(0);
    seed_wal_frames(&mut calm, &mut calm_ops, 1, 1, 1);
    calm.submit(DeviceOp::SyncDir {
        op_id: calm_ops.next(),
    });
    assert!(drain_one(&mut calm).is_ok());
    calm.submit(DeviceOp::Write {
        op_id: calm_ops.next(),
        file_id: FileId::wal_segment(1),
        offset: 512,
        payload: Bytes::from(vec![0xDDu8; 64]),
    });
    assert!(drain_one(&mut calm).is_ok());
    calm.crash();
    assert_eq!(calm.stats().torn_sectors_persisted, 0);
    assert_eq!(calm.stats().lost_sectors_discarded, 0);

    // A dirty sector past the truncated volatile length is skipped.
    let mut trunc = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(25, "core2-trunc"),
    );
    let mut trunc_ops = OpCounter(0);
    trunc.submit(DeviceOp::Create {
        op_id: trunc_ops.next(),
        file_id: file,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut trunc).is_ok());
    trunc.submit(DeviceOp::Write {
        op_id: trunc_ops.next(),
        file_id: file,
        offset: 600,
        payload: Bytes::from(vec![0xEEu8; 64]),
    });
    assert!(drain_one(&mut trunc).is_ok());
    trunc.submit(DeviceOp::Truncate {
        op_id: trunc_ops.next(),
        file_id: file,
        len: 100,
    });
    assert!(drain_one(&mut trunc).is_ok());
    trunc.crash();

    // An all-zero preallocated file hits the zero-index break in the crash scan.
    let mut zeroes = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(26, "core2-zero"),
    );
    zeroes.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut zeroes).is_ok());
    zeroes.crash();

    // A frame header claiming a gigantic payload breaks the crash scan.
    let mut huge = SimDevice::new(
        SimDeviceConfig::for_test(SectorSize::Sector512),
        DetRng::new(27, "core2-huge"),
    );
    huge.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file,
        preallocate_bytes: 2048,
    });
    assert!(drain_one(&mut huge).is_ok());
    let mut bad_frame = 7u64.to_le_bytes().to_vec();
    bad_frame.extend_from_slice(&[0u8; 8]);
    bad_frame.extend_from_slice(&u32::MAX.to_le_bytes());
    bad_frame.extend_from_slice(&[0u8; 12]);
    huge.submit(DeviceOp::Write {
        op_id: 2,
        file_id: file,
        offset: 512,
        payload: Bytes::from(bad_frame),
    });
    assert!(drain_one(&mut huge).is_ok());
    huge.crash();
}

#[test]
fn test_core2_device_crash_unlinked_reinsert() {
    // An unsynced unlink is undone by the crash: the durable file reappears.
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    config.unsynced_dir_loss_rate_ppm = 1_000_000;
    let mut device = SimDevice::new(config, DetRng::new(28, "core2-unlink"));
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);
    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 512,
    });
    assert!(drain_one(&mut device).is_ok());
    device.submit(DeviceOp::SyncDir { op_id: ops.next() });
    assert!(drain_one(&mut device).is_ok());
    device.submit(DeviceOp::Unlink {
        op_id: ops.next(),
        file_id: file,
    });
    assert!(drain_one(&mut device).is_ok());
    assert!(!device.file_exists(file));
    device.crash();
    assert!(device.file_exists(file));
}

#[test]
fn test_core2_device_crash_atlas_file_loss_veto() {
    // Dropping the only surviving copy of a committed entry is vetoed.
    let atlas = StorageFaultAtlas::new(3);
    let me = node(1);
    let committed = LogIndex::from_raw(5);
    atlas.record_persisted(me, committed);
    atlas.advance_commit_index(committed);
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    config.unsynced_dir_loss_rate_ppm = 1_000_000;
    let mut device =
        SimDevice::new(config, DetRng::new(29, "core2-veto-loss")).with_atlas(atlas, me);
    let mut ops = OpCounter(0);
    seed_wal_frames(&mut device, &mut ops, 1, 5, 3);
    assert!(!device.is_file_durable(FileId::wal_segment(1)));
    device.crash();
    assert!(device.file_exists(FileId::wal_segment(1)));

    // ...while an uncommitted file with the same setup is still dropped.
    let atlas_two = StorageFaultAtlas::new(3);
    let mut config_two = SimDeviceConfig::for_test(SectorSize::Sector512);
    config_two.unsynced_dir_loss_rate_ppm = 1_000_000;
    let mut device_two =
        SimDevice::new(config_two, DetRng::new(30, "core2-drop")).with_atlas(atlas_two, me);
    let mut ops_two = OpCounter(0);
    seed_wal_frames(&mut device_two, &mut ops_two, 1, 50, 2);
    device_two.crash();
    assert!(!device_two.file_exists(FileId::wal_segment(1)));

    // Non-WAL unsynced creates bypass the quorum check entirely.
    let mut config_three = SimDeviceConfig::for_test(SectorSize::Sector512);
    config_three.unsynced_dir_loss_rate_ppm = 1_000_000;
    let mut device_three = SimDevice::new(config_three, DetRng::new(31, "core2-hs-loss"))
        .with_atlas(StorageFaultAtlas::new(3), me);
    device_three.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut device_three).is_ok());
    device_three.crash();
    assert!(!device_three.file_exists(FileId::HARD_STATE));
}

#[test]
fn test_core2_device_crash_atlas_protected_and_immune() {
    let me = node(1);
    let other = node(2);
    let atlas = StorageFaultAtlas::new(3);
    let committed = LogIndex::from_raw(5);
    atlas.record_persisted(me, committed);
    atlas.advance_commit_index(committed);

    // Atlas-protected frames persist whole even under a torn crash.
    let mut torn_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    torn_config.torn_write_rate_ppm = 1_000_000;
    torn_config.unsynced_sector_loss_rate_ppm = 0;
    let mut device =
        SimDevice::new(torn_config, DetRng::new(32, "core2-protect")).with_atlas(atlas, me);
    let mut ops = OpCounter(0);
    let file = FileId::wal_segment(1);
    device.submit(DeviceOp::Create {
        op_id: ops.next(),
        file_id: file,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut device).is_ok());
    let sector = SectorSize::Sector512;
    let header = WalSegmentHeader::new(sector, 1, LogIndex::from_raw(5), Term::from_raw(0));
    let mut header_buf = vec![0u8; sector.as_usize()];
    header.encode(&mut header_buf).expect("header encodes");
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 0,
        payload: Bytes::from(header_buf),
    });
    assert!(drain_one(&mut device).is_ok());
    device.submit(DeviceOp::SyncDir { op_id: ops.next() });
    assert!(drain_one(&mut device).is_ok());
    // The committed frame stays dirty: the crash must persist it whole
    // instead of tearing or losing it.
    let protected_entry = Entry {
        index: committed,
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![0x5Au8; 16]),
    };
    let protected_frame = WalRecordFrame::from_entry(&protected_entry, header.header_crc, 1, 512);
    let mut protected_buf = Vec::new();
    protected_frame.encode_frame(sector, &mut protected_buf);
    device.submit(DeviceOp::Write {
        op_id: ops.next(),
        file_id: file,
        offset: 512,
        payload: Bytes::from(protected_buf),
    });
    assert!(drain_one(&mut device).is_ok());
    device.crash();
    // Sector 0 tears (the only torn sector); the protected frame persists.
    assert_eq!(device.stats().torn_sectors_persisted, 1);
    assert!(device.get_file_durable_bytes(file).expect("durable").len() >= 1024);

    // A truncated volatile image exercises the short-sector continue arms.
    let mut trunc_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    trunc_config.torn_write_rate_ppm = 1_000_000;
    trunc_config.unsynced_sector_loss_rate_ppm = 0;
    let atlas_trunc = StorageFaultAtlas::new(3);
    atlas_trunc.record_persisted(me, committed);
    atlas_trunc.advance_commit_index(committed);
    let mut truncated =
        SimDevice::new(trunc_config, DetRng::new(33, "core2-ptrunc")).with_atlas(atlas_trunc, me);
    let mut trunc_ops = OpCounter(0);
    seed_wal_frames(&mut truncated, &mut trunc_ops, 1, 5, 2);
    // A fresh dirty frame past the truncation point exercises the
    // beyond-length skip in the crash resolver.
    let late_entry = Entry {
        index: LogIndex::from_raw(7),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![0x5Au8; 16]),
    };
    let late_frame = WalRecordFrame::from_entry(&late_entry, 0, 1, 1536);
    let mut late_buf = Vec::new();
    late_frame.encode_frame(SectorSize::Sector512, &mut late_buf);
    truncated.submit(DeviceOp::Write {
        op_id: trunc_ops.next(),
        file_id: file,
        offset: 1536,
        payload: Bytes::from(late_buf),
    });
    assert!(drain_one(&mut truncated).is_ok());
    truncated.submit(DeviceOp::Truncate {
        op_id: trunc_ops.next(),
        file_id: file,
        len: 100,
    });
    assert!(drain_one(&mut truncated).is_ok());
    truncated.crash();

    // Hard state is immune once another node already lost its copy.
    let immune_atlas = StorageFaultAtlas::new(3);
    immune_atlas.record_hard_state_dead(other);
    assert!(!immune_atlas.can_lose_hard_state(me));
    let mut immune_config = SimDeviceConfig::for_test(SectorSize::Sector512);
    immune_config.torn_write_rate_ppm = 1_000_000;
    let mut immune =
        SimDevice::new(immune_config, DetRng::new(34, "core2-immune")).with_atlas(immune_atlas, me);
    immune.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 1024,
    });
    assert!(drain_one(&mut immune).is_ok());
    immune.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        payload: Bytes::from(vec![0x44u8; 32]),
    });
    assert!(drain_one(&mut immune).is_ok());
    immune.crash();
    assert!(
        immune
            .get_file_durable_bytes(FileId::HARD_STATE)
            .expect("durable")
            .len()
            >= 32
    );
    // The immune copy also refuses read-path damage.
    immune.config_mut().read_corruption_rate_ppm = 1_000_000;
    assert!(immune.read_durable(FileId::HARD_STATE).is_ok());
    assert_eq!(immune.stats().read_corruptions_injected, 0);
}

// ---------------------------------------------------------------------------
// SimCluster: tick skew, tracing, packets, and storage-less nodes.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_cluster_tick_skew_cap() {
    let mut cluster = SimCluster::new(3, 10001);
    // Bypassing the validated setter pins a rate above the 4-step cap.
    cluster.tick_rate_per_mille.insert(node(1), 9000);
    cluster.tick_n(3);
    assert_eq!(cluster.current_tick, 3);
    assert_eq!(cluster.tick_rate(node(1)), 9000);
}

#[test]
fn test_core2_cluster_node_without_storage() {
    let mut cluster = SimCluster::new(3, 10002);
    let victim = node(2);
    let _ = cluster.storage.remove(&victim);

    assert!(cluster.engine_last_index(victim).is_none());
    assert!(!cluster.is_node_crashed(victim));
    // Sync, acknowledge, backfill, and reconcile all degrade gracefully.
    cluster.sync_node_storage_with_core(victim);
    cluster.sync_node_storage_with_core(node(99));
    cluster.acknowledge_token(node(99), 7);
    let entry = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"orphan"),
    };
    assert!(!cluster.backfill_log_prefix(victim, vec![entry]));
    assert!(cluster.backfill_log_prefix(victim, Vec::new()));
    cluster.reconcile_snapshot_prefix(victim, &BTreeMap::new(), LogIndex::from_raw(0));

    // A storage-less node still steps consensus and acks persistence inline.
    cluster.step_node(victim, Input::Tick);
    cluster.propose(victim, Bytes::from_static(b"no-storage propose"));
    cluster.tick_n(3);
    assert!(!cluster.is_node_crashed(victim));
}

#[test]
fn test_core2_cluster_pause_then_crash_resume_burst_breaks() {
    let mut cluster = SimCluster::new(3, 10003);
    let target = node(1);
    cluster.pause_node(target);
    assert!(cluster.is_paused(target));
    cluster.tick_n(3);
    cluster.crash_node(target);
    // The resume burst stops at the crashed node instead of stepping it.
    cluster.resume_node(target);
    assert!(!cluster.is_paused(target));
    assert!(cluster.is_node_crashed(target));
}

#[test]
fn test_core2_cluster_elect_twice_trace_and_crashed_inputs() {
    let mut cluster = SimCluster::new(3, 10004);
    cluster.trace_enabled = true;
    let leader = cluster.elect_leader(60);
    // A second election call returns the standing leader immediately.
    assert_eq!(cluster.elect_leader(60), leader);
    cluster.propose(leader, Bytes::from_static(b"trace me"));
    cluster.tick_n(10);
    assert!(!cluster.trace_lines.is_empty());
    // Ticks with tracing off clear scratch projections without recording.
    cluster.trace_enabled = false;
    cluster.tick_n(2);

    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    // Proposals and applied notices to crashed nodes are dropped silently.
    cluster.propose(follower, Bytes::from_static(b"dropped"));
    cluster.notify_applied(follower, LogIndex::from_raw(1));
    assert!(cluster.take_completed_reads(follower).is_empty());
}

#[test]
fn test_core2_cluster_packet_helpers_edges() {
    let mut cluster = SimCluster::new(3, 10005);
    // Empty-queue helpers all report false.
    assert!(!cluster.deliver_packet_at(0));
    assert!(!cluster.drop_packet_at(0));
    assert!(!cluster.duplicate_packet_at(0));

    let leader = cluster.elect_leader(60);
    cluster.plan_net = Some(PlanNet {
        salt: 9,
        drop_pct: 0,
        dup_pct: 50,
        max_delay: 5,
    });
    cluster.propose(leader, Bytes::from_static(b"delayed"));
    // Delayed packets wait in the queue until the cluster ticks.
    assert!(!cluster.network.is_empty());
    for _ in 0..10 {
        let _ = cluster.duplicate_packet_at(0);
        let _ = cluster.drop_packet_at(0);
        let _ = cluster.deliver_packet_at(0);
    }

    // Packets caught by a fresh partition are skipped, not delivered.
    cluster.propose(leader, Bytes::from_static(b"cut me"));
    cluster.partition(&[node(1)], &[node(2), node(3)]);
    cluster.tick_n(6);
    cluster.heal_all();

    // Packets addressed to freshly crashed nodes are skipped on both the
    // single-shot and the batched delivery paths; routing to them is cut.
    let others: Vec<NodeId> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|id| *id != leader)
        .collect();
    for id in &others {
        cluster.crash_node(*id);
    }
    cluster.propose(leader, Bytes::from_static(b"to crashed"));
    for _ in 0..10 {
        let _ = cluster.deliver_packet_at(0);
    }
    cluster.tick();
    cluster.plan_net = None;
    assert!(cluster.is_node_crashed(others[0]));
}

#[test]
fn test_core2_cluster_read_index_round_trip() {
    let mut cluster = SimCluster::new(3, 10006);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"read-idx"));
    cluster.tick_n(10);
    cluster.step_node(
        leader,
        Input::ReadIndex {
            context: Bytes::from_static(b"ctx-1"),
        },
    );
    cluster.tick_n(20);
    let reads = cluster.take_completed_reads(leader);
    assert!(!reads.is_empty());
    assert!(
        reads
            .iter()
            .any(|read| read.context == Bytes::from_static(b"ctx-1"))
    );
}

#[test]
fn test_core2_cluster_plan_net_routing() {
    let mut cluster = SimCluster::new(3, 10015);
    cluster.plan_net = Some(PlanNet {
        salt: 77,
        drop_pct: 0,
        dup_pct: 0,
        max_delay: 0,
    });
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"plan-net"));
    cluster.tick_n(5);
    assert!(cluster.highest_commit_index() >= LogIndex::from_raw(1));
    cluster.plan_net = None;
}

// ---------------------------------------------------------------------------
// SimCluster: restart classification branches.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_cluster_stale_hard_state_refused() {
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut cluster = SimCluster::new_with_storage_faults(3, 10007, &config);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"vote-seed"));
    cluster.tick_n(10);
    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    // Plant a future acknowledged vote the disk never saw: recovery must
    // refuse to resume instead of forgetting the vote.
    cluster
        .storage
        .get_mut(&follower)
        .expect("storage")
        .persisted_votes
        .insert(Term::from_raw(99), Some(leader));
    let error = cluster
        .restart_node(follower)
        .expect_err("stale hard state must refuse");
    assert!(matches!(error, StorageError::StaleHardState { .. }));
    assert!(cluster.is_node_refused_start(follower));
    assert!(cluster.is_node_crashed(follower));
}

#[test]
fn test_core2_cluster_both_slots_refused_with_atlas() {
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut cluster = SimCluster::new_with_storage_faults(3, 10008, &config);
    let leader = cluster.elect_leader(60);
    let follower = follower_of(&cluster, leader);
    cluster.propose(leader, Bytes::from_static(b"atlas-seed"));
    cluster.tick_n(10);
    cluster.crash_node(follower);

    let sector_size = SectorSize::Sector512.as_u64();
    {
        let storage = cluster.storage.get_mut(&follower).expect("storage");
        storage.device.submit(DeviceOp::Write {
            op_id: 7771,
            file_id: FileId::HARD_STATE,
            offset: 0,
            payload: Bytes::from(vec![0xBA, 0xAD, 0xF0, 0x0D]),
        });
        storage.device.submit(DeviceOp::Write {
            op_id: 7772,
            file_id: FileId::HARD_STATE,
            offset: sector_size,
            payload: Bytes::from(vec![0xBA, 0xAD, 0xF0, 0x0D]),
        });
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 7773,
            file_id: FileId::HARD_STATE,
        });
        drain_device(&mut storage.device);
    }
    let error = cluster
        .restart_node(follower)
        .expect_err("both slots corrupt must refuse");
    assert!(matches!(error, StorageError::CorruptHardStateBothSlots));
    assert!(cluster.is_node_refused_start(follower));
}

#[test]
fn test_core2_cluster_restart_fresh_node_with_config_fallback() {
    let mut cluster = SimCluster::new(3, 10009);
    let target = node(1);
    // Dropping the stored config forces the restart onto the fallback config.
    let _ = cluster.configs.remove(&target);
    cluster.crash_node(target);
    let recovered = cluster
        .restart_node(target)
        .expect("fresh restart succeeds");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::Clean
    ));
    assert!(!cluster.is_node_crashed(target));
    assert!(!cluster.is_node_needs_repair(target));
    cluster.tick_n(5);
}

#[test]
fn test_core2_cluster_torn_header_restart_rewrites_resume() {
    let mut cluster = SimCluster::new(3, 10010);
    let leader = cluster.elect_leader(60);
    for i in 1..=4u64 {
        cluster.propose(leader, Bytes::from(format!("torn-{i}")));
    }
    cluster.tick_n(15);
    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    // Destroy the segment header while the frames stay intact: the torn tail
    // is detected, but acknowledged entries past the recovered tip force a
    // mid-log sideline for peer repair.
    {
        let storage = cluster.storage.get_mut(&follower).expect("storage");
        storage.device.submit(DeviceOp::Write {
            op_id: 9001,
            file_id: FileId::wal_segment(1),
            offset: 0,
            payload: Bytes::from(vec![0x5Au8; 512]),
        });
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 9002,
            file_id: FileId::wal_segment(1),
        });
        drain_device(&mut storage.device);
    }
    let recovered = cluster
        .restart_node(follower)
        .expect("torn header restarts");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::MidLogCorruption { .. }
    ));
    assert!(cluster.is_node_needs_repair(follower));

    // Peer repair rebuilds the header from constants plus live peer terms,
    // then the healed node restarts clean and rejoins.
    assert!(cluster.storage.contains_key(&follower));
    cluster.repair_step(None);
    assert!(!cluster.is_node_needs_repair(follower));
    let recovered = cluster
        .restart_node(follower)
        .expect("repaired restart succeeds");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::Clean | LogRecoveryClassification::TornTail { .. }
    ));
    assert!(cluster.nodes.contains_key(&follower));
    cluster.tick_n(20);
    assert_eq!(
        cluster.nodes[&follower].commit_index(),
        cluster.nodes[&leader].commit_index()
    );
}

#[test]
fn test_core2_cluster_torn_tail_without_acked_beyond_tip() {
    let mut cluster = SimCluster::new(3, 10016);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"lonely"));
    cluster.tick_n(10);
    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    // Clearing the acknowledgment record lets the torn header classify as a
    // clean torn tail instead of mid-log corruption.
    cluster
        .storage
        .get_mut(&follower)
        .expect("storage")
        .acknowledged_entries
        .clear();
    {
        let storage = cluster.storage.get_mut(&follower).expect("storage");
        storage.device.submit(DeviceOp::Write {
            op_id: 9003,
            file_id: FileId::wal_segment(1),
            offset: 0,
            payload: Bytes::from(vec![0x5Au8; 512]),
        });
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 9004,
            file_id: FileId::wal_segment(1),
        });
        drain_device(&mut storage.device);
    }
    let recovered = cluster.restart_node(follower).expect("torn tail restarts");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::TornTail { .. }
    ));
    assert!(!cluster.is_node_crashed(follower));
    assert!(!cluster.is_node_needs_repair(follower));
    cluster.tick_n(15);
}

#[test]
fn test_core2_cluster_rebuild_header_without_junction_stays_sidelined() {
    let mut cluster = SimCluster::new(3, 10017);
    let leader = cluster.elect_leader(60);
    cluster.propose(leader, Bytes::from_static(b"first"));
    cluster.propose(leader, Bytes::from_static(b"second"));
    cluster.tick_n(10);
    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    // Corrupt the header and forge a first-frame index no live peer can
    // source a junction term for, then remove every potential source.
    {
        let storage = cluster.storage.get_mut(&follower).expect("storage");
        storage.device.submit(DeviceOp::Write {
            op_id: 9005,
            file_id: FileId::wal_segment(1),
            offset: 0,
            payload: Bytes::from(vec![0x5Au8; 512]),
        });
        let mut forged = 9u64.to_le_bytes().to_vec();
        forged.extend_from_slice(&[0x5Au8; 24]);
        storage.device.submit(DeviceOp::Write {
            op_id: 9006,
            file_id: FileId::wal_segment(1),
            offset: 512,
            payload: Bytes::from(forged),
        });
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 9007,
            file_id: FileId::wal_segment(1),
        });
        drain_device(&mut storage.device);
    }
    for id in cluster.nodes.keys().copied().collect::<Vec<_>>() {
        if id != follower {
            cluster.crash_node(id);
        }
    }
    let recovered = cluster.restart_node(follower).expect("scan completes");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::MidLogCorruption { .. }
    ));
    // No live peer and no archive: the header cannot be rebuilt, the file is
    // left untouched, and the node stays sidelined with a live but flagged
    // core that keeps following without voting.
    cluster.repair_step(None);
    assert!(cluster.is_node_needs_repair(follower));
    assert_eq!(
        cluster.nodes[&follower].repair_from(),
        cluster.storage[&follower].repair_from
    );
    cluster.repair_step(Some(&BTreeMap::new()));
    assert!(cluster.is_node_needs_repair(follower));
}

// ---------------------------------------------------------------------------
// SimCluster: peer and archive repair flows.
// ---------------------------------------------------------------------------

fn sideline_with_midlog_corruption(cluster: &mut SimCluster, target: NodeId) {
    cluster.crash_node(target);
    let sector_size = SectorSize::Sector512.as_u64();
    let mid_offset = sector_size * 2;
    {
        let storage = cluster.storage.get_mut(&target).expect("storage");
        storage.device.submit(DeviceOp::Write {
            op_id: 8881,
            file_id: FileId::wal_segment(1),
            offset: mid_offset,
            payload: Bytes::from(vec![0xFFu8; 64]),
        });
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 8882,
            file_id: FileId::wal_segment(1),
        });
        drain_device(&mut storage.device);
    }
    let recovered = cluster.restart_node(target).expect("scan completes");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::MidLogCorruption { .. }
    ));
    assert!(cluster.is_node_needs_repair(target));
}

#[test]
fn test_core2_cluster_repair_via_peers_with_lagging_follower() {
    let mut cluster = SimCluster::new(3, 10011);
    let leader = cluster.elect_leader(60);
    let all: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    let lagging = *all.iter().find(|id| **id != leader).expect("peer");
    let sidelined = *all
        .iter()
        .find(|id| **id != leader && **id != lagging)
        .expect("target");

    for i in 1..=4u64 {
        cluster.propose(leader, Bytes::from(format!("early-{i}")));
    }
    cluster.tick_n(15);
    // The lagging peer pauses with a covering-but-stale commit.
    cluster.pause_node(lagging);
    for i in 5..=8u64 {
        cluster.propose(leader, Bytes::from(format!("late-{i}")));
    }
    cluster.tick_n(15);
    assert!(cluster.highest_commit_index() >= LogIndex::from_raw(8));

    // A healthy node with a manually planted repair cursor finds nothing to
    // fix and keeps serving.
    cluster
        .storage
        .get_mut(&leader)
        .expect("storage")
        .repair_from = Some(LogIndex::from_raw(2));
    cluster.repair_step(None);
    assert!(!cluster.is_node_needs_repair(leader));
    assert!(cluster.nodes.contains_key(&leader));
    cluster
        .storage
        .get_mut(&leader)
        .expect("storage")
        .repair_from = None;

    sideline_with_midlog_corruption(&mut cluster, sidelined);

    // The leader supplies the corrupt range; the lagging peer is skipped as a
    // worse source.
    cluster.repair_step(None);
    assert!(!cluster.is_node_needs_repair(sidelined));

    cluster.resume_node(lagging);
    cluster
        .restart_node(sidelined)
        .expect("repaired restart succeeds");
    cluster.tick_n(40);
    let final_commit = cluster.nodes[&leader].commit_index();
    for id in cluster.nodes.keys() {
        assert_eq!(
            cluster.nodes[id].commit_index(),
            final_commit,
            "commit mismatch on {id}"
        );
    }
}

#[test]
fn test_core2_cluster_repair_via_archive_mixed_kinds() {
    let mut cluster = SimCluster::new(3, 10012);
    let leader = cluster.elect_leader(60);
    for i in 1..=8u64 {
        cluster.propose(leader, Bytes::from(format!("arch-{i}")));
    }
    cluster.tick_n(25);
    let follower = follower_of(&cluster, leader);
    let commit = cluster.nodes[&leader].commit_index();
    assert!(commit >= LogIndex::from_raw(8));

    // Corrupt two adjacent frames so the repair must source twice.
    cluster.crash_node(follower);
    {
        let storage = cluster.storage.get_mut(&follower).expect("storage");
        for (n, offset) in [1536u64, 2048u64].into_iter().enumerate() {
            storage.device.submit(DeviceOp::Write {
                op_id: 8891 + n as u64,
                file_id: FileId::wal_segment(1),
                offset,
                payload: Bytes::from(vec![0xFFu8; 64]),
            });
        }
        storage.device.submit(DeviceOp::Fdatasync {
            op_id: 8899,
            file_id: FileId::wal_segment(1),
        });
        drain_device(&mut storage.device);
    }
    let recovered = cluster.restart_node(follower).expect("scan completes");
    assert!(matches!(
        recovered.log.classification,
        LogRecoveryClassification::MidLogCorruption { .. }
    ));
    let repair_from = cluster.storage[&follower]
        .repair_from
        .expect("repair cursor");

    // The archive carries branch truth with real terms: a `None` payload for
    // the first corrupt index (NoOp arm) and real payloads after it.
    let mut archive: anvil_sim::CommittedArchive = BTreeMap::new();
    let mut cursor = repair_from.as_u64();
    let mut first = true;
    while cursor <= commit.as_u64() {
        let at = LogIndex::from_raw(cursor);
        let entry = cluster.nodes[&leader]
            .log()
            .entry_at(at)
            .expect("leader entry")
            .clone();
        let payload = if first {
            None
        } else {
            Some(entry.payload.clone())
        };
        archive.insert(at, (entry.term, entry.kind, payload));
        first = false;
        cursor += 1;
    }
    cluster.repair_step(Some(&archive));
    assert!(!cluster.is_node_needs_repair(follower));

    cluster
        .restart_node(follower)
        .expect("repaired restart succeeds");
    cluster.tick_n(30);
    assert_eq!(
        cluster.nodes[&follower].commit_index(),
        cluster.nodes[&leader].commit_index()
    );
}

#[test]
fn test_core2_cluster_repair_without_source_stays_sidelined() {
    let mut cluster = SimCluster::new(3, 10014);
    let leader = cluster.elect_leader(60);
    for i in 1..=8u64 {
        cluster.propose(leader, Bytes::from(format!("iso-{i}")));
    }
    cluster.tick_n(25);
    let follower = follower_of(&cluster, leader);

    sideline_with_midlog_corruption(&mut cluster, follower);
    // Remove every potential source: peers cannot supply the corrupt range.
    for id in cluster.nodes.keys().copied().collect::<Vec<_>>() {
        cluster.crash_node(id);
    }
    cluster.repair_step(None);
    assert!(cluster.is_node_needs_repair(follower));
    // Every core was crashed after sidelining, so none exists to flag.
    assert!(!cluster.nodes.contains_key(&follower));
    cluster.repair_step(Some(&BTreeMap::new()));
    assert!(cluster.is_node_needs_repair(follower));

    // Bring the peers back, let them re-commit, then repair succeeds.
    let peers: Vec<NodeId> = cluster
        .storage
        .keys()
        .copied()
        .filter(|id| *id != follower)
        .collect();
    for id in peers {
        cluster.restart_node(id).expect("peer restarts");
    }
    cluster.tick_n(25);
    cluster.repair_step(None);
    assert!(!cluster.is_node_needs_repair(follower));
    cluster
        .restart_node(follower)
        .expect("repaired restart succeeds");
    cluster.tick_n(30);
    let final_commit = cluster.highest_commit_index();
    for id in cluster.nodes.keys() {
        assert_eq!(cluster.nodes[id].commit_index(), final_commit);
    }
}

// ---------------------------------------------------------------------------
// SimCluster: backfill and reconcile branches.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_cluster_backfill_and_reconcile() {
    let mut cluster = SimCluster::new(3, 10013);
    let leader = cluster.elect_leader(60);
    for i in 1..=5u64 {
        cluster.propose(leader, Bytes::from(format!("rec-{i}")));
    }
    cluster.tick_n(15);

    // A matching archive finds no divergence and returns early.
    let mut matching: anvil_sim::CommittedArchive = BTreeMap::new();
    for raw in 1..=3u64 {
        let at = LogIndex::from_raw(raw);
        let term = cluster.storage[&leader]
            .engine
            .term_at(at)
            .expect("disk term");
        matching.insert(at, (term, anvil_types::EntryKind::NoOp, None));
    }
    cluster.reconcile_snapshot_prefix(leader, &matching, LogIndex::from_raw(3));

    // Reconcile on a crashed node returns before touching the disk.
    let follower = follower_of(&cluster, leader);
    cluster.crash_node(follower);
    cluster.reconcile_snapshot_prefix(follower, &BTreeMap::new(), LogIndex::from_raw(3));
    cluster.restart_node(follower).expect("restart");

    // Backfilling entries past the core tip succeeds without crashing.
    let extra: Vec<Entry> = (50..52u64)
        .map(|raw| Entry {
            index: LogIndex::from_raw(raw),
            term: Term::from_raw(1),
            kind: EntryKind::Normal,
            payload: Bytes::from(format!("extra-{raw}")),
        })
        .collect();
    assert!(cluster.backfill_log_prefix(follower, extra));
    assert!(cluster.engine_last_index(follower).is_some());
    assert!(!cluster.is_node_crashed(follower));

    // A conflicting archive restages the prefix from archive truth plus the
    // entries the core still holds, dropping frames neither side supplies.
    let mut conflicting: anvil_sim::CommittedArchive = BTreeMap::new();
    conflicting.insert(
        LogIndex::from_raw(1),
        (
            Term::from_raw(99),
            anvil_types::EntryKind::Normal,
            Some(Bytes::from_static(b"arch-1")),
        ),
    );
    conflicting.insert(
        LogIndex::from_raw(2),
        (Term::from_raw(99), anvil_types::EntryKind::NoOp, None),
    );
    cluster.reconcile_snapshot_prefix(follower, &conflicting, LogIndex::from_raw(3));
    assert!(!cluster.is_node_crashed(follower));
    assert!(cluster.engine_last_index(follower).is_some());

    // Empty backfills are no-ops that report the crash state.
    assert!(cluster.backfill_log_prefix(follower, Vec::new()));
}

// ---------------------------------------------------------------------------
// Workload: seed sweeps across every major path.
// ---------------------------------------------------------------------------

#[test]
fn test_core2_workload_seed_sweep_plain() {
    for seed in 0..12u64 {
        let profile = swarm_profile(seed);
        assert!(profile.clients >= 1);
        let (history, _audit) = run_kv_seed(seed, false);
        assert!(!history.ops.is_empty(), "seed {seed} must produce ops");
    }
}

#[test]
fn test_core2_workload_storage_stats_canary_and_traced() {
    for seed in [4u64, 8] {
        let (history, _audit, stats) = run_kv_seed_with_stats(seed, true);
        assert!(!history.ops.is_empty());
        assert!(stats.virtual_ticks > 0);
    }
    let (canary_history, _) = run_kv_seed(3, true);
    assert!(!canary_history.ops.is_empty());

    let workload_plan = Plan::generate_for_workload(4, false, workload_spec_small());
    let (traced_history, _audit, _stats, lines) = run_kv_plan_traced(&workload_plan, false);
    assert!(!traced_history.ops.is_empty());
    assert!(!lines.is_empty());
}

#[test]
fn test_core2_workload_swarm_profile_and_stats() {
    let first = swarm_profile(4242);
    let second = swarm_profile(4242);
    assert_eq!(first.clients, second.clients);
    assert_eq!(first.keys, second.keys);
    assert_eq!(first.ops_per_client, second.ops_per_client);

    let (history, audit, stats) = run_kv_seed_with_stats(6, false);
    assert!(!history.ops.is_empty());
    assert!(audit);
    assert!(stats.virtual_ticks > 0);

    let (storage_history, _storage_audit, storage_stats) = run_kv_seed_with_stats(7, true);
    assert!(!storage_history.ops.is_empty());
    assert!(storage_stats.virtual_ticks > 0);
}

// ---------------------------------------------------------------------------
// Workload: hand-built fault plans for every begin/end branch.
// ---------------------------------------------------------------------------

fn custom_chaos_plan() -> Plan {
    let mut plan = Plan::generate_for_workload(4242, false, workload_spec_spread());
    plan.drop_pct = 0;
    plan.dup_pct = 0;
    plan.max_delay = 0;
    plan.events = vec![
        FaultEvent {
            id: 0,
            start: 50,
            end: 150,
            fault: FaultKind::Partition { member: 0 },
        },
        FaultEvent {
            id: 1,
            start: 100,
            end: 200,
            fault: FaultKind::Partition { member: 1 },
        },
        FaultEvent {
            id: 2,
            start: 60,
            end: 120,
            fault: FaultKind::Asymmetric { from: 0, to: 1 },
        },
        FaultEvent {
            id: 3,
            start: 60,
            end: 120,
            fault: FaultKind::Crash { node: 0 },
        },
        FaultEvent {
            id: 4,
            start: 130,
            end: 180,
            fault: FaultKind::Crash { node: 1 },
        },
        FaultEvent {
            id: 5,
            start: 140,
            end: 170,
            fault: FaultKind::Crash { node: 2 },
        },
        FaultEvent {
            id: 6,
            start: 140,
            end: 170,
            fault: FaultKind::Crash { node: 0 },
        },
        FaultEvent {
            id: 7,
            start: 60,
            end: 120,
            fault: FaultKind::Pause { node: 1 },
        },
        FaultEvent {
            id: 8,
            start: 60,
            end: 120,
            fault: FaultKind::Pause { node: 99 },
        },
        FaultEvent {
            id: 9,
            start: 60,
            end: 120,
            fault: FaultKind::SlowClock {
                node: 2,
                rate_per_mille: 500,
            },
        },
        FaultEvent {
            id: 10,
            start: 60,
            end: 120,
            fault: FaultKind::SlowClock {
                node: 99,
                rate_per_mille: 500,
            },
        },
        FaultEvent {
            id: 11,
            start: 30,
            end: 31,
            fault: FaultKind::Transfer { node: 0 },
        },
        FaultEvent {
            id: 12,
            start: 32,
            end: 33,
            fault: FaultKind::Transfer { node: 1 },
        },
        FaultEvent {
            id: 13,
            start: 34,
            end: 35,
            fault: FaultKind::Transfer { node: 2 },
        },
        FaultEvent {
            id: 14,
            start: 210,
            end: 211,
            fault: FaultKind::Transfer { node: 0 },
        },
        FaultEvent {
            id: 15,
            start: 220,
            end: 221,
            fault: FaultKind::Transfer { node: 1 },
        },
        FaultEvent {
            id: 16,
            start: 230,
            end: 231,
            fault: FaultKind::Transfer { node: 2 },
        },
        FaultEvent {
            id: 17,
            start: 60,
            end: 120,
            fault: FaultKind::DelaySpike { extra_ticks: 5 },
        },
        FaultEvent {
            id: 18,
            start: 60,
            end: 120,
            fault: FaultKind::Crash { node: 99 },
        },
        FaultEvent {
            id: 19,
            start: 60,
            end: 120,
            fault: FaultKind::Partition { member: 99 },
        },
    ];
    plan
}

#[test]
fn test_core2_workload_chaos_plan_all_faults() {
    let plan = custom_chaos_plan();
    let (history, _audit, stats) = run_kv_plan(&plan, false);
    assert!(!history.ops.is_empty());
    assert!(stats.virtual_ticks > 0);

    let (_, _, _, lines) = run_kv_plan_traced(&plan, false);
    assert!(!lines.is_empty());

    // A consensus plan without an embedded workload falls back to the seed's
    // swarm profile on both entry points.
    let plain = Plan::generate(4242, false);
    let (plain_history, _, _) = run_kv_plan(&plain, false);
    assert!(!plain_history.ops.is_empty());
    let (traced_history, _, _, traced_lines) = run_kv_plan_traced(&plain, false);
    assert!(!traced_history.ops.is_empty());
    assert!(!traced_lines.is_empty());
}

fn lingering_fault_plan() -> Plan {
    let mut plan = Plan::generate_for_workload(4343, false, workload_spec_spread());
    plan.drop_pct = 0;
    plan.dup_pct = 0;
    plan.max_delay = 0;
    plan.events = vec![
        FaultEvent {
            id: 0,
            start: 50,
            end: 1000,
            fault: FaultKind::Pause { node: 0 },
        },
        FaultEvent {
            id: 1,
            start: 50,
            end: 1000,
            fault: FaultKind::Crash { node: 1 },
        },
        FaultEvent {
            id: 2,
            start: 50,
            end: 1000,
            fault: FaultKind::SlowClock {
                node: 2,
                rate_per_mille: 500,
            },
        },
        FaultEvent {
            id: 3,
            start: 40,
            end: 41,
            fault: FaultKind::Transfer { node: 0 },
        },
        FaultEvent {
            id: 4,
            start: 60,
            end: 120,
            fault: FaultKind::DelaySpike { extra_ticks: 5 },
        },
    ];
    plan
}

#[test]
fn test_core2_workload_lingering_faults_heal_path() {
    // Pauses, crashes, and clock skew outlive the run horizon, so the heal
    // phase itself resumes, restarts, and restores tick rates.
    let plan = lingering_fault_plan();
    let (history, _audit, stats) = run_kv_plan(&plan, false);
    assert!(!history.ops.is_empty());
    assert!(stats.virtual_ticks > 0);
}

// ---------------------------------------------------------------------------
// Workload: direct convergence-audit branch tests.
// ---------------------------------------------------------------------------

fn empty_stores_and_applied(
    ids: &[NodeId],
) -> (BTreeMap<NodeId, KvStore>, BTreeMap<NodeId, LogIndex>) {
    let stores = ids
        .iter()
        .map(|id| (*id, KvStore::new(KvConfig::default())))
        .collect();
    let applied = ids.iter().map(|id| (*id, LogIndex::from_raw(0))).collect();
    (stores, applied)
}

#[test]
fn test_core2_workload_convergence_audit_branches() {
    let cluster = SimCluster::new(3, 10020);
    let ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();
    let (stores, applied) = empty_stores_and_applied(&ids);
    assert!(convergence_audit(&cluster, &stores, &applied));

    // Divergent applied indexes fail the audit.
    let (stores_same, mut applied_mismatch) = empty_stores_and_applied(&ids);
    applied_mismatch.insert(ids[1], LogIndex::from_raw(3));
    assert!(!convergence_audit(
        &cluster,
        &stores_same,
        &applied_mismatch
    ));

    // Equal applied indexes with divergent state hashes fail the audit.
    let (mut stores_diverged, applied_same) = empty_stores_and_applied(&ids);
    {
        let store = stores_diverged.get_mut(&ids[0]).expect("store");
        let mut register = Vec::new();
        KvCommand::RegisterSession.encode(&mut register);
        store
            .apply(LogIndex::from_raw(1), &register)
            .expect("register session");
        let mut put = Vec::new();
        KvCommand::Put {
            session: SessionId::from_raw(1),
            seq: SeqNo::from_raw(1),
            acked: SeqNo::from_raw(0),
            key: Bytes::from_static(b"k0000"),
            value: Bytes::from_static(b"v"),
        }
        .encode(&mut put);
        store.apply(LogIndex::from_raw(2), &put).expect("put");
    }
    assert!(!convergence_audit(
        &cluster,
        &stores_diverged,
        &applied_same
    ));

    // No live nodes converge vacuously.
    let mut dead = SimCluster::new(3, 10021);
    let dead_ids: Vec<NodeId> = dead.nodes.keys().copied().collect();
    for id in &dead_ids {
        dead.crash_node(*id);
    }
    let (dead_stores, dead_applied) = empty_stores_and_applied(&dead_ids);
    assert!(convergence_audit(&dead, &dead_stores, &dead_applied));
}
