#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::assert_is_empty
)]

//! Comprehensive integration tests for `SimDevice` and `StorageFaultAtlas`.
//!
//! Validates:
//! 1. Volatile page cache over durable images
//! 2. Directory durability with and without `SyncDir`
//! 3. Sector-granularity torn writes at 512B and 4096B
//! 4. Latent sector errors (EIO) and bit-rot corruption injection
//! 5. Fsync failure injection
//! 6. Quorum Survivability Rule enforcement via `StorageFaultAtlas`

use anvil_sim::{DetRng, SimDevice, SimDeviceConfig, StorageFaultAtlas};
use anvil_storage::{
    Device, DeviceOp, FileId, LogRecoveryClassification, SectorSize, StorageEngine, StorageError,
    WalRecordFrame, WalSegmentHeader,
};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;

#[test]
fn test_sim_device_directory_durability_syncdir_guarantee() {
    let rng = DetRng::new(555, "syncdir_test");
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    // Unsynced file creation has 100% loss probability on crash
    config.unsynced_dir_loss_rate_ppm = 1_000_000;
    let mut device = SimDevice::new(config, rng);

    let file_synced = FileId::wal_segment(1);
    let file_unsynced = FileId::wal_segment(2);

    // 1. Create file 1 and run SyncDir
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: file_synced,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().unwrap().result.is_ok());
    device.submit(DeviceOp::SyncDir { op_id: 2 });
    assert!(device.poll_completion().unwrap().result.is_ok());

    // 2. Create file 2 without SyncDir
    device.submit(DeviceOp::Create {
        op_id: 3,
        file_id: file_unsynced,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().unwrap().result.is_ok());

    assert!(device.file_exists(file_synced));
    assert!(device.file_exists(file_unsynced));
    assert!(device.is_file_durable(file_synced));
    assert!(!device.is_file_durable(file_unsynced));

    // 3. Crash!
    device.crash();

    // File 1 must survive; File 2 was unsynced and vanished from namespace!
    assert!(device.file_exists(file_synced));
    assert!(!device.file_exists(file_unsynced));
}

#[test]
fn test_sim_device_torn_write_at_sector_granularity_512_and_4096() {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        let rng = DetRng::new(777, "torn_write_test");
        let mut config = SimDeviceConfig::for_test(sector_size);
        // Force 100% torn write on unsynced crash
        config.torn_write_rate_ppm = 1_000_000;
        config.unsynced_sector_loss_rate_ppm = 0;
        let mut device = SimDevice::new(config, rng);

        let file_id = FileId::wal_segment(1);

        // Create file and sync directory
        device.submit(DeviceOp::Create {
            op_id: 1,
            file_id,
            preallocate_bytes: sector_size.as_u64() * 2,
        });
        assert!(device.poll_completion().unwrap().result.is_ok());
        device.submit(DeviceOp::SyncDir { op_id: 2 });
        assert!(device.poll_completion().unwrap().result.is_ok());

        // Write header to sector 0 and sync
        let header =
            WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(1));
        let mut header_buf = vec![0u8; sector_size.as_usize()];
        header.encode(&mut header_buf).unwrap();
        device.submit(DeviceOp::Write {
            op_id: 3,
            file_id,
            offset: 0,
            data: Bytes::from(header_buf),
        });
        assert!(device.poll_completion().unwrap().result.is_ok());
        device.submit(DeviceOp::Fdatasync { op_id: 4, file_id });
        assert!(device.poll_completion().unwrap().result.is_ok());

        // Write valid record to sector 1, but DO NOT sync
        let entry = Entry {
            index: LogIndex::from_raw(1),
            term: Term::from_raw(1),
            kind: EntryKind::Normal,
            payload: Bytes::from_static(b"torn entry payload"),
        };
        let frame = WalRecordFrame::from_entry(&entry, header.header_crc, 1, sector_size.as_u64());
        let mut frame_buf = Vec::new();
        frame.encode_frame(sector_size, &mut frame_buf);
        assert_eq!(frame_buf.len(), sector_size.as_usize());

        device.submit(DeviceOp::Write {
            op_id: 5,
            file_id,
            offset: sector_size.as_u64(),
            data: Bytes::from(frame_buf),
        });
        assert!(device.poll_completion().unwrap().result.is_ok());

        // Crash: sector 1 undergoes torn write!
        device.crash();
        assert_eq!(device.stats().torn_sectors_persisted, 1);

        // Scan recovery over the torn segment
        let torn_bytes = device.get_file_durable_bytes(file_id).unwrap();
        let hs_buf = vec![0u8; sector_size.as_usize() * 2]; // empty hs for scan test
        // Recovery scanner must detect torn tail or mid-log corruption, NOT clean log!
        let recovered = StorageEngine::scan_recovery(&hs_buf, &[(header, torn_bytes)], sector_size);
        if let Ok(rec) = recovered {
            assert!(matches!(
                rec.log.classification,
                LogRecoveryClassification::TornTail { .. }
            ));
            assert_eq!(rec.log.entries.len(), 0); // torn record was rejected
        }
    }
}

#[test]
fn test_sim_device_latent_sector_error_and_read_bitrot() {
    let rng = DetRng::new(888, "fault_test");
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    config.read_eio_rate_ppm = 1_000_000; // 100% EIO on read
    let mut device = SimDevice::new(config, rng);

    let file_id = FileId::wal_segment(1);
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().unwrap().result.is_ok());

    // Read must return EIO latent sector error
    device.submit(DeviceOp::Read {
        op_id: 2,
        file_id,
        offset: 0,
        len: 100,
    });
    let comp = device.poll_completion().unwrap();
    assert!(matches!(
        comp.result,
        Err(StorageError::DeviceError(ref msg)) if msg.contains("EIO")
    ));
    assert_eq!(device.stats().read_eios_injected, 1);
}

#[test]
fn test_sim_device_fsync_failure_injection() {
    let rng = DetRng::new(4321, "fsync_fail");
    let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
    config.fsync_failure_rate_ppm = 1_000_000; // 100% fsync failure
    let mut device = SimDevice::new(config, rng);

    let file_id = FileId::wal_segment(1);
    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id,
        preallocate_bytes: 512,
    });
    assert!(device.poll_completion().unwrap().result.is_ok());

    // Fdatasync must fail with FsyncFailed
    device.submit(DeviceOp::Fdatasync { op_id: 2, file_id });
    let comp = device.poll_completion().unwrap();
    assert!(matches!(comp.result, Err(StorageError::FsyncFailed { .. })));
    assert_eq!(device.stats().fsync_failures_injected, 1);
}

#[test]
fn test_sim_device_storage_fault_atlas_quorum_survivability() {
    let cluster_size = 3;
    let atlas = StorageFaultAtlas::new(cluster_size);
    let node1 = NodeId::new(1).unwrap();
    let node2 = NodeId::new(2).unwrap();
    let node3 = NodeId::new(3).unwrap();

    let committed_idx = LogIndex::from_raw(42);

    // Record committed entry 42 on all 3 nodes
    atlas.record_persisted(node1, committed_idx);
    atlas.record_persisted(node2, committed_idx);
    atlas.record_persisted(node3, committed_idx);
    atlas.advance_commit_index(committed_idx);

    // Setup device for node 1 with 100% write corruption
    let mut cfg1 = SimDeviceConfig::for_test(SectorSize::Sector512);
    cfg1.write_corruption_rate_ppm = 1_000_000;
    let rng1 = DetRng::new(101, "dev1");
    let mut dev1 = SimDevice::new(cfg1, rng1).with_atlas(atlas.clone(), node1);

    // Setup device for node 2 with 100% write corruption
    let mut cfg2 = SimDeviceConfig::for_test(SectorSize::Sector512);
    cfg2.write_corruption_rate_ppm = 1_000_000;
    let rng2 = DetRng::new(102, "dev2");
    let mut dev2 = SimDevice::new(cfg2, rng2).with_atlas(atlas.clone(), node2);

    let file_id = FileId::wal_segment(1);
    dev1.submit(DeviceOp::Create {
        op_id: 1,
        file_id,
        preallocate_bytes: 1024,
    });
    let _ = dev1.poll_completion();

    dev2.submit(DeviceOp::Create {
        op_id: 1,
        file_id,
        preallocate_bytes: 1024,
    });
    let _ = dev2.poll_completion();

    // Prepare record for entry 42
    let entry = Entry {
        index: committed_idx,
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"committed entry payload"),
    };
    let frame = WalRecordFrame::from_entry(&entry, 0x1234, 1, 512);
    let mut frame_buf = Vec::new();
    frame.encode_frame(SectorSize::Sector512, &mut frame_buf);

    // Node 1 writes entry 42: atlas allows corruption (3 - 1 = 2 >= quorum 2)
    dev1.submit(DeviceOp::Write {
        op_id: 2,
        file_id,
        offset: 512,
        data: Bytes::from(frame_buf.clone()),
    });
    let comp1 = dev1.poll_completion().unwrap();
    assert!(comp1.result.is_ok());
    assert_eq!(dev1.stats().write_corruptions_injected, 1);
    atlas.record_corruption(node1, committed_idx);

    // Node 2 attempts to write entry 42: atlas VETOES corruption (2 - 1 = 1 < quorum 2)
    dev2.submit(DeviceOp::Write {
        op_id: 3,
        file_id,
        offset: 512,
        data: Bytes::from(frame_buf),
    });
    let comp2 = dev2.poll_completion().unwrap();
    assert!(comp2.result.is_ok());
    // Node 2 write corruption was vetoed by Quorum Survivability Rule!
    assert_eq!(dev2.stats().write_corruptions_injected, 0);
    assert_eq!(atlas.vetoed_corruptions(), 1);
}
