#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::too_many_lines
)]

//! Differential testing harness: `FsDevice` vs `SimDevice`.
//!
//! Submits identical operation sequences against `FsDevice` (real physical files
//! on a dedicated I/O thread) and `SimDevice` (with faults disabled).
//! Asserts identical completion outcomes and byte-for-byte identical on-disk states.

use anvil_device_fs::FsDevice;
use anvil_sim::{DetRng, SimDevice, SimDeviceConfig};
use anvil_storage::{Device, DeviceOp, FileId, SectorSize};
use bytes::Bytes;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

static TEST_DIR_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Helper to create a clean isolated temporary directory for testing.
fn create_test_dir(test_name: &str) -> PathBuf {
    let mut dir = std::env::temp_dir();
    let pid = std::process::id();
    let seq = TEST_DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    dir.push(format!("anvil-test-{test_name}-{pid}-{seq}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Helper to clean up a test directory on completion.
fn cleanup_test_dir(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn test_differential_basic_operations() {
    let test_dir = create_test_dir("basic");
    let mut fs_dev = FsDevice::open(&test_dir).expect("open FsDevice");

    let rng = DetRng::new(1001, "diff_basic");
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut sim_dev = SimDevice::new(config, rng);

    let file1 = FileId::wal_segment(1);
    let file2 = FileId::HARD_STATE;

    let ops = vec![
        // 1. Create file 1 with 4096 bytes preallocation
        DeviceOp::Create {
            op_id: 1,
            file_id: file1,
            preallocate_bytes: 4096,
        },
        // 2. Sync directory
        DeviceOp::SyncDir { op_id: 2 },
        // 3. Write 512 bytes at offset 0
        DeviceOp::Write {
            op_id: 3,
            file_id: file1,
            offset: 0,
            data: Bytes::from(vec![0xAAu8; 512]),
        },
        // 4. Write 256 bytes at offset 1024
        DeviceOp::Write {
            op_id: 4,
            file_id: file1,
            offset: 1024,
            data: Bytes::from(vec![0xBBu8; 256]),
        },
        // 5. Read back first write
        DeviceOp::Read {
            op_id: 5,
            file_id: file1,
            offset: 0,
            len: 512,
        },
        // 6. Read back second write
        DeviceOp::Read {
            op_id: 6,
            file_id: file1,
            offset: 1024,
            len: 256,
        },
        // 7. Fdatasync file 1
        DeviceOp::Fdatasync {
            op_id: 7,
            file_id: file1,
        },
        // 8. Create file 2 (Hard State) with 1024 bytes
        DeviceOp::Create {
            op_id: 8,
            file_id: file2,
            preallocate_bytes: 1024,
        },
        // 9. Write to Hard State slot 0
        DeviceOp::Write {
            op_id: 9,
            file_id: file2,
            offset: 0,
            data: Bytes::from(vec![0x55u8; 512]),
        },
        // 10. Sync Hard State
        DeviceOp::Fdatasync {
            op_id: 10,
            file_id: file2,
        },
    ];

    for op in ops {
        sim_dev.submit(op.clone());
        fs_dev.submit(op);

        let sim_comp = sim_dev.poll_completion().expect("sim completion");
        let fs_comp = fs_dev.wait_for_completion().expect("fs completion");

        assert_eq!(sim_comp.op_id, fs_comp.op_id);
        match (&sim_comp.result, &fs_comp.result) {
            (Ok(sim_payload), Ok(fs_payload)) => {
                assert_eq!(
                    sim_payload, fs_payload,
                    "payload mismatch for op {}",
                    sim_comp.op_id
                );
            }
            (Err(sim_err), Err(fs_err)) => {
                assert_eq!(
                    std::mem::discriminant(sim_err),
                    std::mem::discriminant(fs_err),
                    "error mismatch for op {}",
                    sim_comp.op_id
                );
            }
            (sim_res, fs_res) => {
                panic!(
                    "Outcome divergence on op {}: sim={sim_res:?}, fs={fs_res:?}",
                    sim_comp.op_id
                );
            }
        }
    }

    // Verify on-disk byte equivalence for durable files
    let sim_file1_bytes = sim_dev.get_file_durable_bytes(file1).unwrap();
    let fs_file1_path = anvil_device_fs::file_id_to_path(&test_dir, file1);
    let fs_file1_bytes = std::fs::read(&fs_file1_path).unwrap();
    assert_eq!(
        sim_file1_bytes,
        fs_file1_bytes.as_slice(),
        "durable bytes mismatch for file 1"
    );

    let sim_file2_bytes = sim_dev.get_file_durable_bytes(file2).unwrap();
    let fs_file2_path = anvil_device_fs::file_id_to_path(&test_dir, file2);
    let fs_file2_bytes = std::fs::read(&fs_file2_path).unwrap();
    assert_eq!(
        sim_file2_bytes,
        fs_file2_bytes.as_slice(),
        "durable bytes mismatch for file 2"
    );

    cleanup_test_dir(&test_dir);
}

#[test]
fn test_differential_rename_truncate_unlink() {
    let test_dir = create_test_dir("rename_trunc");
    let mut fs_dev = FsDevice::open(&test_dir).expect("open FsDevice");

    let rng = DetRng::new(2002, "diff_rename");
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut sim_dev = SimDevice::new(config, rng);

    let from_file = FileId(100);
    let to_file = FileId(101);

    let ops = vec![
        DeviceOp::Create {
            op_id: 1,
            file_id: from_file,
            preallocate_bytes: 2048,
        },
        DeviceOp::SyncDir { op_id: 2 },
        DeviceOp::Write {
            op_id: 3,
            file_id: from_file,
            offset: 0,
            data: Bytes::from_static(b"original data before rename"),
        },
        DeviceOp::Fdatasync {
            op_id: 4,
            file_id: from_file,
        },
        // Rename from 100 to 101
        DeviceOp::Rename {
            op_id: 5,
            from: from_file,
            to: to_file,
        },
        DeviceOp::SyncDir { op_id: 6 },
        // Read from renamed file
        DeviceOp::Read {
            op_id: 7,
            file_id: to_file,
            offset: 0,
            len: 27,
        },
        // Truncate to 10 bytes
        DeviceOp::Truncate {
            op_id: 8,
            file_id: to_file,
            len: 10,
        },
        DeviceOp::Read {
            op_id: 9,
            file_id: to_file,
            offset: 0,
            len: 27,
        },
        // Unlink to_file
        DeviceOp::Unlink {
            op_id: 10,
            file_id: to_file,
        },
        DeviceOp::SyncDir { op_id: 11 },
    ];

    for op in ops {
        sim_dev.submit(op.clone());
        fs_dev.submit(op);

        let sim_comp = sim_dev.poll_completion().expect("sim completion");
        let fs_comp = fs_dev.wait_for_completion().expect("fs completion");

        assert_eq!(sim_comp.op_id, fs_comp.op_id);
        assert_eq!(sim_comp.result, fs_comp.result);
    }

    // Both should report unlinked file does not exist
    let fs_path = anvil_device_fs::file_id_to_path(&test_dir, to_file);
    assert!(!fs_path.exists());
    assert!(!sim_dev.file_exists(to_file));

    cleanup_test_dir(&test_dir);
}

#[test]
fn test_differential_randomized_workload() {
    let test_dir = create_test_dir("randomized");
    let mut fs_dev = FsDevice::open(&test_dir).expect("open FsDevice");

    let rng = DetRng::new(3003, "diff_random");
    let config = SimDeviceConfig::for_test(SectorSize::Sector512);
    let mut sim_dev = SimDevice::new(config, rng);

    let mut workload_rng = DetRng::new(4004, "workload_gen");
    let file_id = FileId::wal_segment(10);

    // Initial create
    let create_op = DeviceOp::Create {
        op_id: 1,
        file_id,
        preallocate_bytes: 64 * 1024,
    };
    sim_dev.submit(create_op.clone());
    fs_dev.submit(create_op);
    assert_eq!(
        sim_dev.poll_completion().unwrap().result,
        fs_dev.wait_for_completion().unwrap().result
    );

    let mut op_id = 2u64;
    let mut current_offset = 0u64;

    // Execute 50 sequential appends and periodic syncs
    for i in 0..50 {
        let payload_len = workload_rng.gen_range(64..512) as usize;
        let payload = vec![(i as u8).wrapping_add(1); payload_len];

        let write_op = DeviceOp::Write {
            op_id,
            file_id,
            offset: current_offset,
            data: Bytes::from(payload.clone()),
        };
        op_id += 1;
        sim_dev.submit(write_op.clone());
        fs_dev.submit(write_op);
        assert_eq!(
            sim_dev.poll_completion().unwrap().result,
            fs_dev.wait_for_completion().unwrap().result
        );

        // Read back
        let read_op = DeviceOp::Read {
            op_id,
            file_id,
            offset: current_offset,
            len: payload_len,
        };
        op_id += 1;
        sim_dev.submit(read_op.clone());
        fs_dev.submit(read_op);
        assert_eq!(
            sim_dev.poll_completion().unwrap().result,
            fs_dev.wait_for_completion().unwrap().result
        );

        current_offset += payload_len as u64;

        if i % 10 == 0 {
            let sync_op = DeviceOp::Fdatasync { op_id, file_id };
            op_id += 1;
            sim_dev.submit(sync_op.clone());
            fs_dev.submit(sync_op);
            assert_eq!(
                sim_dev.poll_completion().unwrap().result,
                fs_dev.wait_for_completion().unwrap().result
            );
        }
    }

    // Final sync
    let final_sync = DeviceOp::Fdatasync { op_id, file_id };
    sim_dev.submit(final_sync.clone());
    fs_dev.submit(final_sync);
    assert_eq!(
        sim_dev.poll_completion().unwrap().result,
        fs_dev.wait_for_completion().unwrap().result
    );

    // Byte-for-byte durable state comparison
    let sim_bytes = sim_dev.get_file_durable_bytes(file_id).unwrap();
    let fs_path = anvil_device_fs::file_id_to_path(&test_dir, file_id);
    let fs_bytes = std::fs::read(&fs_path).unwrap();
    assert_eq!(
        sim_bytes,
        fs_bytes.as_slice(),
        "randomized workload resulted in divergent bytes!"
    );

    cleanup_test_dir(&test_dir);
}
