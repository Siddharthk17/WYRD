#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::{DetRng, SimCluster, SimDevice, SimDeviceConfig};
use anvil_storage::{
    Device, DeviceOp, EngineInput, EngineOutput, FileId, HardStateSlot, LogRecoveryClassification,
    SectorSize, StorageConfig, StorageEngine, WalSegmentHeader,
};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;
use proptest::prelude::*;
use std::collections::BTreeMap;

/// Actions for single-node storage engine + device property crash testing.
#[derive(Debug, Clone)]
enum StorageOp {
    /// Append entries with a given payload size
    Append(u8),
    /// Poll and process in-flight device completions
    CompleteIo(u8),
    /// Crash device (drops un-synced dirty sectors)
    Crash,
    /// Run recovery scan and verify all fsync-acknowledged writes survived
    Recover,
}

prop_compose! {
    fn arb_storage_ops()(
        ops in prop::collection::vec(
            (0u8..4, any::<u8>()),
            5..30
        )
    ) -> Vec<StorageOp> {
        ops.into_iter().map(|(kind, val)| {
            match kind {
                0 => StorageOp::Append(val % 5 + 1),
                1 => StorageOp::CompleteIo(val % 4 + 1),
                2 => StorageOp::Crash,
                _ => StorageOp::Recover,
            }
        }).collect()
    }
}

fn init_test_device(device: &mut SimDevice, config: &StorageConfig) {
    let sector_size = config.sector_size;
    let sector_len = sector_size.as_usize();

    device.submit(DeviceOp::Create {
        op_id: 1,
        file_id: FileId::HARD_STATE,
        preallocate_bytes: 2 * sector_size.as_u64(),
    });
    let slot0 = HardStateSlot::new(0, 0, Term::from_raw(1), None);
    let mut slot_buf = vec![0u8; sector_len];
    let _ = slot0.encode(sector_size, &mut slot_buf);
    device.submit(DeviceOp::Write {
        op_id: 2,
        file_id: FileId::HARD_STATE,
        offset: 0,
        data: Bytes::from(slot_buf),
    });
    device.submit(DeviceOp::Fdatasync {
        op_id: 3,
        file_id: FileId::HARD_STATE,
    });

    device.submit(DeviceOp::Create {
        op_id: 4,
        file_id: FileId::wal_segment(1),
        preallocate_bytes: config.segment_capacity_bytes,
    });
    let hdr = WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(0));
    let mut hdr_buf = vec![0u8; sector_len];
    let _ = hdr.encode(&mut hdr_buf);
    device.submit(DeviceOp::Write {
        op_id: 5,
        file_id: FileId::wal_segment(1),
        offset: 0,
        data: Bytes::from(hdr_buf),
    });
    device.submit(DeviceOp::Fdatasync {
        op_id: 6,
        file_id: FileId::wal_segment(1),
    });
    device.submit(DeviceOp::SyncDir { op_id: 7 });

    while device.poll_completion().is_some() {}
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(30))]

    #[test]
    fn prop_storage_engine_fsync_acknowledged_writes_survive_random_crashes(
        ops in arb_storage_ops(),
        is_4096 in any::<bool>(),
        seed in any::<u64>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };

        let storage_config = StorageConfig::for_test(sector_size, 256 * 1024);
        let dev_config = SimDeviceConfig::for_test(sector_size);
        let dev_rng = DetRng::new(seed, "proptest/storage_device");
        let mut device = SimDevice::new(dev_config, dev_rng);
        init_test_device(&mut device, &storage_config);

        let mut engine = StorageEngine::new_empty(storage_config.clone());
        let mut output = EngineOutput::default();

        let mut next_index = 1u64;
        let mut token_counter = 100u64;
        let mut acknowledged_entries: BTreeMap<LogIndex, Entry> = BTreeMap::new();
        let mut token_to_entries: BTreeMap<u64, Vec<Entry>> = BTreeMap::new();

        let mut is_crashed = false;

        for op in ops {
            match op {
                StorageOp::Append(count) => {
                    if is_crashed {
                        continue;
                    }
                    let mut entries = Vec::new();
                    let prev_idx = LogIndex::from_raw(next_index.saturating_sub(1));
                    for _ in 0..count {
                        let e = Entry {
                            index: LogIndex::from_raw(next_index),
                            term: Term::from_raw(1),
                            kind: EntryKind::Normal,
                            payload: Bytes::from(format!("payload-{next_index}")),
                        };
                        next_index += 1;
                        entries.push(e);
                    }
                    let token = token_counter;
                    token_counter += 1;
                    token_to_entries.insert(token, entries.clone());

                    engine.step(
                        EngineInput::AppendEntries {
                            prev_index: prev_idx,
                            entries,
                            token,
                        },
                        &mut output,
                    );

                    for dev_op in output.device_ops.drain(..) {
                        device.submit(dev_op);
                    }
                }
                StorageOp::CompleteIo(limit) => {
                    if is_crashed {
                        continue;
                    }
                    for _ in 0..limit {
                        if let Some(comp) = device.poll_completion() {
                            engine.step(EngineInput::Completion(comp), &mut output);
                            for dev_op in output.device_ops.drain(..) {
                                device.submit(dev_op);
                            }
                            for token in output.persisted_tokens.drain(..) {
                                if let Some(ents) = token_to_entries.get(&token) {
                                    for e in ents {
                                        acknowledged_entries.insert(e.index, e.clone());
                                    }
                                }
                            }
                        } else {
                            break;
                        }
                    }
                }
                StorageOp::Crash => {
                    device.crash();
                    is_crashed = true;
                }
                StorageOp::Recover => {
                    // Collect durable bytes
                    let hs_bytes = device
                        .get_file_durable_bytes(FileId::HARD_STATE)
                        .unwrap_or(&[]);

                    let mut segments = Vec::new();
                    let mut seg_id = 1u64;
                    loop {
                        let file_id = FileId::wal_segment(seg_id);
                        let Some(seg_bytes) = device.get_file_durable_bytes(file_id) else {
                            break;
                        };
                        if seg_bytes.len() < sector_size.as_usize() {
                            break;
                        }
                        let Ok(hdr) = WalSegmentHeader::decode(&seg_bytes[..sector_size.as_usize()], sector_size) else {
                            break;
                        };
                        segments.push((hdr, seg_bytes));
                        seg_id += 1;
                    }

                    let recovery_res = StorageEngine::scan_recovery(hs_bytes, &segments, sector_size);
                    prop_assert!(recovery_res.is_ok(), "recovery must succeed cleanly: {:?}", recovery_res.err());
                    let recovered = recovery_res.unwrap();

                    prop_assert!(
                        matches!(
                            recovered.log.classification,
                            LogRecoveryClassification::Clean | LogRecoveryClassification::TornTail { .. }
                        ),
                        "classification must be Clean or TornTail, got: {:?}",
                        recovered.log.classification
                    );

                    // Core invariant: EVERY acknowledged write MUST survive intact!
                    let recovered_map: BTreeMap<LogIndex, &Entry> = recovered
                        .log
                        .entries
                        .iter()
                        .map(|e| (e.index, e))
                        .collect();

                    for (ack_idx, ack_entry) in &acknowledged_entries {
                        let recovered_entry = recovered_map.get(ack_idx);
                        prop_assert!(
                            recovered_entry.is_some(),
                            "Recovered log missing acknowledged entry at index {}",
                            ack_idx
                        );
                        let rec = recovered_entry.unwrap();
                        prop_assert_eq!(rec.term, ack_entry.term);
                        prop_assert_eq!(&rec.payload, &ack_entry.payload);
                    }

                    // Reconstruct engine after recovery to continue testing
                    let (write_offset, last_crc) = if let Some((last_hdr, _)) = segments.last() {
                        let mut cur_offset = sector_size.as_u64();
                        let mut cur_crc = last_hdr.header_crc;
                        for e in &recovered.log.entries {
                            let frame = anvil_storage::WalRecordFrame::from_entry(
                                e,
                                cur_crc,
                                1,
                                cur_offset,
                            );
                            cur_crc = frame.crc32c;
                            cur_offset += frame.frame_size(sector_size) as u64;
                        }
                        (cur_offset, cur_crc)
                    } else {
                        (sector_size.as_u64(), 0)
                    };

                    engine = StorageEngine::new(
                        storage_config.clone(),
                        &recovered,
                        1,
                        write_offset,
                        last_crc,
                    );
                    next_index = engine.last_entry_index().as_u64() + 1;
                    token_to_entries.retain(|_, ents| {
                        ents.iter().all(|e| e.index < LogIndex::from_raw(next_index))
                    });
                    is_crashed = false;
                }
            }
        }
    }

    #[test]
    fn prop_cluster_crash_recovery_invariants(
        proposals in prop::collection::vec(1u16..500, 2..10),
        crashes in prop::collection::vec(0u8..3, 1..4),
        seed in any::<u64>(),
    ) {
        let mut cluster = SimCluster::new(3, seed);
        let node_ids: Vec<NodeId> = cluster.nodes.keys().copied().collect();

        // 1. Elect leader
        for _ in 0..20 {
            cluster.tick();
        }

        let leader_opt = cluster.leader();
        if let Some(leader) = leader_opt {
            for val in proposals {
                cluster.propose(leader, Bytes::from(format!("val-{val}")));
                cluster.tick();
            }
        }

        // 2. Perform sequence of crashes and restarts
        for n in crashes {
            let target_node = node_ids[(n as usize) % node_ids.len()];
            cluster.crash_node(target_node);
            cluster.tick_n(5);
            let _ = cluster.restart_node(target_node);
            cluster.tick_n(5);
        }

        // 3. Invariants are continuously asserted on every node step and restart
        cluster.checker.check_invariants(&cluster.nodes);
    }
}
