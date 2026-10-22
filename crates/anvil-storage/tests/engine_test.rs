#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names,
    clippy::assert_is_empty
)]

use anvil_storage::{
    CompletionPayload, DeviceCompletion, DeviceOp, EngineInput, EngineOutput, FileId,
    LogRecoveryClassification, SectorSize, StorageConfig, StorageEngine, StorageError,
    WalRecordFrame, WalSegmentHeader,
};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;

#[test]
fn test_engine_natural_batching_group_commit() {
    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();

    // 1. Submit initial append (token 101)
    let entry1 = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"entry-1"),
    };

    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: vec![entry1],
            token: 101,
        },
        &mut output,
    );

    // Engine should emit a Write and an Fdatasync
    assert_eq!(output.device_ops.len(), 2);
    assert!(matches!(output.device_ops[0], DeviceOp::Write { .. }));
    let sync_op_id = match output.device_ops[1] {
        DeviceOp::Fdatasync { op_id, .. } => op_id,
        ref other => panic!("expected Fdatasync, got {other:?}"),
    };
    assert!(output.persisted_tokens.is_empty());
    output.clear();

    // 2. While fdatasync is in flight, submit append 2 (token 102) and append 3 (token 103)
    let entry2 = Entry {
        index: LogIndex::from_raw(2),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"entry-2"),
    };
    let entry3 = Entry {
        index: LogIndex::from_raw(3),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"entry-3"),
    };

    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(1),
            entries: vec![entry2],
            token: 102,
        },
        &mut output,
    );
    // Should NOT emit new sync because one is already in flight!
    assert!(output.device_ops.is_empty());

    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(2),
            entries: vec![entry3],
            token: 103,
        },
        &mut output,
    );
    assert!(output.device_ops.is_empty());

    // 3. Complete the first fdatasync
    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync_op_id,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );

    // Token 101 is released!
    assert_eq!(output.persisted_tokens, vec![101]);
    // Coalesced natural batch: single Write for entries 2 & 3, followed by single Fdatasync!
    assert_eq!(output.device_ops.len(), 2);
    assert!(matches!(output.device_ops[0], DeviceOp::Write { .. }));
    let sync2_op_id = match output.device_ops[1] {
        DeviceOp::Fdatasync { op_id, .. } => op_id,
        ref other => panic!("expected second Fdatasync, got {other:?}"),
    };
    output.clear();

    // 4. Complete the second fdatasync
    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync2_op_id,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );

    // Tokens 102 and 103 released in exact submission order!
    assert_eq!(output.persisted_tokens, vec![102, 103]);
    assert!(output.device_ops.is_empty());
}

#[test]
fn test_engine_segment_rotation_and_preallocation() {
    // 1536 byte segment capacity (Header 512B + room for 2 frames of 512B)
    let config = StorageConfig::for_test(SectorSize::Sector512, 1536);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();

    // Append 2 entries: 512B header + 512B entry1 + 512B entry2 = 1536B (fills segment 1)
    let entry1 = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![1u8; 100]),
    };
    let entry2 = Entry {
        index: LogIndex::from_raw(2),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![2u8; 100]),
    };
    let entry3 = Entry {
        index: LogIndex::from_raw(3),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![3u8; 100]),
    };

    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: vec![entry1, entry2],
            token: 1,
        },
        &mut output,
    );
    output.clear();

    // Appending entry 3 should trigger segment rotation
    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(2),
            entries: vec![entry3],
            token: 2,
        },
        &mut output,
    );

    // Verify rotation operations: Create for segment 2, SyncDir, Write Header
    let has_create_seg2 = output.device_ops.iter().any(|op| match op {
        DeviceOp::Create {
            file_id,
            preallocate_bytes,
            ..
        } => *file_id == FileId::wal_segment(2) && *preallocate_bytes == 1536,
        _ => false,
    });
    assert!(
        has_create_seg2,
        "engine must create segment 2 with preallocation"
    );

    let has_syncdir = output
        .device_ops
        .iter()
        .any(|op| matches!(op, DeviceOp::SyncDir { .. }));
    assert!(
        has_syncdir,
        "engine must issue SyncDir after creating segment file"
    );

    assert_eq!(engine.active_segment_id(), 2);
}

#[test]
#[should_panic(expected = "FATAL: storage sync operation failed under ADR 0013")]
fn test_engine_fsync_fail_fast_policy() {
    let config = StorageConfig::default();
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: 99,
            result: Err(StorageError::FsyncFailed {
                reason: "injected EIO failure".to_string(),
            }),
        }),
        &mut output,
    );
}

#[test]
fn test_recovery_scanner_clean_and_torn_tail() {
    let sector_size = SectorSize::Sector512;
    let sector_len = sector_size.as_usize();

    // Prepare hard state buffer
    let mut hard_state_buf = vec![0u8; sector_len * 2];
    let slot0 =
        anvil_storage::HardStateSlot::new(0, 5, Term::from_raw(2), Some(NodeId::new(1).unwrap()));
    slot0
        .encode(sector_size, &mut hard_state_buf[..sector_len])
        .unwrap();

    // Prepare WAL segment buffer
    let mut seg_buf = vec![0u8; 4096];
    let seg_hdr = WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(0));
    seg_hdr.encode(&mut seg_buf[..sector_len]).unwrap();

    let entry1 = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"log-record-1"),
    };
    let entry2 = Entry {
        index: LogIndex::from_raw(2),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"log-record-2"),
    };

    let f1 = WalRecordFrame::from_entry(&entry1, seg_hdr.header_crc, 1, 512);
    let mut f1_bytes = Vec::new();
    f1.encode_frame(sector_size, &mut f1_bytes);
    seg_buf[512..512 + f1_bytes.len()].copy_from_slice(&f1_bytes);

    let f2 = WalRecordFrame::from_entry(&entry2, f1.crc32c, 1, 512 + f1_bytes.len() as u64);
    let mut f2_bytes = Vec::new();
    f2.encode_frame(sector_size, &mut f2_bytes);
    let f2_offset = 512 + f1_bytes.len();
    seg_buf[f2_offset..f2_offset + f2_bytes.len()].copy_from_slice(&f2_bytes);

    // 1. Clean recovery
    let recovered_clean =
        StorageEngine::scan_recovery(&hard_state_buf, &[(seg_hdr.clone(), &seg_buf)], sector_size)
            .unwrap();

    assert_eq!(recovered_clean.log.entries.len(), 2);
    assert_eq!(
        recovered_clean.log.classification,
        LogRecoveryClassification::Clean
    );
    assert!(!recovered_clean.log.needs_repair());

    // 2. Torn tail: corrupt entry 2 payload
    let mut torn_buf = seg_buf.clone();
    torn_buf[f2_offset + 30] ^= 0xFF; // flip byte in record 2
    // Rest of segment beyond record 2 is zeroes
    torn_buf[f2_offset + f2_bytes.len()..].fill(0);

    let recovered_torn =
        StorageEngine::scan_recovery(&hard_state_buf, &[(seg_hdr, &torn_buf)], sector_size)
            .unwrap();

    assert_eq!(recovered_torn.log.entries.len(), 1);
    assert_eq!(
        recovered_torn.log.classification,
        LogRecoveryClassification::TornTail {
            truncated_offset: f2_offset as u64,
            last_valid_index: LogIndex::from_raw(1),
        }
    );
    assert!(!recovered_torn.log.needs_repair());
}
