#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty
)]

use anvil_storage::{
    CompletionPayload, DeviceCompletion, DeviceOp, EngineInput, EngineOutput, FileId,
    HardStateRecoveryOutcome, HardStateSlot, SectorSize, SnapshotChunk, SnapshotFooter,
    SnapshotHeader, StorageConfig, StorageEngine, StorageError, WalRecordFrame, WalScanResult,
    WalSegmentHeader, decode_wal_frame, recover_hard_state,
};
use anvil_types::{Entry, EntryKind, GroupId, LogIndex, NodeId, Term};
use bytes::Bytes;

fn sector() -> SectorSize {
    SectorSize::Sector512
}

fn sample_entry(index: u64, term: u64) -> Entry {
    Entry {
        index: LogIndex::from_raw(index),
        term: Term::from_raw(term),
        kind: EntryKind::Normal,
        payload: Bytes::from(vec![0xABu8; 64]),
    }
}

#[test]
fn test_sector_size_helpers() {
    assert_eq!(SectorSize::Sector512.as_u16(), 512);
    assert_eq!(SectorSize::Sector4096.as_u64(), 4096);
    assert_eq!(SectorSize::Sector512.as_usize(), 512);
    assert!(SectorSize::Sector512.is_aligned(1024));
    assert!(!SectorSize::Sector512.is_aligned(100));
    assert_eq!(
        SectorSize::from_u16(512).expect("512"),
        SectorSize::Sector512
    );
    assert!(matches!(
        SectorSize::from_u16(999),
        Err(StorageError::UnsupportedSectorSize { found: 999 })
    ));
    assert_eq!(SectorSize::Sector512.align_up(1), 512);
    assert_eq!(SectorSize::Sector512.align_up(512), 512);
    assert_eq!(SectorSize::Sector512.align_up(513), 1024);
}

#[test]
fn test_file_id_classification() {
    assert!(FileId::HARD_STATE.is_hard_state());
    assert!(!FileId::HARD_STATE.is_wal_segment());
    assert!(!FileId::HARD_STATE.is_snapshot());

    let seg = FileId::wal_segment(7);
    assert!(seg.is_wal_segment());
    assert_eq!(seg.wal_segment_id(), Some(7));
    assert!(!seg.is_hard_state());
    assert!(!seg.is_snapshot());

    let snap = FileId::snapshot(3);
    assert!(snap.is_snapshot());
    assert!(!snap.is_wal_segment());
    assert_eq!(snap.wal_segment_id(), None);
    assert_eq!(FileId::HARD_STATE.wal_segment_id(), None);

    assert_ne!(FileId::HARD_STATE, seg);
    assert_ne!(seg, snap);
    assert_ne!(
        FileId::wal_segment(2).wal_segment_id(),
        seg.wal_segment_id()
    );
    assert_eq!(FileId::snapshot(3).wal_segment_id(), None);
}

#[test]
fn test_wal_header_rejects_bad_inputs() {
    let good = WalSegmentHeader::new(sector(), 1, LogIndex::from_raw(1), Term::from_raw(0));
    let mut buf = vec![0u8; sector().as_usize()];
    good.encode(&mut buf).expect("encode header");

    let small = vec![0u8; 16];
    assert!(matches!(
        WalSegmentHeader::decode(&small, sector()),
        Err(StorageError::BufferTooSmall { .. })
    ));
    let _ = small;

    let mut bad_magic = buf.clone();
    bad_magic[0] = b'X';
    assert!(matches!(
        WalSegmentHeader::decode(&bad_magic, sector()),
        Err(StorageError::InvalidMagic { .. })
    ));

    let mut bad_version = buf.clone();
    bad_version[8] = 0xFF;
    assert!(matches!(
        WalSegmentHeader::decode(&bad_version, sector()),
        Err(StorageError::UnsupportedVersion { .. })
    ));

    let mut bad_sector = buf.clone();
    bad_sector[10] = 0xFF;
    bad_sector[11] = 0xFF;
    assert!(matches!(
        WalSegmentHeader::decode(&bad_sector, sector()),
        Err(StorageError::UnsupportedSectorSize { .. })
    ));

    let mut bad_crc = buf.clone();
    bad_crc[40] ^= 0xFF;
    assert!(matches!(
        WalSegmentHeader::decode(&bad_crc, sector()),
        Err(StorageError::CrcMismatch { .. })
    ));

    let mut bad_pad = buf;
    bad_pad[100] = 0xFF;
    assert!(matches!(
        WalSegmentHeader::decode(&bad_pad, sector()),
        Err(StorageError::NonZeroPadding { .. })
    ));

    let mut tiny = vec![0u8; 32];
    assert!(matches!(
        WalSegmentHeader::new(sector(), 1, LogIndex::from_raw(1), Term::from_raw(0))
            .encode(&mut tiny),
        Err(StorageError::BufferTooSmall { .. })
    ));
}

#[test]
fn test_wal_frame_rejects_bad_inputs() {
    let empty: &[u8] = &[];
    assert!(matches!(
        decode_wal_frame(empty, sector(), 0, 1, 0),
        WalScanResult::Corrupted(StorageError::BufferTooSmall { .. })
    ));

    let zeros = vec![0u8; 512];
    assert!(matches!(
        decode_wal_frame(&zeros, sector(), 0, 1, 0),
        WalScanResult::Unallocated
    ));

    let short = vec![0x01u8; 10];
    assert!(matches!(
        decode_wal_frame(&short, sector(), 0, 1, 0),
        WalScanResult::Corrupted(StorageError::BufferTooSmall { .. })
    ));

    let entry = sample_entry(3, 2);
    let frame = WalRecordFrame::from_entry(&entry, 0x1234, 1, 512);
    let mut encoded = Vec::new();
    frame.encode_frame(sector(), &mut encoded);
    let total = encoded.len();

    let mut bad_kind = encoded.clone();
    bad_kind[20] = 9;
    assert!(matches!(
        decode_wal_frame(&bad_kind, sector(), 0x1234, 1, 512),
        WalScanResult::Corrupted(StorageError::InvalidField { .. })
    ));

    let mut truncated = encoded.clone();
    truncated.truncate(total - 100);
    assert!(matches!(
        decode_wal_frame(&truncated, sector(), 0x1234, 1, 512),
        WalScanResult::Corrupted(StorageError::BufferTooSmall { .. })
    ));

    let mut bad_crc = encoded.clone();
    bad_crc[21] ^= 0xFF;
    assert!(matches!(
        decode_wal_frame(&bad_crc, sector(), 0x1234, 1, 512),
        WalScanResult::Corrupted(StorageError::CrcMismatch { .. })
    ));

    let mut bad_pad = encoded.clone();
    let last = bad_pad.len() - 1;
    bad_pad[last] = 0xFF;
    assert!(matches!(
        decode_wal_frame(&bad_pad, sector(), 0x1234, 1, 512),
        WalScanResult::Corrupted(StorageError::NonZeroPadding { .. })
    ));

    assert_eq!(frame.frame_size(sector()), total);
    assert_eq!(WalRecordFrame::compute_frame_size(64, sector()), total);
    assert_eq!(frame.to_entry(), entry);

    let frame_normal = WalRecordFrame::from_entry(
        &Entry {
            index: LogIndex::from_raw(1),
            term: Term::from_raw(1),
            kind: EntryKind::Normal,
            payload: Bytes::from_static(b"x"),
        },
        0,
        1,
        512,
    );
    assert_eq!(frame_normal.kind, EntryKind::Normal);
    let frame_noop = WalRecordFrame::from_entry(
        &Entry {
            index: LogIndex::from_raw(2),
            term: Term::from_raw(1),
            kind: EntryKind::NoOp,
            payload: Bytes::new(),
        },
        frame_normal.crc32c,
        1,
        1024,
    );
    assert_eq!(frame_noop.kind, EntryKind::NoOp);
    assert_ne!(frame_noop.crc32c, frame_normal.crc32c);
}

#[test]
fn test_hard_state_slot_edges() {
    let sector_len = sector().as_usize();
    let slot = HardStateSlot::new(0, 9, Term::from_raw(4), Some(NodeId::new(3).expect("node")));
    let mut buf = vec![0u8; sector_len];
    slot.encode(sector(), &mut buf).expect("encode slot");

    let mut tiny = vec![0u8; 16];
    assert!(matches!(
        HardStateSlot::encode(&slot, sector(), &mut tiny),
        Err(StorageError::BufferTooSmall { .. })
    ));
    assert!(matches!(
        HardStateSlot::decode(&tiny, 0, sector()),
        Err(StorageError::BufferTooSmall { .. })
    ));

    let mut bad_magic = buf.clone();
    bad_magic[0] = b'X';
    assert!(matches!(
        HardStateSlot::decode(&bad_magic, 0, sector()),
        Err(StorageError::InvalidMagic { .. })
    ));

    let mut bad_version = buf.clone();
    bad_version[8] = 0xFF;
    assert!(matches!(
        HardStateSlot::decode(&bad_version, 0, sector()),
        Err(StorageError::UnsupportedVersion { .. })
    ));

    let mut bad_index = buf.clone();
    bad_index[10] = 0xFF;
    assert!(matches!(
        HardStateSlot::decode(&bad_index, 0, sector()),
        Err(StorageError::InvalidField {
            context: "hard_state_slot_index"
        })
    ));

    let mut bad_vote = buf.clone();
    bad_vote[40] = 7;
    assert!(matches!(
        HardStateSlot::decode(&bad_vote, 0, sector()),
        Err(StorageError::InvalidField { .. })
    ));

    let no_vote = HardStateSlot::new(1, 10, Term::from_raw(5), None);
    let mut no_vote_buf = vec![0u8; sector_len];
    no_vote.encode(sector(), &mut no_vote_buf).expect("encode");
    no_vote_buf[40] = 1;
    assert!(matches!(
        HardStateSlot::decode(&no_vote_buf, 1, sector()),
        Err(StorageError::InvalidField { .. })
    ));

    let mut bad_crc = buf.clone();
    bad_crc[16] ^= 0xFF;
    assert!(matches!(
        HardStateSlot::decode(&bad_crc, 0, sector()),
        Err(StorageError::CrcMismatch { .. })
    ));

    let mut bad_pad = buf.clone();
    bad_pad[sector_len - 1] = 0xFF;
    assert!(matches!(
        HardStateSlot::decode(&bad_pad, 0, sector()),
        Err(StorageError::NonZeroPadding { .. })
    ));
}

#[test]
fn test_snapshot_codec_edges() {
    let header = SnapshotHeader::new(
        sector(),
        LogIndex::from_raw(10),
        Term::from_raw(2),
        anvil_types::ClusterId::from_raw(1),
        GroupId::from_raw(0),
        2,
    );
    let mut tiny = vec![0u8; 16];
    assert!(matches!(
        header.encode(&mut tiny),
        Err(StorageError::BufferTooSmall { .. })
    ));

    let mut buf = vec![0u8; anvil_storage::SNAPSHOT_HEADER_LEN];
    header.encode(&mut buf).expect("encode header");
    assert!(matches!(
        SnapshotHeader::decode(&buf[..16]),
        Err(StorageError::BufferTooSmall { .. })
    ));

    let mut bad_magic = buf.clone();
    bad_magic[0] = b'X';
    assert!(matches!(
        SnapshotHeader::decode(&bad_magic),
        Err(StorageError::InvalidMagic { .. })
    ));

    let mut bad_version = buf.clone();
    bad_version[8] = 0xFF;
    assert!(matches!(
        SnapshotHeader::decode(&bad_version),
        Err(StorageError::UnsupportedVersion { .. })
    ));

    let mut bad_sector = buf.clone();
    bad_sector[10] = 0xFF;
    bad_sector[11] = 0xFF;
    assert!(matches!(
        SnapshotHeader::decode(&bad_sector),
        Err(StorageError::UnsupportedSectorSize { .. })
    ));

    let mut bad_crc = buf.clone();
    bad_crc[56] ^= 0xFF;
    assert!(matches!(
        SnapshotHeader::decode(&bad_crc),
        Err(StorageError::CrcMismatch { .. })
    ));

    let chunk = SnapshotChunk {
        chunk_index: 0,
        payload: Bytes::from_static(b"snapshot-bytes"),
    };
    let mut chunk_buf = Vec::new();
    chunk.encode(&mut chunk_buf);
    assert!(matches!(
        SnapshotChunk::decode(&chunk_buf[..8], 0),
        Err(StorageError::BufferTooSmall { .. })
    ));
    let mut bad_index = chunk_buf.clone();
    bad_index[0] = 0xFF;
    assert!(matches!(
        SnapshotChunk::decode(&bad_index, 0),
        Err(StorageError::InvalidField { .. })
    ));
    let mut bad_chunk_crc = chunk_buf.clone();
    bad_chunk_crc[8] ^= 0xFF;
    assert!(matches!(
        SnapshotChunk::decode(&bad_chunk_crc, 0),
        Err(StorageError::CrcMismatch { .. })
    ));
    let truncated = &chunk_buf[..chunk_buf.len() - 2];
    assert!(matches!(
        SnapshotChunk::decode(truncated, 0),
        Err(StorageError::BufferTooSmall { .. })
    ));

    let footer = SnapshotFooter::new(256, [7u8; 32]);
    let mut footer_buf = vec![0u8; anvil_storage::SNAPSHOT_FOOTER_LEN];
    footer.encode(&mut footer_buf).expect("encode footer");
    assert!(matches!(
        SnapshotFooter::decode(&footer_buf[..16]),
        Err(StorageError::BufferTooSmall { .. })
    ));
    let mut bad_footer_magic = footer_buf.clone();
    bad_footer_magic[0] = b'X';
    assert!(matches!(
        SnapshotFooter::decode(&bad_footer_magic),
        Err(StorageError::InvalidMagic { .. })
    ));
    let mut bad_footer_crc = footer_buf.clone();
    bad_footer_crc[48] ^= 0xFF;
    assert!(matches!(
        SnapshotFooter::decode(&bad_footer_crc),
        Err(StorageError::CrcMismatch { .. })
    ));
    let mut tiny_footer = vec![0u8; 16];
    assert!(matches!(
        footer.encode(&mut tiny_footer),
        Err(StorageError::BufferTooSmall { .. })
    ));
    assert_eq!(footer.total_length, 256);
    assert_eq!(footer.blake3_digest, [7u8; 32]);
}

#[test]
fn test_recover_hard_state_variants() {
    let sector_size = sector();
    let sector_len = sector_size.as_usize();
    let mut both = vec![0u8; sector_len * 2];
    let slot0 = HardStateSlot::new(0, 3, Term::from_raw(2), Some(NodeId::new(1).expect("node")));
    let slot1 = HardStateSlot::new(1, 4, Term::from_raw(3), None);
    slot0
        .encode(sector(), &mut both[..sector_len])
        .expect("slot0");
    slot1
        .encode(sector(), &mut both[sector_len..])
        .expect("slot1");

    match recover_hard_state(&both, sector_size).expect("recover") {
        HardStateRecoveryOutcome::BothValid {
            active_slot, term, ..
        } => {
            assert_eq!(active_slot, 1);
            assert_eq!(term, Term::from_raw(3));
        }
        other @ HardStateRecoveryOutcome::OneSlotCorrupted { .. } => {
            panic!("expected BothValid, got {other:?}")
        }
    }

    match recover_hard_state(&both[..sector_len], sector_size) {
        Err(StorageError::BufferTooSmall { .. }) => {}
        other => panic!("expected BufferTooSmall, got {other:?}"),
    }

    let mut corrupt_first = both.clone();
    corrupt_first[0] = b'X';
    match recover_hard_state(&corrupt_first, sector_size).expect("recover one") {
        HardStateRecoveryOutcome::OneSlotCorrupted { valid_slot, .. } => {
            assert_eq!(valid_slot, 1);
        }
        other @ HardStateRecoveryOutcome::BothValid { .. } => {
            panic!("expected OneSlotCorrupted, got {other:?}")
        }
    }

    let corrupt_all = vec![0xFFu8; sector_len * 2];
    assert!(matches!(
        recover_hard_state(&corrupt_all, sector_size),
        Err(StorageError::CorruptHardStateBothSlots)
    ));
}

#[test]
fn test_engine_empty_and_hard_state_paths() {
    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    assert_eq!(engine.active_segment_id(), 1);
    assert_eq!(engine.segment_write_offset(), 512);
    assert_eq!(engine.last_entry_index(), LogIndex::from_raw(0));
    assert!(engine.fatal_error().is_none());

    engine.reserve_pending_capacity(8, 512);

    let mut output = EngineOutput::default();
    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: Vec::new(),
            token: 7,
        },
        &mut output,
    );
    assert_eq!(output.persisted_tokens, vec![7]);
    output.clear();

    engine.step(
        EngineInput::WriteHardState {
            term: Term::from_raw(2),
            vote: None,
            token: 8,
        },
        &mut output,
    );
    let write_op = output
        .device_ops
        .iter()
        .find(|op| matches!(op, DeviceOp::Write { .. }))
        .expect("hard-state write");
    let write_op_clone = write_op.clone();
    let sync_op_id = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Fdatasync { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("hard-state sync");
    let write_bytes = match &write_op_clone {
        DeviceOp::Write { data, .. } => data.clone(),
        _ => unreachable!("checked above"),
    };
    output.clear();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync_op_id,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    let read_op_id = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Read { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("verification read");
    output.clear();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: read_op_id,
            result: Ok(CompletionPayload::Read(write_bytes)),
        }),
        &mut output,
    );
    assert_eq!(output.persisted_tokens, vec![8]);
    assert!(engine.fatal_error().is_none());
    output.clear();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: 999,
            result: Err(StorageError::DeviceError("boom".to_string())),
        }),
        &mut output,
    );
    assert!(engine.fatal_error().is_some());
}

#[test]
fn test_engine_rejects_stale_and_empty_completions() {
    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: 4242,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    assert!(
        output.persisted_tokens.is_empty(),
        "expected no persisted tokens"
    );
    assert!(engine.fatal_error().is_none());

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: 4243,
            result: Err(StorageError::FsyncFailed {
                reason: "disk gone".to_string(),
            }),
        }),
        &mut output,
    );
    assert!(engine.fatal_error().is_some());

    let before = output.device_ops.len();
    engine.step(
        EngineInput::WriteHardState {
            term: Term::from_raw(9),
            vote: None,
            token: 1,
        },
        &mut output,
    );
    assert_eq!(output.device_ops.len(), before);
}

#[test]
fn test_device_op_ids_all_variants() {
    use anvil_storage::DeviceOp;
    use bytes::Bytes;

    let ops = [
        DeviceOp::Create {
            op_id: 1,
            file_id: FileId::HARD_STATE,
            preallocate_bytes: 10,
        },
        DeviceOp::Open {
            op_id: 2,
            file_id: FileId::HARD_STATE,
        },
        DeviceOp::Write {
            op_id: 3,
            file_id: FileId::HARD_STATE,
            offset: 0,
            data: Bytes::from_static(b"x"),
        },
        DeviceOp::Read {
            op_id: 4,
            file_id: FileId::HARD_STATE,
            offset: 0,
            len: 8,
        },
        DeviceOp::Fdatasync {
            op_id: 5,
            file_id: FileId::HARD_STATE,
        },
        DeviceOp::SyncDir { op_id: 6 },
        DeviceOp::Rename {
            op_id: 7,
            from: FileId::HARD_STATE,
            to: FileId::wal_segment(9),
        },
        DeviceOp::Unlink {
            op_id: 8,
            file_id: FileId::HARD_STATE,
        },
        DeviceOp::Truncate {
            op_id: 9,
            file_id: FileId::HARD_STATE,
            len: 0,
        },
    ];
    for (i, op) in ops.iter().enumerate() {
        assert_eq!(op.op_id(), (i + 1) as u64);
    }
}

#[test]
fn test_empty_append_coalesces_behind_in_flight_sync() {
    use anvil_storage::{EngineInput, EngineOutput, StorageEngine};

    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();
    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(0),
            entries: vec![sample_entry(1, 1)],
            token: 11,
        },
        &mut output,
    );
    let sync = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Fdatasync { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("sync");
    output.clear();

    engine.step(
        EngineInput::AppendEntries {
            prev_index: LogIndex::from_raw(1),
            entries: Vec::new(),
            token: 12,
        },
        &mut output,
    );
    assert!(
        output.persisted_tokens.is_empty(),
        "expected no persisted tokens"
    );

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    assert_eq!(output.persisted_tokens, vec![11]);
    let sync2 = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Fdatasync { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("coalesced sync");
    output.clear();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync2,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    assert_eq!(output.persisted_tokens, vec![12]);
}

#[test]
fn test_hard_state_verify_mismatch_is_fatal() {
    use anvil_storage::{EngineInput, EngineOutput, StorageEngine};

    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();
    engine.step(
        EngineInput::WriteHardState {
            term: Term::from_raw(5),
            vote: None,
            token: 21,
        },
        &mut output,
    );
    let sync = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Fdatasync { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("sync");
    output.clear();
    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    let read_id = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Read { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("verify read");
    output.clear();

    let wrong = HardStateSlot::new(0, 999, Term::from_raw(9), None);
    let mut wrong_buf = vec![0u8; SectorSize::Sector512.as_usize()];
    wrong
        .encode(SectorSize::Sector512, &mut wrong_buf)
        .expect("encode");
    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: read_id,
            result: Ok(CompletionPayload::Read(Bytes::from(wrong_buf))),
        }),
        &mut output,
    );
    assert!(
        output.persisted_tokens.is_empty(),
        "expected no persisted tokens"
    );
    assert!(engine.fatal_error().is_some());
}

#[test]
fn test_wal_sector_size_mismatch_rejected() {
    let header = WalSegmentHeader::new(
        SectorSize::Sector4096,
        1,
        LogIndex::from_raw(1),
        Term::from_raw(0),
    );
    let mut buf = vec![0u8; SectorSize::Sector4096.as_usize()];
    header.encode(&mut buf).expect("encode 4096 header");
    assert!(matches!(
        WalSegmentHeader::decode(&buf, SectorSize::Sector512),
        Err(StorageError::UnsupportedSectorSize { .. })
    ));
}

#[test]
fn test_wal_multi_sector_truncation_detected() {
    let big_payload = vec![0xCDu8; 600];
    let entry = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from(big_payload),
    };
    let frame = WalRecordFrame::from_entry(&entry, 0, 1, 512);
    let mut encoded = Vec::new();
    frame.encode_frame(SectorSize::Sector512, &mut encoded);
    assert!(encoded.len() > 512);

    let truncated = &encoded[..700];
    assert!(matches!(
        decode_wal_frame(truncated, SectorSize::Sector512, 0, 1, 512),
        WalScanResult::Corrupted(StorageError::BufferTooSmall { .. })
    ));
}

#[test]
fn test_verify_completion_with_wrong_payload_is_fatal() {
    use anvil_storage::{EngineInput, EngineOutput, StorageEngine};

    let config = StorageConfig::for_test(SectorSize::Sector512, 1024 * 1024);
    let mut engine = StorageEngine::new_empty(config);
    let mut output = EngineOutput::default();
    engine.step(
        EngineInput::WriteHardState {
            term: Term::from_raw(3),
            vote: None,
            token: 31,
        },
        &mut output,
    );
    let sync = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Fdatasync { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("sync");
    output.clear();
    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: sync,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    let read_id = output
        .device_ops
        .iter()
        .find_map(|op| match op {
            DeviceOp::Read { op_id, .. } => Some(*op_id),
            _ => None,
        })
        .expect("verify read");
    assert_eq!(
        engine.last_record_crc(),
        WalSegmentHeader::new(sector(), 1, LogIndex::from_raw(1), Term::from_raw(0)).header_crc
    );
    output.clear();

    engine.step(
        EngineInput::Completion(DeviceCompletion {
            op_id: read_id,
            result: Ok(CompletionPayload::Synced),
        }),
        &mut output,
    );
    assert!(output.persisted_tokens.is_empty());
    assert!(engine.fatal_error().is_some());
}
