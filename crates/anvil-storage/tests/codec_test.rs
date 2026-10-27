#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_storage::{
    HardStateRecoveryOutcome, HardStateSlot, SectorSize, SnapshotChunk, SnapshotFooter,
    SnapshotHeader, StorageError, WalRecordFrame, WalScanResult, WalSegmentHeader,
    decode_wal_frame, recover_hard_state,
};
use anvil_types::{ClusterId, Entry, EntryKind, GroupId, LogIndex, NodeId, Term};
use bytes::Bytes;

#[test]
fn test_wal_header_roundtrip_512_and_4096() {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        let header =
            WalSegmentHeader::new(sector_size, 42, LogIndex::from_raw(100), Term::from_raw(5));

        let mut buf = vec![0u8; sector_size.as_usize()];
        header
            .encode(&mut buf)
            .expect("header encoding must succeed");

        let decoded =
            WalSegmentHeader::decode(&buf, sector_size).expect("header decoding must succeed");
        assert_eq!(header, decoded);
    }
}

#[test]
fn test_wal_header_corruption_and_padding_rejection() {
    let sector_size = SectorSize::Sector512;
    let header = WalSegmentHeader::new(sector_size, 1, LogIndex::from_raw(1), Term::from_raw(1));

    let mut buf = vec![0u8; sector_size.as_usize()];
    header.encode(&mut buf).unwrap();

    // Corrupt CRC
    let mut bad_crc_buf = buf.clone();
    bad_crc_buf[40] ^= 0xFF;
    let err = WalSegmentHeader::decode(&bad_crc_buf, sector_size).unwrap_err();
    assert!(matches!(err, StorageError::CrcMismatch { .. }));

    // Corrupt magic
    let mut bad_magic_buf = buf.clone();
    bad_magic_buf[0] = b'X';
    let err = WalSegmentHeader::decode(&bad_magic_buf, sector_size).unwrap_err();
    assert!(matches!(err, StorageError::InvalidMagic { .. }));

    // Non-zero padding
    let mut bad_pad_buf = buf.clone();
    bad_pad_buf[50] = 0xAA;
    let err = WalSegmentHeader::decode(&bad_pad_buf, sector_size).unwrap_err();
    assert!(matches!(err, StorageError::NonZeroPadding { offset: 50 }));
}

#[test]
fn test_wal_record_frame_roundtrip_and_chained_crc() {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        let entry = Entry {
            index: LogIndex::from_raw(10),
            term: Term::from_raw(2),
            kind: EntryKind::Normal,
            payload: Bytes::from_static(b"consensus-write-payload"),
        };

        let prev_crc = 0x1234_5678;
        let segment_id = 7;
        let file_offset = sector_size.as_u64();

        let frame = WalRecordFrame::from_entry(&entry, prev_crc, segment_id, file_offset);
        let mut encoded = Vec::new();
        frame.encode_frame(sector_size, &mut encoded);

        assert_eq!(encoded.len() % sector_size.as_usize(), 0);

        let scan_result =
            decode_wal_frame(&encoded, sector_size, prev_crc, segment_id, file_offset);

        match scan_result {
            WalScanResult::Valid {
                frame: decoded,
                bytes_consumed,
                next_crc,
            } => {
                assert_eq!(decoded.to_entry(), entry);
                assert_eq!(bytes_consumed, encoded.len());
                assert_eq!(next_crc, frame.crc32c);
            }
            other => panic!("expected valid frame scan, got {other:?}"),
        }
    }
}

#[test]
fn test_wal_record_relocation_rejection() {
    let sector_size = SectorSize::Sector512;
    let entry = Entry {
        index: LogIndex::from_raw(5),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"relocated-record-payload"),
    };

    let prev_crc = 100;
    let segment_id = 1;
    let original_offset = 512;

    let frame = WalRecordFrame::from_entry(&entry, prev_crc, segment_id, original_offset);
    let mut encoded = Vec::new();
    frame.encode_frame(sector_size, &mut encoded);

    // Attempt to decode frame at a different file offset (misdirected write defense)
    let relocated_offset = 1024;
    let scan_result = decode_wal_frame(
        &encoded,
        sector_size,
        prev_crc,
        segment_id,
        relocated_offset,
    );

    match scan_result {
        WalScanResult::Corrupted(StorageError::CrcMismatch { context, .. }) => {
            assert_eq!(context, "wal_record_frame_crc");
        }
        other => panic!("expected CRC mismatch for relocated record, got {other:?}"),
    }

    // Attempt to decode in a different segment
    let different_segment_id = 2;
    let scan_result_seg = decode_wal_frame(
        &encoded,
        sector_size,
        prev_crc,
        different_segment_id,
        original_offset,
    );

    match scan_result_seg {
        WalScanResult::Corrupted(StorageError::CrcMismatch { context, .. }) => {
            assert_eq!(context, "wal_record_frame_crc");
        }
        other => panic!("expected CRC mismatch for segment swap, got {other:?}"),
    }
}

#[test]
fn test_wal_unallocated_zero_sectors() {
    let sector_size = SectorSize::Sector512;
    let zero_buf = vec![0u8; 1024];

    let result = decode_wal_frame(&zero_buf, sector_size, 0, 1, 512);
    assert_eq!(result, WalScanResult::Unallocated);
}

#[test]
fn test_hard_state_alternating_slots_recovery() {
    for sector_size in [SectorSize::Sector512, SectorSize::Sector4096] {
        let sector_len = sector_size.as_usize();
        let mut file_buf = vec![0u8; sector_len * 2];

        let slot0 = HardStateSlot::new(0, 10, Term::from_raw(3), Some(NodeId::new(1).unwrap()));
        let slot1 = HardStateSlot::new(1, 11, Term::from_raw(3), Some(NodeId::new(2).unwrap()));

        slot0
            .encode(sector_size, &mut file_buf[..sector_len])
            .unwrap();
        slot1
            .encode(sector_size, &mut file_buf[sector_len..])
            .unwrap();

        // 1. Both valid: picks slot 1 (higher seq)
        let outcome = recover_hard_state(&file_buf, sector_size).unwrap();
        assert_eq!(
            outcome,
            HardStateRecoveryOutcome::BothValid {
                active_slot: 1,
                term: Term::from_raw(3),
                voted_for: Some(NodeId::new(2).unwrap()),
                sequence_no: 11,
            }
        );

        // 2. Slot 1 corrupted: recovers from slot 0
        let mut corrupt_slot1 = file_buf.clone();
        corrupt_slot1[sector_len + 12] ^= 0xFF; // corrupt slot1 CRC
        let outcome1 = recover_hard_state(&corrupt_slot1, sector_size).unwrap();
        assert_eq!(
            outcome1,
            HardStateRecoveryOutcome::OneSlotCorrupted {
                valid_slot: 0,
                bad_slot: 1,
                term: Term::from_raw(3),
                voted_for: Some(NodeId::new(1).unwrap()),
                sequence_no: 10,
            }
        );

        // 3. Slot 0 corrupted: recovers from slot 1
        let mut corrupt_slot0 = file_buf.clone();
        corrupt_slot0[12] ^= 0xFF; // corrupt slot0 CRC
        let outcome0 = recover_hard_state(&corrupt_slot0, sector_size).unwrap();
        assert_eq!(
            outcome0,
            HardStateRecoveryOutcome::OneSlotCorrupted {
                valid_slot: 1,
                bad_slot: 0,
                term: Term::from_raw(3),
                voted_for: Some(NodeId::new(2).unwrap()),
                sequence_no: 11,
            }
        );

        // 4. Both corrupted: refuses to start
        let mut both_corrupt = file_buf.clone();
        both_corrupt[12] ^= 0xFF;
        both_corrupt[sector_len + 12] ^= 0xFF;
        let err = recover_hard_state(&both_corrupt, sector_size).unwrap_err();
        assert_eq!(err, StorageError::CorruptHardStateBothSlots);
    }
}

#[test]
fn test_snapshot_header_chunk_and_footer() {
    let sector_size = SectorSize::Sector512;
    let header = SnapshotHeader::new(
        sector_size,
        LogIndex::from_raw(500),
        Term::from_raw(12),
        ClusterId::from_raw(1),
        GroupId::from_raw(1),
        2,
    );

    let mut header_buf = vec![0u8; 64];
    header.encode(&mut header_buf).unwrap();
    let decoded_header = SnapshotHeader::decode(&header_buf).unwrap();
    assert_eq!(header, decoded_header);

    // Chunks
    let chunk0 = SnapshotChunk {
        chunk_index: 0,
        payload: Bytes::from_static(b"state-machine-slice-0"),
    };
    let chunk1 = SnapshotChunk {
        chunk_index: 1,
        payload: Bytes::from_static(b"state-machine-slice-1"),
    };

    let mut file_data = Vec::new();
    file_data.extend_from_slice(&header_buf);
    chunk0.encode(&mut file_data);
    chunk1.encode(&mut file_data);

    // Compute whole-file BLAKE3 digest over all bytes before footer
    let digest = blake3::hash(&file_data);
    let total_file_len = file_data.len() as u64 + 64;
    let footer = SnapshotFooter::new(total_file_len, *digest.as_bytes());

    let mut footer_buf = vec![0u8; 64];
    footer.encode(&mut footer_buf).unwrap();
    file_data.extend_from_slice(&footer_buf);

    // Verify footer decode
    let decoded_footer = SnapshotFooter::decode(&file_data[file_data.len() - 64..]).unwrap();
    assert_eq!(footer, decoded_footer);

    // Verify whole-file hash
    let recomputed_digest = blake3::hash(&file_data[..file_data.len() - 64]);
    assert_eq!(*recomputed_digest.as_bytes(), decoded_footer.blake3_digest);
}

#[test]
fn test_fuzz_corpora_generation() {
    let workspace_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let corpus_dir = workspace_root.join("fuzz").join("corpus");

    // 1. Wal Header
    let wal_hdr_dir = corpus_dir.join("fuzz_wal_header");
    let _ = std::fs::create_dir_all(&wal_hdr_dir);
    let hdr512 = WalSegmentHeader::new(
        SectorSize::Sector512,
        1,
        LogIndex::from_raw(1),
        Term::from_raw(0),
    );
    let mut buf512 = vec![0u8; 512];
    hdr512.encode(&mut buf512).unwrap();
    let _ = std::fs::write(wal_hdr_dir.join("seed_512"), buf512);

    let hdr4096 = WalSegmentHeader::new(
        SectorSize::Sector4096,
        2,
        LogIndex::from_raw(100),
        Term::from_raw(3),
    );
    let mut buf4096 = vec![0u8; 4096];
    hdr4096.encode(&mut buf4096).unwrap();
    let _ = std::fs::write(wal_hdr_dir.join("seed_4096"), buf4096);

    // 2. Wal Frame
    let wal_frame_dir = corpus_dir.join("fuzz_wal_frame");
    let _ = std::fs::create_dir_all(&wal_frame_dir);
    let entry_normal = Entry {
        index: LogIndex::from_raw(1),
        term: Term::from_raw(1),
        kind: EntryKind::Normal,
        payload: Bytes::from_static(b"hello anvil consensus"),
    };
    let frame_normal = WalRecordFrame::from_entry(&entry_normal, 0, 1, 512);
    let mut enc_normal = Vec::new();
    frame_normal.encode_frame(SectorSize::Sector512, &mut enc_normal);
    let _ = std::fs::write(wal_frame_dir.join("seed_normal"), enc_normal);

    let entry_noop = Entry {
        index: LogIndex::from_raw(2),
        term: Term::from_raw(1),
        kind: EntryKind::NoOp,
        payload: Bytes::new(),
    };
    let frame_noop = WalRecordFrame::from_entry(
        &entry_noop,
        frame_normal.crc32c,
        1,
        512 + frame_normal.frame_size(SectorSize::Sector512) as u64,
    );
    let mut enc_noop = Vec::new();
    frame_noop.encode_frame(SectorSize::Sector512, &mut enc_noop);
    let _ = std::fs::write(wal_frame_dir.join("seed_noop"), enc_noop);

    // 3. Hard State
    let hs_dir = corpus_dir.join("fuzz_hard_state");
    let _ = std::fs::create_dir_all(&hs_dir);
    let slot0 = HardStateSlot::new(0, 1, Term::from_raw(2), Some(NodeId::new(1).unwrap()));
    let slot1 = HardStateSlot::new(1, 2, Term::from_raw(2), Some(NodeId::new(1).unwrap()));
    let mut hs_buf = vec![0u8; 1024];
    slot0
        .encode(SectorSize::Sector512, &mut hs_buf[..512])
        .unwrap();
    slot1
        .encode(SectorSize::Sector512, &mut hs_buf[512..])
        .unwrap();
    let _ = std::fs::write(hs_dir.join("seed_both_slots"), &hs_buf);
    let _ = std::fs::write(hs_dir.join("seed_slot0"), &hs_buf[..512]);

    // 4. Snapshot
    let snp_dir = corpus_dir.join("fuzz_snapshot");
    let _ = std::fs::create_dir_all(&snp_dir);
    let snp_hdr = SnapshotHeader::new(
        SectorSize::Sector512,
        LogIndex::from_raw(500),
        Term::from_raw(5),
        ClusterId::from_raw(1),
        GroupId::from_raw(1),
        1,
    );
    let mut snp_hdr_buf = [0u8; 64];
    snp_hdr.encode(&mut snp_hdr_buf).unwrap();
    let _ = std::fs::write(snp_dir.join("seed_header"), snp_hdr_buf);

    let chunk = SnapshotChunk {
        chunk_index: 0,
        payload: Bytes::from_static(b"k1:v1,k2:v2,snapshot_data"),
    };
    let mut chunk_buf = Vec::new();
    chunk.encode(&mut chunk_buf);
    let _ = std::fs::write(snp_dir.join("seed_chunk"), chunk_buf);

    let footer = SnapshotFooter::new(1024, [0xAA; 32]);
    let mut footer_buf = [0u8; 64];
    footer.encode(&mut footer_buf).unwrap();
    let _ = std::fs::write(snp_dir.join("seed_footer"), footer_buf);
}
