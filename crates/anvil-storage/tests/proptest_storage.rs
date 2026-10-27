#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::match_wildcard_for_single_variants
)]

use anvil_storage::{
    HardStateRecoveryOutcome, HardStateSlot, SectorSize, SnapshotChunk, SnapshotFooter,
    SnapshotHeader, WalRecordFrame, WalScanResult, WalSegmentHeader, decode_wal_frame,
    recover_hard_state,
};
use anvil_types::{ClusterId, Entry, EntryKind, GroupId, LogIndex, NodeId, Term};
use bytes::Bytes;
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn prop_wal_header_roundtrip(
        segment_id in any::<u64>(),
        first_index in 1u64..1_000_000,
        prev_term in 0u64..100_000,
        is_4096 in any::<bool>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };

        let header = WalSegmentHeader::new(
            sector_size,
            segment_id,
            LogIndex::from_raw(first_index),
            Term::from_raw(prev_term),
        );

        let mut buf = vec![0u8; sector_size.as_usize()];
        header.encode(&mut buf).unwrap();

        let decoded = WalSegmentHeader::decode(&buf, sector_size).unwrap();
        prop_assert_eq!(header, decoded);
    }

    #[test]
    fn prop_wal_record_roundtrip(
        index in 1u64..1_000_000,
        term in 1u64..100_000,
        is_noop in any::<bool>(),
        payload in prop::collection::vec(any::<u8>(), 0..2048),
        prev_crc in any::<u32>(),
        segment_id in any::<u64>(),
        is_4096 in any::<bool>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };

        let file_offset = sector_size.as_u64();
        let entry = Entry {
            index: LogIndex::from_raw(index),
            term: Term::from_raw(term),
            kind: if is_noop { EntryKind::NoOp } else { EntryKind::Normal },
            payload: Bytes::copy_from_slice(&payload),
        };

        let frame = WalRecordFrame::from_entry(&entry, prev_crc, segment_id, file_offset);
        let mut encoded = Vec::new();
        frame.encode_frame(sector_size, &mut encoded);

        let scan_result = decode_wal_frame(
            &encoded,
            sector_size,
            prev_crc,
            segment_id,
            file_offset,
        );

        match scan_result {
            WalScanResult::Valid { frame: decoded, bytes_consumed, next_crc } => {
                prop_assert_eq!(decoded.to_entry(), entry);
                prop_assert_eq!(bytes_consumed, encoded.len());
                prop_assert_eq!(next_crc, frame.crc32c);
            }
            other => panic!("expected valid scan, got {other:?}"),
        }
    }

    #[test]
    fn prop_arbitrary_bytes_never_panics(
        bytes in prop::collection::vec(any::<u8>(), 0..1024),
        is_4096 in any::<bool>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };

        // Header decode must never panic
        let _ = WalSegmentHeader::decode(&bytes, sector_size);

        // Frame decode must never panic
        let _ = decode_wal_frame(&bytes, sector_size, 0, 1, 0);

        // Hard state recover must never panic
        let _ = recover_hard_state(&bytes, sector_size);

        // Snapshot decoders must never panic
        let _ = SnapshotHeader::decode(&bytes);
        let _ = SnapshotChunk::decode(&bytes, 0);
        let _ = SnapshotFooter::decode(&bytes);
    }

    #[test]
    fn prop_hard_state_roundtrip(
        seq0 in 0u64..1_000_000,
        seq1 in 0u64..1_000_000,
        term in 1u64..100_000,
        node_id_val in 1u64..100,
        has_vote in any::<bool>(),
        is_4096 in any::<bool>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };
        let sector_len = sector_size.as_usize();

        let vote = if has_vote {
            Some(NodeId::new(node_id_val).unwrap())
        } else {
            None
        };

        let slot0 = HardStateSlot::new(0, seq0, Term::from_raw(term), vote);
        let slot1 = HardStateSlot::new(1, seq1, Term::from_raw(term), vote);

        let mut file_buf = vec![0u8; sector_len * 2];
        slot0.encode(sector_size, &mut file_buf[..sector_len]).unwrap();
        slot1.encode(sector_size, &mut file_buf[sector_len..]).unwrap();

        let outcome = recover_hard_state(&file_buf, sector_size).unwrap();
        let expected_active = usize::from(seq0 < seq1);
        let expected_seq = seq0.max(seq1);

        match outcome {
            HardStateRecoveryOutcome::BothValid { active_slot, sequence_no, term: recovered_term, voted_for } => {
                prop_assert_eq!(active_slot, expected_active);
                prop_assert_eq!(sequence_no, expected_seq);
                prop_assert_eq!(recovered_term, Term::from_raw(term));
                prop_assert_eq!(voted_for, vote);
            }
            other @ HardStateRecoveryOutcome::OneSlotCorrupted { .. } => panic!("expected BothValid outcome, got {other:?}"),
        }
    }

    #[test]
    fn prop_snapshot_roundtrip(
        last_index in 1u64..1_000_000,
        last_term in 1u64..100_000,
        cluster_id in 1u64..100,
        group_id in 1u64..100,
        chunk_idx in 0u32..100,
        payload in prop::collection::vec(any::<u8>(), 0..2048),
        total_len in 128u64..10_000_000,
        digest_bytes in prop::collection::vec(any::<u8>(), 32..=32),
        is_4096 in any::<bool>(),
    ) {
        let sector_size = if is_4096 {
            SectorSize::Sector4096
        } else {
            SectorSize::Sector512
        };

        // 1. Snapshot Header roundtrip
        let header = SnapshotHeader::new(
            sector_size,
            LogIndex::from_raw(last_index),
            Term::from_raw(last_term),
            ClusterId::from_raw(cluster_id),
            GroupId::from_raw(group_id),
            1,
        );
        let mut hdr_buf = [0u8; 64];
        header.encode(&mut hdr_buf).unwrap();
        let decoded_hdr = SnapshotHeader::decode(&hdr_buf).unwrap();
        prop_assert_eq!(header, decoded_hdr);

        // 2. Snapshot Chunk roundtrip
        let chunk = SnapshotChunk {
            chunk_index: chunk_idx,
            payload: Bytes::copy_from_slice(&payload),
        };
        let mut chunk_buf = Vec::new();
        chunk.encode(&mut chunk_buf);
        let (decoded_chunk, consumed) = SnapshotChunk::decode(&chunk_buf, chunk_idx).unwrap();
        prop_assert_eq!(chunk, decoded_chunk);
        prop_assert_eq!(chunk_buf.len(), consumed);

        // 3. Snapshot Footer roundtrip
        let mut digest = [0u8; 32];
        digest.copy_from_slice(&digest_bytes);
        let footer = SnapshotFooter::new(total_len, digest);
        let mut footer_buf = [0u8; 64];
        footer.encode(&mut footer_buf).unwrap();
        let decoded_footer = SnapshotFooter::decode(&footer_buf).unwrap();
        prop_assert_eq!(footer, decoded_footer);
    }
}
