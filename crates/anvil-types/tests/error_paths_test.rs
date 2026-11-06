#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_types::codec::decode_varint_u64_slice;
use anvil_types::{
    ClusterId, CodecError, Entry, EntryKind, Frame, FrameHeader, FrameType, GroupId, LogIndex,
    NodeId, SeqNo, SessionId, Term, decode_u16_le, decode_u32_le, decode_u64_le, decode_varint_i64,
    decode_varint_u64, encode_u16_le, encode_u32_le, encode_u64_le, encode_varint_i64,
    encode_varint_u64,
};
use bytes::Bytes;

#[test]
fn test_frame_type_discriminants() {
    assert_eq!(FrameType::from_u16(1), Some(FrameType::Entry));
    assert_eq!(FrameType::from_u16(2), Some(FrameType::Heartbeat));
    assert_eq!(FrameType::from_u16(3), Some(FrameType::Message));
    assert_eq!(FrameType::from_u16(0), None);
    assert_eq!(FrameType::from_u16(99), None);
    assert_eq!(FrameType::Entry.as_u16(), 1);
    assert_eq!(FrameType::Heartbeat.as_u16(), 2);
    assert_eq!(FrameType::Message.as_u16(), 3);
}

#[test]
fn test_frame_header_rejects_bad_version() {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"ANVL");
    raw.extend_from_slice(&2u16.to_le_bytes());
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    let mut cursor = &raw[..];
    assert_eq!(
        FrameHeader::decode(&mut cursor),
        Err(CodecError::UnsupportedVersion(2))
    );
}

#[test]
fn test_frame_header_rejects_oversize_payload() {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"ANVL");
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&1u16.to_le_bytes());
    raw.extend_from_slice(&u32::MAX.to_le_bytes());
    raw.extend_from_slice(&0u32.to_le_bytes());
    let mut cursor = &raw[..];
    match FrameHeader::decode(&mut cursor) {
        Err(CodecError::PayloadTooLarge { .. }) => {}
        other => panic!("expected PayloadTooLarge, got {other:?}"),
    }
}

#[test]
fn test_frame_header_rejects_truncated_input() {
    let mut cursor = &[][..];
    assert!(FrameHeader::decode(&mut cursor).is_err());
    let mut short = &[0x41u8; 15][..];
    assert!(FrameHeader::decode(&mut short).is_err());
}

#[test]
fn test_frame_slice_decode_rejects_truncation() {
    let mut cursor = &[][..];
    assert!(Frame::decode_from_slice(&mut cursor).is_err());

    let entry = Entry::noop(LogIndex::from_raw(1), Term::from_raw(1));
    let mut buf = Vec::new();
    entry.encode(&mut buf);
    let mut clipped = &buf[..buf.len() - 1];
    assert!(Frame::decode_from_slice(&mut clipped).is_err());

    let mut header_only = &buf[..FrameHeader::HEADER_LEN];
    assert!(Frame::decode_from_slice(&mut header_only).is_err());
}

#[test]
fn test_frame_bytes_decode_rejects_truncation() {
    let mut empty = Bytes::new();
    assert!(Frame::decode_from_bytes(&mut empty).is_err());

    let entry = Entry::noop(LogIndex::from_raw(1), Term::from_raw(1));
    let mut buf = Vec::new();
    entry.encode(&mut buf);
    let mut clipped = Bytes::from(buf[..buf.len() - 2].to_vec());
    assert!(Frame::decode_from_bytes(&mut clipped).is_err());
}

#[test]
fn test_varint_rejects_empty_and_overflow() {
    let mut empty = &[][..];
    assert!(decode_varint_u64(&mut empty).is_err());
    assert!(decode_varint_u64_slice(&[]).is_err());

    let bad = [0xFFu8; 11];
    let mut cursor = &bad[..];
    assert_eq!(
        decode_varint_u64(&mut cursor),
        Err(CodecError::VarintOverflow)
    );

    let mut wide = [0x80u8; 10];
    wide[9] = 0x02;
    let mut wide_cursor = &wide[..];
    assert_eq!(
        decode_varint_u64(&mut wide_cursor),
        Err(CodecError::VarintOverflow)
    );

    let cont = [0x80u8; 10];
    let mut cont_cursor = &cont[..];
    assert_eq!(
        decode_varint_u64(&mut cont_cursor),
        Err(CodecError::VarintOverflow)
    );

    let mut max = [0xFFu8; 10];
    max[9] = 0x01;
    let mut max_cursor = &max[..];
    assert_eq!(
        decode_varint_u64(&mut max_cursor).expect("u64 max decodes"),
        u64::MAX
    );
}

#[test]
fn test_varint_i64_extremes() {
    for extreme in [i64::MIN, i64::MAX] {
        let mut buf = Vec::new();
        encode_varint_i64(extreme, &mut buf);
        let mut cursor = &buf[..];
        assert_eq!(decode_varint_i64(&mut cursor).expect("decode"), extreme);
    }
}

#[test]
fn test_fixed_width_roundtrip_and_truncation() {
    let mut buf = Vec::new();
    encode_u16_le(0xABCD, &mut buf);
    encode_u32_le(0xDEAD_BEEF, &mut buf);
    encode_u64_le(0x0123_4567_89AB_CDEF, &mut buf);
    let mut cursor = &buf[..];
    assert_eq!(decode_u16_le(&mut cursor).expect("u16"), 0xABCD);
    assert_eq!(decode_u32_le(&mut cursor).expect("u32"), 0xDEAD_BEEF);
    assert_eq!(
        decode_u64_le(&mut cursor).expect("u64"),
        0x0123_4567_89AB_CDEF
    );
    assert_eq!(cursor, []);

    let mut one = &[0xAAu8][..];
    assert!(decode_u16_le(&mut one).is_err());
    let mut three = &[0xAAu8; 3][..];
    assert!(decode_u32_le(&mut three).is_err());
    let mut seven = &[0xAAu8; 7][..];
    assert!(decode_u64_le(&mut seven).is_err());
}

#[test]
fn test_entry_kind_discriminants() {
    assert_eq!(EntryKind::from_u8(1), Some(EntryKind::Normal));
    assert_eq!(EntryKind::from_u8(2), Some(EntryKind::NoOp));
    assert_eq!(EntryKind::from_u8(0), None);
    assert_eq!(EntryKind::from_u8(3), None);
    assert_eq!(EntryKind::Normal.as_u8(), 1);
    assert_eq!(EntryKind::NoOp.as_u8(), 2);
}

#[test]
fn test_entry_new_constructor() {
    let payload = Bytes::from_static(b"cmd");
    let entry = Entry::new(
        LogIndex::from_raw(9),
        Term::from_raw(4),
        EntryKind::Normal,
        payload,
    );
    assert_eq!(entry.index, LogIndex::from_raw(9));
    assert_eq!(entry.term, Term::from_raw(4));
    assert_eq!(entry.kind, EntryKind::Normal);
    assert_eq!(entry.payload.as_ref(), b"cmd");
}

#[test]
fn test_entry_slice_decode_rejects_bad_body() {
    let mut heartbeat = Vec::new();
    Frame::encode(FrameType::Heartbeat, b"ping", &mut heartbeat);
    let mut cursor = &heartbeat[..];
    assert_eq!(
        Entry::decode_from_slice(&mut cursor),
        Err(CodecError::UnknownFrameType(2))
    );

    let mut body = Vec::new();
    encode_varint_u64(7, &mut body);
    encode_varint_u64(3, &mut body);
    body.push(9);
    let mut framed = Vec::new();
    Frame::encode(FrameType::Entry, &body, &mut framed);
    let mut bad_kind = &framed[..];
    assert_eq!(
        Entry::decode_from_slice(&mut bad_kind),
        Err(CodecError::UnknownEntryKind(9))
    );

    let mut no_kind_body = Vec::new();
    encode_varint_u64(7, &mut no_kind_body);
    encode_varint_u64(3, &mut no_kind_body);
    let mut no_kind_framed = Vec::new();
    Frame::encode(FrameType::Entry, &no_kind_body, &mut no_kind_framed);
    let mut no_kind = &no_kind_framed[..];
    assert!(Entry::decode_from_slice(&mut no_kind).is_err());
}

#[test]
fn test_entry_bytes_decode_rejects_bad_body() {
    let mut heartbeat = Vec::new();
    Frame::encode(FrameType::Heartbeat, b"ping", &mut heartbeat);
    let mut heartbeat_bytes = Bytes::from(heartbeat);
    assert_eq!(
        Entry::decode_from_bytes(&mut heartbeat_bytes),
        Err(CodecError::UnknownFrameType(2))
    );

    let mut body = Vec::new();
    encode_varint_u64(7, &mut body);
    encode_varint_u64(3, &mut body);
    let mut framed = Vec::new();
    Frame::encode(FrameType::Entry, &body, &mut framed);
    let mut framed_bytes = Bytes::from(framed);
    assert!(Entry::decode_from_bytes(&mut framed_bytes).is_err());

    let mut entry_buf = Vec::new();
    Entry::normal(
        LogIndex::from_raw(1),
        Term::from_raw(1),
        Bytes::from_static(b"payload"),
    )
    .encode(&mut entry_buf);
    let last = entry_buf.len() - 1;
    entry_buf[last] ^= 0xFF;
    let mut corrupt = Bytes::from(entry_buf);
    match Entry::decode_from_bytes(&mut corrupt) {
        Err(CodecError::ChecksumMismatch { .. }) => {}
        other => panic!("expected ChecksumMismatch, got {other:?}"),
    }
}

#[test]
fn test_node_id_conversions() {
    let nonzero = core::num::NonZeroU64::new(7).expect("nonzero");
    let from_nz = NodeId::from_nonzero(nonzero);
    assert_eq!(from_nz.get(), 7);
    assert_eq!(from_nz.as_nonzero(), nonzero);
    assert_eq!(NodeId::try_from(7u64).expect("try from"), from_nz);
    assert!(NodeId::try_from(0u64).is_err());
}

#[test]
fn test_term_edges() {
    assert_eq!(
        Term::from_raw(u64::MAX).checked_add(1),
        None,
        "term add overflow"
    );
    assert_eq!(Term::from_raw(0).checked_sub(1), None, "term sub underflow");
    assert_eq!(
        Term::from_raw(u64::MAX).checked_add(0),
        Some(Term::from_raw(u64::MAX))
    );
    assert_eq!(Term::from_raw(5).checked_sub(5), Some(Term::from_raw(0)));
    assert_eq!(format!("{}", Term::from_raw(12)), "12", "term display");
}

#[test]
fn test_log_index_edges() {
    assert_eq!(LogIndex::from_raw(1).checked_add(u64::MAX), None);
    assert_eq!(
        LogIndex::from_raw(3).checked_add(4),
        Some(LogIndex::from_raw(7))
    );
    assert_eq!(
        LogIndex::from_raw(0).saturating_sub(9),
        LogIndex::from_raw(0)
    );
    assert_eq!(
        LogIndex::from_raw(9).saturating_sub(4),
        LogIndex::from_raw(5)
    );
    assert_eq!(
        LogIndex::from_raw(u64::MAX).checked_sub(u64::MAX),
        Some(LogIndex::from_raw(0))
    );
    assert_eq!(
        LogIndex::from_raw(9).checked_sub(4),
        Some(LogIndex::from_raw(5))
    );
    assert_eq!(format!("{}", LogIndex::from_raw(3)), "3", "index display");
}

#[test]
fn test_group_id_edges() {
    let group = GroupId::from_raw(u64::MAX);
    assert_eq!(group.checked_add(1), None);
    assert_eq!(GroupId::from_raw(0).checked_sub(1), None);
    assert_eq!(
        GroupId::from_raw(4).checked_add(4),
        Some(GroupId::from_raw(8))
    );
    assert_eq!(
        GroupId::from_raw(4).checked_sub(4),
        Some(GroupId::from_raw(0))
    );
    assert_eq!(format!("{group}"), format!("{}", u64::MAX));
}

#[test]
fn test_cluster_id_edges() {
    let cluster = ClusterId::from_raw(u64::MAX);
    assert_eq!(cluster.checked_add(1), None);
    assert_eq!(ClusterId::from_raw(0).checked_sub(1), None);
    assert_eq!(
        ClusterId::from_raw(4).checked_add(4),
        Some(ClusterId::from_raw(8))
    );
    assert_eq!(
        ClusterId::from_raw(4).checked_sub(4),
        Some(ClusterId::from_raw(0))
    );
    assert_eq!(format!("{cluster}"), format!("{}", u64::MAX));
}

#[test]
fn test_session_id_edges() {
    let session = SessionId::from_raw(u64::MAX);
    assert_eq!(session.checked_add(1), None);
    assert_eq!(SessionId::from_raw(0).checked_sub(1), None);
    assert_eq!(
        SessionId::from_raw(4).checked_add(4),
        Some(SessionId::from_raw(8))
    );
    assert_eq!(
        SessionId::from_raw(4).checked_sub(4),
        Some(SessionId::from_raw(0))
    );
    assert_eq!(format!("{session}"), format!("{}", u64::MAX));
}

#[test]
fn test_seq_no_edges() {
    assert_eq!(SeqNo::from_raw(u64::MAX).next(), None);
    assert_eq!(SeqNo::from_raw(u64::MAX).checked_add(1), None);
    assert_eq!(SeqNo::from_raw(0).checked_sub(1), None);
    assert_eq!(SeqNo::from_raw(4).checked_add(4), Some(SeqNo::from_raw(8)));
    assert_eq!(SeqNo::from_raw(4).checked_sub(4), Some(SeqNo::from_raw(0)));
    assert_eq!(format!("{}", SeqNo::from_raw(6)), "6", "seq display");
}
