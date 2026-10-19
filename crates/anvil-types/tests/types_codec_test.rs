#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_types::{
    ClusterId, CodecError, Entry, EntryKind, Frame, FrameType, GroupId, LogIndex, NodeId, SeqNo,
    SessionId, Term, decode_varint_i64, decode_varint_u64, encode_varint_i64, encode_varint_u64,
};
use bytes::Bytes;

#[test]
fn test_node_id_non_zero() {
    assert!(NodeId::new(0).is_none());
    let id = NodeId::new(42).expect("valid node id");
    assert_eq!(id.get(), 42);
    assert_eq!(format!("{id}"), "42");
}

#[test]
fn test_term_arithmetic() {
    let t0 = Term::from_raw(0);
    assert!(t0.is_zero());
    assert_eq!(t0.as_u64(), 0);

    let t1 = t0.next().expect("valid increment");
    assert_eq!(t1.as_u64(), 1);
    assert!(!t1.is_zero());

    let t_max = Term::from_raw(u64::MAX);
    assert!(t_max.next().is_none());
    assert_eq!(t_max.saturating_add(10), Term::from_raw(u64::MAX));

    let t_sub = t1.checked_sub(1).expect("valid sub");
    assert_eq!(t_sub.as_u64(), 0);
    assert!(t_sub.checked_sub(1).is_none());
}

#[test]
fn test_log_index_arithmetic() {
    let idx0 = LogIndex::from_raw(0);
    assert!(idx0.is_zero());
    assert!(idx0.prev().is_none());

    let idx1 = idx0.next().expect("increment to 1");
    assert_eq!(idx1.as_u64(), 1);
    assert_eq!(idx1.prev(), Some(idx0));

    let idx_max = LogIndex::from_raw(u64::MAX);
    assert!(idx_max.next().is_none());
    assert_eq!(idx_max.saturating_add(5), LogIndex::from_raw(u64::MAX));
}

#[test]
fn test_group_cluster_session_seq_no() {
    let g = GroupId::from_raw(7);
    assert_eq!(g.as_u64(), 7);
    assert_eq!(g.checked_add(3), Some(GroupId::from_raw(10)));

    let c = ClusterId::from_raw(99);
    assert_eq!(c.as_u64(), 99);

    let s = SessionId::from_raw(1001);
    assert_eq!(s.as_u64(), 1001);

    let seq = SeqNo::from_raw(0);
    assert_eq!(seq.next(), Some(SeqNo::from_raw(1)));
}

#[test]
fn test_varint_u64_edge_cases() {
    let cases = [
        0,
        1,
        127,
        128,
        255,
        16383,
        16384,
        u64::from(u32::MAX),
        u64::MAX,
    ];
    for val in cases {
        let mut buf = Vec::new();
        encode_varint_u64(val, &mut buf);
        let mut cursor = &buf[..];
        let decoded = decode_varint_u64(&mut cursor).expect("successful decode");
        assert_eq!(decoded, val);
        assert_eq!(cursor, []);
    }
}

#[test]
fn test_varint_overflow_rejection() {
    // 11 continuation bytes
    let bad_buf = [0xFF; 11];
    let mut cursor = &bad_buf[..];
    let err = decode_varint_u64(&mut cursor).unwrap_err();
    assert_eq!(err, CodecError::VarintOverflow);
}

#[test]
fn test_varint_i64_zigzag() {
    let cases: [i64; 9] = [
        0,
        -1,
        1,
        -2,
        2,
        i64::from(i32::MIN),
        i64::from(i32::MAX),
        i64::MIN,
        i64::MAX,
    ];
    for val in cases {
        let mut buf = Vec::new();
        encode_varint_i64(val, &mut buf);
        let mut cursor = &buf[..];
        let decoded = decode_varint_i64(&mut cursor).expect("successful decode");
        assert_eq!(decoded, val);
        assert_eq!(cursor, []);
    }
}

#[test]
fn test_entry_roundtrip_bytes_zero_copy() {
    let payload_bytes = vec![0xAA; 1024];
    let entry = Entry::normal(
        LogIndex::from_raw(100),
        Term::from_raw(3),
        Bytes::from(payload_bytes.clone()),
    );

    let mut buf = Vec::new();
    entry.encode(&mut buf);

    let mut bytes_buf = Bytes::from(buf);
    let decoded = Entry::decode_from_bytes(&mut bytes_buf).expect("decode from bytes");

    assert_eq!(decoded.index, LogIndex::from_raw(100));
    assert_eq!(decoded.term, Term::from_raw(3));
    assert_eq!(decoded.kind, EntryKind::Normal);
    assert_eq!(decoded.payload.as_ref(), &payload_bytes[..]);
    assert_eq!(bytes_buf.len(), 0);
}

#[test]
fn test_entry_noop_roundtrip() {
    let entry = Entry::noop(LogIndex::from_raw(42), Term::from_raw(5));
    let mut buf = Vec::new();
    entry.encode(&mut buf);

    let mut cursor = &buf[..];
    let decoded = Entry::decode_from_slice(&mut cursor).expect("decode from slice");
    assert_eq!(decoded.kind, EntryKind::NoOp);
    assert_eq!(decoded.payload.len(), 0);
}

#[test]
fn test_crc_corruption_rejection() {
    let entry = Entry::normal(
        LogIndex::from_raw(1),
        Term::from_raw(1),
        Bytes::from_static(b"hello world"),
    );
    let mut buf = Vec::new();
    entry.encode(&mut buf);

    // Corrupt one payload byte
    let len = buf.len();
    buf[len - 1] ^= 0xFF;

    let mut cursor = &buf[..];
    let err = Entry::decode_from_slice(&mut cursor).unwrap_err();
    match err {
        CodecError::ChecksumMismatch { .. } => {}
        other => panic!("expected ChecksumMismatch, got: {other:?}"),
    }
}

#[test]
fn test_invalid_magic_rejection() {
    let mut buf = Vec::new();
    Frame::encode(FrameType::Entry, b"test", &mut buf);
    buf[0] = b'X';

    let mut cursor = &buf[..];
    let err = Frame::decode_from_slice(&mut cursor).unwrap_err();
    match err {
        CodecError::InvalidMagic { .. } => {}
        other => panic!("expected InvalidMagic, got: {other:?}"),
    }
}

#[test]
fn test_dependency_allowlist() {
    // Charter Rule: deterministic crates must not depend on forbidden libraries
    let cargo_toml = include_str!("../Cargo.toml");
    let forbidden = ["tokio", "rand ", "instant", "hashbrown", "crossbeam"];
    for keyword in forbidden {
        assert!(
            !cargo_toml.contains(&format!("dependencies.{keyword}")),
            "forbidden dependency detected: {keyword}"
        );
    }
}
