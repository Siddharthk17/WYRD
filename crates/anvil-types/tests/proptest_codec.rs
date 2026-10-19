#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_types::{
    ClusterId, Entry, Frame, FrameHeader, GroupId, LogIndex, NodeId, SeqNo, SessionId, Term,
    decode_varint_i64, decode_varint_u64, encode_varint_i64, encode_varint_u64,
};
use bytes::Bytes;
use proptest::prelude::*;

proptest! {
    #[test]
    fn prop_varint_u64_roundtrip(val in any::<u64>()) {
        let mut buf = Vec::new();
        encode_varint_u64(val, &mut buf);
        let mut cursor = &buf[..];
        let decoded = decode_varint_u64(&mut cursor).expect("varint must decode successfully");
        prop_assert_eq!(decoded, val);
        prop_assert!(cursor.is_empty());
    }

    #[test]
    fn prop_varint_i64_roundtrip(val in any::<i64>()) {
        let mut buf = Vec::new();
        encode_varint_i64(val, &mut buf);
        let mut cursor = &buf[..];
        let decoded = decode_varint_i64(&mut cursor).expect("zigzag varint must decode");
        prop_assert_eq!(decoded, val);
        prop_assert!(cursor.is_empty());
    }

    #[test]
    fn prop_newtypes_roundtrip(
        node_raw in 1u64..=u64::MAX,
        term_raw in any::<u64>(),
        index_raw in any::<u64>(),
        group_raw in any::<u64>(),
        cluster_raw in any::<u64>(),
        session_raw in any::<u64>(),
        seq_raw in any::<u64>(),
    ) {
        let node_id = NodeId::new(node_raw).expect("non zero");
        let term = Term::from_raw(term_raw);
        let index = LogIndex::from_raw(index_raw);
        let group = GroupId::from_raw(group_raw);
        let cluster = ClusterId::from_raw(cluster_raw);
        let session = SessionId::from_raw(session_raw);
        let seq = SeqNo::from_raw(seq_raw);

        let mut buf = Vec::new();
        node_id.encode(&mut buf);
        term.encode(&mut buf);
        index.encode(&mut buf);
        group.encode(&mut buf);
        cluster.encode(&mut buf);
        session.encode(&mut buf);
        seq.encode(&mut buf);

        let mut cursor = &buf[..];
        prop_assert_eq!(NodeId::decode(&mut cursor).expect("node decode"), node_id);
        prop_assert_eq!(Term::decode(&mut cursor).expect("term decode"), term);
        prop_assert_eq!(LogIndex::decode(&mut cursor).expect("index decode"), index);
        prop_assert_eq!(GroupId::decode(&mut cursor).expect("group decode"), group);
        prop_assert_eq!(ClusterId::decode(&mut cursor).expect("cluster decode"), cluster);
        prop_assert_eq!(SessionId::decode(&mut cursor).expect("session decode"), session);
        prop_assert_eq!(SeqNo::decode(&mut cursor).expect("seq decode"), seq);
        prop_assert!(cursor.is_empty());
    }

    #[test]
    fn prop_entry_roundtrip(
        index_raw in any::<u64>(),
        term_raw in any::<u64>(),
        is_noop in any::<bool>(),
        payload in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let index = LogIndex::from_raw(index_raw);
        let term = Term::from_raw(term_raw);
        let entry = if is_noop {
            Entry::noop(index, term)
        } else {
            Entry::normal(index, term, Bytes::from(payload.clone()))
        };

        let mut buf = Vec::new();
        entry.encode(&mut buf);

        // Test slice decode
        let mut slice_cursor = &buf[..];
        let decoded_slice = Entry::decode_from_slice(&mut slice_cursor)
            .expect("slice decode should succeed");
        prop_assert_eq!(&decoded_slice, &entry);
        prop_assert!(slice_cursor.is_empty());

        // Test zero-copy Bytes decode
        let mut bytes_buf = Bytes::from(buf);
        let decoded_bytes = Entry::decode_from_bytes(&mut bytes_buf)
            .expect("bytes decode should succeed");
        prop_assert_eq!(&decoded_bytes, &entry);
        prop_assert!(bytes_buf.is_empty());
    }

    #[test]
    fn prop_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..8192)) {
        let mut cursor1 = &bytes[..];
        let _ = decode_varint_u64(&mut cursor1);

        let mut cursor2 = &bytes[..];
        let _ = decode_varint_i64(&mut cursor2);

        let mut cursor3 = &bytes[..];
        let _ = FrameHeader::decode(&mut cursor3);

        let mut cursor4 = &bytes[..];
        let _ = Frame::decode_from_slice(&mut cursor4);

        let mut cursor5 = &bytes[..];
        let _ = Entry::decode_from_slice(&mut cursor5);

        let mut bytes_buf = Bytes::from(bytes.clone());
        let _ = Entry::decode_from_bytes(&mut bytes_buf);
    }
}
