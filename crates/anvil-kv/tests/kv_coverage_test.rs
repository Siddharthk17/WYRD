#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(clippy::assert_is_empty)]

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore};
use anvil_types::{LogIndex, SeqNo, SessionId};
use bytes::Bytes;

fn idx(n: u64) -> LogIndex {
    LogIndex::from_raw(n)
}
fn sess(n: u64) -> SessionId {
    SessionId::from_raw(n)
}
fn seq(n: u64) -> SeqNo {
    SeqNo::from_raw(n)
}
fn reg(store: &mut KvStore, index: u64) -> SessionId {
    let mut buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut buf);
    match store.apply(idx(index), &buf).expect("register") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("unexpected {other:?}"),
    }
}
fn put_cmd(session: SessionId, n: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    KvCommand::Put {
        session,
        seq: seq(n),
        acked: seq(n.saturating_sub(1)),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    }
    .encode(&mut buf);
    buf
}

#[test]
fn test_kv_accessors_and_empty_state() {
    let store = KvStore::new(KvConfig::default());
    assert_eq!(store.config().max_key_bytes, 1024);
    assert_eq!(store.virtual_tick(), 0);
    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
    assert_eq!(store.applied_index(), idx(0));
    assert_eq!(store.state_hash(), [0u8; 32]);
    assert!(matches!(
        store.read_register(b"missing"),
        KvResponse::Value { value: None, .. }
    ));
    assert!(matches!(
        store.read_stale(b"missing"),
        KvResponse::Value { value: None, .. }
    ));
}

#[test]
fn test_kv_unknown_session_rejected() {
    let mut store = KvStore::new(KvConfig::default());
    let err = store.apply(idx(5), &put_cmd(sess(999), 1, b"k", b"v"));
    assert!(err.is_err());
}

#[test]
fn test_kv_too_many_sessions_evicts_oldest() {
    let config = KvConfig {
        max_sessions: 2,
        ..KvConfig::default()
    };
    let mut store = KvStore::new(config);
    let a = reg(&mut store, 1);
    let _b = reg(&mut store, 2);
    let _c = reg(&mut store, 3);
    assert!(store.apply(idx(4), &put_cmd(a, 1, b"k", b"v")).is_err());
}

#[test]
fn test_kv_cas_paths() {
    let mut store = KvStore::new(KvConfig::default());
    let s = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(s, 1, b"k", b"v1"))
        .expect("put");
    let mut cas = Vec::new();
    KvCommand::Cas {
        session: s,
        seq: seq(2),
        acked: seq(1),
        key: Bytes::from_static(b"k"),
        expected_version: idx(2),
        value: Bytes::from_static(b"v2"),
    }
    .encode(&mut cas);
    match store.apply(idx(3), &cas).expect("cas") {
        KvResponse::Value { value: Some(v), .. } => assert_eq!(v, Bytes::from_static(b"v2")),
        other => panic!("{other:?}"),
    }
    let mut cas_missing = Vec::new();
    KvCommand::Cas {
        session: s,
        seq: seq(3),
        acked: seq(2),
        key: Bytes::from_static(b"absent"),
        expected_version: idx(0),
        value: Bytes::from_static(b"x"),
    }
    .encode(&mut cas_missing);
    match store.apply(idx(4), &cas_missing).expect("cas missing") {
        KvResponse::Value { value: Some(v), .. } => assert_eq!(v, Bytes::from_static(b"x")),
        other => panic!("{other:?}"),
    }
    let mut put_t = Vec::new();
    KvCommand::Put {
        session: s,
        seq: seq(4),
        acked: seq(3),
        key: Bytes::from_static(b"t"),
        value: Bytes::from_static(b"v"),
    }
    .encode(&mut put_t);
    store.apply(idx(5), &put_t).expect("put t");
    let mut del = Vec::new();
    KvCommand::Delete {
        session: s,
        seq: seq(5),
        acked: seq(4),
        key: Bytes::from_static(b"t"),
    }
    .encode(&mut del);
    match store.apply(idx(6), &del).expect("delete") {
        KvResponse::Value {
            value: None,
            version,
        } => assert_eq!(version, idx(6)),
        other => panic!("{other:?}"),
    }
    let mut cas_after_delete = Vec::new();
    KvCommand::Cas {
        session: s,
        seq: seq(6),
        acked: seq(5),
        key: Bytes::from_static(b"t"),
        expected_version: idx(6),
        value: Bytes::from_static(b"w"),
    }
    .encode(&mut cas_after_delete);
    match store
        .apply(idx(7), &cas_after_delete)
        .expect("cas tombstone")
    {
        KvResponse::Value { value: Some(v), .. } => assert_eq!(v, Bytes::from_static(b"w")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_snapshot_decode_rejects_garbage() {
    let config = KvConfig::default();
    assert!(KvStore::decode_snapshot(config, b"").is_err());
    assert!(KvStore::decode_snapshot(config, b"NOPE").is_err());
    let mut store = KvStore::new(config);
    let s = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(s, 1, b"k", b"v"))
        .expect("put");
    let snap = store.encode_snapshot();
    let mut truncated = snap.to_vec();
    truncated.truncate(10);
    assert!(KvStore::decode_snapshot(config, &truncated).is_err());
    let mut trailing = snap.to_vec();
    trailing.push(0xFF);
    assert!(KvStore::decode_snapshot(config, &trailing).is_err());
    let mut bad_count = snap.to_vec();
    // v03 header before the map count: magic(8) + tick(8) + applied(8) +
    // hash(32) + config(40) = 96 bytes. Overwriting the map count with
    // u64::MAX must fail the 1M-entry bound before any reservation.
    let map_count_offset = 8 + 8 + 8 + 32 + 40;
    assert!(bad_count.len() > map_count_offset + 8);
    bad_count[map_count_offset..map_count_offset + 8].copy_from_slice(&u64::MAX.to_le_bytes());
    assert!(KvStore::decode_snapshot(config, &bad_count).is_err());
    let restored = KvStore::decode_snapshot(config, &snap).expect("roundtrip");
    assert_eq!(restored.len(), 1);
    assert!(!restored.is_empty());
}

#[test]
fn test_kv_snapshot_decode_rejects_oversized_fields() {
    let config = KvConfig {
        max_key_bytes: 4,
        max_value_bytes: 4,
        ..KvConfig::default()
    };
    let mut big = Vec::from(b"ANVKVS02".as_slice());
    big.extend_from_slice(&0u64.to_le_bytes());
    big.extend_from_slice(&1u64.to_le_bytes());
    big.extend_from_slice(&[0u8; 32]);
    big.extend_from_slice(&1u64.to_le_bytes());
    big.extend_from_slice(&100u64.to_le_bytes());
    big.extend_from_slice(b"way-too-long-key");
    assert!(KvStore::decode_snapshot(config, &big).is_err());
}

#[test]
fn test_kv_command_decode_errors() {
    let config = KvConfig::default();
    assert!(KvCommand::decode(&[], config.max_key_bytes, config.max_value_bytes).is_err());
    assert!(KvCommand::decode(&[99], config.max_key_bytes, config.max_value_bytes).is_err());
    let mut trailing = Vec::new();
    KvCommand::RegisterSession.encode(&mut trailing);
    trailing.push(0);
    assert!(KvCommand::decode(&trailing, config.max_key_bytes, config.max_value_bytes).is_err());
    let mut bad_len = vec![1u8];
    bad_len.extend_from_slice(&[0xFF; 12]);
    assert!(KvCommand::decode(&bad_len, 0, 0).is_err());
}

#[test]
fn test_kv_range_bounds_and_empty() {
    let mut store = KvStore::new(KvConfig::default());
    let s = reg(&mut store, 1);
    for (i, key) in [b"m".as_slice(), b"a", b"z", b"b"].iter().enumerate() {
        store
            .apply(idx(2 + i as u64), &put_cmd(s, 1 + i as u64, key, b"v"))
            .expect("put");
    }
    match store.read_range(b"a", b"z", 2) {
        KvResponse::Range { pairs } => assert_eq!(pairs.len(), 2),
        other => panic!("{other:?}"),
    }
    match store.read_range(b"", b"", 100) {
        KvResponse::Range { pairs } => assert_eq!(pairs.len(), 4),
        other => panic!("{other:?}"),
    }
    match store.read_stale(b"m") {
        KvResponse::Value { value: Some(_), .. } => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_response_memo_bounded() {
    let config = KvConfig {
        max_responses_per_session: 2,
        ..KvConfig::default()
    };
    let mut store = KvStore::new(config);
    let s = reg(&mut store, 1);
    for i in 1..=6u64 {
        store
            .apply(idx(1 + i), &put_cmd(s, i, b"k", b"v"))
            .expect("put");
    }
    match store.read_register(b"k") {
        KvResponse::Value { value: Some(_), .. } => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_dedup_hits_for_all_write_kinds() {
    let mut store = KvStore::new(KvConfig::default());
    let s = reg(&mut store, 1);
    let mut del = Vec::new();
    KvCommand::Delete {
        session: s,
        seq: seq(1),
        acked: seq(0),
        key: Bytes::from_static(b"k"),
    }
    .encode(&mut del);
    let first = store.apply(idx(2), &del).expect("delete");
    let second = store.apply(idx(3), &del).expect("delete replay");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    let mut cas = Vec::new();
    KvCommand::Cas {
        session: s,
        seq: seq(2),
        acked: seq(1),
        key: Bytes::from_static(b"k"),
        expected_version: idx(2),
        value: Bytes::from_static(b"v"),
    }
    .encode(&mut cas);
    let first = store.apply(idx(4), &cas).expect("cas");
    let second = store.apply(idx(5), &cas).expect("cas replay");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
    let mut app = Vec::new();
    KvCommand::Append {
        session: s,
        seq: seq(3),
        acked: seq(2),
        key: Bytes::from_static(b"l"),
        value: Bytes::from_static(b"x"),
    }
    .encode(&mut app);
    let first = store.apply(idx(6), &app).expect("append");
    let second = store.apply(idx(7), &app).expect("append replay");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
}

#[test]
fn test_kv_read_list_missing_and_snapshot_with_tombstones() {
    let mut store = KvStore::new(KvConfig::default());
    match store.read_list(b"nope") {
        KvResponse::List { values, .. } => assert!(values.is_empty()),
        other => panic!("{other:?}"),
    }
    let s = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(s, 1, b"gone", b"v"))
        .expect("put");
    let mut del = Vec::new();
    KvCommand::Delete {
        session: s,
        seq: seq(2),
        acked: seq(1),
        key: Bytes::from_static(b"gone"),
    }
    .encode(&mut del);
    store.apply(idx(3), &del).expect("delete");
    let snap = store.encode_snapshot();
    let restored =
        KvStore::decode_snapshot(KvConfig::default(), &snap).expect("snapshot with tombstone");
    match restored.read_register(b"gone") {
        KvResponse::Value {
            value: None,
            version,
        } => assert_eq!(version, idx(3)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_snapshot_decode_truncations() {
    let config = KvConfig::default();
    let mut store = KvStore::new(config);
    let s = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(s, 1, b"k", b"v"))
        .expect("put");
    store
        .apply(idx(3), &put_cmd(s, 2, b"j", b"w"))
        .expect("put");
    let snap = store.encode_snapshot();
    let bytes = snap.to_vec();
    for cut in [8usize, 20, 56, 57, 60, 72, 100] {
        if cut >= bytes.len() {
            continue;
        }
        let _ = KvStore::decode_snapshot(config, &bytes[..cut]);
    }
    assert!(KvStore::decode_snapshot(config, &bytes).is_ok());
    let short: Vec<u8> = b"ANVKVS02".to_vec();
    assert!(KvStore::decode_snapshot(config, &short).is_err());
    let mut no_room = Vec::from(b"ANVKVS02".as_slice());
    no_room.extend_from_slice(&[0u8; 47]);
    assert!(KvStore::decode_snapshot(config, &no_room).is_err());
}

#[test]
fn test_kv_bounds_and_session_limits() {
    let mut store = KvStore::new(KvConfig::default());
    let s = reg(&mut store, 1);
    let mut big_key = Vec::new();
    KvCommand::Delete {
        session: s,
        seq: seq(1),
        acked: seq(0),
        key: Bytes::from(vec![b'k'; 2048]),
    }
    .encode(&mut big_key);
    assert!(store.apply(idx(2), &big_key).is_err());
    let mut big_val = Vec::new();
    KvCommand::Append {
        session: s,
        seq: seq(1),
        acked: seq(0),
        key: Bytes::from_static(b"k"),
        value: Bytes::from(vec![1u8; 2 * 1024 * 1024]),
    }
    .encode(&mut big_val);
    assert!(store.apply(idx(3), &big_val).is_err());
    let dup = reg(&mut store, 1);
    assert_eq!(dup, s);
    let config = KvConfig {
        max_sessions: 0,
        ..KvConfig::default()
    };
    let mut tiny = KvStore::new(config);
    let mut buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut buf);
    assert!(tiny.apply(idx(1), &buf).is_err());
}

#[test]
fn test_kv_response_memo_eviction() {
    let config = KvConfig {
        max_responses_per_session: 0,
        ..KvConfig::default()
    };
    let mut store = KvStore::new(config);
    let s = reg(&mut store, 1);
    for i in 1..=3u64 {
        let mut buf = Vec::new();
        KvCommand::Put {
            session: s,
            seq: SeqNo::from_raw(i),
            acked: SeqNo::from_raw(0),
            key: Bytes::from_static(b"k"),
            value: Bytes::from_static(b"v"),
        }
        .encode(&mut buf);
        store.apply(idx(1 + i), &buf).expect("put");
    }
    match store.read_register(b"k") {
        KvResponse::Value { value: Some(_), .. } => {}
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_command_trailing_and_truncated() {
    let mut s_buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut s_buf);
    let s = sess(9);
    let cases: Vec<Vec<u8>> = vec![
        {
            let mut b = Vec::new();
            KvCommand::Put {
                session: s,
                seq: seq(1),
                acked: seq(0),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            }
            .encode(&mut b);
            b
        },
        {
            let mut b = Vec::new();
            KvCommand::Delete {
                session: s,
                seq: seq(1),
                acked: seq(0),
                key: Bytes::from_static(b"k"),
            }
            .encode(&mut b);
            b
        },
        {
            let mut b = Vec::new();
            KvCommand::Cas {
                session: s,
                seq: seq(1),
                acked: seq(0),
                key: Bytes::from_static(b"k"),
                expected_version: idx(1),
                value: Bytes::from_static(b"v"),
            }
            .encode(&mut b);
            b
        },
        {
            let mut b = Vec::new();
            KvCommand::Append {
                session: s,
                seq: seq(1),
                acked: seq(0),
                key: Bytes::from_static(b"k"),
                value: Bytes::from_static(b"v"),
            }
            .encode(&mut b);
            b
        },
        {
            let mut b = Vec::new();
            KvCommand::ClockTick { tick: 7 }.encode(&mut b);
            b
        },
        {
            let mut b = Vec::new();
            KvCommand::ExpireSessions { up_to_tick: 7 }.encode(&mut b);
            b
        },
        s_buf.clone(),
    ];
    for buf in cases {
        let mut trailing = buf.clone();
        trailing.push(0xFF);
        assert!(
            KvCommand::decode(&trailing, 1024, 1024 * 1024).is_err(),
            "trailing must fail"
        );
        let cut = buf.len().saturating_sub(1).max(1);
        assert!(
            KvCommand::decode(&buf[..cut], 1024, 1024 * 1024).is_err() || buf.len() == 1,
            "truncated must fail"
        );
    }
    assert!(KvCommand::decode(&[], 1024, 1024).is_err());
}
