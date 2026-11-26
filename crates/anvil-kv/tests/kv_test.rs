#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore};
use anvil_types::{LogIndex, SeqNo, SessionId};
use bytes::Bytes;

fn idx(n: u64) -> LogIndex {
    LogIndex::from_raw(n)
}
fn seq(n: u64) -> SeqNo {
    SeqNo::from_raw(n)
}

fn put_payload(session: SessionId, n: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
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
fn test_kv_put_get_delete_roundtrip() {
    let mut store = KvStore::new(KvConfig::default());
    let reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg.clone());
    let response = store
        .apply(idx(1), &{
            let mut b = Vec::new();
            KvCommand::RegisterSession.encode(&mut b);
            b
        })
        .expect("register");
    let session = match response {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("expected session, got {other:?}"),
    };
    assert_eq!(session.as_u64(), 1);

    store
        .apply(idx(2), &put_payload(session, 1, b"k1", b"v1"))
        .expect("put");
    match store.read_register(b"k1") {
        KvResponse::Value { value, version } => {
            assert_eq!(value, Some(Bytes::from_static(b"v1")));
            assert_eq!(version, idx(2));
        }
        other => panic!("read {other:?}"),
    }

    let mut del = Vec::new();
    KvCommand::Delete {
        session,
        seq: seq(2),
        acked: seq(1),
        key: Bytes::from_static(b"k1"),
    }
    .encode(&mut del);
    store.apply(idx(3), &del).expect("delete");
    match store.read_register(b"k1") {
        KvResponse::Value { value, .. } => assert_eq!(value, None),
        other => panic!("read {other:?}"),
    }
    let _ = reg;
}

#[test]
fn test_kv_session_dedup_returns_cached_response() {
    let mut store = KvStore::new(KvConfig::default());
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    let session = match store.apply(idx(1), &reg).expect("reg") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("{other:?}"),
    };
    let payload = put_payload(session, 7, b"dup", b"one");
    let first = store.apply(idx(2), &payload).expect("first");
    let second = store.apply(idx(3), &payload).expect("replay");
    assert_eq!(first, second);
    match store.read_register(b"dup") {
        KvResponse::Value { version, .. } => assert_eq!(version, idx(2)),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_cas_conflict_and_append_list() {
    let mut store = KvStore::new(KvConfig::default());
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    let session = match store.apply(idx(1), &reg).expect("reg") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("{other:?}"),
    };
    store
        .apply(idx(2), &put_payload(session, 1, b"c", b"v1"))
        .expect("put");
    let mut cas = Vec::new();
    KvCommand::Cas {
        session,
        seq: seq(2),
        acked: seq(1),
        key: Bytes::from_static(b"c"),
        expected_version: idx(99),
        value: Bytes::from_static(b"v2"),
    }
    .encode(&mut cas);
    match store.apply(idx(3), &cas).expect("cas") {
        KvResponse::CasConflict { version } => assert_eq!(version, idx(2)),
        other => panic!("{other:?}"),
    }
    let mut app = Vec::new();
    KvCommand::Append {
        session,
        seq: seq(3),
        acked: seq(2),
        key: Bytes::from_static(b"l"),
        value: Bytes::from_static(b"a"),
    }
    .encode(&mut app);
    store.apply(idx(4), &app).expect("append");
    let mut app2 = Vec::new();
    KvCommand::Append {
        session,
        seq: seq(4),
        acked: seq(3),
        key: Bytes::from_static(b"l"),
        value: Bytes::from_static(b"b"),
    }
    .encode(&mut app2);
    store.apply(idx(5), &app2).expect("append");
    match store.read_list(b"l") {
        KvResponse::List { values, .. } => assert_eq!(
            values,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        ),
        other => panic!("{other:?}"),
    }
    let hash_before = store.state_hash();
    store
        .apply(idx(6), &put_payload(session, 5, b"k", b"v"))
        .expect("put");
    assert_ne!(hash_before, store.state_hash());
}

#[test]
fn test_kv_session_expiry_and_snapshot_roundtrip() {
    let config = KvConfig {
        session_idle_ticks: 10,
        ..KvConfig::default()
    };
    let mut store = KvStore::new(config);
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    let session = match store.apply(idx(1), &reg).expect("reg") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("{other:?}"),
    };
    store
        .apply(idx(2), &put_payload(session, 1, b"k", b"v"))
        .expect("put");
    let mut tick = Vec::new();
    KvCommand::ClockTick { tick: 100 }.encode(&mut tick);
    store.apply(idx(3), &tick).expect("tick");
    let mut exp = Vec::new();
    KvCommand::ExpireSessions { up_to_tick: 100 }.encode(&mut exp);
    store.apply(idx(4), &exp).expect("expire");
    let dup = put_payload(session, 1, b"k", b"v2");
    let _ = store.apply(idx(5), &dup);
    let snap = store.encode_snapshot();
    let restored = KvStore::decode_snapshot(config, &snap).expect("snapshot");
    assert_eq!(restored.applied_index(), store.applied_index());
    assert_eq!(restored.state_hash(), store.state_hash());
    match restored.read_register(b"k") {
        KvResponse::Value { value, .. } => assert_eq!(value, Some(Bytes::from_static(b"v"))),
        other => panic!("{other:?}"),
    }
}

#[test]
fn test_kv_bounds_rejected_before_allocation() {
    let config = KvConfig {
        max_key_bytes: 4,
        max_value_bytes: 8,
        ..KvConfig::default()
    };
    let mut store = KvStore::new(config);
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    let session = match store.apply(idx(1), &reg).expect("reg") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("{other:?}"),
    };
    let big_key = put_payload(session, 1, b"toolongkey", b"v");
    assert!(store.apply(idx(2), &big_key).is_err());
    let big_value = put_payload(session, 1, b"k", b"way-too-long-value");
    assert!(store.apply(idx(2), &big_value).is_err());
}

#[test]
fn test_kv_range_bounded() {
    let mut store = KvStore::new(KvConfig::default());
    let mut reg = Vec::new();
    KvCommand::RegisterSession.encode(&mut reg);
    let session = match store.apply(idx(1), &reg).expect("reg") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("{other:?}"),
    };
    for (i, key) in [b"a".as_slice(), b"b", b"c"].iter().enumerate() {
        store
            .apply(
                idx(2 + i as u64),
                &put_payload(session, 1 + i as u64, key, b"v"),
            )
            .expect("put");
    }
    match store.read_range(b"a", b"c", 10) {
        KvResponse::Range { pairs } => assert_eq!(pairs.len(), 2),
        other => panic!("{other:?}"),
    }
}
