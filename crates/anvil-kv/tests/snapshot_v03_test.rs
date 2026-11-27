#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore, SnapshotPolicy};
use anvil_types::{LogIndex, SeqNo, SessionId};
use bytes::Bytes;

fn idx(n: u64) -> LogIndex {
    LogIndex::from_raw(n)
}

fn register(store: &mut KvStore, index: u64) -> SessionId {
    let mut buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut buf);
    match store.apply(idx(index), &buf).expect("register") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("unexpected {other:?}"),
    }
}

fn put(store: &mut KvStore, index: u64, session: SessionId, n: u64, key: &[u8], value: &[u8]) {
    let mut buf = Vec::new();
    KvCommand::Put {
        session,
        seq: SeqNo::from_raw(n),
        acked: SeqNo::from_raw(n.saturating_sub(1)),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    }
    .encode(&mut buf);
    store.apply(idx(index), &buf).expect("put");
}

#[test]
fn test_snapshot_v03_roundtrips_session_table() {
    let config = KvConfig::default();
    let mut store = KvStore::new(config);
    let session = register(&mut store, 1);
    put(&mut store, 2, session, 1, b"k", b"v1");
    put(&mut store, 3, session, 2, b"k", b"v2");
    assert_eq!(store.session_count(), 1);

    let snap = store.encode_snapshot();
    assert!(snap.starts_with(b"ANVKVS03"));
    let restored = KvStore::decode_snapshot(config, &snap).expect("v03 roundtrip");
    assert_eq!(restored.session_count(), 1);
    assert_eq!(restored.applied_index(), store.applied_index());
    assert_eq!(restored.state_hash(), store.state_hash());
    assert_eq!(restored.snapshot_index(), store.applied_index());

    match restored.read_register(b"k") {
        KvResponse::Value { value, .. } => assert_eq!(value, Some(Bytes::from_static(b"v2"))),
        other => panic!("{other:?}"),
    }

    let mut restored = restored;
    let mut replay = Vec::new();
    KvCommand::Put {
        session,
        seq: SeqNo::from_raw(2),
        acked: SeqNo::from_raw(1),
        key: Bytes::from_static(b"k"),
        value: Bytes::from_static(b"v2"),
    }
    .encode(&mut replay);
    let cached = restored.apply(idx(4), &replay).expect("replay");
    match cached {
        KvResponse::Value { value, version } => {
            assert_eq!(value, Some(Bytes::from_static(b"v2")));
            assert_eq!(version, idx(3));
        }
        other => panic!("dedup lost across snapshot: {other:?}"),
    }
}

#[test]
fn test_snapshot_v03_rejects_config_mismatch() {
    let mut store = KvStore::new(KvConfig::default());
    let session = register(&mut store, 1);
    put(&mut store, 2, session, 1, b"k", b"v");
    let snap = store.encode_snapshot();
    let other = KvConfig {
        max_key_bytes: 512,
        ..KvConfig::default()
    };
    assert!(KvStore::decode_snapshot(other, &snap).is_err());
}

#[test]
fn test_snapshot_v02_legacy_still_decodes() {
    let config = KvConfig::default();
    let mut raw = Vec::from(b"ANVKVS02".as_slice());
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&2u64.to_le_bytes());
    raw.extend_from_slice(&[9u8; 32]);
    raw.extend_from_slice(&0u64.to_le_bytes());
    raw.extend_from_slice(&0u64.to_le_bytes());
    let restored = KvStore::decode_snapshot(config, &raw).expect("v02 legacy");
    assert_eq!(restored.applied_index(), idx(2));
    assert_eq!(restored.virtual_tick(), 7);
    assert_eq!(restored.session_count(), 0);
}

#[test]
fn test_snapshot_policy_triggers_and_compaction_bounds() {
    let policy = SnapshotPolicy::default();
    assert!(!policy.should_snapshot(0, 0));
    assert!(policy.should_snapshot(policy.max_entries_since_snapshot, 0));
    assert!(policy.should_snapshot(0, policy.max_bytes_since_snapshot));

    assert_eq!(policy.compaction_index(idx(100), idx(90), false), None);
    assert_eq!(policy.compaction_index(idx(50), idx(90), true), None);
    assert_eq!(policy.compaction_index(idx(600), idx(100), true), None);

    let compact = policy
        .compaction_index(idx(2_000), idx(1_500), true)
        .expect("compacts with retain window");
    assert_eq!(compact, idx(1_500 - policy.retain_entries));
    assert!(compact <= idx(2_000));
}

#[test]
fn test_snapshot_chunks_roundtrip_and_reject_damage() {
    let config = KvConfig::default();
    let mut store = KvStore::new(config);
    let session = register(&mut store, 1);
    put(&mut store, 2, session, 1, b"k", b"v");
    let (chunks, digest) = store.encode_snapshot_chunked(64);
    assert!(chunks.len() > 1);
    let restored =
        KvStore::decode_snapshot_chunked(config, &chunks, &digest).expect("chunk roundtrip");
    assert_eq!(restored.state_hash(), store.state_hash());

    let mut bad_crc = chunks.clone();
    let mut first = bad_crc[0].payload.to_vec();
    first[0] ^= 0xFF;
    bad_crc[0].payload = Bytes::from(first);
    assert!(KvStore::decode_snapshot_chunked(config, &bad_crc, &digest).is_err());

    let mut bad_digest = digest;
    bad_digest[0] ^= 0xFF;
    assert!(KvStore::decode_snapshot_chunked(config, &chunks, &bad_digest).is_err());
}

#[test]
fn test_snapshot_handle_shares_map_clone() {
    let config = KvConfig::default();
    let mut store = KvStore::new(config);
    let session = register(&mut store, 1);
    for i in 0..2_000u64 {
        let key = format!("key-{i:05}");
        put(&mut store, 2 + i, session, 1 + i, key.as_bytes(), b"v");
    }
    let handle = store.snapshot_handle();
    assert_eq!(handle.state_hash(), store.state_hash());
    assert_eq!(handle.len(), store.len());
}
