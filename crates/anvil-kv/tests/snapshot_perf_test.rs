#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation)]

use anvil_kv::{KvCommand, KvConfig, KvResponse, KvStore};
use anvil_types::{LogIndex, SeqNo};
use bytes::Bytes;

fn idx(n: u64) -> LogIndex {
    LogIndex::from_raw(n)
}

/// Builds a store with `keys` distinct 8-byte keys and single-digit values.
/// Returns the store; the build itself is not measured.
fn build_store(keys: usize) -> KvStore {
    let mut store = KvStore::new(KvConfig::default());
    let mut buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut buf);
    let session = match store.apply(idx(1), &buf).expect("register") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("unexpected {other:?}"),
    };
    let mut key = [0u8; 8];
    for (i, n) in (0..keys).enumerate() {
        let seq = (i + 2) as u64;
        key.copy_from_slice(&(n as u64).to_le_bytes());
        buf.clear();
        KvCommand::Put {
            session,
            seq: SeqNo::from_raw(seq),
            acked: SeqNo::from_raw(seq - 1),
            key: Bytes::copy_from_slice(&key),
            value: Bytes::copy_from_slice(b"v"),
        }
        .encode(&mut buf);
        store.apply(idx(seq), &buf).expect("put");
    }
    store
}

#[test]
fn test_snapshot_clone_million_keys_under_1ms() {
    let store = build_store(1_000_000);
    assert_eq!(store.len(), 1_000_000);
    let start = std::time::Instant::now();
    let snapshot = store.snapshot_handle();
    let elapsed = start.elapsed();
    assert_eq!(snapshot.len(), 1_000_000);
    assert!(
        elapsed.as_millis() < 1,
        "O(1) snapshot clone of 1M keys took {elapsed:?}"
    );
}
