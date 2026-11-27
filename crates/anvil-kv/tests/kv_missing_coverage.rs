#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#![allow(clippy::assert_is_empty)]

// Focused coverage for snapshot decode error paths, v02 legacy entries,
// chunk reassembly failures, and every session-response variant.
// Layouts below mirror `KvStore::encode_snapshot`; offsets are asserted
// so any format drift fails fast instead of covering the wrong line.

use anvil_kv::{
    ChunkError, KvCommand, KvConfig, KvResponse, KvStore, reassemble_chunks, split_into_chunks,
};
use anvil_types::{LogIndex, MembershipSnapshot, SeqNo, SessionId};
use bytes::Bytes;

fn idx(n: u64) -> LogIndex {
    LogIndex::from_raw(n)
}

fn sess(n: u64) -> SessionId {
    SessionId::from_raw(n)
}

fn reg(store: &mut KvStore, index: u64) -> SessionId {
    let mut buf = Vec::new();
    KvCommand::RegisterSession.encode(&mut buf);
    match store.apply(idx(index), &buf).expect("register session") {
        KvResponse::SessionRegistered { session } => session,
        other => panic!("expected session, got {other:?}"),
    }
}

fn put_cmd(session: SessionId, seq_no: u64, acked: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    KvCommand::Put {
        session,
        seq: SeqNo::from_raw(seq_no),
        acked: SeqNo::from_raw(acked),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    }
    .encode(&mut buf);
    buf
}

fn del_cmd(session: SessionId, seq_no: u64, acked: u64, key: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    KvCommand::Delete {
        session,
        seq: SeqNo::from_raw(seq_no),
        acked: SeqNo::from_raw(acked),
        key: Bytes::copy_from_slice(key),
    }
    .encode(&mut buf);
    buf
}

fn append_cmd(session: SessionId, seq_no: u64, acked: u64, key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut buf = Vec::new();
    KvCommand::Append {
        session,
        seq: SeqNo::from_raw(seq_no),
        acked: SeqNo::from_raw(acked),
        key: Bytes::copy_from_slice(key),
        value: Bytes::copy_from_slice(value),
    }
    .encode(&mut buf);
    buf
}

fn cas_cmd(
    session: SessionId,
    seq_no: u64,
    acked: u64,
    key: &[u8],
    expected: u64,
    value: &[u8],
) -> Vec<u8> {
    let mut buf = Vec::new();
    KvCommand::Cas {
        session,
        seq: SeqNo::from_raw(seq_no),
        acked: SeqNo::from_raw(acked),
        key: Bytes::copy_from_slice(key),
        expected_version: LogIndex::from_raw(expected),
        value: Bytes::copy_from_slice(value),
    }
    .encode(&mut buf);
    buf
}

fn replay(store: &mut KvStore, index: u64, session: SessionId, seq_no: u64) -> KvResponse {
    store
        .apply(idx(index), &put_cmd(session, seq_no, 0, b"probe", b"probe"))
        .expect("replay cached response")
}

fn patch_u64(bytes: &[u8], offset: usize, value: u64) -> Vec<u8> {
    let mut out = bytes.to_vec();
    out[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
    out
}

// One put of b"k" -> b"v" (session 1, applied 2). Layout: header+config
// 96, map count 96..104, key_len 104..112, key 112, is_list 113,
// version 114..122, elements_len 122..130, elem len 130..138, elem 138,
// tomb count 139..147, session count 147..155, id 155..163, activity
// 163..171, highest 171..179, responses_len 179..187, seq 187..195,
// blob len 195..203, blob 203..222, order tick 222..230. Total 230.
fn base_v03_single() -> Vec<u8> {
    let mut store = KvStore::new(KvConfig::default());
    let session = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(session, 1, 0, b"k", b"v"))
        .expect("put");
    let snap = store.encode_snapshot(&MembershipSnapshot::empty()).to_vec();
    assert_eq!(snap.len(), 233);
    snap
}

// Put then delete of b"k": map empty, one tombstone. Layout: header 96,
// map count 96..104 (=0), tomb count 104..112 (=1), tomb key_len
// 112..120, key 120, tomb version 121..129, session count 129..137,
// entry 137..195 (id, activity, highest=2, one response seq 2 with a
// 10-byte None-value blob), order tick 195..203, empty membership 203..206. Total 206.
fn base_v03_tomb() -> Vec<u8> {
    let mut store = KvStore::new(KvConfig::default());
    let session = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(session, 1, 0, b"k", b"v"))
        .expect("put");
    store
        .apply(idx(3), &del_cmd(session, 2, 1, b"k"))
        .expect("delete");
    let snap = store.encode_snapshot(&MembershipSnapshot::empty()).to_vec();
    assert_eq!(snap.len(), 206);
    snap
}

// Hand-built v02: tick 7, applied 3, one entry (b"k" -> [b"v"] ver 2),
// one tombstone (b"gone" ver 3). Layout: magic 0..8, tick 8..16,
// applied 16..24, hash 24..56, count 56..64, key_len 64..72, key 72,
// is_list 73, version 74..82, elements_len 82..90, elem len 90..98,
// elem 98, tomb count 99..107, tomb key_len 107..115, tomb key
// 115..119, tomb version 119..127. Total 127.
fn base_v02_entry_tomb() -> Vec<u8> {
    let mut raw = Vec::new();
    raw.extend_from_slice(b"ANVKVS02");
    raw.extend_from_slice(&7u64.to_le_bytes());
    raw.extend_from_slice(&3u64.to_le_bytes());
    raw.extend_from_slice(&[9u8; 32]);
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(b"k");
    raw.push(0);
    raw.extend_from_slice(&2u64.to_le_bytes());
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(b"v");
    raw.extend_from_slice(&1u64.to_le_bytes());
    raw.extend_from_slice(&4u64.to_le_bytes());
    raw.extend_from_slice(b"gone");
    raw.extend_from_slice(&3u64.to_le_bytes());
    assert_eq!(raw.len(), 127);
    raw
}

fn v03_header(tick: u64, applied: u64) -> Vec<u8> {
    let config = KvConfig::default();
    let mut head = Vec::new();
    head.extend_from_slice(b"ANVKVS03");
    head.extend_from_slice(&tick.to_le_bytes());
    head.extend_from_slice(&applied.to_le_bytes());
    head.extend_from_slice(&[7u8; 32]);
    head.extend_from_slice(&(config.max_key_bytes as u64).to_le_bytes());
    head.extend_from_slice(&(config.max_value_bytes as u64).to_le_bytes());
    head.extend_from_slice(&config.session_idle_ticks.to_le_bytes());
    head.extend_from_slice(&(config.max_sessions as u64).to_le_bytes());
    head.extend_from_slice(&(config.max_responses_per_session as u64).to_le_bytes());
    head
}

fn map_entry(key: &[u8], is_list: bool, version: u64, elements: &[Vec<u8>]) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&(key.len() as u64).to_le_bytes());
    entry.extend_from_slice(key);
    entry.push(u8::from(is_list));
    entry.extend_from_slice(&version.to_le_bytes());
    entry.extend_from_slice(&(elements.len() as u64).to_le_bytes());
    for element in elements {
        entry.extend_from_slice(&(element.len() as u64).to_le_bytes());
        entry.extend_from_slice(element);
    }
    entry
}

fn tomb_entry(key: &[u8], version: u64) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&(key.len() as u64).to_le_bytes());
    entry.extend_from_slice(key);
    entry.extend_from_slice(&version.to_le_bytes());
    entry
}

fn session_entry(
    id: u64,
    activity: u64,
    highest: u64,
    responses: &[(u64, Vec<u8>)],
    tick: u64,
) -> Vec<u8> {
    let mut entry = Vec::new();
    entry.extend_from_slice(&id.to_le_bytes());
    entry.extend_from_slice(&activity.to_le_bytes());
    entry.extend_from_slice(&highest.to_le_bytes());
    entry.extend_from_slice(&(responses.len() as u64).to_le_bytes());
    for (seq_no, blob) in responses {
        entry.extend_from_slice(&seq_no.to_le_bytes());
        entry.extend_from_slice(&(blob.len() as u64).to_le_bytes());
        entry.extend_from_slice(blob);
    }
    entry.extend_from_slice(&tick.to_le_bytes());
    entry
}

fn blob_ok() -> Vec<u8> {
    vec![0u8]
}

fn blob_registered(session: u64) -> Vec<u8> {
    let mut blob = vec![1u8];
    blob.extend_from_slice(&session.to_le_bytes());
    blob
}

fn blob_value_some(value: &[u8], version: u64) -> Vec<u8> {
    let mut blob = vec![2u8, 1u8];
    blob.extend_from_slice(&(value.len() as u64).to_le_bytes());
    blob.extend_from_slice(value);
    blob.extend_from_slice(&version.to_le_bytes());
    blob
}

fn blob_value_none(version: u64) -> Vec<u8> {
    let mut blob = vec![2u8, 0u8];
    blob.extend_from_slice(&version.to_le_bytes());
    blob
}

fn blob_list(values: &[Vec<u8>], version: u64) -> Vec<u8> {
    let mut blob = vec![3u8];
    blob.extend_from_slice(&(values.len() as u64).to_le_bytes());
    for value in values {
        blob.extend_from_slice(&(value.len() as u64).to_le_bytes());
        blob.extend_from_slice(value);
    }
    blob.extend_from_slice(&version.to_le_bytes());
    blob
}

fn blob_range(pairs: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut blob = vec![4u8];
    blob.extend_from_slice(&(pairs.len() as u64).to_le_bytes());
    for (key, value) in pairs {
        blob.extend_from_slice(&(key.len() as u64).to_le_bytes());
        blob.extend_from_slice(key);
        blob.extend_from_slice(&(value.len() as u64).to_le_bytes());
        blob.extend_from_slice(value);
    }
    blob
}

fn blob_conflict(version: u64) -> Vec<u8> {
    let mut blob = vec![5u8];
    blob.extend_from_slice(&version.to_le_bytes());
    blob
}

fn blob_tag(tag: u8) -> Vec<u8> {
    vec![tag]
}

// Empty map/tombs with one session holding the given response blobs.
fn v03_single_session(blobs: &[(u64, Vec<u8>)]) -> Vec<u8> {
    let mut snap = v03_header(9, 11);
    snap.extend_from_slice(&0u64.to_le_bytes());
    snap.extend_from_slice(&0u64.to_le_bytes());
    snap.extend_from_slice(&1u64.to_le_bytes());
    snap.extend_from_slice(&session_entry(1, 5, 10, blobs, 5));
    snap
}

// Map with an empty-elements record plus a normal record, one tombstone,
// and one session holding one response of every variant.
fn v03_full() -> Vec<u8> {
    let mut snap = v03_header(9, 11);
    snap.extend_from_slice(&2u64.to_le_bytes());
    snap.extend_from_slice(&map_entry(b"empty", false, 3, &[]));
    snap.extend_from_slice(&map_entry(b"k", false, 2, &[b"v".to_vec()]));
    snap.extend_from_slice(&1u64.to_le_bytes());
    snap.extend_from_slice(&tomb_entry(b"gone", 4));
    let responses: Vec<(u64, Vec<u8>)> = vec![
        (1, blob_ok()),
        (2, blob_registered(9)),
        (3, blob_value_some(b"sv", 7)),
        (4, blob_value_none(8)),
        (5, blob_list(&[b"a".to_vec(), b"b".to_vec()], 9)),
        (6, blob_range(&[(b"rk".to_vec(), b"rv".to_vec())])),
        (7, blob_conflict(10)),
        (8, blob_tag(6)),
        (9, blob_tag(7)),
        (10, blob_tag(8)),
    ];
    snap.extend_from_slice(&1u64.to_le_bytes());
    snap.extend_from_slice(&session_entry(1, 5, 10, &responses, 5));
    snap
}

#[test]
fn test_v03_rejects_map_key_len_over_limit() {
    // key_len at 104..112; u64::MAX exceeds max_key_bytes.
    let bad = patch_u64(&base_v03_single(), 104, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_entry_missing_is_list_byte() {
    // Truncate right after the key byte: no is_list flag remains.
    let snap = base_v03_single();
    assert!(KvStore::decode_snapshot(KvConfig::default(), &snap[..113]).is_err());
}

#[test]
fn test_v03_rejects_elements_len_over_limit() {
    // elements_len at 122..130; above the 1M entry bound.
    let bad = patch_u64(&base_v03_single(), 122, 2_000_000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_element_len_over_limit() {
    // First element length at 130..138; above max_value_bytes.
    let bad = patch_u64(&base_v03_single(), 130, 2_000_000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_snapshot_missing_tomb_count() {
    // Map entries end exactly at 139; no tomb count remains.
    let snap = base_v03_single();
    assert!(KvStore::decode_snapshot(KvConfig::default(), &snap[..139]).is_err());
}

#[test]
fn test_v03_rejects_tomb_count_over_limit() {
    // tomb_count at 139..147; above the 1M bound.
    let bad = patch_u64(&base_v03_single(), 139, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_truncated_session_table() {
    // Session order tick at 222..230 cut to 7 bytes: sessions fail to decode.
    let snap = base_v03_single();
    assert!(KvStore::decode_snapshot(KvConfig::default(), &snap[..229]).is_err());
}

#[test]
fn test_v03_rejects_zero_max_key_bytes() {
    // max_key_bytes at 56..64.
    let bad = patch_u64(&base_v03_single(), 56, 0);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_zero_max_value_bytes() {
    // max_value_bytes at 64..72.
    let bad = patch_u64(&base_v03_single(), 64, 0);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_zero_max_sessions() {
    // max_sessions at 80..88.
    let bad = patch_u64(&base_v03_single(), 80, 0);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_zero_max_responses() {
    // max_responses_per_session at 88..96.
    let bad = patch_u64(&base_v03_single(), 88, 0);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_session_count_over_limit() {
    // session count at 147..155; default max_sessions is 4096.
    let bad = patch_u64(&base_v03_single(), 147, 5000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_responses_len_over_limit() {
    // responses_len at 179..187; default per-session cap is 512.
    let bad = patch_u64(&base_v03_single(), 179, 600);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_response_blob_len_over_limit() {
    // response blob length at 195..203; hard cap is 8MiB.
    let bad = patch_u64(&base_v03_single(), 195, 9_000_000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v03_rejects_duplicate_session_id() {
    let config = KvConfig::default();
    let base = base_v03_single();
    // Session entry spans 155..230 with its id at 155..163. Two entries
    // with distinct ids decode fine.
    let mut two = base[..155].to_vec();
    two.extend_from_slice(&base[155..230]);
    two.extend_from_slice(&base[155..230]);
    // v04 membership section: empty voter, learner, and joint counts.
    two.extend_from_slice(&[0, 0, 0]);
    two[147..155].copy_from_slice(&2u64.to_le_bytes());
    two[230..238].copy_from_slice(&2u64.to_le_bytes());
    let decoded = KvStore::decode_snapshot(config, &two).expect("two sessions decode");
    assert_eq!(decoded.session_count(), 2);
    // Reusing id 1 in the second entry must be rejected.
    two[230..238].copy_from_slice(&1u64.to_le_bytes());
    assert!(KvStore::decode_snapshot(config, &two).is_err());
}

#[test]
fn test_v03_rejects_tomb_key_len_over_limit() {
    // tomb key_len at 112..120 in the tombstone snapshot.
    let bad = patch_u64(&base_v03_tomb(), 112, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_decodes_entry_with_tombstone() {
    let config = KvConfig::default();
    let store = KvStore::decode_snapshot(config, &base_v02_entry_tomb()).expect("v02 with entries");
    assert_eq!(store.applied_index(), idx(3));
    assert_eq!(store.virtual_tick(), 7);
    assert_eq!(store.session_count(), 0);
    assert_eq!(store.len(), 1);
    match store.read_register(b"k") {
        KvResponse::Value {
            value: Some(value),
            version,
        } => {
            assert_eq!(value, Bytes::from_static(b"v"));
            assert_eq!(version, idx(2));
        }
        other => panic!("unexpected read: {other:?}"),
    }
    match store.read_register(b"gone") {
        KvResponse::Value {
            value: None,
            version,
        } => assert_eq!(version, idx(3)),
        other => panic!("unexpected tombstone read: {other:?}"),
    }
    match store.read_list(b"gone") {
        KvResponse::List { values, version } => {
            assert!(values.is_empty());
            assert_eq!(version, idx(3));
        }
        other => panic!("unexpected tombstone list read: {other:?}"),
    }
}

#[test]
fn test_v02_rejects_key_len_over_limit() {
    // key_len at 64..72.
    let bad = patch_u64(&base_v02_entry_tomb(), 64, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_rejects_entry_missing_is_list_byte() {
    // Truncate right after the key byte.
    let raw = base_v02_entry_tomb();
    assert!(KvStore::decode_snapshot(KvConfig::default(), &raw[..73]).is_err());
}

#[test]
fn test_v02_rejects_elements_len_over_limit() {
    // elements_len at 82..90; above the 1M entry bound.
    let bad = patch_u64(&base_v02_entry_tomb(), 82, 2_000_000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_rejects_element_len_over_limit() {
    // Element length at 90..98; above max_value_bytes.
    let bad = patch_u64(&base_v02_entry_tomb(), 90, 2_000_000);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_rejects_tomb_count_over_limit() {
    // tomb_count at 99..107; above the 1M bound.
    let bad = patch_u64(&base_v02_entry_tomb(), 99, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_rejects_tomb_key_len_over_limit() {
    // tomb key_len at 107..115.
    let bad = patch_u64(&base_v02_entry_tomb(), 107, u64::MAX);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &bad).is_err());
}

#[test]
fn test_v02_rejects_trailing_bytes() {
    let mut raw = base_v02_entry_tomb();
    raw.push(0xFF);
    assert!(KvStore::decode_snapshot(KvConfig::default(), &raw).is_err());
}

#[test]
fn test_v03_roundtrips_every_response_variant() {
    let config = KvConfig::default();
    let mut store = KvStore::decode_snapshot(config, &v03_full()).expect("decode full snapshot");
    assert_eq!(store.len(), 2);
    assert_eq!(store.session_count(), 1);
    assert_eq!(store.applied_index(), idx(11));
    let session = sess(1);
    assert_eq!(replay(&mut store, 12, session, 1), KvResponse::Ok);
    assert_eq!(
        replay(&mut store, 13, session, 2),
        KvResponse::SessionRegistered { session: sess(9) }
    );
    assert_eq!(
        replay(&mut store, 14, session, 3),
        KvResponse::Value {
            value: Some(Bytes::from_static(b"sv")),
            version: idx(7)
        }
    );
    assert_eq!(
        replay(&mut store, 15, session, 4),
        KvResponse::Value {
            value: None,
            version: idx(8)
        }
    );
    assert_eq!(
        replay(&mut store, 16, session, 5),
        KvResponse::List {
            values: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
            version: idx(9),
        }
    );
    assert_eq!(
        replay(&mut store, 17, session, 6),
        KvResponse::Range {
            pairs: vec![(Bytes::from_static(b"rk"), Bytes::from_static(b"rv"))]
        }
    );
    assert_eq!(
        replay(&mut store, 18, session, 7),
        KvResponse::CasConflict { version: idx(10) }
    );
    assert_eq!(
        replay(&mut store, 19, session, 8),
        KvResponse::SessionExpired
    );
    assert_eq!(
        replay(&mut store, 20, session, 9),
        KvResponse::UnknownSession
    );
    assert_eq!(replay(&mut store, 21, session, 10), KvResponse::Duplicate);
    // The empty-elements record surfaces as an empty value in range scans.
    match store.read_range(b"", b"", 10) {
        KvResponse::Range { pairs } => {
            assert_eq!(pairs.len(), 2);
            assert_eq!(pairs[0].0, Bytes::from_static(b"empty"));
            assert!(pairs[0].1.is_empty());
            assert_eq!(
                pairs[1],
                (Bytes::from_static(b"k"), Bytes::from_static(b"v"))
            );
        }
        other => panic!("range failed: {other:?}"),
    }
    // Re-encoding serializes every response variant; decoding proves it valid.
    let reencoded = store.encode_snapshot(&MembershipSnapshot::empty());
    let again = KvStore::decode_snapshot(config, &reencoded).expect("re-encode roundtrip");
    assert_eq!(again.session_count(), 1);
    assert_eq!(again.len(), 2);
    assert_eq!(again.applied_index(), idx(21));
}

#[test]
fn test_decode_scalar_responses_rejects_malformed() {
    let config = KvConfig::default();
    let missing = v03_single_session(&[(1u64, vec![])]);
    assert!(KvStore::decode_snapshot(config, &missing).is_err());
    let trailing_ok = v03_single_session(&[(1u64, vec![0u8, 0xFF])]);
    assert!(KvStore::decode_snapshot(config, &trailing_ok).is_err());
    let short_reg = v03_single_session(&[(1u64, vec![1u8, 2u8, 3u8])]);
    assert!(KvStore::decode_snapshot(config, &short_reg).is_err());
    let short_conflict = v03_single_session(&[(1u64, vec![5u8])]);
    assert!(KvStore::decode_snapshot(config, &short_conflict).is_err());
    for tag in [6u8, 7u8, 8u8] {
        let trailing = v03_single_session(&[(1u64, vec![tag, 0u8])]);
        assert!(
            KvStore::decode_snapshot(config, &trailing).is_err(),
            "tag {tag} with trailing bytes must fail"
        );
    }
    let unknown = v03_single_session(&[(1u64, vec![9u8])]);
    assert!(KvStore::decode_snapshot(config, &unknown).is_err());
    let good = v03_single_session(&[(1u64, blob_ok())]);
    let store = KvStore::decode_snapshot(config, &good).expect("ok decodes");
    assert_eq!(store.session_count(), 1);
}

#[test]
fn test_decode_value_response_rejects_malformed() {
    let config = KvConfig::default();
    let missing_flag = v03_single_session(&[(1u64, vec![2u8])]);
    assert!(KvStore::decode_snapshot(config, &missing_flag).is_err());
    let mut huge = vec![2u8, 1u8];
    huge.extend_from_slice(&2_000_000u64.to_le_bytes());
    let huge_value = v03_single_session(&[(1u64, huge)]);
    assert!(KvStore::decode_snapshot(config, &huge_value).is_err());
    let short_version = v03_single_session(&[(1u64, vec![2u8, 0u8, 1u8, 2u8, 3u8, 4u8])]);
    assert!(KvStore::decode_snapshot(config, &short_version).is_err());
    let good = v03_single_session(&[(1u64, blob_value_none(8))]);
    let mut store = KvStore::decode_snapshot(config, &good).expect("none value decodes");
    assert_eq!(
        replay(&mut store, 12, sess(1), 1),
        KvResponse::Value {
            value: None,
            version: idx(8)
        }
    );
}

#[test]
fn test_decode_list_response_rejects_malformed() {
    let config = KvConfig::default();
    let mut huge = vec![3u8];
    huge.extend_from_slice(&u64::MAX.to_le_bytes());
    let huge_count = v03_single_session(&[(1u64, huge)]);
    assert!(KvStore::decode_snapshot(config, &huge_count).is_err());
    let mut big = vec![3u8];
    big.extend_from_slice(&1u64.to_le_bytes());
    big.extend_from_slice(&2_000_000u64.to_le_bytes());
    let big_element = v03_single_session(&[(1u64, big)]);
    assert!(KvStore::decode_snapshot(config, &big_element).is_err());
    let mut short = vec![3u8];
    short.extend_from_slice(&0u64.to_le_bytes());
    let missing_version = v03_single_session(&[(1u64, short)]);
    assert!(KvStore::decode_snapshot(config, &missing_version).is_err());
    let good = v03_single_session(&[(1u64, blob_list(&[b"a".to_vec()], 9))]);
    let mut store = KvStore::decode_snapshot(config, &good).expect("list decodes");
    assert_eq!(
        replay(&mut store, 12, sess(1), 1),
        KvResponse::List {
            values: vec![Bytes::from_static(b"a")],
            version: idx(9)
        }
    );
}

#[test]
fn test_decode_range_response_rejects_malformed() {
    let config = KvConfig::default();
    let mut huge = vec![4u8];
    huge.extend_from_slice(&200_000u64.to_le_bytes());
    let huge_count = v03_single_session(&[(1u64, huge)]);
    assert!(KvStore::decode_snapshot(config, &huge_count).is_err());
    let mut big_key = vec![4u8];
    big_key.extend_from_slice(&1u64.to_le_bytes());
    big_key.extend_from_slice(&2048u64.to_le_bytes());
    let oversized_key = v03_single_session(&[(1u64, big_key)]);
    assert!(KvStore::decode_snapshot(config, &oversized_key).is_err());
    let mut big_value = vec![4u8];
    big_value.extend_from_slice(&1u64.to_le_bytes());
    big_value.extend_from_slice(&1u64.to_le_bytes());
    big_value.extend_from_slice(b"k");
    big_value.extend_from_slice(&2_000_000u64.to_le_bytes());
    let oversized_value = v03_single_session(&[(1u64, big_value)]);
    assert!(KvStore::decode_snapshot(config, &oversized_value).is_err());
    let mut trailing = vec![4u8];
    trailing.extend_from_slice(&0u64.to_le_bytes());
    trailing.push(0xAA);
    let trailing_bytes = v03_single_session(&[(1u64, trailing)]);
    assert!(KvStore::decode_snapshot(config, &trailing_bytes).is_err());
    let good = v03_single_session(&[(1u64, blob_range(&[(b"x".to_vec(), b"y".to_vec())]))]);
    let mut store = KvStore::decode_snapshot(config, &good).expect("range decodes");
    assert_eq!(
        replay(&mut store, 12, sess(1), 1),
        KvResponse::Range {
            pairs: vec![(Bytes::from_static(b"x"), Bytes::from_static(b"y"))]
        }
    );
}

#[test]
fn test_reassemble_rejects_empty_and_gapped() {
    assert!(matches!(reassemble_chunks(&[]), Err(ChunkError::Empty)));
    let full = Bytes::from_static(b"hello snapshot");
    let mut chunks = split_into_chunks(&full, 2);
    assert!(chunks.len() > 2);
    let _ = chunks.remove(1);
    assert!(matches!(reassemble_chunks(&chunks), Err(ChunkError::Gap)));
}

#[test]
fn test_decode_snapshot_chunked_rejects_bad_chunks() {
    let config = KvConfig::default();
    let digest = [0u8; 32];
    assert!(KvStore::decode_snapshot_chunked(config, &[], &digest).is_err());
    let full = Bytes::from_static(b"hello snapshot");
    let mut chunks = split_into_chunks(&full, 2);
    let _ = chunks.remove(1);
    assert!(KvStore::decode_snapshot_chunked(config, &chunks, &digest).is_err());
}

#[test]
fn test_snapshot_roundtrips_list_and_cas_conflict() {
    let mut store = KvStore::new(KvConfig::default());
    let session = reg(&mut store, 1);
    store
        .apply(idx(2), &put_cmd(session, 1, 0, b"k", b"v"))
        .expect("put");
    store
        .apply(idx(3), &append_cmd(session, 2, 0, b"l", b"a"))
        .expect("append");
    store
        .apply(idx(4), &append_cmd(session, 3, 0, b"l", b"b"))
        .expect("append");
    store
        .apply(idx(5), &cas_cmd(session, 4, 0, b"k", 99, b"nope"))
        .expect("cas");
    let snap = store.encode_snapshot(&MembershipSnapshot::empty());
    let mut restored =
        KvStore::decode_snapshot(KvConfig::default(), &snap).expect("snapshot decodes");
    assert_eq!(restored.session_count(), 1);
    match restored
        .apply(idx(6), &append_cmd(session, 3, 0, b"l", b"b"))
        .expect("replay append")
    {
        KvResponse::List { values, version } => {
            assert_eq!(
                values,
                vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
            );
            assert_eq!(version, idx(4));
        }
        other => panic!("cached list lost: {other:?}"),
    }
    match restored
        .apply(idx(7), &cas_cmd(session, 4, 0, b"k", 99, b"nope"))
        .expect("replay cas")
    {
        KvResponse::CasConflict { version } => assert_eq!(version, idx(2)),
        other => panic!("cached conflict lost: {other:?}"),
    }
    let again = restored.encode_snapshot(&MembershipSnapshot::empty());
    KvStore::decode_snapshot(KvConfig::default(), &again).expect("re-encode decodes");
}
