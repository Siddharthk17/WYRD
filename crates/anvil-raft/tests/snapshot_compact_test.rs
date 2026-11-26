#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_raft::RaftLog;
use anvil_types::{Entry, EntryKind, LogIndex, Term};
use bytes::Bytes;

fn entry(index: u64, term: u64) -> Entry {
    Entry {
        index: LogIndex::from_raw(index),
        term: Term::from_raw(term),
        kind: EntryKind::Normal,
        payload: Bytes::from(format!("payload-{index}")),
    }
}

fn log_with(n: u64) -> RaftLog {
    RaftLog::new((1..=n).map(|i| entry(i, 1)).collect())
}

#[test]
fn test_log_compact_drops_prefix_and_keeps_suffix() {
    let mut log = log_with(10);
    assert_eq!(log.base_index(), LogIndex::from_raw(0));
    log.compact(LogIndex::from_raw(6));
    assert_eq!(log.base_index(), LogIndex::from_raw(6));
    assert_eq!(log.last_index(), LogIndex::from_raw(10));
    assert_eq!(log.len(), 4);
    assert!(log.term_at(LogIndex::from_raw(6)).is_none());
    assert!(log.entry_at(LogIndex::from_raw(6)).is_none());
    assert_eq!(log.term_at(LogIndex::from_raw(7)), Some(Term::from_raw(1)));
    let sliced = log.slice_from(LogIndex::from_raw(1), usize::MAX);
    assert_eq!(sliced.len(), 4);
    assert_eq!(sliced[0].index, LogIndex::from_raw(7));
}

#[test]
fn test_log_compact_never_passes_last_index() {
    let mut log = log_with(5);
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        log.compact(LogIndex::from_raw(6));
    }));
    assert!(result.is_err());
}

#[test]
fn test_log_truncate_handles_compacted_prefix() {
    let mut log = log_with(10);
    log.compact(LogIndex::from_raw(6));
    log.truncate_and_append(LogIndex::from_raw(7), vec![entry(7, 2), entry(8, 2)]);
    assert_eq!(log.last_index(), LogIndex::from_raw(8));
    assert_eq!(log.term_at(LogIndex::from_raw(7)), Some(Term::from_raw(2)));
}
