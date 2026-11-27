#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty
)]

use anvil_raft::{
    Input, Message, Output, PersistOp, ProgressState, RaftConfig, RaftCore, RecoveredState, Role,
    SnapshotChunkData,
};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::{Rng, SeedableRng};
use std::collections::BTreeSet;

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("node")
}

fn make_core(n: u64, peers: &[u64]) -> RaftCore {
    let mut set = BTreeSet::new();
    for &p in peers {
        set.insert(node_id(p));
    }
    RaftCore::new(
        RaftConfig {
            node_id: node_id(n),
            peers: set,
            election_timeout_ticks: 5,
            heartbeat_interval_ticks: 2,
            max_inflight_messages: 256,
            max_bytes_per_message: 64 * 1024,
            pre_vote_enabled: true,
            check_quorum: false,
        },
        RecoveredState::default(),
        ChaCha8Rng::seed_from_u64(n),
    )
}

fn make_core_with_log(node_id_raw: u64, peers: &[u64], term: Term, entries: Vec<Entry>) -> RaftCore {
    let mut set = BTreeSet::new();
    for &p in peers {
        set.insert(node_id(p));
    }
    RaftCore::new(
        RaftConfig {
            node_id: node_id(node_id_raw),
            peers: set,
            election_timeout_ticks: 5,
            heartbeat_interval_ticks: 2,
            max_inflight_messages: 256,
            max_bytes_per_message: 64 * 1024,
            pre_vote_enabled: true,
            check_quorum: false,
        },
        RecoveredState {
            current_term: term,
            voted_for: None,
            commit_index: LogIndex::from_raw(0),
            entries,
        },
        ChaCha8Rng::seed_from_u64(node_id_raw),
    )
}

fn persisted_tokens(output: &Output) -> Vec<anvil_raft::PersistToken> {
    output.persist_ops.iter().map(PersistOp::token).collect()
}

fn ack_all(core: &mut RaftCore, output: &mut Output) {
    for _ in 0..16 {
        let tokens = persisted_tokens(output);
        if tokens.is_empty() {
            break;
        }
        output.persist_ops.clear();
        for token in tokens {
            core.step(Input::Persisted { token }, output);
        }
    }
}

fn elect_three_seed(seed: u64) -> (RaftCore, NodeId, Output) {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    let _ = seed;
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer = node_id(2);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: peer,
            },
        },
        &mut output,
    );
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::RequestVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Leader);
    (leader, peer, output)
}

fn fake_snapshot(total_bytes: usize, chunks: usize) -> (Bytes, Vec<SnapshotChunkData>, [u8; 32]) {
    let mut full = Vec::with_capacity(total_bytes);
    for i in 0..total_bytes {
        full.push(u8::try_from(i % 251).unwrap_or(0));
    }
    let full = Bytes::from(full);
    let digest = *blake3::hash(&full).as_bytes();
    let mut out = Vec::new();
    let size = total_bytes.div_ceil(chunks);
    let mut offset = 0;
    while offset < full.len() {
        let end = (offset + size).min(full.len());
        let payload = full.slice(offset..end);
        let crc = crc32c::crc32c(&payload);
        out.push(SnapshotChunkData {
            chunk: payload,
            chunk_crc: crc,
        });
        offset = end;
    }
    (full, out, digest)
}

#[allow(clippy::too_many_arguments)]
fn install_chunk(
    term: Term,
    leader: NodeId,
    last_index: u64,
    last_term: u64,
    chunk_index: u32,
    total_chunks: u32,
    chunk: &SnapshotChunkData,
    digest: [u8; 32],
    snapshot_bytes: u64,
) -> Message {
    Message::InstallSnapshotRequest {
        term,
        leader_id: leader,
        last_included_index: LogIndex::from_raw(last_index),
        last_included_term: Term::from_raw(last_term),
        chunk_index,
        total_chunks,
        chunk: chunk.chunk.clone(),
        chunk_crc: chunk.chunk_crc,
        snapshot_digest: digest,
        snapshot_bytes,
    }
}

#[test]
fn test_testing_helpers_set_role_and_term() {
    let mut core = make_core(1, &[2, 3]);
    core.testing_set_role(Role::Candidate);
    assert_eq!(core.role(), Role::Candidate);
    core.testing_set_term(Term::from_raw(7));
    assert_eq!(core.term(), Term::from_raw(7));
    core.testing_set_role(Role::Follower);
    assert_eq!(core.role(), Role::Follower);
}

#[test]
fn test_leader_tick_with_sixteen_peers_uses_heap_path() {
    let peers: Vec<u64> = (2..=17).collect();
    assert_eq!(peers.len(), 16);
    let mut leader = make_core(1, &peers);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    output.clear();
    for _ in 0..3 {
        leader.step(Input::Tick, &mut output);
        ack_all(&mut leader, &mut output);
    }
    assert_eq!(leader.role(), Role::Leader);
    assert_eq!(leader.progress().len(), 16);
}

#[test]
fn test_request_vote_denied_with_higher_term_queues_rejection() {
    let entries = vec![Entry::normal(
        LogIndex::from_raw(1),
        Term::from_raw(3),
        Bytes::from_static(b"newer"),
    )];
    let mut core = make_core_with_log(2, &[1], Term::from_raw(3), entries);
    let mut output = Output::new();
    let challenger = node_id(1);
    core.step(
        Input::Recv {
            from: challenger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(4),
                candidate_id: challenger,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.term(), Term::from_raw(4));
    assert!(
        output.messages.is_empty(),
        "denial must wait for the term bump to reach disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    core.step(Input::Persisted { token: tokens[0] }, &mut output);
    let denied = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::RequestVoteResponse {
                vote_granted: false,
                term,
                ..
            } if *term == Term::from_raw(4)
        )
    });
    assert!(denied, "stale candidate must be denied after persist");
    assert_eq!(core.voted_for(), None);
}

#[test]
fn test_single_vote_grant_without_quorum_stays_candidate() {
    let mut core = make_core(1, &[2, 3, 4, 5]);
    let mut output = Output::new();
    for _ in 0..20 {
        core.step(Input::Tick, &mut output);
        if core.role() != Role::Follower {
            break;
        }
    }
    assert_eq!(core.role(), Role::PreCandidate);
    let peer2 = node_id(2);
    let peer3 = node_id(3);
    output.clear();
    core.step(
        Input::Recv {
            from: peer2,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: peer2,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::PreCandidate);
    output.clear();
    core.step(
        Input::Recv {
            from: peer3,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: peer3,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(core.term(), Term::from_raw(1));
    output.clear();
    core.step(
        Input::Recv {
            from: peer2,
            message: Message::RequestVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: peer2,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Candidate);
    output.clear();
    core.step(
        Input::Recv {
            from: peer3,
            message: Message::RequestVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: peer3,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Leader);
}

#[test]
fn test_become_leader_aborts_pending_transfer() {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    let peer = node_id(2);
    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    output.clear();
    leader.testing_become_leader(&mut output);
    assert!(
        output
            .sometimes_hits
            .contains(&"raft.transfer.aborted"),
        "re-election with a pending transfer must mark the abort"
    );
    ack_all(&mut leader, &mut output);
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"after-abort"),
        },
        &mut output,
    );
    assert_eq!(leader.last_log_index(), LogIndex::from_raw(3));
}

fn append_single_entry(core: &mut RaftCore, output: &mut Output, leader: NodeId, index: u64) {
    core.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(index - 1),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(index),
                    Term::from_raw(0),
                    Bytes::from_static(b"d"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        output,
    );
    ack_all(core, output);
    output.clear();
}

#[test]
fn test_append_entries_gap_rejected_same_term() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    append_single_entry(&mut follower, &mut output, leader, 1);
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(1),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(3),
                    Term::from_raw(0),
                    Bytes::from_static(b"gap"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    let hinted = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::AppendEntriesResponse {
                success: false,
                conflict_index: Some(idx),
                ..
            } if *idx == LogIndex::from_raw(2)
        )
    });
    assert!(hinted, "gapped append must hint the next expected index");
    assert!(
        output
            .persist_ops
            .iter()
            .all(|op| !matches!(op, PersistOp::TruncateAndAppend { .. }))
    );
}

#[test]
fn test_append_entries_gap_rejected_higher_term_queued() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(2),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(2),
                    Term::from_raw(2),
                    Bytes::from_static(b"gap"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(2));
    assert!(
        output.messages.is_empty(),
        "gapped denial must wait for the term bump to reach disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    follower.step(Input::Persisted { token: tokens[0] }, &mut output);
    let hinted = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::AppendEntriesResponse {
                success: false,
                conflict_index: Some(idx),
                term,
                ..
            } if *idx == LogIndex::from_raw(1) && *term == Term::from_raw(2)
        )
    });
    assert!(hinted, "gapped append must hint index 1 after persist");
}

#[test]
fn test_append_overwrites_conflicting_suffix_and_skips_snapshotted() {
    let entries = vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(1),
            Bytes::from_static(b"a"),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(1),
            Bytes::from_static(b"b"),
        ),
        Entry::normal(
            LogIndex::from_raw(3),
            Term::from_raw(1),
            Bytes::from_static(b"c"),
        ),
    ];
    let mut follower = make_core_with_log(2, &[1], Term::from_raw(1), entries);
    let mut output = Output::new();
    follower.step(
        Input::Applied {
            index: LogIndex::from_raw(2),
        },
        &mut output,
    );
    follower.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(2),
        },
        &mut output,
    );
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(2));
    let leader = node_id(1);
    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(1),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(2),
                prev_log_term: Term::from_raw(1),
                entries: vec![
                    Entry::normal(
                        LogIndex::from_raw(3),
                        Term::from_raw(2),
                        Bytes::from_static(b"c2"),
                    ),
                    Entry::normal(
                        LogIndex::from_raw(4),
                        Term::from_raw(2),
                        Bytes::from_static(b"d2"),
                    ),
                    Entry::normal(
                        LogIndex::from_raw(2),
                        Term::from_raw(1),
                        Bytes::from_static(b"stale"),
                    ),
                ],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    let truncates: Vec<_> = output
        .persist_ops
        .iter()
        .filter_map(|op| match op {
            PersistOp::TruncateAndAppend { from_index, entries, .. } => {
                Some((*from_index, entries.len()))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        truncates,
        vec![(LogIndex::from_raw(3), 2)],
        "stale snapshotted entry must be skipped, conflicting suffix truncated"
    );
    assert!(
        output.messages.is_empty(),
        "overwrite ack must wait for disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    follower.step(Input::Persisted { token: tokens[0] }, &mut output);
    let acked = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::AppendEntriesResponse {
                success: true,
                match_index,
                ..
            } if *match_index == LogIndex::from_raw(4)
        )
    });
    assert!(acked);
    assert_eq!(
        follower.log().term_at(LogIndex::from_raw(3)),
        Some(Term::from_raw(2))
    );
    assert_eq!(follower.last_log_index(), LogIndex::from_raw(4));
}

#[test]
fn test_lagging_ack_below_commit_advances_nothing() {
    let (mut leader, peer, mut output) = elect_three_seed(701);
    ack_all(&mut leader, &mut output);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(leader.commit_index(), LogIndex::from_raw(1));
    let lagger = node_id(3);
    output.clear();
    leader.step(
        Input::Recv {
            from: lagger,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(0),
                conflict_term: None,
                conflict_index: None,
                from: lagger,
            },
        },
        &mut output,
    );
    assert_eq!(leader.commit_index(), LogIndex::from_raw(1));
}

#[test]
fn test_heartbeat_commit_clamped_to_local_log() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                commit_index: LogIndex::from_raw(5),
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(follower.commit_index(), LogIndex::from_raw(0));
    assert!(
        output
            .messages
            .iter()
            .any(|m| matches!(&m.message, Message::HeartbeatResponse { .. }))
    );
}

#[test]
fn test_overlapping_reads_complete_and_retain_independently() {
    let (mut leader, peer, mut output) = elect_three_seed(702);
    ack_all(&mut leader, &mut output);
    let first = Bytes::from_static(b"first");
    let second = Bytes::from_static(b"second");
    output.clear();
    leader.step(Input::ReadIndex { context: first.clone() }, &mut output);
    leader.step(
        Input::ReadIndex {
            context: second.clone(),
        },
        &mut output,
    );
    assert!(output.completed_reads.is_empty());
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(1),
                from: peer,
                read_context: Some(first.clone()),
            },
        },
        &mut output,
    );
    assert_eq!(output.completed_reads.len(), 1);
    assert_eq!(output.completed_reads[0].context, first);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(1),
                from: peer,
                read_context: Some(second.clone()),
            },
        },
        &mut output,
    );
    assert_eq!(output.completed_reads.len(), 1);
    assert_eq!(output.completed_reads[0].context, second);
}

#[test]
fn test_compact_log_rejects_invalid_targets() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    append_single_entry(&mut follower, &mut output, leader, 1);
    follower.step(
        Input::Applied {
            index: LogIndex::from_raw(1),
        },
        &mut output,
    );
    follower.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(5),
        },
        &mut output,
    );
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(0));
    follower.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(0),
        },
        &mut output,
    );
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(0));
    follower.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(1),
        },
        &mut output,
    );
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(1));
}

#[test]
fn test_snapshot_ready_rejected_when_not_leader() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let (_, chunks, digest) = fake_snapshot(64, 2);
    follower.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(2),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: 64,
            snapshot_digest: digest,
            chunks,
        },
        &mut output,
    );
    assert!(output.messages.is_empty());
    assert!(output.persist_ops.is_empty());
    assert!(output.snapshot_requests.is_empty());
}

#[test]
fn test_snapshot_ready_rejects_malformed_inputs() {
    let (_, chunks, digest) = fake_snapshot(64, 2);
    let bad_cases: Vec<(&str, u64, u64, Vec<SnapshotChunkData>)> = vec![
        ("zero index", 0, 64, chunks.clone()),
        (
            "empty chunks",
            2,
            64,
            Vec::new(),
        ),
        ("zero bytes", 2, 0, chunks.clone()),
        (
            "empty chunk",
            2,
            64,
            vec![SnapshotChunkData {
                chunk: Bytes::new(),
                chunk_crc: 0,
            }],
        ),
        ("length mismatch", 2, 63, chunks.clone()),
    ];
    for (name, last_index, snapshot_bytes, case_chunks) in bad_cases {
        let mut leader = make_core(1, &[2, 3]);
        let mut output = Output::new();
        leader.testing_become_leader(&mut output);
        output.clear();
        leader.step(
            Input::SnapshotReady {
                last_included_index: LogIndex::from_raw(last_index),
                last_included_term: Term::from_raw(1),
                snapshot_bytes,
                snapshot_digest: digest,
                chunks: case_chunks,
            },
            &mut output,
        );
        assert!(
            output.messages.is_empty(),
            "{name}: malformed snapshot must not trigger replication"
        );
        assert!(
            output.snapshot_requests.is_empty(),
            "{name}: malformed snapshot must not request bytes"
        );
    }
}

#[test]
fn test_install_snapshot_same_term_candidate_steps_down() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..20 {
        core.step(Input::Tick, &mut output);
        if core.role() != Role::Follower {
            break;
        }
    }
    let peer = node_id(2);
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Candidate);
    let chunk = Bytes::from(vec![7u8; 10]);
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotRequest {
                term: Term::from_raw(1),
                leader_id: peer,
                last_included_index: LogIndex::from_raw(5),
                last_included_term: Term::from_raw(1),
                chunk_index: 0,
                total_chunks: 1,
                chunk: chunk.clone(),
                chunk_crc: crc32c::crc32c(&chunk),
                snapshot_digest: [99u8; 32],
                snapshot_bytes: 10,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Follower);
    let rejected = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(rejected, "digest mismatch must be rejected");
    assert!(output.ready_snapshots.is_empty());
}

#[test]
fn test_install_snapshot_invalid_chunk_index_rejected() {
    let (_, chunks, digest) = fake_snapshot(64, 2);
    assert_eq!(chunks.len(), 2);
    let leader = node_id(1);
    let bad = install_chunk(
        Term::from_raw(0),
        leader,
        4,
        1,
        7,
        2,
        &chunks[0],
        digest,
        64,
    );
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    follower.step(Input::Recv { from: leader, message: bad }, &mut output);
    let rejected = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(rejected, "out-of-range chunk must be rejected immediately");

    let bad_higher = install_chunk(
        Term::from_raw(3),
        leader,
        4,
        1,
        7,
        2,
        &chunks[0],
        digest,
        64,
    );
    let mut fresh = make_core(2, &[1]);
    let mut fresh_out = Output::new();
    fresh.step(
        Input::Recv {
            from: leader,
            message: bad_higher,
        },
        &mut fresh_out,
    );
    assert_eq!(fresh.term(), Term::from_raw(3));
    assert!(
        fresh_out.messages.is_empty(),
        "higher-term denial must wait for disk"
    );
    let tokens = persisted_tokens(&fresh_out);
    assert_eq!(tokens.len(), 1);
    fresh_out.clear();
    fresh.step(Input::Persisted { token: tokens[0] }, &mut fresh_out);
    let queued = fresh_out.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(queued, "higher-term denial must release after persist");
}

#[test]
fn test_install_snapshot_below_commit_acked_after_persist() {
    let entries = vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(1),
            Bytes::from_static(b"a"),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(1),
            Bytes::from_static(b"b"),
        ),
    ];
    let mut follower = make_core_with_log(2, &[1], Term::from_raw(1), entries);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(1),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(2),
                prev_log_term: Term::from_raw(1),
                entries: Vec::new(),
                leader_commit: LogIndex::from_raw(2),
            },
        },
        &mut output,
    );
    assert_eq!(follower.commit_index(), LogIndex::from_raw(2));
    let (_, chunks, digest) = fake_snapshot(64, 2);
    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(2),
                leader,
                1,
                1,
                0,
                2,
                &chunks[0],
                digest,
                64,
            ),
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(2));
    assert!(
        output.messages.is_empty(),
        "already-committed snapshot ack must wait for disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    follower.step(Input::Persisted { token: tokens[0] }, &mut output);
    let acked = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse {
                success: true,
                match_index,
                next_chunk,
                ..
            } if *match_index == LogIndex::from_raw(1) && *next_chunk == 2
        )
    });
    assert!(acked, "stale snapshot must ack as complete after persist");
    assert!(output.ready_snapshots.is_empty());
}

#[test]
fn test_install_snapshot_out_of_order_chunks_complete_stream() {
    let (full, chunks, digest) = fake_snapshot(300, 3);
    assert_eq!(chunks.len(), 3);
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(0),
                leader,
                4,
                1,
                2,
                3,
                &chunks[2],
                digest,
                300,
            ),
        },
        &mut output,
    );
    let wants_zero = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse {
                success: true,
                next_chunk: 0,
                ..
            }
        )
    });
    assert!(wants_zero, "first missing chunk must be chunk 0");
    assert!(output.ready_snapshots.is_empty());
    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(0),
                leader,
                4,
                1,
                0,
                3,
                &chunks[0],
                digest,
                300,
            ),
        },
        &mut output,
    );
    let wants_one = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse {
                success: true,
                next_chunk: 1,
                ..
            }
        )
    });
    assert!(wants_one, "next missing chunk must be chunk 1");
    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(0),
                leader,
                4,
                1,
                1,
                3,
                &chunks[1],
                digest,
                300,
            ),
        },
        &mut output,
    );
    assert_eq!(output.ready_snapshots.len(), 1);
    assert_eq!(output.ready_snapshots[0].data, full);
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(4));
    assert_eq!(follower.commit_index(), LogIndex::from_raw(4));
    let done = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse {
                success: true,
                match_index,
                next_chunk: 3,
                ..
            } if *match_index == LogIndex::from_raw(4)
        )
    });
    assert!(done);
}

#[test]
fn test_install_snapshot_digest_mismatch_queued_when_term_bumps() {
    let (full, chunks, digest) = fake_snapshot(64, 1);
    assert_eq!(chunks.len(), 1);
    let _ = full;
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(1),
                leader,
                5,
                1,
                0,
                1,
                &chunks[0],
                [77u8; 32],
                64,
            ),
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(1));
    assert!(
        output.messages.is_empty(),
        "digest rejection must wait for the term bump to reach disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    follower.step(Input::Persisted { token: tokens[0] }, &mut output);
    let rejected = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(rejected);
    assert!(output.ready_snapshots.is_empty());
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(0));
    let _ = digest;
}

#[test]
fn test_install_snapshot_higher_term_completion_queued_ack() {
    let (full, chunks, digest) = fake_snapshot(64, 1);
    assert_eq!(chunks.len(), 1);
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = node_id(1);
    follower.step(
        Input::Recv {
            from: leader,
            message: install_chunk(
                Term::from_raw(1),
                leader,
                5,
                1,
                0,
                1,
                &chunks[0],
                digest,
                64,
            ),
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(1));
    assert_eq!(output.ready_snapshots.len(), 1);
    assert_eq!(output.ready_snapshots[0].data, full);
    assert_eq!(follower.log().base_index(), LogIndex::from_raw(5));
    assert!(
        output.messages.is_empty(),
        "completion ack must wait for the term bump to reach disk"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    follower.step(Input::Persisted { token: tokens[0] }, &mut output);
    let acked = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse {
                success: true,
                match_index,
                next_chunk: 1,
                ..
            } if *match_index == LogIndex::from_raw(5)
        )
    });
    assert!(acked);
}

#[test]
fn test_install_snapshot_responses_drive_leader_state() {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    let peer = node_id(2);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(5),
                success: true,
                match_index: LogIndex::from_raw(0),
                next_chunk: 0,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.term(), Term::from_raw(5));

    let mut follower = make_core(2, &[1]);
    let mut follower_out = Output::new();
    let other = node_id(1);
    follower.step(
        Input::Recv {
            from: other,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(0),
                success: true,
                match_index: LogIndex::from_raw(0),
                next_chunk: 1,
                from: other,
            },
        },
        &mut follower_out,
    );
    assert!(follower_out.messages.is_empty());
    assert_eq!(follower.role(), Role::Follower);
}

fn snapshot_leader_with_behind_peer() -> (RaftCore, NodeId, Output) {
    let entries = vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(1),
            Bytes::from_static(b"a"),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(1),
            Bytes::from_static(b"b"),
        ),
        Entry::normal(
            LogIndex::from_raw(3),
            Term::from_raw(1),
            Bytes::from_static(b"c"),
        ),
    ];
    let mut set = BTreeSet::new();
    set.insert(node_id(2));
    let config = RaftConfig {
        node_id: node_id(1),
        peers: set,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let recovered = RecoveredState {
        current_term: Term::from_raw(1),
        voted_for: None,
        commit_index: LogIndex::from_raw(3),
        entries,
    };
    let mut leader = RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(99));
    let mut output = Output::new();
    leader.step(
        Input::Applied {
            index: LogIndex::from_raw(3),
        },
        &mut output,
    );
    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(2),
        },
        &mut output,
    );
    assert_eq!(leader.log().base_index(), LogIndex::from_raw(2));
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    let peer = node_id(2);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: false,
                match_index: LogIndex::from_raw(0),
                conflict_term: None,
                conflict_index: Some(LogIndex::from_raw(1)),
                from: peer,
            },
        },
        &mut output,
    );
    output.clear();
    let (full, chunks, digest) = fake_snapshot(300, 3);
    assert_eq!(chunks.len(), 3);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(2),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: full.len() as u64,
            snapshot_digest: digest,
            chunks: chunks.clone(),
        },
        &mut output,
    );
    (leader, peer, output)
}

fn install_messages(output: &Output) -> Vec<Message> {
    output
        .messages
        .iter()
        .filter_map(|m| match &m.message {
            Message::InstallSnapshotRequest { .. } => Some(m.message.clone()),
            _ => None,
        })
        .collect()
}

#[test]
fn test_snapshot_leader_nack_resends_and_fills_window() {
    let (mut leader, peer, mut output) = snapshot_leader_with_behind_peer();
    let first = install_messages(&output);
    assert_eq!(first.len(), 1);
    output.clear();

    leader.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(0),
                next_chunk: 1,
                from: peer,
            },
        },
        &mut output,
    );
    let second = install_messages(&output);
    let second_idx: Vec<u32> = second
        .iter()
        .filter_map(|m| match m {
            Message::InstallSnapshotRequest { chunk_index, .. } => Some(*chunk_index),
            _ => None,
        })
        .collect();
    assert_eq!(
        second_idx,
        vec![1u32, 2u32],
        "unacked chunk 1 must be resent while the window fills with chunk 2"
    );
    output.clear();

    leader.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(1),
                success: false,
                match_index: LogIndex::from_raw(0),
                next_chunk: 0,
                from: peer,
            },
        },
        &mut output,
    );
    let resent: Vec<u32> = install_messages(&output)
        .iter()
        .filter_map(|m| match m {
            Message::InstallSnapshotRequest { chunk_index, .. } => Some(*chunk_index),
            _ => None,
        })
        .collect();
    assert_eq!(
        resent,
        vec![0u32],
        "failed chunk 0 must be resent, saw {resent:?}"
    );
    output.clear();

    leader.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(0),
                next_chunk: 3,
                from: peer,
            },
        },
        &mut output,
    );
    let dupes: Vec<u32> = install_messages(&output)
        .iter()
        .filter_map(|m| match m {
            Message::InstallSnapshotRequest { chunk_index, .. } => Some(*chunk_index),
            _ => None,
        })
        .collect();
    assert!(
        dupes.contains(&2),
        "unacked chunk 2 must be resent, saw {dupes:?}"
    );
    output.clear();

    leader.step(Input::Tick, &mut output);
    assert!(
        install_messages(&output).is_empty(),
        "fully acked stream must not resend on tick"
    );
    output.clear();

    leader.step(
        Input::Recv {
            from: peer,
            message: Message::InstallSnapshotResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(2),
                next_chunk: 3,
                from: peer,
            },
        },
        &mut output,
    );
    let progress = leader.progress().get(&peer).expect("peer");
    assert_eq!(progress.match_index, LogIndex::from_raw(2));
    assert!(!matches!(progress.state, ProgressState::Snapshot(_)));
}

#[test]
fn test_snapshot_leader_replaces_stream_and_rebuilds_cursor() {
    let (mut leader, _peer, mut output) = snapshot_leader_with_behind_peer();
    output.clear();
    let (full_b, chunks_b, digest_b) = fake_snapshot(200, 2);
    assert_eq!(chunks_b.len(), 2);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(3),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: full_b.len() as u64,
            snapshot_digest: digest_b,
            chunks: chunks_b,
        },
        &mut output,
    );
    output.clear();
    leader.step(Input::Tick, &mut output);
    let rebuilt: Vec<u64> = install_messages(&output)
        .iter()
        .filter_map(|m| match m {
            Message::InstallSnapshotRequest {
                last_included_index,
                ..
            } => Some(last_included_index.as_u64()),
            _ => None,
        })
        .collect();
    assert!(
        rebuilt.contains(&3),
        "replaced stream must send under the new snapshot index, saw {rebuilt:?}"
    );
}

#[test]
fn test_transfer_immediate_timeout_when_peer_caught_up() {
    let (mut leader, peer, mut output) = elect_three_seed(703);
    ack_all(&mut leader, &mut output);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    let timeout = output.messages.iter().any(|m| {
        matches!(&m.message, Message::TimeoutNow { .. }) && m.to == peer
    });
    assert!(timeout, "caught-up transferee must get TimeoutNow at once");
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"unblocked"),
        },
        &mut output,
    );
    assert_eq!(leader.last_log_index(), LogIndex::from_raw(2));
}

#[test]
fn test_transfer_aborts_after_election_timeout_without_progress() {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    let peer = node_id(2);
    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::TimeoutNow { .. })),
        "lagging transferee must not get TimeoutNow yet"
    );
    for _ in 0..5 {
        output.clear();
        leader.step(Input::Tick, &mut output);
        ack_all(&mut leader, &mut output);
    }
    assert!(
        output.sometimes_hits.contains(&"raft.transfer.aborted"),
        "stalled transfer must abort after one election timeout"
    );
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"resumed"),
        },
        &mut output,
    );
    assert_eq!(leader.last_log_index(), LogIndex::from_raw(2));
}

#[test]
fn test_stepdown_with_pending_transfer_marks_abort() {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    ack_all(&mut leader, &mut output);
    let peer = node_id(2);
    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(4),
                leader_id: peer,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.term(), Term::from_raw(4));
    assert!(
        output.sometimes_hits.contains(&"raft.transfer.aborted"),
        "stepdown must abort the pending transfer"
    );
    let tokens = persisted_tokens(&output);
    assert_eq!(tokens.len(), 1);
    output.clear();
    leader.step(Input::Persisted { token: tokens[0] }, &mut output);
    let echoed = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::HeartbeatResponse { term, .. } if *term == Term::from_raw(4)
        )
    });
    assert!(echoed);
}

#[test]
fn test_timeout_now_stale_ignored_and_higher_starts_election() {
    let mut stale = make_core_with_log(
        2,
        &[1],
        Term::from_raw(3),
        vec![Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(3),
            Bytes::from_static(b"e"),
        )],
    );
    let mut stale_out = Output::new();
    let leader = node_id(1);
    stale.step(
        Input::Recv {
            from: leader,
            message: Message::TimeoutNow {
                term: Term::from_raw(1),
                leader_id: leader,
            },
        },
        &mut stale_out,
    );
    assert_eq!(stale.role(), Role::Follower);
    assert_eq!(stale.term(), Term::from_raw(3));
    assert!(stale_out.messages.is_empty());

    let mut fresh = make_core(2, &[1]);
    let mut fresh_out = Output::new();
    fresh.step(
        Input::Recv {
            from: leader,
            message: Message::TimeoutNow {
                term: Term::from_raw(2),
                leader_id: leader,
            },
        },
        &mut fresh_out,
    );
    assert_eq!(fresh.role(), Role::Candidate);
    assert_eq!(fresh.term(), Term::from_raw(3));
    assert!(
        fresh_out
            .persist_ops
            .iter()
            .any(|op| matches!(op, PersistOp::HardState { .. }))
    );
}

#[test]
fn test_message_term_covers_snapshot_variants() {
    let term = Term::from_raw(4);
    let leader = node_id(1);
    let peer = node_id(2);
    let chunk = Bytes::from_static(b"payload");
    let request = Message::InstallSnapshotRequest {
        term,
        leader_id: leader,
        last_included_index: LogIndex::from_raw(3),
        last_included_term: term,
        chunk_index: 0,
        total_chunks: 1,
        chunk,
        chunk_crc: 0,
        snapshot_digest: [0u8; 32],
        snapshot_bytes: 7,
    };
    let response = Message::InstallSnapshotResponse {
        term,
        success: true,
        match_index: LogIndex::from_raw(3),
        next_chunk: 1,
        from: peer,
    };
    assert_eq!(request.term(), term);
    assert_eq!(response.term(), term);
}

#[test]
fn test_peer_progress_snapshot_window_edges() {
    let mut progress = anvil_raft::PeerProgress::new(LogIndex::from_raw(5), 4);
    progress.enter_snapshot(LogIndex::from_raw(4), 3, 0);
    assert!(!progress.can_send());
    assert!(progress.can_send_snapshot());
    progress.on_snapshot_send_new();
    progress.on_snapshot_ack(1);
    assert!(progress.can_send_snapshot());

    let mut probe = anvil_raft::PeerProgress::new(LogIndex::from_raw(5), 4);
    probe.on_send(1, LogIndex::from_raw(5));
    assert_eq!(probe.inflight_count, 1);
    probe.on_snapshot_ack(0);
    assert_eq!(probe.inflight_count, 0);
    assert_eq!(probe.state, ProgressState::Probe);
}

#[test]
fn test_config_validation_rejects_bad_configs() {
    let mut peers = BTreeSet::new();
    peers.insert(node_id(2));
    let valid = RaftConfig {
        node_id: node_id(1),
        peers: peers.clone(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    assert!(valid.validate().is_ok());

    let no_window = RaftConfig {
        max_inflight_messages: 0,
        ..valid.clone()
    };
    assert_eq!(
        no_window.validate(),
        Err("max_inflight_messages must be non-zero")
    );

    let no_bytes = RaftConfig {
        max_bytes_per_message: 0,
        ..valid.clone()
    };
    assert_eq!(
        no_bytes.validate(),
        Err("max_bytes_per_message must be non-zero")
    );

    let mut selfish = BTreeSet::new();
    selfish.insert(node_id(1));
    let loops_back = RaftConfig {
        peers: selfish,
        ..valid.clone()
    };
    assert_eq!(
        loops_back.validate(),
        Err("peers must not contain local node_id")
    );
}

#[test]
fn test_log_compacted_term_and_truncate_edges() {
    let entries: Vec<Entry> = (1..=5)
        .map(|i| {
            Entry::normal(
                LogIndex::from_raw(i),
                Term::from_raw(1),
                Bytes::from_static(b"x"),
            )
        })
        .collect();
    let mut log = anvil_raft::RaftLog::new(entries);
    assert_eq!(log.base_term(), Term::from_raw(0));
    log.compact_with_term(LogIndex::from_raw(3), Term::from_raw(1));
    assert_eq!(log.base_index(), LogIndex::from_raw(3));
    assert_eq!(log.base_term(), Term::from_raw(1));
    assert_eq!(log.term_at(LogIndex::from_raw(2)), None);
    assert_eq!(
        log.term_at(LogIndex::from_raw(3)),
        Some(Term::from_raw(1))
    );
    assert_eq!(log.find_last_index_of_term(Term::from_raw(7)), None);

    let before = log.last_index();
    log.truncate_and_append(
        LogIndex::from_raw(1),
        vec![Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(9),
            Bytes::from_static(b"stale"),
        )],
    );
    assert_eq!(log.last_index(), before);

    log.truncate_and_append(
        LogIndex::from_raw(2),
        vec![
            Entry::normal(
                LogIndex::from_raw(2),
                Term::from_raw(9),
                Bytes::from_static(b"stale"),
            ),
            Entry::normal(
                LogIndex::from_raw(3),
                Term::from_raw(9),
                Bytes::from_static(b"stale"),
            ),
            Entry::normal(
                LogIndex::from_raw(4),
                Term::from_raw(9),
                Bytes::from_static(b"fresh"),
            ),
        ],
    );
    assert_eq!(
        log.term_at(LogIndex::from_raw(4)),
        Some(Term::from_raw(9))
    );
    assert_eq!(log.last_index(), LogIndex::from_raw(4));
}

#[test]
fn test_election_sampler_rejection_loop_redraws() {
    // span = base with 2^64 mod base == threshold large, so the Lemire
    // redraw loop below is entered on ordinary seeds instead of never.
    let base: u64 = 2_049_854_317_994_589_153;
    let span = base;
    let threshold = span.wrapping_neg() % span;
    assert!(threshold > 0);
    let mut chosen: Option<u64> = None;
    for seed in 0..100_000u64 {
        let mut rng = ChaCha8Rng::seed_from_u64(seed);
        let sample = rng.next_u64();
        let low_bits = (u128::from(sample) * u128::from(span)) as u64;
        if low_bits < span && low_bits < threshold {
            chosen = Some(seed);
            break;
        }
    }
    let seed = chosen.expect("a seed must trigger the sampler rejection loop");
    let config = RaftConfig {
        node_id: node_id(1),
        peers: BTreeSet::new(),
        election_timeout_ticks: base,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let core = RaftCore::new(
        config,
        RecoveredState::default(),
        ChaCha8Rng::seed_from_u64(seed),
    );
    assert_eq!(core.role(), Role::Follower);
}

