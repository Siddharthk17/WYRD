#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::assert_is_empty
)]

use anvil_raft::{
    Input, Message, Output, PersistOp, ProgressState, RaftConfig, RaftCore, ReceivedSnapshot,
    RecoveredState, Role, SnapshotChunkData,
};
use anvil_types::{ClusterId, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
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
            cluster_id: ClusterId::from_raw(1),
            learners: BTreeSet::new(),
            node_id: node_id(n),
            peers: set,
            election_timeout_ticks: 5,
            heartbeat_interval_ticks: 2,
            max_inflight_messages: 4,
            max_bytes_per_message: 64 * 1024,
            pre_vote_enabled: true,
            check_quorum: false,
        },
        RecoveredState::default(),
        ChaCha8Rng::seed_from_u64(n),
    )
}

fn ack_all(core: &mut RaftCore, output: &mut Output) {
    for _ in 0..16 {
        let tokens: Vec<_> = output.persist_ops.iter().map(PersistOp::token).collect();
        if tokens.is_empty() {
            break;
        }
        output.persist_ops.clear();
        for token in tokens {
            core.step(Input::Persisted { token }, output);
        }
    }
}

fn apply_all(core: &mut RaftCore, output: &mut Output) {
    let committed = std::mem::take(&mut output.committed_entries);
    for entry in &committed {
        core.step(Input::Applied { index: entry.index }, output);
    }
}

/// Delivers every pending message from `from_out` to `to`, acking and
/// applying on arrival. Returns the messages `to` emitted in response.
fn deliver(from: NodeId, from_out: &mut Output, to: &mut RaftCore, to_out: &mut Output) {
    let messages = std::mem::take(&mut from_out.messages);
    for outbound in messages {
        to.step(
            Input::Recv {
                from,
                message: outbound.message,
            },
            to_out,
        );
    }
    ack_all(to, to_out);
    apply_all(to, to_out);
}

fn elect_leader(core: &mut RaftCore, output: &mut Output, voter: NodeId) {
    for _ in 0..10 {
        core.step(Input::Tick, output);
    }
    output.clear();
    core.step(
        Input::Recv {
            from: voter,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: voter,
            },
        },
        output,
    );
    output.clear();
    core.step(
        Input::Recv {
            from: voter,
            message: Message::RequestVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: voter,
            },
        },
        output,
    );
    assert_eq!(core.role(), Role::Leader);
    ack_all(core, output);
    apply_all(core, output);
}

/// Brings one follower to the leader's no-op entry so later appends have
/// their previous entry present. Drops traffic for every other peer.
fn sync_follower(
    leader_id: NodeId,
    leader: &mut RaftCore,
    leader_out: &mut Output,
    follower_id: NodeId,
    follower: &mut RaftCore,
    follower_out: &mut Output,
) {
    deliver(leader_id, leader_out, follower, follower_out);
    deliver(follower_id, follower_out, leader, leader_out);
    ack_all(leader, leader_out);
    apply_all(leader, leader_out);
    apply_all(follower, follower_out);
    leader_out.clear();
    follower_out.clear();
}

fn propose_and_replicate(
    leader: &mut RaftCore,
    leader_out: &mut Output,
    follower: &mut RaftCore,
    follower_out: &mut Output,
    payload: &[u8],
) {
    leader.step(
        Input::Propose {
            payload: Bytes::copy_from_slice(payload),
        },
        leader_out,
    );
    ack_all(leader, leader_out);
    sync_follower(
        node_id(1),
        leader,
        leader_out,
        node_id(2),
        follower,
        follower_out,
    );
}

/// Sends heartbeats until the follower learns the leader's commit index.
fn sync_commit(
    leader: &mut RaftCore,
    leader_out: &mut Output,
    follower: &mut RaftCore,
    follower_out: &mut Output,
) {
    for _ in 0..4 {
        leader.step(Input::Tick, leader_out);
        ack_all(leader, leader_out);
        sync_follower(
            node_id(1),
            leader,
            leader_out,
            node_id(2),
            follower,
            follower_out,
        );
        if follower.commit_index() == leader.commit_index() {
            break;
        }
    }
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

fn install_requests(output: &Output) -> Vec<Message> {
    output
        .messages
        .iter()
        .filter_map(|m| match &m.message {
            Message::InstallSnapshotRequest { .. } => Some(m.message.clone()),
            _ => None,
        })
        .collect()
}

fn pump_snapshot_stream(
    leader: &mut RaftCore,
    leader_out: &mut Output,
    follower: &mut RaftCore,
    follower_out: &mut Output,
    follower_id: NodeId,
) -> Vec<ReceivedSnapshot> {
    for _ in 0..32 {
        let requests = install_requests(leader_out);
        leader_out
            .messages
            .retain(|m| !matches!(m.message, Message::InstallSnapshotRequest { .. }));
        if requests.is_empty() && follower_out.messages.is_empty() {
            break;
        }
        for message in requests {
            follower.step(
                Input::Recv {
                    from: node_id(1),
                    message,
                },
                follower_out,
            );
        }
        ack_all(follower, follower_out);
        apply_all(follower, follower_out);
        let responses = std::mem::take(&mut follower_out.messages);
        for outbound in responses {
            leader.step(
                Input::Recv {
                    from: follower_id,
                    message: outbound.message,
                },
                leader_out,
            );
        }
        ack_all(leader, leader_out);
        if !follower_out.ready_snapshots.is_empty() {
            break;
        }
    }
    apply_all(follower, follower_out);
    std::mem::take(&mut follower_out.ready_snapshots)
}

fn three_cores() -> (RaftCore, RaftCore, RaftCore, Output, Output, Output) {
    (
        make_core(1, &[2, 3]),
        make_core(2, &[1, 3]),
        make_core(3, &[1, 2]),
        Output::new(),
        Output::new(),
        Output::new(),
    )
}

#[test]
fn test_snapshot_stream_installs_and_rejoins_by_log() {
    let (mut leader, mut second, mut third, mut leader_out, mut second_out, mut third_out) =
        three_cores();

    elect_leader(&mut leader, &mut leader_out, node_id(2));
    sync_follower(
        node_id(1),
        &mut leader,
        &mut leader_out,
        node_id(2),
        &mut second,
        &mut second_out,
    );

    for i in 0..5u64 {
        propose_and_replicate(
            &mut leader,
            &mut leader_out,
            &mut second,
            &mut second_out,
            format!("entry-{i}").as_bytes(),
        );
    }
    sync_commit(&mut leader, &mut leader_out, &mut second, &mut second_out);
    assert_eq!(leader.commit_index(), LogIndex::from_raw(6));
    assert_eq!(second.commit_index(), LogIndex::from_raw(6));

    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(4),
        },
        &mut leader_out,
    );
    assert_eq!(leader.log().base_index(), LogIndex::from_raw(4));
    leader_out.clear();

    let (full, chunks, digest) = fake_snapshot(200, 3);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(4),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: full.len() as u64,
            snapshot_digest: digest,
            chunks,
        },
        &mut leader_out,
    );
    assert!(leader_out.snapshot_requests.is_empty());

    let requests = install_requests(&leader_out);
    assert!(!requests.is_empty(), "leader must stream snapshot");
    assert!(
        leader_out
            .messages
            .iter()
            .any(|m| matches!(m.message, Message::AppendEntriesRequest { .. })),
        "live peer stays on log"
    );

    let installed = pump_snapshot_stream(
        &mut leader,
        &mut leader_out,
        &mut third,
        &mut third_out,
        node_id(3),
    );
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].data, full);
    assert_eq!(installed[0].last_included_index, LogIndex::from_raw(4));
    assert_eq!(third.log().base_index(), LogIndex::from_raw(4));
    assert_eq!(third.commit_index(), LogIndex::from_raw(4));

    let progress = leader.progress().get(&node_id(3)).expect("peer");
    assert_eq!(progress.match_index, LogIndex::from_raw(4));
    assert!(!matches!(progress.state, ProgressState::Snapshot(_)));

    third_out.clear();
    // Drain the stream: a duplicate chunk may still be in flight from window
    // fill. The installed follower acks it as complete and the leader, now
    // out of Snapshot state, ignores the stale ack.
    deliver(node_id(1), &mut leader_out, &mut third, &mut third_out);
    let stale = std::mem::take(&mut third_out.messages);
    for outbound in stale {
        leader.step(
            Input::Recv {
                from: node_id(3),
                message: outbound.message,
            },
            &mut leader_out,
        );
    }
    ack_all(&mut leader, &mut leader_out);
    leader_out
        .messages
        .retain(|m| !matches!(m.message, Message::InstallSnapshotRequest { .. }));
    assert!(
        install_requests(&leader_out).is_empty(),
        "installed peer rejoins by log, not snapshot"
    );
    deliver(node_id(1), &mut leader_out, &mut third, &mut third_out);
    let flo_responses = std::mem::take(&mut third_out.messages);
    for outbound in flo_responses {
        leader.step(
            Input::Recv {
                from: node_id(3),
                message: outbound.message,
            },
            &mut leader_out,
        );
    }
    ack_all(&mut leader, &mut leader_out);
    apply_all(&mut leader, &mut leader_out);
    apply_all(&mut third, &mut third_out);
    assert_eq!(third.commit_index(), LogIndex::from_raw(6));
}

#[test]
fn test_snapshot_requests_bytes_when_missing() {
    let (mut leader, mut second, mut third, mut leader_out, mut second_out, mut third_out) =
        three_cores();
    elect_leader(&mut leader, &mut leader_out, node_id(2));
    sync_follower(
        node_id(1),
        &mut leader,
        &mut leader_out,
        node_id(2),
        &mut second,
        &mut second_out,
    );

    for i in 0..2u64 {
        propose_and_replicate(
            &mut leader,
            &mut leader_out,
            &mut second,
            &mut second_out,
            format!("entry-{i}").as_bytes(),
        );
    }
    assert_eq!(leader.commit_index(), LogIndex::from_raw(3));
    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(2),
        },
        &mut leader_out,
    );
    assert_eq!(leader.log().base_index(), LogIndex::from_raw(2));
    leader_out.clear();

    // Heartbeats discover the lagging peer: the follower answers, and the
    // leader routes it to a snapshot stream that has no bytes yet.
    for _ in 0..2 {
        leader.step(Input::Tick, &mut leader_out);
    }
    deliver(node_id(1), &mut leader_out, &mut third, &mut third_out);
    deliver(node_id(3), &mut third_out, &mut leader, &mut leader_out);
    assert!(
        install_requests(&leader_out).is_empty(),
        "no snapshot bytes buffered, nothing to send"
    );
    assert_eq!(leader_out.snapshot_requests.len(), 1);
    assert_eq!(leader_out.snapshot_requests[0].to, node_id(3));
    assert_eq!(
        leader_out.snapshot_requests[0].least_index,
        LogIndex::from_raw(1)
    );

    leader_out.clear();
    third_out.clear();
    for _ in 0..2 {
        leader.step(Input::Tick, &mut leader_out);
    }
    deliver(node_id(1), &mut leader_out, &mut third, &mut third_out);
    deliver(node_id(3), &mut third_out, &mut leader, &mut leader_out);
    assert!(
        leader_out.snapshot_requests.is_empty(),
        "duplicate requests are suppressed while the need is unchanged"
    );
}

#[test]
fn test_snapshot_corrupt_chunk_is_resent() {
    let (mut leader, mut second, mut third, mut leader_out, mut second_out, mut third_out) =
        three_cores();
    elect_leader(&mut leader, &mut leader_out, node_id(2));
    sync_follower(
        node_id(1),
        &mut leader,
        &mut leader_out,
        node_id(2),
        &mut second,
        &mut second_out,
    );

    for i in 0..2u64 {
        propose_and_replicate(
            &mut leader,
            &mut leader_out,
            &mut second,
            &mut second_out,
            format!("entry-{i}").as_bytes(),
        );
    }
    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(2),
        },
        &mut leader_out,
    );
    leader_out.clear();

    let (full, chunks, digest) = fake_snapshot(120, 3);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(2),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: full.len() as u64,
            snapshot_digest: digest,
            chunks,
        },
        &mut leader_out,
    );

    let progress = leader.progress().get(&node_id(3)).expect("peer");
    assert!(progress.next_index <= leader.log().base_index());

    let mut first = install_requests(&leader_out);
    assert!(!first.is_empty());
    leader_out.messages.clear();

    let bad = match first.remove(0) {
        Message::InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            chunk_index,
            total_chunks,
            chunk,
            chunk_crc,
            snapshot_digest,
            snapshot_bytes,
        } => Message::InstallSnapshotRequest {
            term,
            leader_id,
            last_included_index,
            last_included_term,
            chunk_index,
            total_chunks,
            chunk,
            chunk_crc: chunk_crc.wrapping_add(1),
            snapshot_digest,
            snapshot_bytes,
        },
        other => panic!("expected snapshot request, got {other:?}"),
    };
    third.step(
        Input::Recv {
            from: node_id(1),
            message: bad,
        },
        &mut third_out,
    );
    ack_all(&mut third, &mut third_out);
    let rejected = third_out.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(rejected, "CRC mismatch must be rejected");
    assert!(third_out.ready_snapshots.is_empty());

    let installed = pump_snapshot_stream(
        &mut leader,
        &mut leader_out,
        &mut third,
        &mut third_out,
        node_id(3),
    );
    assert_eq!(installed.len(), 1);
    assert_eq!(installed[0].data, full);
}

#[test]
fn test_snapshot_stream_aborts_on_leader_change() {
    let (mut leader, mut second, mut third, mut leader_out, mut second_out, mut third_out) =
        three_cores();
    elect_leader(&mut leader, &mut leader_out, node_id(2));
    sync_follower(
        node_id(1),
        &mut leader,
        &mut leader_out,
        node_id(2),
        &mut second,
        &mut second_out,
    );

    propose_and_replicate(
        &mut leader,
        &mut leader_out,
        &mut second,
        &mut second_out,
        b"entry",
    );
    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(1),
        },
        &mut leader_out,
    );
    leader_out.clear();

    let (_, chunks, digest) = fake_snapshot(120, 3);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(1),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: 120,
            snapshot_digest: digest,
            chunks,
        },
        &mut leader_out,
    );
    let requests = install_requests(&leader_out);
    assert!(!requests.is_empty());
    leader_out.messages.clear();

    third.step(
        Input::Recv {
            from: node_id(1),
            message: requests[0].clone(),
        },
        &mut third_out,
    );
    ack_all(&mut third, &mut third_out);
    third_out.clear();

    third.step(
        Input::Recv {
            from: node_id(2),
            message: Message::RequestVoteRequest {
                term: Term::from_raw(2),
                candidate_id: node_id(2),
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut third_out,
    );
    ack_all(&mut third, &mut third_out);
    assert!(third_out.ready_snapshots.is_empty());

    third.step(
        Input::Recv {
            from: node_id(1),
            message: requests[0].clone(),
        },
        &mut third_out,
    );
    ack_all(&mut third, &mut third_out);
    let stale_rejected = third_out.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::InstallSnapshotResponse { success: false, .. }
        )
    });
    assert!(stale_rejected, "old-term stream must abort");
    assert!(third_out.ready_snapshots.is_empty());
}

/// Chunked snapshot transfer must not stall the consensus path: while one
/// follower streams a multi-window snapshot, heartbeats keep flowing to the
/// healthy follower every heartbeat interval, and the stream finishes well
/// within a bounded tick budget. This is the virtual-time half of the Week 5
/// performance gate (chunking off the consensus path).
#[test]
fn test_snapshot_stream_does_not_stall_heartbeats() {
    let (mut leader, mut second, mut third, mut leader_out, mut second_out, mut third_out) =
        three_cores();
    elect_leader(&mut leader, &mut leader_out, node_id(2));
    sync_follower(
        node_id(1),
        &mut leader,
        &mut leader_out,
        node_id(2),
        &mut second,
        &mut second_out,
    );
    for i in 0..5u64 {
        propose_and_replicate(
            &mut leader,
            &mut leader_out,
            &mut second,
            &mut second_out,
            format!("entry-{i}").as_bytes(),
        );
    }
    sync_commit(&mut leader, &mut leader_out, &mut second, &mut second_out);
    leader.step(
        Input::CompactLog {
            up_to: LogIndex::from_raw(4),
        },
        &mut leader_out,
    );
    assert_eq!(leader.log().base_index(), LogIndex::from_raw(4));
    leader_out.clear();

    // Twelve chunks over a window of four: several fill-and-ack rounds,
    // long enough that heartbeat cadence is observable mid-stream.
    let (full, chunks, digest) = fake_snapshot(2400, 12);
    leader.step(
        Input::SnapshotReady {
            last_included_index: LogIndex::from_raw(4),
            last_included_term: Term::from_raw(1),
            snapshot_bytes: full.len() as u64,
            snapshot_digest: digest,
            chunks,
        },
        &mut leader_out,
    );
    assert!(!install_requests(&leader_out).is_empty());

    let mut heartbeats_to_second = 0u32;
    let mut silent_ticks = 0u32;
    let mut completed = false;
    for _ in 0..48 {
        leader.step(Input::Tick, &mut leader_out);
        ack_all(&mut leader, &mut leader_out);
        let mut contacted_second = false;
        for outbound in std::mem::take(&mut leader_out.messages) {
            match &outbound.message {
                Message::AppendEntriesRequest { .. } | Message::HeartbeatRequest { .. }
                    if outbound.to == node_id(2) =>
                {
                    contacted_second = true;
                    second.step(
                        Input::Recv {
                            from: node_id(1),
                            message: outbound.message,
                        },
                        &mut second_out,
                    );
                }
                Message::InstallSnapshotRequest { .. } | Message::AppendEntriesRequest { .. } => {
                    third.step(
                        Input::Recv {
                            from: node_id(1),
                            message: outbound.message,
                        },
                        &mut third_out,
                    );
                }
                _ => {}
            }
        }
        if contacted_second {
            heartbeats_to_second += 1;
            silent_ticks = 0;
        } else {
            silent_ticks += 1;
        }
        assert!(
            silent_ticks <= 4,
            "healthy follower starved of heartbeats during snapshot stream"
        );
        ack_all(&mut second, &mut second_out);
        ack_all(&mut third, &mut third_out);
        apply_all(&mut second, &mut second_out);
        apply_all(&mut third, &mut third_out);
        for outbound in second_out
            .messages
            .drain(..)
            .chain(third_out.messages.drain(..))
            .collect::<Vec<_>>()
        {
            leader.step(
                Input::Recv {
                    from: if matches!(outbound.message, Message::InstallSnapshotResponse { .. }) {
                        node_id(3)
                    } else {
                        node_id(2)
                    },
                    message: outbound.message,
                },
                &mut leader_out,
            );
        }
        ack_all(&mut leader, &mut leader_out);
        if !third_out.ready_snapshots.is_empty() {
            completed = true;
            break;
        }
    }
    assert!(completed, "12-chunk stream finishes within 48 ticks");
    assert!(
        heartbeats_to_second >= 3,
        "heartbeats must flow during the stream, saw {heartbeats_to_second}"
    );
}
