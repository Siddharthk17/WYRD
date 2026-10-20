#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::too_many_lines
)]

use anvil_raft::{Input, Message, Output, PersistOp, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn make_config(node_id: u64, peers: &[u64]) -> RaftConfig {
    let mut peer_set = BTreeSet::new();
    for &p in peers {
        peer_set.insert(NodeId::new(p).expect("valid peer"));
    }
    RaftConfig {
        node_id: NodeId::new(node_id).expect("valid node id"),
        peers: peer_set,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    }
}

fn make_core(node_id: u64, peers: &[u64], seed: u64) -> RaftCore {
    let config = make_config(node_id, peers);
    let recovered = RecoveredState::default();
    let rng = ChaCha8Rng::seed_from_u64(seed);
    RaftCore::new(config, recovered, rng)
}

#[test]
fn test_initial_state_is_follower() {
    let core = make_core(1, &[2, 3], 42);
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term().as_u64(), 0);
    assert_eq!(core.voted_for(), None);
    assert_eq!(core.commit_index().as_u64(), 0);
    assert_eq!(core.last_log_index().as_u64(), 0);
}

#[test]
fn test_prevote_to_candidate_and_election_win() {
    let mut node1 = make_core(1, &[2, 3], 100);
    let mut output = Output::new();

    // Tick until election timeout triggers PreVote
    for _ in 0..10 {
        node1.step(Input::Tick, &mut output);
    }
    assert_eq!(node1.role(), Role::PreCandidate);
    assert!(!output.messages.is_empty());

    // Filter PreVote requests sent to peers
    let peer2 = NodeId::new(2).unwrap();
    let peer3 = NodeId::new(3).unwrap();

    output.clear();

    // Node 2 grants PreVote
    node1.step(
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

    // Node 1 should now transition to Candidate and broadcast RequestVoteRequest
    assert_eq!(node1.role(), Role::Candidate);
    assert_eq!(node1.term().as_u64(), 1);
    assert_eq!(node1.voted_for(), Some(NodeId::new(1).unwrap()));

    // Verify persistence contract: HardState persist op was emitted
    let has_hard_state = output
        .persist_ops
        .iter()
        .any(|op| matches!(op, PersistOp::HardState { term, vote, .. } if term.as_u64() == 1 && *vote == Some(NodeId::new(1).unwrap())));
    assert!(has_hard_state);

    output.clear();

    // Node 3 grants formal vote
    node1.step(
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

    // Node 1 should now be Leader
    assert_eq!(node1.role(), Role::Leader);
    // Leader appends a NoOp on election (charter rule)
    assert_eq!(node1.last_log_index().as_u64(), 1);
    assert_eq!(
        node1.log().entry_at(LogIndex::from_raw(1)).unwrap().kind,
        anvil_types::EntryKind::NoOp
    );
}

#[test]
fn test_leader_propose_and_parallel_persist_contract() {
    let mut leader = make_core(1, &[2, 3], 200);
    let mut output = Output::new();

    // Bootstrap leader directly by stepping tick and votes
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer2 = NodeId::new(2).unwrap();
    output.clear();
    leader.step(
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
    output.clear();
    leader.step(
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
    assert_eq!(leader.role(), Role::Leader);

    // Disk and peer2 acknowledge initial NoOp entry #1
    let noop_token = output
        .persist_ops
        .iter()
        .find_map(|op| match op {
            PersistOp::AppendEntries { entries, token, .. } if entries[0].index.as_u64() == 1 => {
                Some(*token)
            }
            _ => None,
        })
        .expect("must emit persist op for noop");
    output.clear();

    leader.step(Input::Persisted { token: noop_token }, &mut output);
    output.clear();

    leader.step(
        Input::Recv {
            from: peer2,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: peer2,
            },
        },
        &mut output,
    );
    output.clear();

    // Client proposes a command
    let cmd_payload = Bytes::from_static(b"set key=val");
    leader.step(
        Input::Propose {
            payload: cmd_payload.clone(),
        },
        &mut output,
    );

    // Leader appends entry #2
    assert_eq!(leader.last_log_index().as_u64(), 2);
    // Emits PersistOp::AppendEntries
    let token = output
        .persist_ops
        .iter()
        .find_map(|op| match op {
            PersistOp::AppendEntries { entries, token, .. } if entries[0].index.as_u64() == 2 => {
                Some(*token)
            }
            _ => None,
        })
        .expect("must emit persist op for proposed entry");

    // In parallel, leader sent AppendEntriesRequest to peers (peer 2 is now in Replicate mode)
    let sent_replications = output
        .messages
        .iter()
        .filter(|m| matches!(&m.message, Message::AppendEntriesRequest { entries, .. } if !entries.is_empty()))
        .count();
    assert!(sent_replications >= 1);

    output.clear();

    // Peer 2 responds with success for index 2
    leader.step(
        Input::Recv {
            from: peer2,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(2),
                conflict_term: None,
                conflict_index: None,
                from: peer2,
            },
        },
        &mut output,
    );

    // Entry #1 was already committed, but entry #2 requires leader's own persist to complete first!
    assert_eq!(leader.commit_index().as_u64(), 1);

    // Disk notifies leader that entry #2 is durable
    leader.step(Input::Persisted { token }, &mut output);

    // Now quorum (leader + peer2) is achieved for current-term entry #2 -> commits!
    assert_eq!(leader.commit_index().as_u64(), 2);
    assert_eq!(output.committed_entries.len(), 1); // normal #2 newly committed
}

#[test]
fn test_conflict_hints_1_rtt_repair() {
    let mut follower = make_core(2, &[1, 3], 300);
    let mut output = Output::new();

    // Populate follower log with an entry at term 1
    follower.step(
        Input::Recv {
            from: NodeId::new(1).unwrap(),
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(1),
                leader_id: NodeId::new(1).unwrap(),
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(1),
                    Term::from_raw(1),
                    Bytes::from_static(b"old"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    output.clear();

    // Leader sends AppendEntries for term 2 expecting prev_log_term = 2 at index 1
    follower.step(
        Input::Recv {
            from: NodeId::new(1).unwrap(),
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(2),
                leader_id: NodeId::new(1).unwrap(),
                prev_log_index: LogIndex::from_raw(1),
                prev_log_term: Term::from_raw(2), // Mismatch! Local is term 1
                entries: vec![Entry::normal(
                    LogIndex::from_raw(2),
                    Term::from_raw(2),
                    Bytes::from_static(b"new"),
                )],
                leader_commit: LogIndex::from_raw(1),
            },
        },
        &mut output,
    );

    // Follower must reject with conflict hint
    let rejection = output.messages.first().expect("rejection response");
    match &rejection.message {
        Message::AppendEntriesResponse {
            success,
            conflict_term,
            conflict_index,
            ..
        } => {
            assert!(!success);
            assert_eq!(*conflict_term, Some(Term::from_raw(1)));
            assert_eq!(*conflict_index, Some(LogIndex::from_raw(1)));
        }
        other => panic!("unexpected message: {other:?}"),
    }
}

#[test]
fn test_linearizable_read_index() {
    let mut leader = make_core(1, &[2, 3], 400);
    let mut output = Output::new();

    // Advance to leader
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer2 = NodeId::new(2).unwrap();
    output.clear();
    leader.step(
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
    output.clear();
    leader.step(
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
    assert_eq!(leader.role(), Role::Leader);
    output.clear();

    // Submit ReadIndex request
    let ctx = Bytes::from_static(b"client-req-999");
    leader.step(
        Input::ReadIndex {
            context: ctx.clone(),
        },
        &mut output,
    );

    // Heartbeats sent out to peers with context
    assert!(output.messages.iter().any(|m| matches!(&m.message, Message::HeartbeatRequest { read_context: Some(c), .. } if c == &ctx)));
    assert!(output.completed_reads.is_empty());
    output.clear();

    // Peer 2 responds to heartbeat
    leader.step(
        Input::Recv {
            from: peer2,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(1),
                from: peer2,
                read_context: Some(ctx.clone()),
            },
        },
        &mut output,
    );

    // Quorum confirmed (leader + peer2): read is completed and linearizable
    assert_eq!(output.completed_reads.len(), 1);
    assert_eq!(output.completed_reads[0].context, ctx);
}

#[test]
fn test_leadership_transfer_timeout_now() {
    let mut leader = make_core(1, &[2, 3], 500);
    let mut output = Output::new();

    // Bootstrap leader
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer2 = NodeId::new(2).unwrap();
    output.clear();
    leader.step(
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
    output.clear();
    leader.step(
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
    output.clear();

    // Update peer 2 match index to leader's last log index
    leader.step(
        Input::Recv {
            from: peer2,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: peer2,
            },
        },
        &mut output,
    );
    output.clear();

    // Request leader transfer to peer 2
    leader.step(Input::TransferLeader { to: peer2 }, &mut output);

    // TimeoutNow must be dispatched immediately
    let timeout_now = output
        .messages
        .iter()
        .find(|m| matches!(&m.message, Message::TimeoutNow { .. }))
        .expect("must send TimeoutNow");
    assert_eq!(timeout_now.to, peer2);
}

#[test]
fn test_check_quorum_step_down() {
    let mut leader = make_core(1, &[2, 3], 600);
    let mut output = Output::new();

    // Bootstrap leader
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer2 = NodeId::new(2).unwrap();
    output.clear();
    leader.step(
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
    output.clear();
    leader.step(
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
    assert_eq!(leader.role(), Role::Leader);
    output.clear();

    // Run 5 ticks (election_timeout_ticks) without any peer responses
    for _ in 0..5 {
        leader.step(Input::Tick, &mut output);
    }

    // Leader must step down to follower
    assert_eq!(leader.role(), Role::Follower);
    assert!(
        output
            .role_changes
            .iter()
            .any(|r| r.current == Role::Follower)
    );
}
