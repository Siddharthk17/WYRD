#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::too_many_lines
)]

use anvil_raft::{Input, Message, Output, PersistOp, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_raft::{PeerProgress, ProgressState, RaftLog};
use anvil_types::{Entry, EntryKind, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn make_core(node_id: u64, peers: &[u64]) -> RaftCore {
    let mut peer_set = BTreeSet::new();
    for &peer in peers {
        peer_set.insert(NodeId::new(peer).expect("peer"));
    }
    let config = RaftConfig {
        node_id: NodeId::new(node_id).expect("node"),
        peers: peer_set,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let rng = ChaCha8Rng::seed_from_u64(node_id);
    RaftCore::new(config, RecoveredState::default(), rng)
}

fn grant_prevote(peer: NodeId) -> Message {
    Message::PreVoteResponse {
        term: Term::from_raw(0),
        vote_granted: true,
        from: peer,
    }
}

fn grant_vote(peer: NodeId) -> Message {
    Message::RequestVoteResponse {
        term: Term::from_raw(1),
        vote_granted: true,
        from: peer,
    }
}

fn persisted_tokens(output: &Output) -> Vec<anvil_raft::PersistToken> {
    output.persist_ops.iter().map(PersistOp::token).collect()
}

#[test]
fn test_message_term_accessor() {
    let term = Term::from_raw(4);
    let leader = NodeId::new(1).expect("node");
    let peer = NodeId::new(2).expect("peer");
    let index = LogIndex::from_raw(3);
    let messages = [
        Message::PreVoteRequest {
            term,
            candidate_id: peer,
            last_log_index: index,
            last_log_term: term,
        },
        Message::PreVoteResponse {
            term,
            vote_granted: true,
            from: peer,
        },
        Message::RequestVoteRequest {
            term,
            candidate_id: peer,
            last_log_index: index,
            last_log_term: term,
        },
        Message::RequestVoteResponse {
            term,
            vote_granted: true,
            from: peer,
        },
        Message::AppendEntriesRequest {
            term,
            leader_id: leader,
            prev_log_index: index,
            prev_log_term: term,
            entries: Vec::new(),
            leader_commit: index,
        },
        Message::AppendEntriesResponse {
            term,
            success: true,
            match_index: index,
            conflict_term: None,
            conflict_index: None,
            from: peer,
        },
        Message::HeartbeatRequest {
            term,
            leader_id: leader,
            commit_index: index,
            read_context: None,
        },
        Message::HeartbeatResponse {
            term,
            from: peer,
            read_context: None,
        },
        Message::TimeoutNow {
            term,
            leader_id: leader,
        },
    ];
    for message in &messages {
        assert_eq!(message.term(), term);
    }
}

#[test]
fn test_role_display_and_config_sizes() {
    assert_eq!(anvil_raft::Role::Follower.as_str(), "Follower");
    assert_eq!(anvil_raft::Role::PreCandidate.as_str(), "PreCandidate");
    assert_eq!(anvil_raft::Role::Candidate.as_str(), "Candidate");
    assert_eq!(anvil_raft::Role::Leader.as_str(), "Leader");

    let core = make_core(1, &[2, 3]);
    assert_eq!(core.log().len(), 0);
    assert!(core.log().is_empty());
}

#[test]
fn test_single_node_cluster_elects_and_commits() {
    let mut core = make_core(1, &[]);
    let mut output = Output::new();

    for _ in 0..20 {
        core.step(Input::Tick, &mut output);
    }
    assert_eq!(core.role(), Role::Leader);
    assert_eq!(core.last_log_index(), LogIndex::from_raw(1));

    for token in persisted_tokens(&output) {
        output.clear();
        core.step(Input::Persisted { token }, &mut output);
    }

    let entry_index = LogIndex::from_raw(2);
    output.clear();
    core.step(
        Input::Propose {
            payload: Bytes::from_static(b"solo"),
        },
        &mut output,
    );
    assert_eq!(core.last_log_index(), entry_index);

    for token in persisted_tokens(&output) {
        output.clear();
        core.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(core.commit_index(), entry_index);

    output.clear();
    core.step(
        Input::ReadIndex {
            context: Bytes::from_static(b"ctx"),
        },
        &mut output,
    );
    assert_eq!(output.completed_reads.len(), 1);
    assert_eq!(output.completed_reads[0].read_index, entry_index);
}

#[test]
fn test_large_cluster_uses_heap_fallback_paths() {
    let peers: Vec<u64> = (2..=17).collect();
    let mut leader = make_core(1, &peers);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);

    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(leader.commit_index(), LogIndex::from_raw(0));

    for peer_raw in 2..=17u64 {
        let peer = NodeId::new(peer_raw).expect("peer");
        output.clear();
        leader.step(
            Input::Recv {
                from: peer,
                message: Message::AppendEntriesResponse {
                    term: Term::from_raw(0),
                    success: true,
                    match_index: LogIndex::from_raw(1),
                    conflict_term: None,
                    conflict_index: None,
                    from: peer,
                },
            },
            &mut output,
        );
    }
    assert_eq!(leader.commit_index(), LogIndex::from_raw(1));

    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"wide"),
        },
        &mut output,
    );
    let replicate_count = output
        .messages
        .iter()
        .filter(|m| matches!(&m.message, Message::AppendEntriesRequest { .. }))
        .count();
    assert_eq!(replicate_count, 16);

    let entry_token = persisted_tokens(&output)
        .into_iter()
        .next()
        .expect("entry persist token");
    output.clear();
    leader.step(Input::Persisted { token: entry_token }, &mut output);
    assert_eq!(leader.commit_index(), LogIndex::from_raw(1));

    for peer_raw in 2..=17u64 {
        let peer = NodeId::new(peer_raw).expect("peer");
        output.clear();
        leader.step(
            Input::Recv {
                from: peer,
                message: Message::AppendEntriesResponse {
                    term: Term::from_raw(0),
                    success: true,
                    match_index: LogIndex::from_raw(2),
                    conflict_term: None,
                    conflict_index: None,
                    from: peer,
                },
            },
            &mut output,
        );
    }
    assert_eq!(leader.commit_index(), LogIndex::from_raw(2));
}

#[test]
fn test_vote_denied_when_already_voted_or_log_stale() {
    let voted_for = NodeId::new(9).expect("voter");
    let config = RaftConfig {
        node_id: NodeId::new(1).expect("node"),
        peers: BTreeSet::from([NodeId::new(2).expect("peer")]),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let recovered = RecoveredState {
        current_term: Term::from_raw(3),
        voted_for: Some(voted_for),
        commit_index: LogIndex::from_raw(0),
        entries: Vec::new(),
    };
    let mut core = RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(7));
    let mut output = Output::new();

    let challenger = NodeId::new(2).expect("challenger");
    core.step(
        Input::Recv {
            from: challenger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(3),
                candidate_id: challenger,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    let denied = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::RequestVoteResponse {
                vote_granted: false,
                ..
            }
        )
    });
    assert!(denied);

    output.clear();
    core.step(
        Input::Recv {
            from: challenger,
            message: Message::PreVoteRequest {
                term: Term::from_raw(2),
                candidate_id: challenger,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    let prevote_denied = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::PreVoteResponse {
                vote_granted: false,
                ..
            }
        )
    });
    assert!(prevote_denied);

    let log_config = RaftConfig {
        node_id: NodeId::new(5).expect("node"),
        peers: BTreeSet::from([NodeId::new(6).expect("peer")]),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let log_recovered = RecoveredState {
        current_term: Term::from_raw(3),
        voted_for: None,
        commit_index: LogIndex::from_raw(0),
        entries: vec![Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(5),
            Bytes::from_static(b"newer"),
        )],
    };
    let mut log_core = RaftCore::new(log_config, log_recovered, ChaCha8Rng::seed_from_u64(8));
    let mut log_output = Output::new();
    let log_challenger = NodeId::new(6).expect("challenger");
    log_core.step(
        Input::Recv {
            from: log_challenger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(3),
                candidate_id: log_challenger,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut log_output,
    );
    let stale_denied = log_output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::RequestVoteResponse {
                vote_granted: false,
                ..
            }
        )
    });
    assert!(stale_denied);
}

#[test]
fn test_stale_and_mismatched_messages_are_ignored() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");

    core_step_append_dummy(&mut follower, &mut output, leader);

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(5),
                prev_log_term: Term::from_raw(0),
                entries: Vec::new(),
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
    assert!(hinted, "empty suffix must hint last_index + 1");

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .any(|m| matches!(&m.message, Message::HeartbeatResponse { .. }))
    );

    output.clear();
    follower.step(
        Input::ReadIndex {
            context: Bytes::from_static(b"x"),
        },
        &mut output,
    );
    assert!(output.completed_reads.is_empty());

    output.clear();
    follower.step(
        Input::Propose {
            payload: Bytes::from_static(b"nope"),
        },
        &mut output,
    );
    assert_eq!(follower.last_log_index(), LogIndex::from_raw(1));

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(0),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: leader,
            },
        },
        &mut output,
    );
    assert_eq!(follower.role(), Role::Follower);

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: leader,
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(follower.role(), Role::Follower);

    output.clear();
    follower.step(
        Input::Persisted {
            token: anvil_raft::PersistToken(u64::MAX),
        },
        &mut output,
    );
    assert!(output.messages.is_empty());
}

fn core_step_append_dummy(core: &mut RaftCore, output: &mut Output, leader: NodeId) {
    core.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(1),
                    Term::from_raw(0),
                    Bytes::from_static(b"d"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        output,
    );
    let tokens = persisted_tokens(output);
    for token in tokens {
        output.clear();
        core.step(Input::Persisted { token }, output);
    }
}

#[test]
fn test_transfer_and_timeout_edge_cases() {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    let peer = NodeId::new(2).expect("peer");

    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"blocked"),
        },
        &mut output,
    );
    assert_eq!(leader.last_log_index(), LogIndex::from_raw(1));

    let stranger = NodeId::new(99).expect("stranger");
    let mut solo = make_core(1, &[2, 3]);
    let mut solo_output = Output::new();
    solo.step(Input::TransferLeader { to: stranger }, &mut solo_output);
    assert!(solo_output.messages.is_empty());

    let mut timeout = make_core(2, &[1]);
    let mut timeout_output = Output::new();
    timeout.step(
        Input::Recv {
            from: NodeId::new(1).expect("leader"),
            message: Message::TimeoutNow {
                term: Term::from_raw(0),
                leader_id: NodeId::new(1).expect("leader"),
            },
        },
        &mut timeout_output,
    );
    assert_eq!(timeout.role(), Role::Candidate);

    let mut stale = make_core(2, &[1]);
    let mut stale_output = Output::new();
    stale.step(
        Input::Recv {
            from: NodeId::new(1).expect("leader"),
            message: Message::RequestVoteResponse {
                term: Term::from_raw(9),
                vote_granted: false,
                from: NodeId::new(1).expect("leader"),
            },
        },
        &mut stale_output,
    );
    assert_eq!(stale.role(), Role::Follower);
}

#[test]
fn test_prevote_and_vote_response_edge_cases() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    let peer = NodeId::new(2).expect("peer");

    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: grant_prevote(peer),
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Follower);

    for _ in 0..10 {
        core.step(Input::Tick, &mut output);
    }
    assert_eq!(core.role(), Role::PreCandidate);
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: Message::PreVoteResponse {
                term: Term::from_raw(99),
                vote_granted: true,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Follower);
    assert_eq!(core.term(), Term::from_raw(99));

    let mut candidate = make_core(1, &[2, 3]);
    let mut candidate_output = Output::new();
    for _ in 0..10 {
        candidate.step(Input::Tick, &mut candidate_output);
    }
    candidate_output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: grant_prevote(peer),
        },
        &mut candidate_output,
    );
    assert_eq!(candidate.role(), Role::Candidate);
    candidate_output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: Message::RequestVoteResponse {
                term: Term::from_raw(0),
                vote_granted: true,
                from: peer,
            },
        },
        &mut candidate_output,
    );
    assert_eq!(candidate.role(), Role::Candidate);

    candidate_output.clear();
    candidate.step(
        Input::Applied {
            index: LogIndex::from_raw(1),
        },
        &mut candidate_output,
    );
    assert_eq!(candidate.applied_index(), LogIndex::from_raw(1));
    candidate_output.clear();
    candidate.step(
        Input::Applied {
            index: LogIndex::from_raw(0),
        },
        &mut candidate_output,
    );
    assert_eq!(candidate.applied_index(), LogIndex::from_raw(1));
}

#[test]
fn test_persist_op_tokens_and_entry_kinds() {
    let normal = Entry::normal(
        LogIndex::from_raw(1),
        Term::from_raw(1),
        Bytes::from_static(b"x"),
    );
    assert_eq!(normal.kind, EntryKind::Normal);
    let noop = Entry::noop(LogIndex::from_raw(2), Term::from_raw(1));
    assert_eq!(noop.kind, EntryKind::NoOp);

    let hard_state = PersistOp::HardState {
        term: Term::from_raw(1),
        vote: None,
        token: anvil_raft::PersistToken(7),
    };
    assert_eq!(hard_state.token(), anvil_raft::PersistToken(7));
}

#[test]
fn test_log_helpers_edge_cases() {
    let empty = RaftLog::default();
    assert!(empty.is_empty());
    assert_eq!(empty.entry_at(LogIndex::from_raw(0)), None);
    assert_eq!(empty.entry_at(LogIndex::from_raw(9)), None);
    assert_eq!(empty.term_at(LogIndex::from_raw(9)), None);
    assert!(empty.slice_from(LogIndex::from_raw(0), 1024).is_empty());
    assert!(empty.slice_from(LogIndex::from_raw(9), 1024).is_empty());
    assert_eq!(empty.find_first_index_of_term(Term::from_raw(1)), None);

    let mut log = RaftLog::new(vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(1),
            Bytes::from_static(b"a"),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(2),
            Bytes::from_static(b"b"),
        ),
    ]);
    let capped = log.slice_from(LogIndex::from_raw(1), 1);
    assert_eq!(capped.len(), 1);

    log.truncate_and_append(
        LogIndex::from_raw(2),
        vec![Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(3),
            Bytes::from_static(b"c"),
        )],
    );
    assert_eq!(log.term_at(LogIndex::from_raw(2)), Some(Term::from_raw(3)));
    assert_eq!(
        log.find_last_index_of_term(Term::from_raw(1)),
        Some(LogIndex::from_raw(1))
    );

    log.truncate_and_append(LogIndex::from_raw(1), Vec::new());
    assert_eq!(log.len(), 0);
    assert_ne!(log.compute_digest(), [0u8; 32]);
}

#[test]
fn test_peer_progress_fallback_paths() {
    let mut probe = PeerProgress::new(LogIndex::from_raw(5), 4);
    assert!(probe.can_send());
    probe.on_send(3, LogIndex::from_raw(7));
    assert!(!probe.can_send());

    let mut follower = PeerProgress::new(LogIndex::from_raw(5), 4);
    follower.on_rejection(None, None, |_| None);
    assert_eq!(follower.state, ProgressState::Probe);
    assert_eq!(follower.next_index, LogIndex::from_raw(4));

    let mut zeroed = PeerProgress::new(LogIndex::from_raw(0), 4);
    zeroed.on_rejection(None, None, |_| None);
    assert_eq!(zeroed.next_index, LogIndex::from_raw(1));

    let mut clamped = PeerProgress::new(LogIndex::from_raw(2), 4);
    clamped.on_success(LogIndex::from_raw(5));
    clamped.on_rejection(None, Some(LogIndex::from_raw(1)), |_| None);
    assert_eq!(clamped.next_index, LogIndex::from_raw(6));

    let mut conflicted = PeerProgress::new(LogIndex::from_raw(8), 4);
    conflicted.on_rejection(Some(Term::from_raw(7)), None, |_| None);
    assert_eq!(conflicted.state, ProgressState::Probe);
    assert_eq!(conflicted.next_index, LogIndex::from_raw(7));

    let mut hinted = PeerProgress::new(LogIndex::from_raw(8), 4);
    hinted.on_rejection(Some(Term::from_raw(7)), Some(LogIndex::from_raw(3)), |_| {
        None
    });
    assert_eq!(hinted.next_index, LogIndex::from_raw(3));

    let mut repaired = PeerProgress::new(LogIndex::from_raw(8), 4);
    repaired.on_rejection(Some(Term::from_raw(7)), None, |_| {
        Some(LogIndex::from_raw(5))
    });
    assert_eq!(repaired.next_index, LogIndex::from_raw(6));

    let mut replicating = PeerProgress::new(LogIndex::from_raw(2), 4);
    replicating.state = ProgressState::Replicate;
    replicating.on_send(2, LogIndex::from_raw(2));
    assert_eq!(replicating.next_index, LogIndex::from_raw(3));
    replicating.on_success(LogIndex::from_raw(1));
    assert_eq!(replicating.next_index, LogIndex::from_raw(3));
}

fn make_core_with(config: RaftConfig, seed: u64) -> RaftCore {
    RaftCore::new(
        config,
        RecoveredState::default(),
        ChaCha8Rng::seed_from_u64(seed),
    )
}

fn base_config(node_id: u64, peers: &[u64]) -> RaftConfig {
    let mut peer_set = BTreeSet::new();
    for &peer in peers {
        peer_set.insert(NodeId::new(peer).expect("peer"));
    }
    RaftConfig {
        node_id: NodeId::new(node_id).expect("node"),
        peers: peer_set,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    }
}

#[test]
fn test_prevote_disabled_goes_straight_to_candidate() {
    let mut config = base_config(1, &[2, 3]);
    config.pre_vote_enabled = false;
    config.election_timeout_ticks = 1;
    let mut core = make_core_with(config, 21);
    let mut output = Output::new();
    core.step(Input::Tick, &mut output);
    assert_eq!(core.role(), Role::Candidate);
    assert_eq!(core.term(), Term::from_raw(1));
}

#[test]
fn test_stale_term_append_entries_rejected() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");

    follower.step(
        Input::Recv {
            from: leader,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(3),
                candidate_id: leader,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(3));
    for token in persisted_tokens(&output) {
        output.clear();
        follower.step(Input::Persisted { token }, &mut output);
    }

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(2),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: Vec::new(),
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    let rejected = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::AppendEntriesResponse { success: false, .. }
        )
    });
    assert!(rejected);
    assert_eq!(follower.term(), Term::from_raw(3));
}

#[test]
fn test_higher_term_response_steps_down_and_persist_after_stepdown() {
    let (mut leader, peer, mut output) = elect_via_helper(500);
    let entry_token = persisted_tokens(&output)
        .into_iter()
        .next()
        .expect("entry token");

    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(9),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.term(), Term::from_raw(9));

    output.clear();
    leader.step(Input::Persisted { token: entry_token }, &mut output);
    assert_eq!(leader.commit_index(), LogIndex::from_raw(0));

    output.clear();
    let stranger = NodeId::new(77).expect("stranger");
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(9),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: stranger,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Follower);

    output.clear();
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(9),
                from: stranger,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(output.messages.is_empty());
}

fn elect_via_helper(seed: u64) -> (RaftCore, NodeId, Output) {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..10 {
        leader.step(Input::Tick, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
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
    let _ = seed;
    (leader, peer, output)
}

#[test]
fn test_transfer_catch_up_then_timeout() {
    let (mut leader, peer, mut output) = elect_via_helper(501);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(leader.progress().len(), 2);

    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    let immediate = output
        .messages
        .iter()
        .any(|m| matches!(&m.message, Message::TimeoutNow { .. }));
    assert!(!immediate);

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
    let timeout = output
        .messages
        .iter()
        .any(|m| matches!(&m.message, Message::TimeoutNow { .. }) && m.to == peer);
    assert!(timeout);
}

#[test]
fn test_heartbeat_with_higher_term() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(6),
                leader_id: leader,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(follower.term(), Term::from_raw(6));
    assert_eq!(follower.role(), Role::Follower);
    for token in persisted_tokens(&output) {
        output.clear();
        follower.step(Input::Persisted { token }, &mut output);
    }
    let echoed = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::HeartbeatResponse { term, .. } if *term == Term::from_raw(6)
        )
    });
    assert!(echoed);
}

fn become_test_leader() -> (RaftCore, NodeId, Output) {
    let mut leader = make_core(1, &[2, 3]);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
    (leader, peer, output)
}

#[test]
fn test_leader_ignores_stranger_responses() {
    let (mut leader, _peer, mut output) = become_test_leader();
    let stranger = NodeId::new(77).expect("stranger");
    assert_eq!(leader.progress().len(), 2);

    output.clear();
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(0),
                success: true,
                match_index: LogIndex::from_raw(1),
                conflict_term: None,
                conflict_index: None,
                from: stranger,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Leader);
    assert!(output.messages.is_empty());

    output.clear();
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: stranger,
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Leader);
    assert!(output.messages.is_empty());
}

#[test]
fn test_leader_steps_down_on_higher_term_heartbeat() {
    let (mut leader, _peer, mut output) = become_test_leader();
    let peer = NodeId::new(2).expect("peer");

    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(7),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.term(), Term::from_raw(7));
}

fn count_ticks_to_leave_follower(mut core: RaftCore) -> u64 {
    let mut output = Output::new();
    for tick in 1..=50u64 {
        core.step(Input::Tick, &mut output);
        if core.role() != Role::Follower {
            return tick;
        }
        output.clear();
    }
    51
}

#[test]
fn test_election_timing_is_exact_per_seed() {
    let leader_5 = make_core(1, &[2, 3]);
    assert_eq!(count_ticks_to_leave_follower(leader_5), 7);

    let mut single = base_config(1, &[]);
    single.election_timeout_ticks = 1;
    let solo = make_core_with(single, 7);
    assert_eq!(count_ticks_to_leave_follower(solo), 1);
}

#[test]
fn test_heartbeat_count_over_ticks_is_exact() {
    let (mut leader, _peer, mut output) = elect_via_helper(610);
    output.clear();
    for _ in 0..6 {
        leader.step(Input::Tick, &mut output);
    }
    let heartbeats = output
        .messages
        .iter()
        .filter(|m| matches!(&m.message, Message::HeartbeatRequest { .. }))
        .count();
    assert_eq!(heartbeats, 6);
}

fn check_quorum_config(node_id: u64, peers: &[u64]) -> RaftConfig {
    let mut config = base_config(node_id, peers);
    config.check_quorum = true;
    config
}

#[test]
fn test_check_quorum_steps_down_at_exact_tick() {
    let mut leader = {
        let config = check_quorum_config(1, &[2, 3]);
        let mut core = make_core_with(config, 611);
        let mut out = Output::new();
        for _ in 0..10 {
            core.step(Input::Tick, &mut out);
        }
        let p = NodeId::new(2).expect("peer");
        out.clear();
        core.step(
            Input::Recv {
                from: p,
                message: grant_prevote(p),
            },
            &mut out,
        );
        out.clear();
        core.step(
            Input::Recv {
                from: p,
                message: grant_vote(p),
            },
            &mut out,
        );
        assert_eq!(core.role(), Role::Leader);
        core
    };
    let mut output = Output::new();
    for _ in 0..4 {
        leader.step(Input::Tick, &mut output);
        assert_eq!(leader.role(), Role::Leader);
        output.clear();
    }
    leader.step(Input::Tick, &mut output);
    assert_eq!(leader.role(), Role::Follower);
    assert!(
        output
            .persist_ops
            .iter()
            .all(|op| !matches!(op, PersistOp::HardState { .. })),
        "same-term stepdown must not persist hard state"
    );
}

#[test]
fn test_check_quorum_survives_with_one_peer_contact() {
    let config = check_quorum_config(1, &[2, 3]);
    let mut core = make_core_with(config, 612);
    let mut output = Output::new();
    for _ in 0..10 {
        core.step(Input::Tick, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: grant_prevote(peer),
        },
        &mut output,
    );
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: grant_vote(peer),
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::Leader);
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(1),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    for _ in 0..5 {
        output.clear();
        core.step(Input::Tick, &mut output);
    }
    assert_eq!(core.role(), Role::Leader);
}

#[test]
fn test_prevote_denied_leaves_precandidate() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..10 {
        core.step(Input::Tick, &mut output);
    }
    assert_eq!(core.role(), Role::PreCandidate);
    let peer = NodeId::new(2).expect("peer");
    output.clear();
    core.step(
        Input::Recv {
            from: peer,
            message: Message::PreVoteResponse {
                term: Term::from_raw(0),
                vote_granted: false,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(core.role(), Role::PreCandidate);
    assert_eq!(core.term(), Term::from_raw(0));
}

#[test]
fn test_request_vote_granted_when_eligible() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    let challenger = NodeId::new(2).expect("challenger");
    core.step(
        Input::Recv {
            from: challenger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(0),
                candidate_id: challenger,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    for token in persisted_tokens(&output) {
        output.clear();
        core.step(Input::Persisted { token }, &mut output);
    }
    let granted = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::RequestVoteResponse {
                vote_granted: true,
                ..
            }
        )
    });
    assert!(granted);
    assert_eq!(core.voted_for(), Some(challenger));
}

#[test]
fn test_send_append_advances_only_past_last_sent() {
    let (mut leader, peer, mut output) = elect_via_helper(620);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
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
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"a"),
        },
        &mut output,
    );
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"b"),
        },
        &mut output,
    );
    let mut prevs = Vec::new();
    for m in &output.messages {
        if let Message::AppendEntriesRequest {
            prev_log_index,
            entries,
            ..
        } = &m.message
        {
            prevs.push((*prev_log_index, entries.len()));
        }
    }
    assert_eq!(prevs, vec![(LogIndex::from_raw(2), 1)]);
}

#[test]
fn test_broadcast_reaches_all_seventeen_peers() {
    let peers: Vec<u64> = (2..=18).collect();
    let mut leader = make_core(1, &peers);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    for peer_raw in 2..=18u64 {
        let peer = NodeId::new(peer_raw).expect("peer");
        output.clear();
        leader.step(
            Input::Recv {
                from: peer,
                message: Message::AppendEntriesResponse {
                    term: Term::from_raw(0),
                    success: true,
                    match_index: LogIndex::from_raw(1),
                    conflict_term: None,
                    conflict_index: None,
                    from: peer,
                },
            },
            &mut output,
        );
    }
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"wide"),
        },
        &mut output,
    );
    let count = output
        .messages
        .iter()
        .filter(|m| matches!(&m.message, Message::AppendEntriesRequest { .. }))
        .count();
    assert_eq!(count, 17);
}

#[test]
fn test_higher_term_append_transitions_follower() {
    let mut core = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");
    core.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(5),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: Vec::new(),
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.term(), Term::from_raw(5));
    assert_eq!(core.role(), Role::Follower);
}

#[test]
fn test_candidate_steps_down_on_same_term_append() {
    let mut candidate = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..10 {
        candidate.step(Input::Tick, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
    output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: grant_prevote(peer),
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Candidate);
    output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(1),
                leader_id: peer,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: Vec::new(),
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Follower);
}

#[test]
fn test_duplicate_entry_does_not_truncate() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");
    core_step_append_dummy(&mut follower, &mut output, leader);
    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(1),
                    Term::from_raw(0),
                    Bytes::from_static(b"d"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    assert!(
        output
            .persist_ops
            .iter()
            .all(|op| !matches!(op, PersistOp::TruncateAndAppend { .. }))
    );
    let immediate = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::AppendEntriesResponse { success: true, .. }
        )
    });
    assert!(immediate);
}

#[test]
fn test_wrong_term_success_response_ignored() {
    let (mut leader, peer, mut output) = elect_via_helper(621);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"held"),
        },
        &mut output,
    );
    let entry_token = persisted_tokens(&output)
        .into_iter()
        .next()
        .expect("entry token");
    output.clear();
    leader.step(Input::Persisted { token: entry_token }, &mut output);
    assert_eq!(leader.commit_index(), LogIndex::from_raw(0));

    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(0),
                success: true,
                match_index: LogIndex::from_raw(2),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    assert_eq!(leader.commit_index(), LogIndex::from_raw(0));
}

#[test]
fn test_transfer_catch_up_sends_timeout_on_completion() {
    let (mut leader, peer, mut output) = elect_via_helper(622);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    output.clear();
    leader.step(Input::TransferLeader { to: peer }, &mut output);
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::TimeoutNow { .. }))
    );
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(0),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::TimeoutNow { .. })),
        "lagging match must not trigger transfer"
    );
}

#[test]
fn test_same_term_heartbeat_preserves_vote() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    let candidate = NodeId::new(2).expect("candidate");
    core.step(
        Input::Recv {
            from: candidate,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(0),
                candidate_id: candidate,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.voted_for(), Some(candidate));
    output.clear();
    let leader = NodeId::new(3).expect("leader");
    core.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(core.voted_for(), Some(candidate));
}

#[test]
fn test_heartbeat_resets_election_timer() {
    let mut core = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..4 {
        core.step(Input::Tick, &mut output);
        output.clear();
    }
    let leader = NodeId::new(2).expect("leader");
    core.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(0),
                leader_id: leader,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    for _ in 0..4 {
        output.clear();
        core.step(Input::Tick, &mut output);
        assert_eq!(core.role(), Role::Follower);
    }
}

#[test]
fn test_candidate_steps_down_on_same_term_heartbeat() {
    let mut candidate = make_core(1, &[2, 3]);
    let mut output = Output::new();
    for _ in 0..10 {
        candidate.step(Input::Tick, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
    output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: grant_prevote(peer),
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Candidate);
    output.clear();
    candidate.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(1),
                leader_id: peer,
                commit_index: LogIndex::from_raw(0),
                read_context: None,
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Follower);
}

#[test]
fn test_heartbeat_does_not_advance_commit_on_term_mismatch() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let leader = NodeId::new(1).expect("leader");
    core_step_append_dummy(&mut follower, &mut output, leader);
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(2),
                leader_id: leader,
                prev_log_index: LogIndex::from_raw(1),
                prev_log_term: Term::from_raw(0),
                entries: vec![Entry::normal(
                    LogIndex::from_raw(2),
                    Term::from_raw(2),
                    Bytes::from_static(b"b"),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    for token in persisted_tokens(&output) {
        output.clear();
        follower.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(
        follower.log().term_at(LogIndex::from_raw(2)),
        Some(Term::from_raw(2))
    );

    output.clear();
    follower.step(
        Input::Recv {
            from: leader,
            message: Message::HeartbeatRequest {
                term: Term::from_raw(3),
                leader_id: leader,
                commit_index: LogIndex::from_raw(5),
                read_context: None,
            },
        },
        &mut output,
    );
    for token in persisted_tokens(&output) {
        output.clear();
        follower.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(follower.commit_index(), LogIndex::from_raw(0));
}

#[test]
fn test_heartbeat_response_drives_replication_only_when_behind() {
    let mut config = base_config(1, &[2]);
    config.max_inflight_messages = 1;
    let mut leader = make_core_with(config, 630);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    let peer = NodeId::new(2).expect("peer");
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(0),
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
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"a"),
        },
        &mut output,
    );
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::AppendEntriesRequest { .. })),
        "in-flight window full: no resend"
    );

    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(0),
                success: true,
                match_index: LogIndex::from_raw(2),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::AppendEntriesRequest { .. })),
        "caught-up peer: no resend"
    );
}

#[test]
fn test_transfer_on_follower_is_ignored() {
    let mut follower = make_core(2, &[1]);
    let mut output = Output::new();
    let peer = NodeId::new(1).expect("peer");
    follower.step(Input::TransferLeader { to: peer }, &mut output);
    assert!(output.messages.is_empty());
}

#[test]
fn test_log_helpers_exact_values() {
    let log = RaftLog::new(vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(1),
            Bytes::from_static(b"a"),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(2),
            Bytes::from_static(b"b"),
        ),
    ]);
    assert_eq!(log.len(), 2);
    assert!(!log.is_empty());
    assert_eq!(log.last_term(), Term::from_raw(2));
    assert_eq!(log.term_at(LogIndex::from_raw(1)), Some(Term::from_raw(1)));
    assert_eq!(log.term_at(LogIndex::from_raw(0)), Some(Term::from_raw(0)));
    assert_eq!(log.entry_at(LogIndex::from_raw(0)), None);
    assert_eq!(
        log.entry_at(LogIndex::from_raw(1)).map(|e| e.index),
        Some(LogIndex::from_raw(1))
    );
    assert_eq!(log.entry_at(LogIndex::from_raw(3)), None);
    assert_eq!(
        log.find_first_index_of_term(Term::from_raw(2)),
        Some(LogIndex::from_raw(2))
    );
    assert_eq!(log.entries().len(), 2);
    let digest_before = log.compute_digest();
    assert_ne!(digest_before, RaftLog::default().compute_digest());
    assert_ne!(digest_before, [1u8; 32]);

    let three = RaftLog::new(vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(0),
            Bytes::from(vec![]),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(0),
            Bytes::from(vec![]),
        ),
        Entry::normal(
            LogIndex::from_raw(3),
            Term::from_raw(0),
            Bytes::from(vec![]),
        ),
    ]);
    assert_eq!(three.slice_from(LogIndex::from_raw(1), 64).len(), 2);
    assert_eq!(three.slice_from(LogIndex::from_raw(1), 96).len(), 3);

    let two = RaftLog::new(vec![
        Entry::normal(
            LogIndex::from_raw(1),
            Term::from_raw(0),
            Bytes::from(vec![0u8; 2]),
        ),
        Entry::normal(
            LogIndex::from_raw(2),
            Term::from_raw(0),
            Bytes::from(vec![0u8; 2]),
        ),
        Entry::normal(
            LogIndex::from_raw(3),
            Term::from_raw(0),
            Bytes::from(vec![0u8; 2]),
        ),
    ]);
    assert_eq!(two.slice_from(LogIndex::from_raw(1), 68).len(), 2);
}

#[test]
fn test_cluster_and_quorum_sizes() {
    let config = base_config(1, &[2, 3]);
    assert_eq!(config.cluster_size(), 3);
    assert_eq!(config.quorum_size(), 2);
    let solo = base_config(1, &[]);
    assert_eq!(solo.cluster_size(), 1);
    assert_eq!(solo.quorum_size(), 1);
}

#[test]
fn test_is_log_up_to_date_matrix() {
    let challenger = NodeId::new(2).expect("challenger");

    let mut higher = make_core(1, &[2]);
    let mut output = Output::new();
    core_step_append_dummy(&mut higher, &mut output, challenger);
    output.clear();
    higher.step(
        Input::Recv {
            from: challenger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(5),
                candidate_id: challenger,
                last_log_index: LogIndex::from_raw(1),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    for token in persisted_tokens(&output) {
        output.clear();
        higher.step(Input::Persisted { token }, &mut output);
    }
    let granted_high = output.messages.iter().any(|m| {
        matches!(
            &m.message,
            Message::RequestVoteResponse {
                vote_granted: true,
                ..
            }
        )
    });
    assert!(granted_high);

    let log_entries = vec![Entry::normal(
        LogIndex::from_raw(1),
        Term::from_raw(5),
        Bytes::from_static(b"e"),
    )];
    for (candidate_term, candidate_index, expect_grant) in
        [(5u64, 0u64, false), (5u64, 1u64, true), (2u64, 0u64, false)]
    {
        let mut core = make_core_with_log(1, &[2], Term::from_raw(5), log_entries.clone());
        let mut out = Output::new();
        core.step(
            Input::Recv {
                from: challenger,
                message: Message::RequestVoteRequest {
                    term: Term::from_raw(candidate_term),
                    candidate_id: challenger,
                    last_log_index: LogIndex::from_raw(candidate_index),
                    last_log_term: Term::from_raw(candidate_term),
                },
            },
            &mut out,
        );
        for token in persisted_tokens(&out) {
            out.clear();
            core.step(Input::Persisted { token }, &mut out);
        }
        let granted = out.messages.iter().any(|m| {
            matches!(
                &m.message,
                Message::RequestVoteResponse {
                    vote_granted: true,
                    ..
                }
            )
        });
        assert_eq!(
            granted, expect_grant,
            "term {candidate_term} index {candidate_index}"
        );
    }
}

fn make_core_with_log(node_id: u64, peers: &[u64], term: Term, entries: Vec<Entry>) -> RaftCore {
    let mut peer_set = BTreeSet::new();
    for &peer in peers {
        peer_set.insert(NodeId::new(peer).expect("peer"));
    }
    let config = RaftConfig {
        node_id: NodeId::new(node_id).expect("node"),
        peers: peer_set,
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: false,
    };
    let recovered = RecoveredState {
        current_term: term,
        voted_for: None,
        commit_index: LogIndex::from_raw(0),
        entries,
    };
    RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(node_id))
}

#[test]
fn test_mixed_quorum_commits_despite_lagging_minority() {
    let peers: Vec<u64> = (2..=17).collect();
    let mut leader = make_core(1, &peers);
    let mut output = Output::new();
    leader.testing_become_leader(&mut output);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(leader.commit_index(), LogIndex::from_raw(0));

    for peer_raw in 2..=9u64 {
        let peer = NodeId::new(peer_raw).expect("peer");
        output.clear();
        leader.step(
            Input::Recv {
                from: peer,
                message: Message::AppendEntriesResponse {
                    term: Term::from_raw(0),
                    success: true,
                    match_index: LogIndex::from_raw(0),
                    conflict_term: None,
                    conflict_index: None,
                    from: peer,
                },
            },
            &mut output,
        );
    }
    for peer_raw in 10..=17u64 {
        let peer = NodeId::new(peer_raw).expect("peer");
        output.clear();
        leader.step(
            Input::Recv {
                from: peer,
                message: Message::AppendEntriesResponse {
                    term: Term::from_raw(0),
                    success: true,
                    match_index: LogIndex::from_raw(1),
                    conflict_term: None,
                    conflict_index: None,
                    from: peer,
                },
            },
            &mut output,
        );
    }

    output.clear();
    leader.step(
        Input::Propose {
            payload: Bytes::from_static(b"mixed"),
        },
        &mut output,
    );
    let entry_token = persisted_tokens(&output)
        .into_iter()
        .next()
        .expect("entry token");
    output.clear();
    leader.step(Input::Persisted { token: entry_token }, &mut output);
    assert_eq!(leader.commit_index(), LogIndex::from_raw(1));
}

#[test]
fn test_same_term_append_preserves_vote() {
    let mut core = make_core(1, &[2]);
    let mut output = Output::new();
    let candidate = NodeId::new(2).expect("candidate");
    core.step(
        Input::Recv {
            from: candidate,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(3),
                candidate_id: candidate,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.voted_for(), Some(candidate));
    for token in persisted_tokens(&output) {
        output.clear();
        core.step(Input::Persisted { token }, &mut output);
    }
    output.clear();
    core.step(
        Input::Recv {
            from: candidate,
            message: Message::AppendEntriesRequest {
                term: Term::from_raw(3),
                leader_id: candidate,
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: Vec::new(),
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.voted_for(), Some(candidate));
}

#[test]
fn test_stale_heartbeat_response_ignored_by_leader() {
    let (mut leader, peer, mut output) = elect_via_helper(700);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    assert_eq!(leader.term(), Term::from_raw(1));
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(output.messages.is_empty());
    let stranger = NodeId::new(77).expect("stranger");
    output.clear();
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(0),
                from: stranger,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(output.messages.is_empty());
}

#[test]
fn test_ahead_follower_heartbeat_sends_nothing() {
    let (mut leader, peer, mut output) = elect_via_helper(701);
    for token in persisted_tokens(&output) {
        output.clear();
        leader.step(Input::Persisted { token }, &mut output);
    }
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: LogIndex::from_raw(5),
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::HeartbeatResponse {
                term: Term::from_raw(1),
                from: peer,
                read_context: None,
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .all(|m| !matches!(&m.message, Message::AppendEntriesRequest { .. }))
    );
}

#[test]
fn test_zero_timeout_elects_immediately() {
    let mut config = base_config(1, &[2]);
    config.election_timeout_ticks = 0;
    let mut core = make_core_with(config, 44);
    let mut output = Output::new();
    core.step(Input::Tick, &mut output);
    assert_ne!(core.role(), Role::Follower);
}

#[test]
fn test_send_append_requires_all_preconditions() {
    let mut probing = PeerProgress::new(LogIndex::from_raw(5), 4);
    probing.on_send(3, LogIndex::from_raw(7));
    assert_eq!(probing.next_index, LogIndex::from_raw(5));

    let mut replicating = PeerProgress::new(LogIndex::from_raw(2), 4);
    replicating.state = ProgressState::Replicate;
    replicating.on_send(0, LogIndex::from_raw(9));
    assert_eq!(replicating.next_index, LogIndex::from_raw(2));
}
