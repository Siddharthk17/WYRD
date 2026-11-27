#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::too_many_lines
)]

use anvil_raft::{
    ConfigReject, Input, MembershipEvent, Message, Output, PersistOp, RaftConfig, RaftCore,
    RecoveredState, Role,
};
use anvil_types::{ClusterConfig, ClusterId, EntryKind, LogIndex, NodeId, Term, config_entry};
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("valid node id")
}

fn set(nodes: &[u64]) -> BTreeSet<NodeId> {
    nodes.iter().map(|n| node_id(*n)).collect()
}

fn make_config(node: u64, peers: &[u64], learners: &[u64]) -> RaftConfig {
    RaftConfig {
        cluster_id: ClusterId::from_raw(1),
        learners: set(learners),
        node_id: node_id(node),
        peers: set(peers),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    }
}

fn make_core(node: u64, peers: &[u64], learners: &[u64], seed: u64) -> RaftCore {
    RaftCore::new(
        make_config(node, peers, learners),
        RecoveredState::default(),
        ChaCha8Rng::seed_from_u64(seed),
    )
}

/// Runs ticks until the node starts campaigning.
fn tick_until_campaign(core: &mut RaftCore, output: &mut Output) {
    for _ in 0..20 {
        if matches!(core.role(), Role::PreCandidate | Role::Candidate) {
            return;
        }
        core.step(Input::Tick, output);
    }
}

/// Completes every pending persist operation.
fn ack_persists(core: &mut RaftCore, output: &mut Output) {
    for _ in 0..32 {
        let tokens: Vec<_> = output.persist_ops.iter().map(PersistOp::token).collect();
        if tokens.is_empty() {
            return;
        }
        for token in tokens {
            output.clear();
            core.step(Input::Persisted { token }, output);
        }
    }
}

/// Elects node 1 leader of {1, 2, 3} and commits its inaugural no-op.
fn elect_and_settle() -> (RaftCore, Output) {
    let mut leader = make_core(1, &[2, 3], &[], 7);
    let mut output = Output::new();
    tick_until_campaign(&mut leader, &mut output);
    assert_eq!(leader.role(), Role::PreCandidate);

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
    assert_eq!(leader.role(), Role::Candidate);
    ack_persists(&mut leader, &mut output);
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

    // Commit the inaugural no-op: persist it, then count peer 2's match.
    ack_persists(&mut leader, &mut output);
    let last = leader.last_log_index();
    output.clear();
    leader.step(
        Input::Recv {
            from: peer,
            message: Message::AppendEntriesResponse {
                term: leader.term(),
                success: true,
                match_index: last,
                conflict_term: None,
                conflict_index: None,
                from: peer,
            },
        },
        &mut output,
    );
    assert!(
        leader.commit_index() >= last,
        "no-op must commit on a two-node quorum"
    );
    output.clear();
    (leader, output)
}

/// Acks `index` from every listed peer.
fn ack_from(core: &mut RaftCore, output: &mut Output, peers: &[u64], index: LogIndex) {
    for peer in peers {
        let from = node_id(*peer);
        core.step(
            Input::Recv {
                from,
                message: Message::AppendEntriesResponse {
                    term: core.term(),
                    success: true,
                    match_index: index,
                    conflict_term: None,
                    conflict_index: None,
                    from,
                },
            },
            output,
        );
    }
}

#[test]
fn test_config_change_refused_before_current_term_commit() {
    let mut fresh = make_core(1, &[2, 3], &[], 7);
    let mut output = Output::new();
    tick_until_campaign(&mut fresh, &mut output);
    let peer = node_id(2);
    output.clear();
    fresh.step(
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
    ack_persists(&mut fresh, &mut output);
    output.clear();
    fresh.step(
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
    assert_eq!(fresh.role(), Role::Leader);

    output.clear();
    fresh.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3), node_id(4)],
            learners: vec![],
        },
        &mut output,
    );
    assert!(
        output.membership_events.iter().any(|event| matches!(
            event,
            MembershipEvent::Rejected {
                reason: ConfigReject::NoCurrentTermCommit
            }
        )),
        "config change before a current-term commit must be refused"
    );
    assert!(!fresh.in_joint());
}

#[test]
fn test_config_change_refused_when_not_leader() {
    let mut follower = make_core(2, &[1, 3], &[], 9);
    let mut output = Output::new();
    follower.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2)],
            learners: vec![],
        },
        &mut output,
    );
    assert!(output.membership_events.iter().any(|event| matches!(
        event,
        MembershipEvent::Rejected {
            reason: ConfigReject::NotLeader
        }
    )));
}

#[test]
fn test_config_change_refused_when_invalid() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![],
            learners: vec![],
        },
        &mut output,
    );
    assert!(output.membership_events.iter().any(|event| matches!(
        event,
        MembershipEvent::Rejected {
            reason: ConfigReject::InvalidConfig(_)
        }
    )));
    assert!(!leader.in_joint());
}

#[test]
fn test_enter_joint_takes_effect_on_append() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3), node_id(4)],
            learners: vec![node_id(5)],
        },
        &mut output,
    );
    assert!(leader.in_joint());
    assert!(
        output
            .membership_events
            .iter()
            .any(|event| matches!(event, MembershipEvent::Proposed { .. }))
    );
    let voters = leader.membership_voters();
    assert!(voters.contains(&node_id(4)));
    assert!(leader.membership_learners().contains(&node_id(5)));
    // The new node immediately becomes a replication target.
    assert!(leader.progress().contains_key(&node_id(4)));
    assert!(leader.progress().contains_key(&node_id(5)));

    // A second change while joint is refused.
    output.clear();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2)],
            learners: vec![],
        },
        &mut output,
    );
    assert!(output.membership_events.iter().any(|event| matches!(
        event,
        MembershipEvent::Rejected {
            reason: ConfigReject::ConfigInProgress
        }
    )));
}

#[test]
fn test_joint_commit_needs_both_majorities() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3), node_id(4), node_id(5)],
            learners: vec![],
        },
        &mut output,
    );
    let enter = leader.last_log_index();
    assert!(leader.in_joint());
    ack_persists(&mut leader, &mut output);

    // Old majority alone (self plus node 2) must not commit under AND.
    let before = leader.commit_index();
    output.clear();
    ack_from(&mut leader, &mut output, &[2], enter);
    assert_eq!(
        leader.commit_index(),
        before,
        "old majority without new majority must not advance commit"
    );

    // New majority (self plus nodes 2 and 4) completes both sides.
    output.clear();
    ack_from(&mut leader, &mut output, &[4], enter);
    assert!(
        leader.commit_index() >= enter,
        "both majorities together commit the joint entry"
    );
}

#[test]
fn test_auto_leave_and_joint_clear_on_commit() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3), node_id(4)],
            learners: vec![],
        },
        &mut output,
    );
    let enter = leader.last_log_index();
    ack_persists(&mut leader, &mut output);
    output.clear();
    ack_from(&mut leader, &mut output, &[2, 4], enter);
    assert!(leader.commit_index() >= enter);

    // The leader leaves the joint config automatically once it commits.
    let leave = leader.last_log_index();
    assert!(leave > enter);
    assert!(
        output
            .membership_events
            .iter()
            .any(|event| matches!(event, MembershipEvent::AutoLeave { .. }))
    );
    let kinds: Vec<EntryKind> = leader
        .log()
        .entries()
        .iter()
        .filter(|entry| entry.index >= enter)
        .map(|entry| entry.kind)
        .collect();
    assert_eq!(kinds, vec![EntryKind::EnterJoint, EntryKind::LeaveJoint]);

    // Committing LeaveJoint clears the joint state.
    ack_persists(&mut leader, &mut output);
    output.clear();
    ack_from(&mut leader, &mut output, &[2, 4], leave);
    assert!(!leader.in_joint());
    assert_eq!(leader.membership_voters().len(), 4);
}

#[test]
fn test_removed_leader_steps_down_and_hands_off() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(2), node_id(3)],
            learners: vec![],
        },
        &mut output,
    );
    let enter = leader.last_log_index();
    ack_persists(&mut leader, &mut output);
    // Old majority (self plus node 2) and new majority (nodes 2 and 3).
    output.clear();
    ack_from(&mut leader, &mut output, &[2, 3], enter);
    assert!(leader.commit_index() >= enter);
    let leave = leader.last_log_index();
    ack_persists(&mut leader, &mut output);
    output.clear();
    ack_from(&mut leader, &mut output, &[2, 3], leave);

    assert_eq!(leader.role(), Role::Follower);
    assert!(
        output
            .membership_events
            .contains(&MembershipEvent::RemovedSteppedDown)
    );
    // Handoff goes to a remaining voter.
    let handoff = output.messages.iter().any(|msg| {
        matches!(&msg.message, Message::TimeoutNow { .. })
            && (msg.to == node_id(2) || msg.to == node_id(3))
    });
    assert!(handoff, "removed leader must hand leadership off");
}

#[test]
fn test_learner_never_campaigns_or_votes() {
    let mut learner = make_core(4, &[1, 2, 3], &[4], 11);
    let mut output = Output::new();
    for _ in 0..30 {
        learner.step(Input::Tick, &mut output);
    }
    assert_eq!(learner.role(), Role::Follower);
    assert!(output.messages.is_empty());

    // A learner answers vote requests with false.
    output.clear();
    learner.step(
        Input::Recv {
            from: node_id(1),
            message: Message::RequestVoteRequest {
                term: Term::from_raw(1),
                candidate_id: node_id(1),
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    let granted = output.messages.iter().any(|msg| {
        matches!(
            &msg.message,
            Message::RequestVoteResponse {
                vote_granted: true,
                ..
            }
        )
    });
    assert!(!granted, "learner must never grant a vote");
}

#[test]
fn test_unknown_sender_messages_dropped() {
    let (mut leader, mut output) = elect_and_settle();
    let stranger = node_id(99);
    let before_term = leader.term();
    output.clear();
    leader.step(
        Input::Recv {
            from: stranger,
            message: Message::RequestVoteRequest {
                term: Term::from_raw(50),
                candidate_id: stranger,
                last_log_index: LogIndex::from_raw(1),
                last_log_term: Term::from_raw(1),
            },
        },
        &mut output,
    );
    // No stepdown, no vote, no persist: strangers cannot disrupt.
    assert_eq!(leader.term(), before_term);
    assert!(output.messages.is_empty());
    assert!(output.persist_ops.is_empty());
    assert!(
        output.sometimes_hits.contains(&"raft.unknown.sender"),
        "drops must be visible to the farm coverage gate"
    );
}

#[test]
fn test_removed_node_does_not_campaign() {
    // A node the committed config removed never campaigns again.
    let config = ClusterConfig::new(vec![node_id(1), node_id(2)], vec![]).expect("valid");
    let entry = config_entry(
        EntryKind::LeaveJoint,
        LogIndex::from_raw(1),
        Term::from_raw(2),
        &config,
    );
    let recovered = RecoveredState {
        current_term: Term::from_raw(2),
        voted_for: None,
        commit_index: LogIndex::from_raw(1),
        entries: vec![entry],
    };
    let mut removed = RaftCore::new(
        make_config(3, &[1, 2], &[]),
        recovered,
        ChaCha8Rng::seed_from_u64(13),
    );
    assert!(!removed.in_joint());
    let mut out = Output::new();
    for _ in 0..30 {
        removed.step(Input::Tick, &mut out);
    }
    assert_eq!(removed.role(), Role::Follower);
    assert!(out.messages.is_empty());
    // Even an explicit transfer order cannot make it campaign.
    out.clear();
    removed.step(
        Input::Recv {
            from: node_id(1),
            message: Message::TimeoutNow {
                term: Term::from_raw(2),
                leader_id: node_id(1),
            },
        },
        &mut out,
    );
    assert_eq!(removed.role(), Role::Follower);
}

#[test]
fn test_membership_survives_recovery() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3), node_id(4)],
            learners: vec![],
        },
        &mut output,
    );
    let entries = leader.log().entries().to_vec();
    let commit = leader.commit_index();

    let recovered = RecoveredState {
        current_term: leader.term(),
        voted_for: None,
        commit_index: commit,
        entries,
    };
    let config = make_config(2, &[1, 3], &[]);
    let follower = RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(21));
    assert!(follower.in_joint());
    assert!(follower.membership_voters().contains(&node_id(4)));
}

#[test]
fn test_transfer_to_learner_refused() {
    let (mut leader, mut output) = elect_and_settle();
    leader.step(
        Input::ProposeConfig {
            voters: vec![node_id(1), node_id(2), node_id(3)],
            learners: vec![node_id(4)],
        },
        &mut output,
    );
    ack_persists(&mut leader, &mut output);
    output.clear();
    leader.step(Input::TransferLeader { to: node_id(4) }, &mut output);
    let timeout_to_learner = output
        .messages
        .iter()
        .any(|msg| msg.to == node_id(4) && matches!(&msg.message, Message::TimeoutNow { .. }));
    assert!(
        !timeout_to_learner,
        "leadership must never transfer to a learner"
    );
}

#[test]
fn test_joint_election_needs_both_majorities() {
    // Node 1 campaigns inside joint old {1,2,3} -> new {1,2,3,4}.
    let joint = ClusterConfig::new(vec![node_id(1), node_id(2), node_id(3), node_id(4)], vec![])
        .expect("valid");
    let noop = anvil_types::Entry::noop(LogIndex::from_raw(1), Term::from_raw(1));
    let enter = config_entry(
        EntryKind::EnterJoint,
        LogIndex::from_raw(2),
        Term::from_raw(1),
        &joint,
    );
    let recovered = RecoveredState {
        current_term: Term::from_raw(1),
        voted_for: None,
        commit_index: LogIndex::from_raw(1),
        entries: vec![noop, enter],
    };
    let mut candidate = RaftCore::new(
        make_config(1, &[2, 3], &[]),
        recovered,
        ChaCha8Rng::seed_from_u64(31),
    );
    assert!(candidate.in_joint());
    let mut output = Output::new();
    tick_until_campaign(&mut candidate, &mut output);
    assert_eq!(candidate.role(), Role::PreCandidate);

    // Old majority alone (self plus node 2) must not end pre-vote.
    output.clear();
    candidate.step(
        Input::Recv {
            from: node_id(2),
            message: Message::PreVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: node_id(2),
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::PreCandidate);

    // New majority too (plus node 3) advances to a real election.
    output.clear();
    candidate.step(
        Input::Recv {
            from: node_id(3),
            message: Message::PreVoteResponse {
                term: Term::from_raw(1),
                vote_granted: true,
                from: node_id(3),
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Candidate);

    // Election votes follow the same AND rule: self plus node 2 is short
    // of the new majority of three.
    ack_persists(&mut candidate, &mut output);
    output.clear();
    candidate.step(
        Input::Recv {
            from: node_id(2),
            message: Message::RequestVoteResponse {
                term: Term::from_raw(2),
                vote_granted: true,
                from: node_id(2),
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Candidate);
    output.clear();
    candidate.step(
        Input::Recv {
            from: node_id(3),
            message: Message::RequestVoteResponse {
                term: Term::from_raw(2),
                vote_granted: true,
                from: node_id(3),
            },
        },
        &mut output,
    );
    assert_eq!(candidate.role(), Role::Leader);
}

#[test]
fn test_stale_snapshot_restore_ignored() {
    let (mut leader, mut output) = elect_and_settle();
    let before = leader.membership_voters();
    // No snapshot was installed, so any restore is stale and ignored.
    leader.step(
        Input::RestoreMembership {
            base: LogIndex::from_raw(41),
            membership: anvil_types::MembershipSnapshot {
                voters: vec![node_id(1)],
                learners: vec![],
                joint: None,
            },
        },
        &mut output,
    );
    assert_eq!(leader.membership_voters(), before);
    assert!(!output.sometimes_hits.contains(&"raft.config.restore"));
}

#[test]
fn test_repair_node_does_not_vote_or_campaign() {
    let (mut leader, mut output) = elect_and_settle();
    assert_eq!(leader.role(), Role::Leader);
    output.clear();
    leader.step(
        Input::SetNeedsRepair {
            from_index: LogIndex::from_raw(1),
        },
        &mut output,
    );
    // A flagged leader steps down immediately and hands off.
    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.repair_from(), Some(LogIndex::from_raw(1)));
    assert!(output.sometimes_hits.contains(&"raft.repair.enter"));

    // No campaigning while flagged, even after many timeouts.
    output.clear();
    for _ in 0..30 {
        leader.step(Input::Tick, &mut output);
    }
    assert_eq!(leader.role(), Role::Follower);
    assert!(output.messages.is_empty());

    // No vote grants while flagged.
    output.clear();
    leader.step(
        Input::Recv {
            from: node_id(2),
            message: Message::RequestVoteRequest {
                term: Term::from_raw(2),
                candidate_id: node_id(2),
                last_log_index: LogIndex::from_raw(1),
                last_log_term: Term::from_raw(1),
            },
        },
        &mut output,
    );
    let granted = output.messages.iter().any(|msg| {
        matches!(
            &msg.message,
            Message::RequestVoteResponse {
                vote_granted: true,
                ..
            }
        )
    });
    assert!(!granted, "repair node must never grant a vote");
}

#[test]
fn test_repair_node_answers_append_as_last_good() {
    let (mut leader, mut output) = elect_and_settle();
    // Commit one client entry so the log extends past the repair base.
    ack_persists(&mut leader, &mut output);
    output.clear();

    // Flag from index 1: every ack reports match 0 (nothing durable).
    leader.step(
        Input::SetNeedsRepair {
            from_index: LogIndex::from_raw(1),
        },
        &mut output,
    );
    // Follower behavior: step down first (tested above), then handle an
    // append as a flagged follower and check the clamped ack.
    let mut follower = make_core(2, &[1, 3], &[], 77);
    let mut follower_out = Output::new();
    follower.step(
        Input::Recv {
            from: node_id(1),
            message: Message::AppendEntriesRequest {
                term: leader.term(),
                leader_id: node_id(1),
                prev_log_index: LogIndex::from_raw(0),
                prev_log_term: Term::from_raw(0),
                entries: vec![anvil_types::Entry::noop(
                    LogIndex::from_raw(1),
                    leader.term(),
                )],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut follower_out,
    );
    ack_persists(&mut follower, &mut follower_out);
    follower_out.clear();
    follower.step(
        Input::SetNeedsRepair {
            from_index: LogIndex::from_raw(2),
        },
        &mut follower_out,
    );
    follower_out.clear();
    follower.step(
        Input::Recv {
            from: node_id(1),
            message: Message::AppendEntriesRequest {
                term: leader.term(),
                leader_id: node_id(1),
                prev_log_index: LogIndex::from_raw(1),
                prev_log_term: leader.term(),
                entries: vec![],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut follower_out,
    );
    let acked = follower_out
        .messages
        .iter()
        .find_map(|msg| match &msg.message {
            Message::AppendEntriesResponse { match_index, .. } => Some(*match_index),
            _ => None,
        })
        .expect("append answered");
    assert_eq!(
        acked,
        LogIndex::from_raw(1),
        "repair node acks only the last good index"
    );

    // Clearing resumes full matching.
    follower_out.clear();
    follower.step(Input::ClearNeedsRepair, &mut follower_out);
    assert!(follower.repair_from().is_none());
    assert!(follower_out.sometimes_hits.contains(&"raft.repair.cleared"));
    follower_out.clear();
    follower.step(
        Input::Recv {
            from: node_id(1),
            message: Message::AppendEntriesRequest {
                term: leader.term(),
                leader_id: node_id(1),
                prev_log_index: LogIndex::from_raw(1),
                prev_log_term: leader.term(),
                entries: vec![],
                leader_commit: LogIndex::from_raw(0),
            },
        },
        &mut follower_out,
    );
    let acked = follower_out
        .messages
        .iter()
        .find_map(|msg| match &msg.message {
            Message::AppendEntriesResponse { match_index, .. } => Some(*match_index),
            _ => None,
        })
        .expect("append answered");
    assert_eq!(acked, LogIndex::from_raw(1));
}

#[test]
fn test_repair_zero_index_ignored() {
    let (mut leader, mut output) = elect_and_settle();
    output.clear();
    leader.step(
        Input::SetNeedsRepair {
            from_index: LogIndex::from_raw(0),
        },
        &mut output,
    );
    assert!(leader.repair_from().is_none());
    assert_eq!(leader.role(), Role::Leader);
}
