#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::similar_names
)]

use anvil_raft::{Input, Output, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_sim::RaftInvariantChecker;
use anvil_types::{Entry, LogIndex, NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};

fn two_node_configs() -> (RaftConfig, RaftConfig, NodeId, NodeId) {
    let node1 = NodeId::new(1).expect("id");
    let node2 = NodeId::new(2).expect("id");
    let mut peers1 = BTreeSet::new();
    peers1.insert(node2);
    let mut peers2 = BTreeSet::new();
    peers2.insert(node1);
    let base = |node_id: NodeId, peers: BTreeSet<NodeId>| RaftConfig {
        node_id,
        peers,
        election_timeout_ticks: 10,
        heartbeat_interval_ticks: 3,
        max_inflight_messages: 256,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: true,
        check_quorum: true,
    };
    (base(node1, peers1), base(node2, peers2), node1, node2)
}

fn normal_entry(index: u64, term: u64) -> Entry {
    Entry::normal(
        LogIndex::from_raw(index),
        Term::from_raw(term),
        Bytes::from_static(b"v"),
    )
}

fn core_with_log(
    config: RaftConfig,
    term: u64,
    commit: u64,
    entries: Vec<Entry>,
    seed: u64,
) -> RaftCore {
    let recovered = RecoveredState {
        current_term: Term::from_raw(term),
        voted_for: None,
        commit_index: LogIndex::from_raw(commit),
        entries,
    };
    RaftCore::new(config, recovered, ChaCha8Rng::seed_from_u64(seed))
}

#[test]
fn test_leader_append_only_catches_future_term_entry() {
    let (config1, _, node1, _) = two_node_configs();
    let mut core = core_with_log(
        config1,
        1,
        0,
        vec![normal_entry(1, 1), normal_entry(2, 2)],
        11,
    );
    core.testing_set_role(Role::Leader);

    let mut nodes = BTreeMap::new();
    nodes.insert(node1, core);
    let mut checker = RaftInvariantChecker::new();
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::LeaderAppendOnly { .. }
    ));
}

#[test]
fn test_log_matching_catches_divergent_prefix() {
    let (config1, config2, node1, node2) = two_node_configs();
    let core1 = core_with_log(
        config1,
        2,
        0,
        vec![normal_entry(1, 1), normal_entry(2, 2)],
        21,
    );
    let core2 = core_with_log(
        config2,
        2,
        0,
        vec![normal_entry(1, 9), normal_entry(2, 2)],
        22,
    );
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, core1);
    nodes.insert(node2, core2);
    let mut checker = RaftInvariantChecker::new();
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::LogMatching { .. }
    ));
}

#[test]
fn test_leader_completeness_catches_missing_committed_entry() {
    let (config1, config2, node1, node2) = two_node_configs();
    let committer1 = core_with_log(config1.clone(), 1, 1, vec![normal_entry(1, 1)], 31);
    let committer2 = core_with_log(config2.clone(), 1, 1, vec![normal_entry(1, 1)], 32);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, committer1);
    nodes.insert(node2, committer2);
    let mut checker = RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());

    let mut higher = core_with_log(config1, 2, 0, vec![], 33);
    higher.testing_set_role(Role::Leader);
    nodes.insert(node1, higher);
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::LeaderCompleteness { .. }
    ));
}

#[test]
fn test_committed_never_lost_catches_overwritten_entry() {
    let (config1, config2, node1, node2) = two_node_configs();
    let committer = core_with_log(config1.clone(), 1, 1, vec![normal_entry(1, 1)], 41);
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, committer);
    let mut checker = RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());

    let divergent = core_with_log(config2, 1, 1, vec![normal_entry(1, 2)], 42);
    nodes.insert(node2, divergent);
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::CommittedEntriesNeverLost { .. }
    ));
}

#[test]
fn test_applied_bounds_catches_apply_ahead_of_commit() {
    let (config1, _, node1, _) = two_node_configs();
    let mut core = core_with_log(config1, 1, 0, vec![normal_entry(1, 1)], 51);
    let mut output = Output::new();
    core.step(
        Input::Applied {
            index: LogIndex::from_raw(1),
        },
        &mut output,
    );
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, core);
    let mut checker = RaftInvariantChecker::new();
    let err = checker.try_check_invariants(&nodes).unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::AppliedLeqCommittedLeqLastIndex { .. }
    ));
}

#[test]
fn test_vote_never_forgotten_catches_changed_vote() {
    let (config1, _, node1, node2) = two_node_configs();
    let mut core = core_with_log(config1, 1, 0, vec![], 61);
    let mut output = Output::new();
    core.step(
        Input::Recv {
            from: node2,
            message: anvil_raft::Message::RequestVoteRequest {
                term: Term::from_raw(1),
                candidate_id: node2,
                last_log_index: LogIndex::from_raw(0),
                last_log_term: Term::from_raw(0),
            },
        },
        &mut output,
    );
    assert_eq!(core.voted_for(), Some(node2));
    let mut nodes = BTreeMap::new();
    nodes.insert(node1, core);
    let mut checker = RaftInvariantChecker::new();
    assert!(checker.try_check_invariants(&nodes).is_ok());

    let err = checker
        .check_vote_never_forgotten(node1, Term::from_raw(1), Some(node1))
        .unwrap_err();
    assert!(matches!(
        err,
        anvil_sim::InvariantViolation::VoteNeverForgotten { .. }
    ));
    assert!(
        checker
            .check_vote_never_forgotten(node1, Term::from_raw(1), Some(node2))
            .is_ok()
    );
}
