#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines
)]

use anvil_raft::{Input, Message, Output, PersistOp, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_types::{NodeId, Term};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::BTreeSet;

fn node_id(n: u64) -> NodeId {
    NodeId::new(n).expect("node")
}

fn make_config(n: u64, peers: &[u64]) -> RaftConfig {
    RaftConfig {
        node_id: node_id(n),
        peers: peers.iter().map(|p| node_id(*p)).collect::<BTreeSet<_>>(),
        election_timeout_ticks: 5,
        heartbeat_interval_ticks: 2,
        max_inflight_messages: 4,
        max_bytes_per_message: 64 * 1024,
        pre_vote_enabled: false,
        check_quorum: false,
    }
}

fn make_core(n: u64, peers: &[u64]) -> RaftCore {
    RaftCore::new(
        make_config(n, peers),
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

fn elect_two_node() -> (RaftCore, Output) {
    let mut leader = make_core(1, &[2]);
    let mut output = Output::new();
    for _ in 0..20 {
        leader.step(Input::Tick, &mut output);
        if leader.role() == Role::Candidate {
            break;
        }
    }
    assert_eq!(leader.role(), Role::Candidate);
    let term = leader.term();
    output.clear();
    leader.step(
        Input::Recv {
            from: node_id(2),
            message: Message::RequestVoteResponse {
                term,
                vote_granted: true,
                from: node_id(2),
            },
        },
        &mut output,
    );
    assert_eq!(leader.role(), Role::Leader);
    ack_all(&mut leader, &mut output);
    output.clear();
    (leader, output)
}

fn propose_count(core: &mut RaftCore, output: &mut Output) -> usize {
    let before = core.log().last_index();
    core.step(
        Input::Propose {
            payload: Bytes::from_static(b"x"),
        },
        output,
    );
    ack_all(core, output);
    usize::try_from(core.log().last_index().as_u64() - before.as_u64()).unwrap_or(0)
}

#[test]
fn test_transfer_blocks_proposals_and_aborts_on_timeout() {
    let (mut leader, mut output) = elect_two_node();
    assert_eq!(propose_count(&mut leader, &mut output), 1);

    // Transferee is behind: match 0 < last, so no instant TimeoutNow.
    leader.step(Input::TransferLeader { to: node_id(2) }, &mut output);
    let sent_timeout = output
        .messages
        .iter()
        .any(|m| matches!(m.message, Message::TimeoutNow { .. }));
    assert!(
        !sent_timeout,
        "lagging transferee must not get TimeoutNow yet"
    );

    // Proposals stop while the transfer is pending.
    output.clear();
    assert_eq!(propose_count(&mut leader, &mut output), 0);

    // One election timeout without completion aborts the transfer.
    for _ in 0..5 {
        leader.step(Input::Tick, &mut output);
    }
    output.clear();
    assert_eq!(
        propose_count(&mut leader, &mut output),
        1,
        "proposals resume after transfer abort"
    );
}

#[test]
fn test_transfer_to_unknown_peer_is_ignored() {
    let (mut leader, mut output) = elect_two_node();
    let stranger = NodeId::new(99).expect("node");
    leader.step(Input::TransferLeader { to: stranger }, &mut output);
    assert_eq!(propose_count(&mut leader, &mut output), 1);
    assert!(
        !output
            .messages
            .iter()
            .any(|m| { matches!(m.message, Message::TimeoutNow { .. }) }),
        "no TimeoutNow to strangers"
    );
}

#[test]
fn test_transfer_completes_when_caught_up() {
    let (mut leader, mut output) = elect_two_node();
    // Bring the peer fully up to date: match == last triggers TimeoutNow.
    leader.step(Input::TransferLeader { to: node_id(2) }, &mut output);
    for _ in 0..4 {
        leader.step(Input::Tick, &mut output);
    }
    // Abort first so the response below is not shadowed by a pending timer.
    output.clear();
    leader.step(
        Input::Recv {
            from: node_id(2),
            message: Message::AppendEntriesResponse {
                term: Term::from_raw(1),
                success: true,
                match_index: leader.log().last_index(),
                conflict_term: None,
                conflict_index: None,
                from: node_id(2),
            },
        },
        &mut output,
    );
    assert!(
        output
            .messages
            .iter()
            .any(|m| { matches!(m.message, Message::TimeoutNow { .. }) }),
        "caught-up transferee gets TimeoutNow"
    );
    output.clear();
    let _ = propose_count(&mut leader, &mut output);
    assert_eq!(propose_count(&mut leader, &mut output), 1);
}
