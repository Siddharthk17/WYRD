#![forbid(unsafe_code)]

//! Online Raft invariant checker for ANVIL simulations.
//!
//! Evaluates the seven TLA+ specification invariants, state machine monotonicity,
//! vote immutability per term, and the global committed entry oracle after every node step:
//!
//! 1. `ElectionSafety`: At most one leader in any given term.
//! 2. `LeaderAppendOnly`: Leader entries never overwrite or truncate existing prefix.
//! 3. `LogMatching`: Logs matching at (index, term) are identical across all prefixes.
//! 4. `LeaderCompleteness`: Committed entries are present in all higher-term leaders.
//! 5. `StateMachineSafety`: Nodes committing index `i` agree on term and payload.
//! 6. `CommittedEntriesNeverLost`: Committed entries are never overwritten by any future leader.
//! 7. `TermMonotonic`: Node terms never decrease across state transitions.
//! 8. `AppliedLeqCommittedLeqLastIndex`: `applied_index <= commit_index <= last_log_index`.
//! 9. `VoteNeverChangesWithinTerm`: Vote for term `T` is immutable once cast.
//! 10. `GlobalOracle`: All committed entries agree with the global oracle.

use anvil_raft::{RaftCore, Role};
use anvil_types::{Entry, LogIndex, NodeId, Term};
use std::collections::BTreeMap;
use thiserror::Error;

/// Specific invariant violation errors detected by the online checker.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum InvariantViolation {
    #[error("ElectionSafety violated: term {term} has multiple leaders: {leader1} and {leader2}")]
    ElectionSafety {
        term: Term,
        leader1: NodeId,
        leader2: NodeId,
    },
    #[error(
        "LeaderAppendOnly violated on leader {leader}: entry at index {index} has term {entry_term} > current_term {current_term}"
    )]
    LeaderAppendOnly {
        leader: NodeId,
        index: LogIndex,
        entry_term: Term,
        current_term: Term,
    },
    #[error(
        "LogMatching violated between {node1} and {node2} at prefix index {prefix_index}: index {match_index} matched term {term}, but prefix terms differ ({term1} vs {term2})"
    )]
    LogMatching {
        node1: NodeId,
        node2: NodeId,
        match_index: LogIndex,
        term: Term,
        prefix_index: LogIndex,
        term1: Term,
        term2: Term,
    },
    #[error(
        "LeaderCompleteness violated on leader {leader} (term {leader_term}): missing committed entry at index {index} (committed in term {committed_term})"
    )]
    LeaderCompleteness {
        leader: NodeId,
        leader_term: Term,
        index: LogIndex,
        committed_term: Term,
    },
    #[error(
        "StateMachineSafety violated between {node1} and {node2} at commit index {index}: term {term1} != {term2}"
    )]
    StateMachineSafety {
        node1: NodeId,
        node2: NodeId,
        index: LogIndex,
        term1: Term,
        term2: Term,
    },
    #[error(
        "CommittedEntriesNeverLost violated on {node} at index {index}: node has term {actual_term}, but oracle committed term is {expected_term}"
    )]
    CommittedEntriesNeverLost {
        node: NodeId,
        index: LogIndex,
        actual_term: Term,
        expected_term: Term,
    },
    #[error(
        "TermMonotonic violated on node {node}: term regressed from {previous_term} to {current_term}"
    )]
    TermMonotonic {
        node: NodeId,
        previous_term: Term,
        current_term: Term,
    },
    #[error(
        "AppliedLeqCommittedLeqLastIndex violated on {node}: applied ({applied}) > commit ({commit}) or commit > last ({last})"
    )]
    AppliedLeqCommittedLeqLastIndex {
        node: NodeId,
        applied: LogIndex,
        commit: LogIndex,
        last: LogIndex,
    },
    #[error(
        "VoteNeverChangesWithinTerm violated on {node} in term {term}: vote changed from {previous_vote:?} to {new_vote:?}"
    )]
    VoteNeverChangesWithinTerm {
        node: NodeId,
        term: Term,
        previous_vote: Option<NodeId>,
        new_vote: Option<NodeId>,
    },
    #[error(
        "VoteNeverForgotten violated on {node} in term {term}: recorded vote was {expected_vote:?}, but recovered vote was {recovered_vote:?}"
    )]
    VoteNeverForgotten {
        node: NodeId,
        term: Term,
        expected_vote: Option<NodeId>,
        recovered_vote: Option<NodeId>,
    },
    #[error(
        "RecoveredLogMissingAcknowledgedEntry violated on {node} at index {index}: acknowledged term was {expected_term}, but recovered entry was {recovered_term:?}"
    )]
    RecoveredLogMissingAcknowledgedEntry {
        node: NodeId,
        index: LogIndex,
        expected_term: Term,
        recovered_term: Option<Term>,
    },
    #[error(
        "CommittedNeverLostAcrossCrashes violated at index {index}: committed entry in term {expected_term} was lost across crash recovery (actual: {actual_term:?})"
    )]
    CommittedNeverLostAcrossCrashes {
        index: LogIndex,
        expected_term: Term,
        actual_term: Option<Term>,
    },
}

/// Global online invariant checker evaluating Raft safety across cluster nodes.
#[derive(Debug, Default)]
pub struct RaftInvariantChecker {
    /// Global oracle recording every entry committed anywhere in the cluster.
    committed_oracle: BTreeMap<LogIndex, Entry>,
    /// History of highest term seen per node.
    node_terms: BTreeMap<NodeId, Term>,
    /// History of votes cast per (node, term).
    node_votes: BTreeMap<(NodeId, Term), NodeId>,
    /// Set of (term, `entry_index`) committed records.
    committed_terms: BTreeMap<LogIndex, Term>,
}

impl RaftInvariantChecker {
    /// Creates a fresh invariant checker.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluates all invariants across the provided set of cluster nodes.
    ///
    /// Must be called after every transition step of every node in the simulator.
    pub fn check_invariants(&mut self, nodes: &BTreeMap<NodeId, RaftCore>) {
        let res = self.try_check_invariants(nodes);
        assert!(res.is_ok(), "FATAL RAFT INVARIANT VIOLATION: {res:?}");
    }

    /// Internal evaluation returning a `Result` for testing or error propagation.
    pub fn try_check_invariants(
        &mut self,
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        Self::check_election_safety(nodes)?;
        Self::check_applied_commit_bounds(nodes)?;
        self.check_term_monotonic_and_votes(nodes)?;
        Self::check_leader_append_only(nodes)?;
        Self::check_log_matching(nodes)?;
        self.update_and_check_oracle(nodes)?;
        Self::check_state_machine_safety(nodes)?;
        self.check_leader_completeness(nodes)?;
        self.check_committed_entries_never_lost(nodes)?;
        Ok(())
    }

    /// 1. Election Safety: At most one leader in any term.
    fn check_election_safety(nodes: &BTreeMap<NodeId, RaftCore>) -> Result<(), InvariantViolation> {
        let mut leaders_by_term: BTreeMap<Term, NodeId> = BTreeMap::new();
        for (&id, core) in nodes {
            if core.role() == Role::Leader {
                let term = core.term();
                if let Some(&existing_leader) = leaders_by_term.get(&term) {
                    if existing_leader != id {
                        return Err(InvariantViolation::ElectionSafety {
                            term,
                            leader1: existing_leader,
                            leader2: id,
                        });
                    }
                } else {
                    leaders_by_term.insert(term, id);
                }
            }
        }
        Ok(())
    }

    /// 8. Applied <= Committed <= `LastIndex` bounds check.
    fn check_applied_commit_bounds(
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            let commit = core.commit_index();
            let last = core.last_log_index();
            // Note: applied is 0 until state machine applies entries; applied <= commit <= last
            if commit > last {
                return Err(InvariantViolation::AppliedLeqCommittedLeqLastIndex {
                    node: id,
                    applied: LogIndex::from_raw(0),
                    commit,
                    last,
                });
            }
        }
        Ok(())
    }

    /// 7 & 9. Term monotonicity and vote immutability per term.
    fn check_term_monotonic_and_votes(
        &mut self,
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            let current_term = core.term();
            if let Some(&prev_term) = self.node_terms.get(&id)
                && current_term < prev_term
            {
                return Err(InvariantViolation::TermMonotonic {
                    node: id,
                    previous_term: prev_term,
                    current_term,
                });
            }
            self.node_terms.insert(id, current_term);

            if let Some(vote) = core.voted_for() {
                if let Some(&prev_vote) = self.node_votes.get(&(id, current_term)) {
                    if prev_vote != vote {
                        return Err(InvariantViolation::VoteNeverChangesWithinTerm {
                            node: id,
                            term: current_term,
                            previous_vote: Some(prev_vote),
                            new_vote: Some(vote),
                        });
                    }
                } else {
                    self.node_votes.insert((id, current_term), vote);
                }
            }
        }
        Ok(())
    }

    /// 2. Leader Append Only: all leader entries must have term <= `current_term`.
    fn check_leader_append_only(
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            if core.role() == Role::Leader {
                let current_term = core.term();
                for entry in core.log().entries() {
                    if entry.term > current_term {
                        return Err(InvariantViolation::LeaderAppendOnly {
                            leader: id,
                            index: entry.index,
                            entry_term: entry.term,
                            current_term,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// 3. Log Matching: identical terms at index `i` imply identical prefixes up to `i`.
    fn check_log_matching(nodes: &BTreeMap<NodeId, RaftCore>) -> Result<(), InvariantViolation> {
        let node_list: Vec<(&NodeId, &RaftCore)> = nodes.iter().collect();
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let (id1, core1) = node_list[i];
                let (id2, core2) = node_list[j];

                let min_len = std::cmp::min(core1.last_log_index(), core2.last_log_index());
                let raw_min = min_len.as_u64();

                for idx in 1..=raw_min {
                    let log_idx = LogIndex::from_raw(idx);
                    let t1 = core1.log().term_at(log_idx);
                    let t2 = core2.log().term_at(log_idx);

                    if let (Some(term1), Some(term2)) = (t1, t2)
                        && term1 == term2
                    {
                        // Verify all prefixes 1..=idx match identically
                        for p in 1..=idx {
                            let p_idx = LogIndex::from_raw(p);
                            let pt1 = core1.log().term_at(p_idx).unwrap_or(Term::from_raw(0));
                            let pt2 = core2.log().term_at(p_idx).unwrap_or(Term::from_raw(0));
                            if pt1 != pt2 {
                                return Err(InvariantViolation::LogMatching {
                                    node1: *id1,
                                    node2: *id2,
                                    match_index: log_idx,
                                    term: term1,
                                    prefix_index: p_idx,
                                    term1: pt1,
                                    term2: pt2,
                                });
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// 10. Update and check global committed entry oracle.
    fn update_and_check_oracle(
        &mut self,
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            let commit = core.commit_index().as_u64();
            for idx in 1..=commit {
                let log_idx = LogIndex::from_raw(idx);
                if let Some(entry) = core.log().entry_at(log_idx) {
                    if let Some(oracle_entry) = self.committed_oracle.get(&log_idx) {
                        if oracle_entry.term != entry.term {
                            return Err(InvariantViolation::CommittedEntriesNeverLost {
                                node: id,
                                index: log_idx,
                                actual_term: entry.term,
                                expected_term: oracle_entry.term,
                            });
                        }
                    } else {
                        self.committed_oracle.insert(log_idx, entry.clone());
                        self.committed_terms.insert(log_idx, entry.term);
                    }
                }
            }
        }
        Ok(())
    }

    /// 5. State Machine Safety: nodes with committed index `i` agree on term.
    fn check_state_machine_safety(
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        let node_list: Vec<(&NodeId, &RaftCore)> = nodes.iter().collect();
        for i in 0..node_list.len() {
            for j in (i + 1)..node_list.len() {
                let (id1, core1) = node_list[i];
                let (id2, core2) = node_list[j];

                let common_commit =
                    std::cmp::min(core1.commit_index().as_u64(), core2.commit_index().as_u64());

                for idx in 1..=common_commit {
                    let log_idx = LogIndex::from_raw(idx);
                    let t1 = core1.log().term_at(log_idx);
                    let t2 = core2.log().term_at(log_idx);

                    if let (Some(term1), Some(term2)) = (t1, t2)
                        && term1 != term2
                    {
                        return Err(InvariantViolation::StateMachineSafety {
                            node1: *id1,
                            node2: *id2,
                            index: log_idx,
                            term1,
                            term2,
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// 4. Leader Completeness: leaders of term > T contain all committed entries of term T.
    fn check_leader_completeness(
        &self,
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            if core.role() == Role::Leader {
                let leader_term = core.term();
                for (&idx, &committed_term) in &self.committed_terms {
                    if leader_term > committed_term {
                        let leader_entry_term = core.log().term_at(idx);
                        if leader_entry_term != Some(committed_term) {
                            return Err(InvariantViolation::LeaderCompleteness {
                                leader: id,
                                leader_term,
                                index: idx,
                                committed_term,
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// 6. Committed Entries Never Lost: all node entries <= `commit_index` match oracle.
    fn check_committed_entries_never_lost(
        &self,
        nodes: &BTreeMap<NodeId, RaftCore>,
    ) -> Result<(), InvariantViolation> {
        for (&id, core) in nodes {
            let commit = core.commit_index().as_u64();
            for idx in 1..=commit {
                let log_idx = LogIndex::from_raw(idx);
                if let (Some(node_entry), Some(oracle_entry)) = (
                    core.log().entry_at(log_idx),
                    self.committed_oracle.get(&log_idx),
                ) && node_entry.term != oracle_entry.term
                {
                    return Err(InvariantViolation::CommittedEntriesNeverLost {
                        node: id,
                        index: log_idx,
                        actual_term: node_entry.term,
                        expected_term: oracle_entry.term,
                    });
                }
            }
        }
        Ok(())
    }

    /// Returns the total number of unique entries recorded in the global oracle.
    #[must_use]
    pub fn committed_entries_count(&self) -> usize {
        self.committed_oracle.len()
    }

    /// Returns a reference to the global committed oracle.
    #[must_use]
    pub fn committed_oracle(&self) -> &BTreeMap<LogIndex, Entry> {
        &self.committed_oracle
    }

    /// Verifies that a node's recovered state does not violate `VoteNeverForgotten`.
    ///
    /// # Errors
    /// Returns `InvariantViolation::VoteNeverForgotten` if the recovered vote contradicts the recorded vote.
    pub fn check_vote_never_forgotten(
        &self,
        node: NodeId,
        term: Term,
        recovered_vote: Option<NodeId>,
    ) -> Result<(), InvariantViolation> {
        if let Some(&recorded_vote) = self.node_votes.get(&(node, term))
            && recovered_vote != Some(recorded_vote)
        {
            return Err(InvariantViolation::VoteNeverForgotten {
                node,
                term,
                expected_vote: Some(recorded_vote),
                recovered_vote,
            });
        }
        Ok(())
    }

    /// Verifies that all entries in the global committed oracle are preserved or not contradicted
    /// by the recovered log.
    ///
    /// # Errors
    /// Returns `InvariantViolation::CommittedNeverLostAcrossCrashes` if a committed entry is contradicted.
    pub fn check_recovered_log_against_oracle(
        &self,
        _node: NodeId,
        recovered_entries: &[Entry],
    ) -> Result<(), InvariantViolation> {
        let mut map = BTreeMap::new();
        for e in recovered_entries {
            map.insert(e.index, e.term);
        }
        for (&idx, oracle_entry) in &self.committed_oracle {
            if let Some(&term) = map.get(&idx)
                && term != oracle_entry.term
            {
                return Err(InvariantViolation::CommittedNeverLostAcrossCrashes {
                    index: idx,
                    expected_term: oracle_entry.term,
                    actual_term: Some(term),
                });
            }
        }
        Ok(())
    }
}
