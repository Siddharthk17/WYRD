# ADR 0009: TLA+ Formal Specification and Negative Model Checking

## Status
Accepted

## Context
Raft consensus safety relies on nuanced invariants—specifically around leader election, log matching, and log entry commitment rules. The classic Raft paper (Section 5.4.2) identifies a subtle safety hazard (Figure 8): if a leader counts replicas of an entry from an older term and commits it before writing an entry in its current term, an elected successor can legally overwrite the supposedly committed entry, violating state machine safety.

To guarantee that the implementation matches mathematical invariants, we require a formal model of the protocol that executes under a model checker and catches regression violations before implementation.

## Options Considered
1. **Informal Paper Adherence**: Write the Rust code by manually translating the paper pseudocode.
2. **Post-Hoc Verification**: Implement Raft first and construct a TLA+ model later as documentation.
3. **Spec-First Verification with Negative Counterexample**: Write the formal TLA+ specification first, verify all seven core invariants with TLC, and construct a negative test configuration proving that removing the current-term commit rule produces the Figure 8 counterexample.

## Decision
ANVIL adopts spec-first verification. The formal specification lives in `specs/anvil_raft.tla` with:
- Modeling of persistent and volatile state, PreVote, CheckQuorum, leader stickiness, and lossy/reordering networks.
- Verification of seven primary safety invariants: `ElectionSafety`, `LeaderAppendOnly`, `LogMatching`, `LeaderCompleteness`, `StateMachineSafety`, `CommittedEntriesNeverLost`, and `TermMonotonic`.
- A positive configuration (`specs/anvil_raft.cfg`) proving correctness across bounded state spaces with symmetry reduction.
- A negative configuration (`specs/anvil_raft_negative.cfg`) proving that setting `RequireCurrentTermCommit = FALSE` produces the Figure 8 counterexample.
- An xtask command (`cargo xtask tlc`) wired into the continuous integration suite.

## Consequences
- Every state transition in `anvil-raft` maps directly to an action in `specs/anvil_raft.tla`.
- Trace events emitted by `anvil-raft` carry a `SpecProjection` struct matching spec variables, allowing future trace validation against the formal model.
- Model checking provides mathematical assurance that protocol extensions (PreVote, CheckQuorum) do not introduce safety violations.
