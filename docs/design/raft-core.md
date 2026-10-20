# ANVIL Raft Sans-IO Consensus Core Design

## Overview

`anvil-raft` implements the deterministic, sans-IO consensus core of ANVIL.
It exposes a single state machine transition entry point:
```rust
pub fn step(&mut self, input: Input, output: &mut Output);
```

The core executes with zero network I/O, zero clock syscalls, and zero background threads. All physical interactions (network packets, disk flushes, timer triggers) are converted into discrete `Input` events by the surrounding environment (`anvil-sim` in simulation, or `anvil-node` in production). All side effects (network frames, persistence operations, committed entries, completed reads, role transitions, and formal trace projections) are written to a caller-supplied `Output` buffer that is cleared and reused across steps.

---

## Mapping from TLA+ Formal Specification Actions to Implementation

Every state transition in `specs/anvil_raft.tla` maps 1:1 to an implementation function in `anvil-raft`:

| TLA+ Spec Action | Rust Implementation Function | File & Context |
| :--- | :--- | :--- |
| `StartPreVote(s)` | `RaftCore::start_pre_vote` | `crates/anvil-raft/src/core.rs` |
| `HandlePreVoteReq(s, m)` | `RaftCore::handle_pre_vote_request` | `crates/anvil-raft/src/core.rs` |
| `HandlePreVoteResp(s, m)` | `RaftCore::handle_pre_vote_response` | `crates/anvil-raft/src/core.rs` |
| `HandleRequestVoteReq(s, m)` | `RaftCore::handle_request_vote_request` | `crates/anvil-raft/src/core.rs` |
| `HandleRequestVoteResp(s, m)` | `RaftCore::handle_request_vote_response` | `crates/anvil-raft/src/core.rs` |
| `ClientRequest(s)` | `RaftCore::handle_propose` | `crates/anvil-raft/src/core.rs` |
| `SendAppendEntries(s, p)` | `RaftCore::broadcast_append_entries` | `crates/anvil-raft/src/core.rs` |
| `HandleAppendEntriesReq(s, m)` | `RaftCore::handle_append_entries_request` | `crates/anvil-raft/src/core.rs` |
| `HandleAppendEntriesResp(s, m)` | `RaftCore::handle_append_entries_response` | `crates/anvil-raft/src/core.rs` |
| `AdvanceCommit(s)` | `RaftCore::maybe_advance_commit` | `crates/anvil-raft/src/core.rs` |
| `CheckQuorum(s)` | `RaftCore::handle_tick` (CheckQuorum check) | `crates/anvil-raft/src/core.rs` |
| `ExpireSticky(s)` | `RaftCore::handle_tick` (reset timer) | `crates/anvil-raft/src/core.rs` |
| `Restart(s)` | `RaftCore::new(config, recovered_state, rng)` | `crates/anvil-raft/src/core.rs` |
| `DropMessage(m)` | Handled by transport layer (`SimNetwork` drop hooks) | `crates/anvil-sim/src/cluster.rs` |
| `DuplicateMessage(m)` | Handled by transport layer (`SimNetwork` duplicate hooks) | `crates/anvil-sim/src/cluster.rs` |

---

## Consensus Protocol Features

### 1. PreVote Protocol
To prevent partitioned or recovering nodes from causing election churn and bumping terms unnecessarily, nodes first enter `Role::PreCandidate`. A prospective candidate solicits votes for `current_term + 1` without incrementing its own term. Only when a quorum of peers confirms that the prospective candidate's log is up-to-date and that they are willing to vote does the candidate increment its term, transition to `Role::Candidate`, and formally solicit votes via `RequestVote`.

### 2. Leader No-Op Entry on Election
Upon winning an election, the newly elected leader appends a `EntryKind::NoOp` entry to its log with its new term and replicates it immediately. This guarantees that uncommitted entries from prior terms are committed indirectly once the leader's own current-term no-op is committed by a quorum (Section 5.4.2).

### 3. Current-Term Commit Rule
As formalized in Raft Section 5.4.2 and verified in `specs/anvil_raft.tla`, a leader never advances its `commit_index` directly by counting replicas of entries from older terms. The commit index advances only when an entry from the leader's **current term** is confirmed on a majority quorum.

### 4. 1-RTT Divergent Log Repair (Conflict Hints)
When a follower rejects an `AppendEntriesRequest` due to a mismatch at `prev_log_index`:
- If the follower does not have an entry at `prev_log_index`, it returns `conflict_term: None` and `conflict_index: Some(last_log_index + 1)`.
- If the follower has an entry at `prev_log_index` with term $T \ne prev\_log\_term$, it returns `conflict_term: Some(T)` and `conflict_index: Some(first_index_with_term_T)`.
The leader immediately bypasses the entire conflicting term or missing suffix, repairing log divergence in a single round trip.

### 5. Peer Progress & Flow Control
Each peer is tracked with `PeerProgress`:
- **`Probe` state**: Sends at most one unacknowledged replication message per round trip.
- **`Replicate` state**: Pipelined replication allowing up to `max_inflight_messages` in flight with optimistic `next_index` advance. On rejection, progress drops back to `Probe` and steps back using conflict hints.

### 6. Strict Persistence Contract
ANVIL enforces a strict storage contract to ensure safety across crashes:
- Follower append responses and vote responses are placed into `pending_persists` and released into `output.messages` **only after** storage emits `Input::Persisted` for the relevant token.
- Leaders persist new proposals in parallel with broadcasting `AppendEntriesRequest` to followers; however, the leader does **not** count its own match towards quorum until its own persist token is acknowledged.
- Term and vote updates are always persisted via `PersistOp::HardState` before releasing any message that reveals them.

### 7. SpecProjection Trace Validation
On every state transition, `RaftCore` emits a `SpecProjection` event:
```rust
pub struct SpecProjection {
    pub term: Term,
    pub role: Role,
    pub voted_for: Option<NodeId>,
    pub log_digest: [u8; 32],
    pub commit_index: LogIndex,
}
```
This enables Week 9 offline trace validation against the TLA+ formal specification.
