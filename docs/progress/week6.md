# ANVIL Progress Report: Week 6 of 10

**Date**: 2026-11-28
**Status**: INCOMPLETE - two exit gates fail; see section 5.

## 1. What Shipped

### A. Joint consensus and learners (ADRs 0033, 0034)

`EnterJoint` and `LeaveJoint` entry kinds, joint quorums that require both
old and new majorities, learners that replicate but never vote, automatic
`LeaveJoint` on commit, and removal handoff with `TimeoutNow`. Config entries
take effect on append. Changes are refused while one is uncommitted or
before a current-term commit. Snapshots carry the config.

### B. Node lifecycle and snapshot config (ADR 0035)

Bootstrap config at index 1, cluster and node identity checks, joining nodes
that wait to be configured, and v04 snapshots that restore membership on
install.

### C. Reconfiguration TLA+ model

`specs/anvil_reconfig.tla` with serialized changes, joint quorum overlap,
and `LeaderCompleteness` over a bounded 4-server model. The negative config
produces a `SerializedConfig` counterexample. `cargo xtask tlc` runs both.

### D. Protocol-aware repair (ADR 0036)

A `NeedsRepair` node follows but clamps acks, never campaigns or votes, and
rejoins only after a validated rewrite and clean restart. Repair validates
every fetched entry against the committed archive or a peer commit.

### E. Reconfiguration nemesis (ADR 0037 and the committed S6b change)

Plan-generated add, remove, replace and promote events; a driver with
deadlines, catch-up-based promotion, and a per-index committed-config oracle.
Nodes are never deleted from the simulator while a committed config may
still name them. Stale followers accept replication from a leader their old
config does not know, which restores liveness across a missed change.

### F. Mutant library (ADR 0038, `docs/mutants.md`)

Twenty planted bugs with stable ids behind the `mutants` feature, hooked at
their real source sites. `cargo xtask mutant ID` re-executes a killer and its
control; `cargo xtask mutant all` prints the matrix. CI checks that the
feature is unreachable from `anvild`.

### G. Liveness and safety fixes found while running the gates

- Pre-vote responses from a lagging peer carried its older term and were
  discarded; they are now counted. This fixed seed 2771.
- A leader whose quorum shrank after the last ack never rechecked commit;
  commit is now rechecked on each leader tick. This fixed seed 12608.
- A removed node granted votes to later-term stale-config candidates; the
  grant now requires the candidate's log to cover the removal. This fixed
  seed 1469 while preserving the seed 131 handover.
- Expired add intents no longer delete simulator nodes.

## 2. Exit-Gate Commands and Real Output

All commands were run in this session on the 12-core i5-12450H host.

- `cargo fmt --all -- --check`: `FMT_OK`.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`:
  no errors after fixing doc-markdown and collapsible-if lints in the mutant
  hooks.
- `cargo nextest run --workspace`: **595 passed, 3 skipped**.
- `cargo test --doc --workspace`: `ok. 0 passed; 0 failed`.
- `cargo xtask tlc`: base and reconfig positive models clean on 3 seeds;
  negative model violates `SerializedConfig` as expected; `30.84s`.
- `cargo xtask determinism`: `ALL 10 SEEDS PASSED`, in-process, out-of-process
  and golden hashes match.
- `cargo llvm-cov nextest --workspace --summary-only`:
  `anvil-raft/src/core.rs ... 2145 lines, 75 missed, 96.50%`; gate floor 95%
  met for anvil-raft.
- `cargo xtask mutant all`: 18 killed, **M15 and M20 survived**; gate exits
  nonzero. Full output is in `/tmp/opencode/week6-final/mutants.txt` and the
  matrix is committed in `docs/mutants.md`.
- `anvil-sim farm --seeds 0..500000 --jobs 12 --profile kv`: **FAIL** at seed
  25526 with `ElectionSafety { term: 2, leader1: NodeId(2), leader2: NodeId(4) }`.
  The kv-storage leg did not run after that failure. Earlier runs also found
  and fixed seeds 2771, 12608 and 1469.
- Control farm `0..10000` on kv after the fixes: zero failures.
- Targeted regression seeds 131, 501, 1166, 1469, 1855, 2649, 2771 and 12608
  all pass in both kv and kv-storage profiles.

## 3. Performance

No Week 6 budget changes. Coverage build compiles cleanly with
`CARGO_INCREMENTAL=0`; a full workspace coverage run needs roughly 3 GB of
scratch space on this host.

## 4. Architecture Decisions Taken

- ADR 0033 membership config format.
- ADR 0034 joint consensus and learners.
- ADR 0035 node lifecycle and snapshot config.
- ADR 0036 protocol-aware repair.
- ADR 0037 replication from unknown leaders.
- ADR 0038 hand-authored mutant library.

## 5. Open Items and Why the Exit Gate Is Not Green

1. **500,000-seed farm is not clean.** Seed 25526 reports two leaders in the
   same term under reconfiguration. The removed-node vote exception that
   fixed seed 131 is the likely source: a removed node can add a second
   leader in a term when two stale-config candidates campaign. A strict
   no-vote rule restores safety but deadlocks the seed 131 handover. A fix
   needs the candidate's term at the removal index in the vote request so
   the removed node can distinguish a handover from a later-term stale
   candidate. This is not implemented, so the safety gate is not claimed.
2. **M15 and M20 survive.** Their hooks are at storage recovery
   classification and simulator repair validation. The farm and current
   tests do not observe those planted behaviors. The mutants remain in the
   library and the gate reports them honestly rather than being deleted.
3. **Full `cargo xtask ci` was not run end to end.** Its component gates
   above were run individually. The fuzz smoke, cargo deny, cargo audit,
   instruction benchmark and geiger legs were not re-run in this session;
   the earlier Week 5 report records them on the prior tree.
4. **Mutation score at least 88 percent is not claimed.** The hand-authored
   matrix has 18/20 kills. A cargo-mutants run was not completed on this
   host.

## 6. Anything Unsure About

- Whether the removed-node vote exception can be made both safe and live
  without extending the vote-request wire format. The current exception is
  safe for the seed 131 handover but not for seed 25526.
- Whether M15 and M20 need new unit tests or stronger farm hooks; both
  options are viable and neither is implemented.
