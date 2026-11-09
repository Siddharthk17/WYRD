# ANVIL Raft TLA+ Specification and Formal Model

## Overview

This specification formally models the ANVIL sans-IO Raft consensus core in TLA+.
The model captures the complete election, replication, crash-recovery, and safety invariants:
- **Persistent State**: `currentTerm`, `votedFor`, and log entries.
- **Volatile State**: Server `role`, `commitIndex`, progress tracking (`nextIndex`, `matchIndex`), `votesGranted`, `preVotesGranted`, and `leaderSticky`.
- **Network Environment**: Lossy, duplicating, and reordering message passing model without physical time.
- **Protocol Features**:
  - **PreVote**: Phase preceding full candidacy to prevent disruption by partitioned or partitioned-rejoining nodes.
  - **CheckQuorum**: Periodic step-down by leaders unable to maintain contact with a quorum.
  - **Leader Stickiness**: Rejection of disruptive candidate votes while a healthy leader is active.
  - **Current-Term Commit Rule**: Enforces Raft Section 5.4.2 safety constraint (leaders never commit log entries from previous terms by counting replicas directly).

---

## State Invariants

The specification verifies seven core Raft safety invariants:

1. **`ElectionSafety`**: At most one leader can be elected in any given term.
   $$\forall s_1, s_2 \in Server: (role[s_1] = \text{"Leader"} \land role[s_2] = \text{"Leader"} \land currentTerm[s_1] = currentTerm[s_2]) \implies s_1 = s_2$$
2. **`LeaderAppendOnly`**: A leader never overwrites or truncates its log entries; all existing entries at earlier indices have terms $\le currentTerm$.
3. **`LogMatching`**: If two logs contain an entry with the same index and term, the logs are identical up to that index.
4. **`LeaderCompleteness`**: If a log entry is committed in a given term, that entry is present in the logs of the leaders for all higher-numbered terms.
5. **`StateMachineSafety`**: If two servers have committed entries at the same index, both entries have the identical term and value.
6. **`CommittedEntriesNeverLost`**: Once an entry is committed by any node, it is recorded in the global oracle and will never be overwritten in any subsequent term.
7. **`TermMonotonic`**: Terms do not exceed the bounded state space parameter and monotonic progress is preserved across transitions.

---

## Model Checking Configuration

The reference configuration ([anvil_raft.cfg](../../specs/anvil_raft.cfg)) defines:
- **Servers**: 3 nodes (`{s1, s2, s3}`)
- **Max Term**: 3
- **Max Log Length**: 3
- **RequireCurrentTermCommit**: `TRUE`
- **Symmetry**: `Permutations(Server)`

### Negative Counterexample Configuration (Figure 8)

The negative configuration ([anvil_raft_negative.cfg](../../specs/anvil_raft_negative.cfg)) disables the current-term commit rule:
- **`RequireCurrentTermCommit = FALSE`**

When this rule is disabled, a leader in a newer term directly commits an entry from an older term because it has been replicated onto a majority. A partitioned node with a higher term then gets elected and overwrites that entry, violating `StateMachineSafety` and `CommittedEntriesNeverLost`. TLC detects this counterexample and reports an invariant violation, proving that the spec accurately reproduces the classic Figure 8 scenario.

### Smoke Bounds for the Per-Change Gate

`cargo xtask tlc` checks the smoke configs
([anvil_raft_smoke.cfg](../../specs/anvil_raft_smoke.cfg),
[anvil_raft_negative_smoke.cfg](../../specs/anvil_raft_negative_smoke.cfg)):
3 servers, 2 terms, 2 log entries, same 7 invariants, same symmetry. The
model is explored with seeded random simulation (3 fixed seeds, depth 500
traces each) instead of exhaustive search: the positive config must stay
violation-free on every trace, and the negative config must produce the
Figure 8 counterexample. Seeded simulation finishes in about a minute on a
development machine, satisfying the charter requirement that CI bounds
finish in 10 minutes. The reference 3-term, 3-entry bound is checked with
`cargo xtask tlc-full` on larger hosts.

Measured 2026-11-09 (i5-12450H, 12 threads): exhaustive checking of even the
smoke bound exceeded 250M generated states and kept growing, and the
reference bound exceeded 300M generated states and exhausted a 5 GiB disk
allocation for TLC state files before completing, so neither exhaustive
check is a per-change gate on this hardware. Run `cargo xtask tlc-full` on
a larger verification host for the reference bounds; the simulation gate
above is the enforced per-change check.

During development, simulation also caught a real spec defect: leader
initialization set `nextIndex` to `Len(log) + 2` unconditionally, which
indexes past the end of a full log. The spec now conditions both
`matchIndex` and `nextIndex` on whether the no-op append fits.

---

## Automated TLC Runner in xtask

The ANVIL toolchain includes a built-in TLC verification gate in `xtask`:
```bash
cargo xtask tlc
```
The command verifies that:
1. `anvil_raft_smoke.cfg` simulates cleanly on all seeds with 0 invariant violations.
2. `anvil_raft_negative_smoke.cfg` simulation finds an invariant violation (`StateMachineSafety` or `CommittedEntriesNeverLost`), validating the negative check.
