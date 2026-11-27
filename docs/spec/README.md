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

When this rule is disabled, a leader in a newer term directly commits an entry from an older term because it has been replicated onto a majority. A partitioned node with a higher term then gets elected and overwrites that entry, violating `StateMachineSafety` and `CommittedEntriesNeverLost`. Exhaustive TLC on a large host detects this counterexample and reports an invariant violation; see the smoke-bounds section for why random simulation on small hosts is not asked to find it.

### Smoke Bounds for the Per-Change Gate

`cargo xtask tlc` checks the smoke config
([anvil_raft_smoke.cfg](../../specs/anvil_raft_smoke.cfg)):
3 servers, 2 terms, 2 log entries, same 7 invariants, same symmetry. The
model is explored with seeded random simulation (3 fixed seeds, 200 traces
of depth 100 each). The run must stay violation-free on every trace.
Measured 2026-11-12 on the dev machine (i5-12450H): one seed explores about
55k states in about 1.5 s, so the whole gate finishes in seconds, inside the
10-minute bound. The trace count bound matters: without `num`, TLC
simulates without end and the gate would never terminate.

The negative configs ([anvil_raft_negative.cfg](../../specs/anvil_raft_negative.cfg),
[anvil_raft_negative_smoke.cfg](../../specs/anvil_raft_negative_smoke.cfg))
disable the current-term commit rule to reproduce the Figure 8 scenario.
They are kept for large-host exhaustive runs via `cargo xtask tlc-full`,
but random simulation is not asked to find the counterexample: it is
unreachable by sampling in the 3-server bound. A server missing a
committed entry can never gather a quorum, because every vote it needs
comes from a server holding that entry, which the log-comparison rule
forces to deny; grounding higher-term entries without the committed prefix
is impossible under AppendEntries prefix matching. Thousands of traces
across seeds and bounds (smoke and reference, depths 100 to 500) found
nothing, while unbounded simulation never terminates. Exhaustive checking
exceeds small hosts: the smoke bound passes 69M generated states in 5
minutes and then exhausts disk, and the reference bound passed 300M
generated states before exhausting a 5 GiB TLC allocation. The Figure 8
rule is instead enforced deterministically by
`crates/anvil-sim/tests/raft_scenario_test.rs::test_raft_paper_figure_8_commit_rule`
and the `proptest_raft` suites, which drive the real core.

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
The command verifies that `anvil_raft_smoke.cfg` simulates cleanly on all
seeds with 0 invariant violations. The negative configs are kept for
`cargo xtask tlc-full` on larger hosts; the per-change gate does not run
them, for the reasons documented above, and the Figure 8 rule is covered by
the deterministic Rust scenario and property tests instead.

---

# ANVIL Joint-Consensus Reconfiguration Spec (`anvil_reconfig.tla`)

This specification models safe membership changes on top of the base Raft
core: learners, joint consensus, serialized config changes, and automatic
leave. The implementation lives in `crates/anvil-raft` (joint quorums,
auto-leave, refusals, stepdown) and `crates/anvil-node` (bootstrap, joining,
snapshot-carried config).

## Bounds

The smoke config
([anvil_reconfig_smoke.cfg](../../specs/anvil_reconfig_smoke.cfg)) defines:
- **Servers**: 4 nodes (3 voters plus `Joiner`, which starts as a learner)
- **Max Term**: 2
- **Max Log Length**: 3
- **Fault Budget**: 3 fault events (loss, crash, spurious stepdown) per trace
- **AllowConcurrentChanges**: `FALSE`

The negative config
([anvil_reconfig_negative.cfg](../../specs/anvil_reconfig_negative.cfg)) is
identical except `AllowConcurrentChanges = TRUE`: a second joint may be
appended while one is uncommitted.

## Invariants

The base seven (ElectionSafety, LeaderAppendOnly, LogMatching,
LeaderCompleteness, StateMachineSafety, CommittedEntriesNeverLost,
TermMonotonic) plus three reconfiguration invariants:

8. **`CommittedConfigNeverLost`**: a committed config entry keeps its value
   in every log that still covers its index.
9. **`SerializedConfig`**: at most one open joint index cluster-wide. A
   joint at index `i` is open while `i` is uncommitted and no `LeaveJoint`
   follows it. The oracle (`committedCfg`) makes the property stable across
   restarts and truncation.
10. **`JointQuorumOverlap`**: every active joint overlaps both the old and
    the new config by quorum, in both directions.

## Results

`cargo xtask tlc` simulates the positive config (3 seeds, 200 traces of
depth 150 each). Measured 2026-11-27 on the dev machine (i5-12450H):
seeds 1, 7 and 42 explore about 61k states each in about 2 s with zero
invariant violations. `Reconfig` fires dozens of times per run and
`FinishJoint` completes joint cycles, so the joint mechanics are genuinely
exercised (action coverage verified with `-coverage`).

The negative config violates `SerializedConfig` within 3 traces and 811
states in under a second: the second `EnterJoint` opens a second joint
index while the first is still uncommitted. The gate asserts the violation
appears; a clean negative run fails the gate (the spec or the model
changed, triage before proceeding).

Exhaustive checking of the 4-server bound does not finish within practical
limits (90M+ states generated in 4 minutes with the queue still growing on
the dev machine), so per the charter the gate uses simulation mode for the
reconfig spec while the negative config still yields its counterexample by
simulation. The base spec keeps its own (vacuous-for-commits) simulation
gate plus large-host exhaustive runs.

## Modeling notes (read before extending the spec)

- Client writes are not modeled: the log holds the election no-op and
  config entries only. Filler entries crowd out every `Reconfig` under
  uniform random simulation, and normal replication is covered by the base
  spec. Measured: with filler entries `Reconfig` fired 0 times in 50
  traces; without them it fires dozens of times per run.
- Faults draw from a per-trace budget (3 by default), mirroring the
  Plan-based nemesis (warmup/chaos/heal): unbounded loss, crash, and
  spurious stepdown starve all progress under simulation (nothing ever
  commits), the same reason the base simulation gate is vacuous for
  commits.
- `Stutter` models idle ticks so a quiescent state (spent budget, maxed
  terms, stale messages no handler accepts) never deadlocks the run. It
  preserves every safety invariant.
- A commit marker that outlives its log prefix after truncation is clamped
  to the log length, matching the implementation (commit advances at most
  to the matched prefix). Without the clamp, `LeaderCompleteness` faults
  on an out-of-bounds access instead of checking a real property; the clamp
  was added after simulation caught exactly that modeling gap.
- Restart preserves `commitIndex` (recovered from durable storage in the
  implementation) and rebuilds the pending config marker from the log.
- Joint state carries the pre-joint voters and learners so a truncated
  joint rolls back exactly like the implementation's
  `rollback_config_for_truncate`.
