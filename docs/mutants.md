# Mutant Library and Kill Matrix

The `mutants` Cargo feature plants twenty specific consensus, membership,
storage and checker bugs. Each bug has a stable id (`M01`..`M20`) and is
selected before a run starts. With the feature off the code is unchanged and
no selector exists. The production server does not depend on `anvil-sim`, so
the feature is unreachable from `anvild`; `cargo xtask ci` checks that.

A mutant is killed when a safety gate fires with it enabled and the same
run passes without it. A killer is either a farm seed or a targeted test.
`cargo xtask mutant M13` verifies one row; `cargo xtask mutant all` verifies
the whole matrix. Both forms are executed, not merely reported.

## Matrix

| Id | Planted bug | Detector | Killer |
|----|-------------|----------|--------|
| M01 | Commit previous-term entries by counting replicas | invariant | `kv` seed 0 |
| M02 | Vote response before hard state is persisted | liveness | `kv-storage` seed 3524 |
| M03 | Follower acks an append before it is persisted | invariant | `kv` seed 927 |
| M04 | Leader counts its own match before it is persisted | unit test | `anvil-raft/core_test:test_leader_propose_and_parallel_persist_contract` |
| M05 | ReadIndex skips leadership confirmation | checker | `kv` seed 0 |
| M06 | Lease read ignores the drift bound | checker | `kv` seed 0 |
| M07 | Conflicting suffix not truncated correctly | invariant | `kv` seed 17 |
| M08 | Uncommitted entry applied | audit | `kv` seed 40 |
| M09 | Snapshot index off by one | unit test | `anvil-node/node_snapshot_test:test_node_snapshot_supply_install_and_rejoin` |
| M10 | InstallSnapshot does not restore sessions | unit test | `anvil-node/node_snapshot_test:test_node_snapshot_supply_install_and_rejoin` |
| M11 | Vote not restored after restart | invariant | `kv` seed 1608 |
| M12 | Config change before a current-term commit | unit test | `anvil-raft/membership_test:test_config_change_refused_before_current_term_commit` |
| M13 | Joint quorum uses OR instead of AND | invariant | `kv` seed 13230 |
| M14 | Session dedup cache evicted too early | unit test | `anvil-kv/kv_test:test_kv_session_dedup_returns_cached_response` |
| M15 | Torn tail accepted or mid-log corruption silently truncated | — | **survived** |
| M16 | fsync error retried | invariant | `kv-storage` seed 4166 |
| M17 | Leader does not step down without CheckQuorum | checker | `kv` seed 14755 |
| M18 | PreVote bypass causes disruption | invariant | `kv` seed 16189 |
| M19 | Removed node keeps campaigning | unit test | `anvil-raft/membership_test:test_removed_node_does_not_campaign` |
| M20 | Repair accepts an unverified entry | — | **survived** |

The farm killer seeds are the first seeds found in a 0..20,000 sweep whose
control run passes. Test killers run the named test with `ANVIL_MUTANT=ID`
and then with `ANVIL_MUTANT=none`; only a fail/pass pair counts as a kill.

## Two surviving mutants

M15 and M20 are planted at the storage recovery classification and the
simulator repair validator. The current farm and test suite do not reach
those sites in a way that makes the planted bug observable: M15 changes a
classification that the simulator's own device path recomputes, and M20
accepts an entry that the repair rewrite never offers. The gate reports them
as survivors and fails. The hook sites are retained so a killer can be added
without changing the mutant identities. This is an open Week 6 item, not a
passing gate.

## Reproducing

```sh
cargo build --release -p anvil-sim --features mutants
cargo xtask mutant all          # full matrix, exits nonzero while M15/M20 survive
cargo xtask mutant M13          # one row
ANVIL_MUTANT=M12 cargo test -p anvil-raft --features mutants \
  --test membership_test test_config_change_refused_before_current_term_commit
```

`cargo hack check --feature-powerset` compiles every mutant-enabled
configuration; `cargo xtask ci` also runs the server isolation gate.

## Negative control

A run with no mutant selected is byte-identical to a normal build: the
selector is `None`, every `is(Mxx)` guard is false, and the only added code is
the feature-gated hook. The 500,000-seed reconfiguration farm is the negative
control and its result is recorded in `docs/progress/week6.md`.
