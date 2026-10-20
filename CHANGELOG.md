# Changelog

All notable changes to ANVIL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.5.0.8]

### Added
- Formal TLA+ specification in `specs/anvil_raft.tla` and model check configuration `specs/anvil_raft.cfg` verifying all 7 core Raft invariants.
- Negative TLC configuration `specs/anvil_raft_figure8_neg.cfg` reproducing the Figure 8 counterexample when current-term commit rule is violated.
- TLC specification check gate in `xtask` (`cargo xtask tlc`).
- Architecture Decision Record 0009 documenting the TLA+ formal specification and model-checking strategy.
- Complete sans-IO Raft consensus core in `anvil-raft` with `step(input, &mut output)`, PreVote, CheckQuorum, leader stickiness, conflict hints for 1-RTT repairs, and optimistic pipelining.
- Strict Raft persistence contract ensuring vote/append responses and leader matches are deferred until `Persisted`.
- Architecture Decision Record 0010 and detailed design in `docs/design/raft-core.md`.
- Online `RaftInvariantChecker` in `anvil-sim` asserting all 7 spec invariants, log bounds, term monotonicity, and global committed entry oracle on every node step.
- Multi-node simulation cluster (`SimCluster`) supporting 3, 5, and 7-node clusters with packet drop, duplicate, delay hooks, and partition controls.
- Raft paper scenario test suite in `anvil-sim` reproducing Figures 7 & 8, split votes, stale leader stepdown, log divergence/truncation, leader crash, and PreVote disruption prevention.
- Proptests with randomized in-flight message reordering, drops, duplicates, and shrinking.
- Counting allocator performance test in `anvil-raft` asserting 0 allocations for follower `Tick` and leader heartbeat, and $\le 1.001$ allocations/entry amortized during replication.
- In-memory 3-node replication benchmark in `anvil-bench` achieving $> 1,100,000$ entries/sec (20x above charter floor).
- Parallel deterministic simulation farm in `anvil-sim` (`cargo xtask farm --seeds <N>`) verifying 100,000 seeds clean with zero invariant violations.
- Gate 10 Simulator Smoke Farm (2,000 seeds) wired into `cargo xtask ci`.
- Week 2 milestone progress report in `docs/progress/week2.md`.

## [1.3.2.7]

### Added
- Repository bootstrap with 14 workspace member crates.
- Architecture Decision Records 0001 through 0007.
- Strict lints and clippy rules for deterministic sans-IO execution.
- Hand-written zero-copy binary codec, domain newtypes, property tests, and fuzz targets in `anvil-types`.
- Deterministic simulation kernel, `SimTime`, isolated `DetRng` streams, `BinaryHeapEventQueue`, `TraceRecorder` with streaming BLAKE3 digest, `Watchdog`, and `buggify!` subsystem in `anvil-sim`.
- Replicated-counter consensus validation test suite proving cross-run determinism, tie ordering, time monotonicity, buggify fault injection, and watchdog trips.
- Determinism verification gate (`cargo xtask determinism`) running 10 golden seeds twice in-process and across separate fresh processes under ASLR and perturbed environments against `tests/golden/seed_hashes.txt`.
- Negative test fixture verifying that introducing non-deterministic `HashMap` iteration causes the determinism gate to fail.
- Test counting global allocator harness (`CountingAlloc` governed under ADR 0008) in `anvil-types` asserting 0 heap allocations for 1 KiB entry and frame decodes in steady state.
- Criterion benchmarks and standalone performance verification harness in `anvil-bench` for event queue, `DetRng`, codec decodes, and single-core simulation kernel event throughput.
- Performance budgets specification and measurement baseline in `docs/perf/BUDGETS.md` ratcheting 80% budgets against real hardware.
- Benchmark command `cargo xtask bench` wired to run release benchmarks and verify performance against charter floors.
- Architecture Decision Record 0008 documenting the test-only counting allocator.
- Unified quality gate runner `cargo xtask ci` executing format check, strict clippy, workspace tests, doc tests, doc generation, zero-unsafe code audit, determinism verification, and performance budgets.
- PR CI GitHub Actions workflow (`.github/workflows/pr.yml`) running `cargo xtask ci` across x86_64 and aarch64 Linux matrices.
- Week 1 milestone progress report in `docs/progress/week1.md` documenting deliverables, real exit-gate outputs, and ratcheted performance budgets.
