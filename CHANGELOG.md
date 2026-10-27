# Changelog

All notable changes to ANVIL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.7.1.9]

### Added
- Storage fault model specification in `docs/storage-fault-model.md` and [ADR 0011](docs/adr/0011-storage-fault-model.md) defining crash semantics, sector torn writes, bit rot, latent sector errors, and cluster survivability rules.
- On-disk binary format specifications in `docs/formats/` for sector-aligned WAL segments (`docs/formats/wal-segment.md`), 2-slot alternating hard state (`docs/formats/hard-state.md`), and snapshot files (`docs/formats/snapshot.md`) under [ADR 0012](docs/adr/0012-on-disk-storage-formats.md).
- Binary codecs in `anvil-storage` with zero-copy decoding, chained CRC32C, zero-padding verification, and CRC validation covering file offsets and segment IDs.
- Pure sans-IO storage engine (`StorageEngine`) in `anvil-storage` under [ADR 0013](docs/adr/0013-sans-io-storage-engine.md) featuring natural batching group commit, segment preallocation and rotation, completion token feedback to Raft persistence contracts, and fail-fast fsync policy.
- Crash recovery scanner with 4-way classification (`Clean`, `TornTail`, `MidLogCorruption`, and atomic alternating `HardState`).
- Simulated storage device (`SimDevice`) in `anvil-sim` with volatile page cache, torn write injection, bit rot, read/write bit flips, latent sector errors, directory sync checks, and cluster-wide `StorageFaultAtlas`.
- Dedicated I/O thread real disk device (`FsDevice`) in `anvil-device-fs` over `std::fs` and `rustix` (`fdatasync`, `fallocate`) with `#![forbid(unsafe_code)]`.
- Differential test harness in `anvil-device-fs` asserting byte-for-byte identical on-disk states between `FsDevice` and `SimDevice`.
- Storage integration in `SimCluster` nodes with crash and restart transitions, and online invariants: `VoteNeverForgotten`, `CommittedNeverLostAcrossCrashes`, and `RecoveredLogMissingAcknowledgedEntry`.
- 4 cargo-fuzz targets (`fuzz_wal_header`, `fuzz_wal_frame`, `fuzz_hard_state`, `fuzz_snapshot`) and checked-in binary seed corpora in `fuzz/corpus/`.
- Property-based tests verifying arbitrary byte crash safety and persistent cluster consensus invariants.
- Counting allocator tests in `anvil-storage` verifying zero heap allocations in steady state for append buffering, empty appends, and `EngineOutput::clear`.
- Storage and disk benchmarks in `anvil-bench` measuring raw `fdatasync`, sequential sync-write ceiling, WAL append throughput at batch sizes 1, 16, and 256, and $> 2$ GB/s recovery scanning.
- Performance budgets and allocation invariants recorded in `docs/perf/BUDGETS.md`.
- Full 100,000-seed clean simulator farm evaluation with network faults and node crash recovery.
- Week 3 milestone progress report in `docs/progress/week3.md`.

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
