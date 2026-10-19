# Changelog

All notable changes to ANVIL will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Repository bootstrap with 14 workspace member crates.
- Architecture Decision Records 0001 through 0007.
- Strict lints and clippy rules for deterministic sans-IO execution.
- Hand-written zero-copy binary codec, domain newtypes, property tests, and fuzz targets in `anvil-types`.
- Deterministic simulation kernel, `SimTime`, isolated `DetRng` streams, `BinaryHeapEventQueue`, `TraceRecorder` with streaming BLAKE3 digest, `Watchdog`, and `buggify!` subsystem in `anvil-sim`.
- Replicated-counter consensus validation test suite proving cross-run determinism, tie ordering, time monotonicity, buggify fault injection, and watchdog trips.
- Determinism verification gate (`cargo xtask determinism`) running 10 golden seeds twice in-process and across separate fresh processes under ASLR and perturbed environments against `tests/golden/seed_hashes.txt`.
- Negative test fixture verifying that introducing non-deterministic `HashMap` iteration causes the determinism gate to fail.
- Baseline `xtask` quality gate runner.
