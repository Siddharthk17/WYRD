# ANVIL project charter

ANVIL is a Raft-replicated key-value store in Rust, plus a deterministic simulator that runs the real consensus and storage code against injected network, disk, process and clock faults, plus a linearizability checker that judges every run. Any failure replays exactly from a 64-bit seed. The same consensus core runs in the simulator and in the production server.

## Rules that never bend

1. Correctness first. Safety is never traded for speed or schedule.
2. Determinism. Given the same seed and plan, a simulation produces a byte-identical trace on every machine and every run, debug or release.
3. Honesty. Never claim a gate passes without running it in this session and pasting the real output into the progress report. Never loosen a test, tolerance, budget or lint to get green. If a deliverable cannot meet its gate, stop and write down why.
4. Ambiguity. If a requirement can be read two ways, stop and list the readings with a recommendation. Do not guess.
5. Final quality now. No placeholder implementations, no TODO or FIXME in merged code, no 'optimize later'. Unfinished work goes in the progress report as an open item with the reason.

## Architecture

Crates, with dependencies pointing downward only:

- anvil-types: ids, entries, binary codec, error types.
- anvil-raft: sans-IO consensus core. `step(input, &mut output)`. No I/O, no clock, no threads.
- anvil-kv: deterministic state machine, client sessions, snapshot encode and decode.
- anvil-storage: WAL, hard state and snapshot formats plus recovery logic, written sans-IO over an abstract Device (submit operations, receive completions).
- anvil-node: glue that owns RaftCore, the storage engine and the state machine, generic over environment traits. Shared by the simulator and the server.
- anvil-check: history model and linearizability checker.
- anvil-sim: simulator kernel, SimDevice, SimNetwork, nemesis, workloads, invariants, farm, shrinker.
- anvil-device-fs (real-file Device on a dedicated I/O thread), anvil-server (anvild), anvil-client, anvilctl, anvil-viz, anvil-bench, xtask.

Deterministic crates are types, raft, kv, storage, node and check. In them: no std::fs, std::net, std::thread, Instant, SystemTime, OS randomness, HashMap, HashSet, RandomState, floating point, or statics with interior mutability. This is enforced by clippy disallowed_methods and disallowed_types, plus a test that fails if their dependency set leaves an allowlist.

Randomness: one u64 master seed. Each component derives its own ChaCha8 stream from the seed and its component path, so adding a component never changes another component's stream. All sampling (ranges, shuffles, Bernoulli, delays) uses ANVIL's own code on top of the raw u64 output. rand_chacha is pinned with an exact version.

Time: the core counts logical ticks. The environment decides what a tick is.

Plans: fault schedules and workloads are materialized as serializable data (a Plan) before a run starts. A run executes the Plan and never draws from a generator mid-run. This is what makes shrinking possible.

## Code standards

- Clean code with no unnecessary comments. Names carry the meaning. A comment is allowed only for a non-obvious why (a protocol subtlety, a paper section, a hardware quirk), never for what.
- unsafe is forbidden: `#![forbid(unsafe_code)]` in every crate. An exception needs an ADR and human sign-off.
- No unwrap, expect or panicking index on data that came from outside the process. Typed errors with thiserror. Protocol invariants use `assert!` (not `debug_assert!`) and stay on in release builds. A violated invariant must crash loudly.
- Public items that are not obvious get doc comments. Every non-trivial decision gets a numbered ADR in docs/adr (context, options, decision, consequences).
- Conventional commits, small and reviewable. Tests are written before or with the code, never after.
- Docs read like a careful engineer wrote them: short plain sentences, concrete numbers, no marketing words, no emoji, no filler such as robust, seamless, leverage or cutting-edge.

Architecture Decision Records (ADRs) are documented in [docs/adr](docs/adr/).

## Gates that run on every change (`cargo xtask ci`)

Format check. Clippy with -D warnings and a pedantic subset. cargo nextest. Doc tests. cargo hack feature-powerset check. cargo deny. cargo audit. cargo geiger (zero unsafe). Coverage through cargo llvm-cov with per-crate floors. Determinism gate (golden seed hashes, in-process twice and across processes). Simulator smoke farm (2,000 seeds). Fuzz smoke (60 seconds per target). Instruction-count benchmark regression gate. TLC spec check once the spec exists.

Nightly: 200,000-seed farm, 30-minute fuzz per target, mutation testing report. Weekly and at release: 1,000,000-seed farm.

## Performance doctrine (from the first commit)

- Budgets live in docs/perf/BUDGETS.md. Measure on day one, set each budget at 80 percent of the measured value, ratchet upward only.
- Hot paths allocate nothing in steady state. Payloads are bytes::Bytes (reference-counted slices), decode is zero-copy, and Output and scratch buffers are reused. Tests with a counting global allocator assert allocation counts.
- Batch everything that crosses a boundary: network frames, WAL appends, fsyncs, applies. Batching is natural (take whatever arrived while the previous operation was in flight) and never adds an artificial delay when idle.
- Never block the consensus thread on disk, network or the state machine.
- Measure before optimizing and keep the evidence (flamegraph, allocation profile, before and after numbers) in docs/perf/.
- Load generators are open-loop with coordinated-omission correction (hdrhistogram). Closed-loop numbers are labelled as such.

## Security doctrine

- Secure by default: mutual TLS 1.3 required for peers, clients and admin. Plaintext exists only behind an explicit insecure flag that logs a warning on every start and is refused in release builds.
- Authenticate before allocating. Bound every length before reserving memory. Fuzz every decoder.
- Least privilege: separate roles for peer, client-read, client-write and admin, bound to certificate identities.
- No secrets or user data in logs by default. Zeroize key material on drop.
- Supply chain: dependencies pinned by lockfile, cargo deny and cargo audit green, SBOM generated, release artifacts reproducible and signed.
- Every security decision is traced in docs/security/threat-model.md.

## Version pins (crates.io index, 15 Oct 2026; re-check on day one)

Rust: the current stable that rustup installs on day one, committed in rust-toolchain.toml with edition 2024. Nightly only in jobs that need it (sanitizers, cargo-fuzz).

Libraries: tokio 1.53, tonic 0.14, prost 0.14, rustls 0.23, tokio-rustls 0.26, bytes 1.12, crc32c 0.6, blake3 1.8, rand_chacha 0.10 (exact pin), thiserror 2, clap 4.6, tracing 0.1, imbl 7, crossbeam-channel 0.5, arc-swap 1.9, zeroize 1.9, chacha20poly1305 0.11, rustix 1.1, hdrhistogram 7.6, proptest 1.12, criterion 0.8, arbitrary 1.5, libfuzzer-sys 0.4, rcgen 0.14 (tests only).

Tools: cargo-nextest 0.9, cargo-deny 0.20, cargo-audit 0.22, cargo-fuzz 0.13, cargo-mutants 27, cargo-llvm-cov 0.9, cargo-hack 0.6, cargo-geiger 0.14, cargo-cyclonedx 0.5, samply 0.13.

Read the docs for the pinned version before using an API. Several of these crates changed their APIs recently, including the tonic code generation crates. Commit Cargo.lock.

## Every week ends the same way

Write docs/progress/weekN.md: what shipped, each exit-gate command with its real output, benchmark numbers with a hardware description, decisions taken (ADR links), open items, and anything you were unsure about. Then stop.
