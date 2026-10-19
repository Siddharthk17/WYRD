# ANVIL: 10-Week Build Plan

Ten prompts for AI coding agents that build a deterministic simulation testing engine and a Raft-replicated KV store in Rust. ANVIL is a working name, rename it freely. Prepared 15 Oct 2026. Crate and tool versions were checked against the crates.io index on that date.

## How to use this document

- Week 1: paste the Master Preamble, then the Week 1 prompt. Week 1 makes the agent save the preamble into the repo as AGENTS.md. From Week 2 on, paste only that week's prompt. Step 0 of every prompt makes the agent re-read AGENTS.md and the previous progress report. If your agent does not auto-load AGENTS.md, paste the preamble again.
- One branch per week. Merge only when the exit gate is green and you have run `cargo xtask ci` yourself. Pasted output is not proof.
- If a week is too big for one agent session, split it at the numbered deliverables. Each one is written to stand alone.
- After every week, spend 30 minutes having the agent walk you through the code, then explain the week back to it. The project is worth exactly as much as you can defend in an interview.
- Cut line if you fall behind, in this order: at-rest encryption, the etcd comparison, TLA+ trace validation (keep the spec), visualizer polish. The gates themselves are never cut.

## Three honest notes

1. No system reaches 'nothing left to improve'. The defensible version is that every gate has a number, every claim has a command that reproduces it, and the known limits are written down. Week 10 produces that list.
2. One Raft group scales vertically: batching, pipelining, follower reads, a fast disk path. It does not scale horizontally. The wire format and transport carry a group id so sharding can be added later (that is project #2 on the shortlist), but this plan does not build it.
3. Throughput and latency targets below are aspirational floors. Agents must measure the machine first (raw fsync rate, loopback bandwidth), report the hardware, and explain any miss with evidence. They may not loosen a target silently.

## What the research changed

- TigerBeetle's simulator uses an explicit storage fault model, injects corruption on both the read path and the write path, and checks invariants inside each replica instead of only at the client API. Weeks 3 and 5 copy that approach.
- LazyFS (VLDB 2024, co-authored by Jepsen's author) loses unsynced writes the way a power cut does. kill -9 cannot, because the OS still flushes dirty pages. It was used on PostgreSQL, etcd, ZooKeeper, Redis and others and found eight new data-loss bugs. Week 9 uses it on the real cluster.
- MongoDB and Microsoft CCF check their implementations against TLA+ specs with trace validation. Week 2 writes the spec first and emits trace events that carry exactly the spec variables. Week 9 validates traces.
- The rand crate does not promise stable distribution output across versions, so ANVIL samples with its own code on top of pinned ChaCha8. Golden seed hashes then survive dependency bumps.
- After an fsync error the kernel may have dropped the dirty pages, and a retry can report success (the fsyncgate lesson). ANVIL never retries fsync. It crashes and recovers.
- turmoil, madsim, shuttle and loom were considered. They simulate at the async-runtime level. ANVIL is sans-IO, so the simulator drives the exact code that ships, with no runtime shim. Record this comparison in an ADR. Interviewers ask about it.

## Master Preamble (becomes AGENTS.md)

**ANVIL project charter**

ANVIL is a Raft-replicated key-value store in Rust, plus a deterministic simulator that runs the real consensus and storage code against injected network, disk, process and clock faults, plus a linearizability checker that judges every run. Any failure replays exactly from a 64-bit seed. The same consensus core runs in the simulator and in the production server.

**Rules that never bend**

1. Correctness first. Safety is never traded for speed or schedule.
2. Determinism. Given the same seed and plan, a simulation produces a byte-identical trace on every machine and every run, debug or release.
3. Honesty. Never claim a gate passes without running it in this session and pasting the real output into the progress report. Never loosen a test, tolerance, budget or lint to get green. If a deliverable cannot meet its gate, stop and write down why.
4. Ambiguity. If a requirement can be read two ways, stop and list the readings with a recommendation. Do not guess.
5. Final quality now. No placeholder implementations, no TODO or FIXME in merged code, no 'optimize later'. Unfinished work goes in the progress report as an open item with the reason.

**Architecture.** Crates, with dependencies pointing downward only:

- anvil-types: ids, entries, binary codec, error types.
- anvil-raft: sans-IO consensus core. `step(input, &mut output)`. No I/O, no clock, no threads.
- anvil-kv: deterministic state machine, client sessions, snapshot encode and decode.
- anvil-storage: WAL, hard state and snapshot formats plus recovery logic, written sans-IO over an abstract Device (submit operations, receive completions).
- anvil-node: glue that owns RaftCore, the storage engine and the state machine, generic over environment traits. Shared by the simulator and the server.
- anvil-check: history model and linearizability checker.
- anvil-sim: simulator kernel, SimDevice, SimNetwork, nemesis, workloads, invariants, farm, shrinker.
- anvil-device-fs (real-file Device on a dedicated I/O thread), anvil-server (anvild), anvil-client, anvilctl, anvil-viz, anvil-bench, xtask.

Deterministic crates are types, raft, kv, storage, node and check. In them: no std::fs, std::net, std::thread, Instant, SystemTime, OS randomness, HashMap, HashSet, RandomState, floating point, or statics with interior mutability. This is enforced by clippy disallowed\_methods and disallowed\_types, plus a test that fails if their dependency set leaves an allowlist.

Randomness: one u64 master seed. Each component derives its own ChaCha8 stream from the seed and its component path, so adding a component never changes another component's stream. All sampling (ranges, shuffles, Bernoulli, delays) uses ANVIL's own code on top of the raw u64 output. rand\_chacha is pinned with an exact version.

Time: the core counts logical ticks. The environment decides what a tick is.

Plans: fault schedules and workloads are materialized as serializable data (a Plan) before a run starts. A run executes the Plan and never draws from a generator mid-run. This is what makes shrinking possible.

**Code standards**

- Clean code with no unnecessary comments. Names carry the meaning. A comment is allowed only for a non-obvious why (a protocol subtlety, a paper section, a hardware quirk), never for what.
- unsafe is forbidden: `#![forbid(unsafe_code)]` in every crate. An exception needs an ADR and human sign-off.
- No unwrap, expect or panicking index on data that came from outside the process. Typed errors with thiserror. Protocol invariants use `assert!` (not `debug_assert!`) and stay on in release builds. A violated invariant must crash loudly.
- Public items that are not obvious get doc comments. Every non-trivial decision gets a numbered ADR in docs/adr (context, options, decision, consequences).
- Conventional commits, small and reviewable. Tests are written before or with the code, never after.
- Docs read like a careful engineer wrote them: short plain sentences, concrete numbers, no marketing words, no emoji, no filler such as robust, seamless, leverage or cutting-edge.

**Gates that run on every change (`cargo xtask ci`)**

Format check. Clippy with -D warnings and a pedantic subset. cargo nextest. Doc tests. cargo hack feature-powerset check. cargo deny. cargo audit. cargo geiger (zero unsafe). Coverage through cargo llvm-cov with per-crate floors. Determinism gate (golden seed hashes, in-process twice and across processes). Simulator smoke farm (2,000 seeds). Fuzz smoke (60 seconds per target). Instruction-count benchmark regression gate. TLC spec check once the spec exists.

Nightly: 200,000-seed farm, 30-minute fuzz per target, mutation testing report. Weekly and at release: 1,000,000-seed farm.

**Performance doctrine (from the first commit)**

- Budgets live in docs/perf/BUDGETS.md. Measure on day one, set each budget at 80 percent of the measured value, ratchet upward only.
- Hot paths allocate nothing in steady state. Payloads are bytes::Bytes (reference-counted slices), decode is zero-copy, and Output and scratch buffers are reused. Tests with a counting global allocator assert allocation counts.
- Batch everything that crosses a boundary: network frames, WAL appends, fsyncs, applies. Batching is natural (take whatever arrived while the previous operation was in flight) and never adds an artificial delay when idle.
- Never block the consensus thread on disk, network or the state machine.
- Measure before optimizing and keep the evidence (flamegraph, allocation profile, before and after numbers) in docs/perf/.
- Load generators are open-loop with coordinated-omission correction (hdrhistogram). Closed-loop numbers are labelled as such.

**Security doctrine**

- Secure by default: mutual TLS 1.3 required for peers, clients and admin. Plaintext exists only behind an explicit insecure flag that logs a warning on every start and is refused in release builds.
- Authenticate before allocating. Bound every length before reserving memory. Fuzz every decoder.
- Least privilege: separate roles for peer, client-read, client-write and admin, bound to certificate identities.
- No secrets or user data in logs by default. Zeroize key material on drop.
- Supply chain: dependencies pinned by lockfile, cargo deny and cargo audit green, SBOM generated, release artifacts reproducible and signed.
- Every security decision is traced in docs/security/threat-model.md.

**Version pins (crates.io index, 15 Oct 2026; re-check on day one)**

Rust: the current stable that rustup installs on day one, committed in rust-toolchain.toml with edition 2024. Nightly only in jobs that need it (sanitizers, cargo-fuzz).

Libraries: tokio 1.53, tonic 0.14, prost 0.14, rustls 0.23, tokio-rustls 0.26, bytes 1.12, crc32c 0.6, blake3 1.8, rand\_chacha 0.10 (exact pin), thiserror 2, clap 4.6, tracing 0.1, imbl 7, crossbeam-channel 0.5, arc-swap 1.9, zeroize 1.9, chacha20poly1305 0.11, rustix 1.1, hdrhistogram 7.6, proptest 1.12, criterion 0.8, arbitrary 1.5, libfuzzer-sys 0.4, rcgen 0.14 (tests only).

Tools: cargo-nextest 0.9, cargo-deny 0.20, cargo-audit 0.22, cargo-fuzz 0.13, cargo-mutants 27, cargo-llvm-cov 0.9, cargo-hack 0.6, cargo-geiger 0.14, cargo-cyclonedx 0.5, samply 0.13.

Read the docs for the pinned version before using an API. Several of these crates changed their APIs recently, including the tonic code generation crates. Commit Cargo.lock.

**Every week ends the same way**

Write docs/progress/weekN.md: what shipped, each exit-gate command with its real output, benchmark numbers with a hardware description, decisions taken (ADR links), open items, and anything you were unsure about. Then stop.

---

## Week 1 prompt: foundation, simulation kernel, determinism gate, CI

Goal: by the end of this week the repository has the permanent skeleton everything else sits on: the workspace, the quality gates, the types and codec, the deterministic simulation kernel, and a gate that proves determinism. No Raft yet.

Step 0: create AGENTS.md from the Master Preamble, verbatim, with one added line pointing to docs/adr. Run `rustup update stable` and record the version.

**Deliverables**

1. Repository bootstrap. Cargo workspace (edition 2024, resolver 3) with every crate from the preamble as a compiling member. rust-toolchain.toml, .cargo/config.toml, clippy.toml with the disallowed lists, deny.toml, rustfmt.toml. An xtask crate exposing ci, determinism, farm, bench, fuzz, docs and release subcommands; subcommands not yet implemented must fail loudly, never silently pass. Apache-2.0 license, SECURITY.md, CONTRIBUTING.md, CODEOWNERS, CHANGELOG.md, issue and PR templates, and GitHub workflows for PR, nightly and weekly pipelines. ADRs 0001 to 0006: sans-IO core, determinism rules, RNG discipline, crate layering, assertion policy, and prior art (turmoil, madsim, shuttle, loom, FoundationDB, TigerBeetle) with why ANVIL differs.
2. anvil-types. Newtypes NodeId (non-zero), Term, LogIndex, GroupId, ClusterId, SessionId, SeqNo with checked arithmetic and no implicit conversions. Entry and EntryKind with only the kinds that exist this week (Normal, NoOp); later weeks add kinds. Hand-written binary codec: versioned frame header, LEB128 varints, little-endian fixed-width integers, payloads as Bytes sliced without copying, hard length limits checked before allocation, typed decode errors, never a panic. Property tests for round-trip and for 'decoding arbitrary bytes never panics'. cargo-fuzz targets for every decoder with a seed corpus checked in.
3. anvil-sim kernel. SimTime in nanoseconds (u64). Event queue ordered by (time, insertion sequence) so ties are deterministic. Benchmark a binary heap against a hierarchical timing wheel, choose on measurements, record the choice in an ADR. Actor registry. Run loop with caps on virtual time and event count. A watchdog that fails the run if the queue empties early, or if N events pass with no progress marker. DetRng with stream derivation (SplitMix64 over the seed and component path feeding ChaCha8) and own samplers: unbiased range, Bernoulli, exponential, bimodal, shuffle. Trace recorder: compact binary events in a ring buffer plus a streaming blake3 hash of the full event stream. A `buggify!` macro that is constant false unless the sim feature is on; in sim it fires with a per-point probability drawn from a named stream, with a registry that lists every point and counts hits.
4. Kernel validation with a toy replicated-counter protocol that exists only in tests: determinism across runs, tie ordering, time never goes backward, buggify points fire, and the watchdog trips on a deliberately stuck protocol.
5. Determinism gate. `cargo xtask determinism` runs a fixed seed set twice in one process, then again in a fresh process with different environment variables and ASLR on, and compares trace hashes against tests/golden/seed\_hashes.txt. CI runs it on x86\_64 and aarch64 Linux. Include a negative test: a fixture that iterates a HashMap must make the gate fail, and the test asserts that it does.
6. Allocation and performance harness. A counting global allocator for tests. criterion benches for the event queue, DetRng and the codec. Instruction-count benches for the CI regression gate (use a valgrind-based harness; check the current name and version of iai-callgrind or its successor). Measure, then write docs/perf/BUDGETS.md. Starting expectations to beat or explain: at least 10 million event-queue push-pop pairs per second, at least 100 million DetRng u64 per second, and codec decode of 1 KiB entries with zero allocations.
7. CI. Every gate from the preamble wired, working and green on a clean checkout. Nightly and weekly pipelines defined; farm and fuzz jobs may run zero seeds until later weeks, but the jobs exist and are exercised.

**Exit gate**

- `cargo xtask ci` green from a clean clone, output pasted.
- Determinism gate green on both architectures; the negative test passes.
- Every fuzz target ran 10 minutes with no crash.
- Coverage at least 95 percent for anvil-types and at least 90 percent for the sim kernel.
- Kernel throughput measured and reported as simulated events per second on one core.
- Zero unsafe, zero TODO, zero clippy warnings.
- docs/progress/week1.md written.

---

## Week 2 prompt: Raft core and the TLA+ spec

Goal: a complete, specified, thoroughly tested sans-IO Raft core for election and log replication, where every state transition traces back to a TLA+ spec variable.

Step 0: read AGENTS.md and docs/progress/week1.md. Run `cargo xtask ci`. It must be green before you change anything.

**Deliverables** (in this order; the spec comes first)

1. specs/anvil\_raft.tla and its .cfg. Model servers with persistent term, vote and log; volatile role, commit index and progress; messages that can be lost, duplicated and reordered; crash and restart that keep persistent state and lose volatile state; PreVote; CheckQuorum; leader stickiness; the commit rule that counts replicas only for entries of the current term. Invariants: ElectionSafety, LeaderAppendOnly, LogMatching, LeaderCompleteness, StateMachineSafety, CommittedEntriesNeverLost, TermMonotonic. Check with TLC at 3 servers, 3 terms and 3 log entries with symmetry reduction; record states explored and runtime in docs/spec/README.md. Wire TLC into CI with bounds that finish in 10 minutes. Add a negative check: a second config with the current-term commit rule removed must produce the Figure 8 counterexample, and CI asserts that TLC reports a violation.
2. anvil-raft core. Public contract: `RaftCore::new(config, recovered_state, rng_stream)` and `step(input, &mut output)`. Inputs: Tick, Recv(from, message), Propose(command), ReadIndex(context), Persisted(token), Applied(index), TransferLeader(to). Output holds messages to send, persist operations, committed entries to apply, completed reads, role-change events and trace events. Roles: Follower, PreCandidate, Candidate, Leader. Randomized election timeout in ticks, drawn from the node's DetRng stream. The leader appends a no-op on election. Per-follower progress with Probe and Replicate states, an in-flight window (max\_inflight\_messages), a byte cap per message, optimistic next-index advance in Replicate, and conflict hints (conflicting term and first index of that term) so a divergent follower is repaired in one round trip. The commit index advances only on a quorum match for an entry of the current term. Persistence contract: a follower's append response and any vote response are released only after Persisted for the data they depend on; the leader sends AppendEntries in parallel with its own persist and counts its own match only after Persisted; term and vote are persisted before any message that reveals them. Document all of it in docs/design/raft-core.md with a table that maps every spec action to the function implementing it.
3. Online invariant checker in anvil-sim, called after every step of every node: the seven spec invariants plus applied at most committed at most last index, vote never changes within a term, and a global oracle that remembers every committed entry.
4. SimCluster harness for 3, 5 and 7 nodes with an in-memory network that supports drop and delay hooks. The full network model arrives in Week 4.
5. Scenario tests that reproduce, by name, Figures 7 and 8 of the Raft paper, split votes, a stale leader after a partition, log divergence and truncation, leader crash during replication, and the PreVote disruption case.
6. Property tests where proptest chooses the order in which in-flight messages are delivered, dropped and duplicated, driving the core directly with shrinking enabled.
7. Trace events emitted on every transition carry exactly the spec variables (term, role, voted-for, log digest, commit index) in a SpecProjection struct. Week 9 depends on this.
8. Performance. A counting-allocator test proves zero allocations for a follower Tick and for a leader heartbeat, and at most one allocation per entry amortized during replication (assert the measured number and explain it). Benchmark in-memory throughput of a leader with two followers and report entries per second per core.

**Exit gate**

- TLC passes all invariants and the negative config produces the counterexample.
- 100,000 seeds clean on lossy, duplicating, reordering networks at 3, 5 and 7 nodes, invariant checker on.
- anvil-raft line coverage at least 95 percent, and a cargo-mutants run with at least 85 percent of mutants caught. List every surviving mutant and either add a test or justify it.
- Determinism gate green with new golden hashes (explain the hash change in the commit message).
- docs/progress/week2.md written.

---

## Week 3 prompt: storage engine, simulated disk, crash recovery

Goal: durable storage that survives the faults real disks produce, with one recovery implementation that runs identically against a simulated disk and real files.

Step 0: read AGENTS.md and docs/progress/week2.md. Run `cargo xtask ci`. It must be green before you change anything.

**Deliverables**

1. docs/storage-fault-model.md, written before any code. List what ANVIL assumes and what it defends against: a crash that loses unsynced data; unsynced writes reaching disk in any order and any subset; torn writes at sector granularity (sector size recorded in the on-disk header, tested at 512 and 4096); misdirected writes; bit rot and corruption on the read path and on the write path; latent sector errors (EIO on read); ENOSPC; fsync failure; fsync stalls lasting seconds (gray failure). An optional unsafe class, lying fsync, exists only to prove detection. State the survivability rule: the injector never destroys every good copy of a committed entry on a majority of nodes, because that scenario has no correct outcome. The checker verifies safety, not miracles.
2. On-disk formats in docs/formats/ with byte layouts. WAL segments: header with magic, version, sector size, segment id, first index and previous term; frames aligned to the sector size, zero padded, padding verified on read; each record carries index, term and length plus a CRC32C that is chained from the previous record and also covers segment id and file offset, so a record copied elsewhere fails. Hard state: two alternating slots with sequence number and CRC, so one update is atomic without a rename. Snapshot files: temporary name, fsync, rename, directory fsync, chunk CRCs, whole-file blake3 footer.
3. anvil-storage engine, sans-IO over an abstract Device: it submits DeviceOps and consumes DeviceCompletions. Group commit with natural batching: while one fdatasync is in flight, everything that arrived is coalesced into the next one. Completion tokens come back in submission order and feed RaftCore's Persisted input. Segment preallocation and rotation, retention after a snapshot, directory fsync after every create, rename and unlink.
4. Recovery with explicit classification. Clean. TornTail: an invalid record with nothing valid after it, truncate and continue. MidLogCorruption: an invalid record followed by valid ones, so the node is marked NeedsRepair from that index, does not vote or lead, and is repaired from peers in Week 6. HardState with one bad slot: use the other. Both slots bad: refuse to start; the node must be replaced under a new node id (document why: a node that forgot its vote can vote twice in one term).
5. fsync policy: any fsync error crashes the process. Never retry. In the sim, fsync error injection makes the node crash and recover.
6. SimDevice in anvil-sim. A volatile page cache over a durable image per file; directory entries with their own durability; asynchronous completions with latency distributions; a crash that persists each unsynced sector independently (persisted, lost, or old content); write reordering before fsync; faults from deliverable 1; and a storage fault atlas that enforces the survivability rule. Corruption rates configurable up to several percent per read and per write.
7. FsDevice in anvil-device-fs: a dedicated I/O thread, std::fs plus rustix for fallocate and fsync, no unsafe. Differential test: identical operation sequences against SimDevice with faults off and FsDevice must give identical logical results.
8. Node integration. Crash and restart actions in SimCluster recover through the real engine. New invariants: CommittedNeverLost across crashes, VoteNeverForgotten, and the recovered log equals what was acknowledged plus anything later that survived.
9. Fuzzing and properties. cargo-fuzz for the WAL, hard-state and snapshot readers (never panic, never accept corrupt data as valid). proptest with random operation sequences and random crash points, checking that every fsync-acknowledged write survives.
10. Performance. Record the raw fdatasync rate and the sequential sync-write ceiling measured with fio on the dev machine in docs/perf/. WAL append throughput at batch sizes 1, 16 and 256 must reach at least 80 percent of that ceiling. The append path allocates nothing in steady state. Recovery scan runs at least 1 GB/s with hardware-accelerated CRC32C.

**Exit gate**

- 100,000 crash-and-corruption seeds clean, all storage faults on, network lossy.
- Fuzz targets ran 30 minutes each with no crash; corpus checked in.
- anvil-storage coverage at least 95 percent; mutation run reported as in Week 2.
- Differential test green; benchmark table with a hardware description.
- docs/progress/week3.md written.

---

## Week 4 prompt: network model, KV state machine, sessions, clients, linearizability checker

Goal: a complete replicated KV store running under a full network fault model, with clients whose histories are judged by a linearizability checker you can trust.

Step 0: read AGENTS.md and docs/progress/week3.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. SimNetwork. Per-link latency distributions (constant, uniform, exponential tail, bimodal), per-link bandwidth caps with queueing, and these faults: drop, duplicate, reorder, delay spikes, partitions (random bisection, isolate one node, bridge node, asymmetric one-way, flapping), connection resets. Two transport modes, both used in every farm configuration: ordered-lossy (TCP-like, per-connection FIFO, a reset drops in-flight messages and forces reconnect with backoff) and unordered (UDP-like, to stress the protocol). Corruption is modeled as drop plus reset, because TLS authenticates every frame. Process faults: crash, restart delay, pause with stale timers firing on resume, and per-node tick-rate skew (0.5x to 2x) to model clock drift.
2. anvil-kv. Commands: RegisterSession, Put, Delete, Cas (expected version), Append (list semantics, Elle-style), Range (bounded, with limit), ClockTick, ExpireSessions. Reads go through the read path, not the log. Keys at most 1 KiB, values at most 1 MiB, both configurable. State in imbl::OrdMap so a snapshot is an O(1) clone. Per-key version is the log index of the last write. A rolling state hash over applied commands lets replicas be compared. Apply is fully deterministic: time enters only through ClockTick entries that the leader proposes periodically.
3. Sessions, as in chapter 6 of the Raft thesis. RegisterSession returns a SessionId equal to its log index. Every request carries (session, seq, acked\_up\_to). The state machine keeps the response for each outstanding seq and returns it on a duplicate instead of re-executing. Sessions expire after a configurable virtual idle time through ExpireSessions, with bounded memory. A client with an expired session gets SessionExpired and never a silent re-execution.
4. Read paths. Leader ReadIndex (a heartbeat round confirms leadership). Follower ReadIndex (the follower asks the leader for the read index and waits until it has applied that far). Lease reads, off by default, safe only under a configured drift bound, with the lease shorter than the minimum election timeout by that bound. A stale local read exists as an explicit non-linearizable API and is excluded from checking.
5. anvil-node. The glue from Week 3 becomes the full node: recover, step, execute effects, apply, answer clients. Written against environment traits so the simulator and the server share it.
6. Workload generator with swarm profiles. Each seed picks client count (1 to 64), key space (1 to 1,000 keys, small spaces force contention), operation mix, value sizes, think times, timeout and retry policy (idempotent retries through sessions), client crashes, and redirect handling. Clients record a history of invoke and return events with virtual time. Timed-out operations are recorded as indeterminate.
7. anvil-check. Wing-Gong-Lowe search with memoization on (bitset of linearized operations, model-state hash) and P-compositionality (partition by key). Single-threaded and deterministic; the farm parallelizes across seeds. Models: register with Put, Get, Delete, Cas; and list-append with Append and Get. Indeterminate operations may linearize at any point after their invoke, or never. Range operations are checked against a whole-store model on bounded histories (at most 300 operations) in dedicated runs. On failure, output a minimal non-linearizable sub-history plus the longest linearizable prefix and candidate partial orders, as JSON for the Week 10 visualizer.
8. Checker validation, because a wrong checker is worse than none. A brute-force permutation checker for histories of at most 8 operations, compared with the real checker on 1,000,000 random tiny histories. A generator of random linearizable histories from a sequential model with known verdicts. Mutators that corrupt one read to make a history non-linearizable. Cross-check against Porcupine (Go) through a JSON export on at least 10,000 histories, as a nightly CI job.
9. After each run's heal phase, a convergence audit: every live replica has the same applied index and the same state hash.
10. Performance. The checker handles a 100,000-operation, 100-key history in under 2 seconds and a 1,000,000-operation history in bounded memory, with early exit on the first violation. Report simulated seconds per real second for a 3-node cluster with 16 clients; the expectation is at least 200 times real time on one core, and any miss needs a profile.

**Exit gate**

- 100,000 seeds clean with every fault class implemented so far turned on (network, process, clock skew, disk).
- Canary bug: temporarily remove the heartbeat confirmation from ReadIndex. The checker must catch the stale read within 1,000 seeds. Then remove the canary. This proves the whole loop works before the full mutant library in Week 6.
- Checker validation suite green, including the Porcupine cross-check.
- Coverage at least 95 percent for anvil-kv and anvil-check; mutation scores reported.
- docs/progress/week4.md written.

---

## Week 5 prompt: snapshots, nemesis, farm, shrinker

Goal: log compaction and snapshot transfer, plus the testing machinery that makes ANVIL a simulator and not just a test suite: plan-based nemesis, a parallel seed farm, a shrinker and coverage tracking.

Step 0: read AGENTS.md and docs/progress/week4.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. Snapshots and compaction. The state machine produces a snapshot at applied index i from an O(1) clone of the persistent map and serializes it in chunks off the consensus path. It includes the session table, the config and the state hash. Triggered by entries or bytes since the last snapshot. The log prefix up to snapshot index minus a retained trailing window is dropped only after the snapshot is durable. Never snapshot beyond the applied index.
2. InstallSnapshot as a chunked stream with a flow-control window, resumable at chunk granularity, with chunk CRC and final blake3 verification. Aborted and restarted on leader change. A follower installs atomically and then truncates its log. The leader prefers the log over a snapshot whenever the follower is reachable by log. Add the Snapshot progress state to the replication progress machine. New invariant: SnapshotEqualsReplay, meaning a snapshot at index i has the same state hash as replaying the log to i.
3. Leader transfer through TimeoutNow. The leader stops accepting proposals during a transfer and aborts if it has not completed within one election timeout. Follower reads use a bounded wait queue.
4. Plan-based nemesis. A Plan is serializable data: cluster config, a list of timed fault events with parameters, and the client operation list. Generation uses swarm testing: each seed enables a random subset of fault classes at random intensities. At least 30 percent of seeds enable only one or two classes, because single-fault runs find different bugs, and some seeds run everything at once. Every run has four phases: warm-up with no faults, chaos, heal with no faults and a bounded convergence time, final audit. The fault vocabulary covers network, process, clock and disk faults from Weeks 3 and 4.
5. Liveness checks after heal: a leader exists within 10 times the maximum election timeout, and a fresh client write commits within a bounded virtual time. Anything else is a violation with diagnostics.
6. Farm. `anvil-sim farm --seeds A..B --jobs N --profile NAME`. Each seed is isolated and single-threaded; parallelism is across seeds. JSONL results, failing seeds saved with their Plan and trace, resumable runs, a continuous time-boxed mode for nightly, and a summary: seeds run, simulated hours, faults injected by class, buggify hit counts, invariants evaluated.
7. Shrinker. Given a failing Plan, delta-debug it: remove fault events, shorten durations, drop clients and operations, reduce cluster size where allowed. Every candidate is re-run and must keep the same failure class (same invariant or violation kind). This requires per-event RNG streams keyed by event id so removing one event does not reshuffle the others. Output the minimal Plan JSON and a repro command `cargo xtask repro plan.json`. Test it on the Week 4 canary: it must reduce to at most 30 events.
8. Coverage that means something. A `sometimes!(name)` macro marks rare protocol paths (snapshot install, leader transfer, term bump during replication, repair, joint config, torn-tail recovery, session expiry, and so on). The farm fails if any registered sometimes-point is never hit across 100,000 seeds. The farm report lists never-hit buggify points and unreached protocol states.
9. A text trace dump CLI for debugging until the visualizer exists in Week 10.
10. Performance. Report seeds per second per core on the smoke profile. Snapshot clone of a 1,000,000-key map must stay under a millisecond; chunking must not stall the consensus path (assert with a virtual-time budget in the sim).

**Exit gate**

- 500,000 seeds clean across all profiles with all faults enabled.
- Every sometimes-point hit; the report is in docs/progress.
- The shrinker reproduces and minimizes the canary; the repro command works from a clean checkout.
- SnapshotEqualsReplay checked after every snapshot in every run.
- Coverage at least 95 percent for anvil-raft and anvil-node; mutation scores reported.
- docs/progress/week5.md written.

---

## Week 6 prompt: membership, repair, mutant library

Goal: safe cluster membership changes, repair of corrupted replicas, and a library of planted bugs that proves the simulator catches real consensus mistakes.

Step 0: read AGENTS.md and docs/progress/week5.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. Joint consensus and learners. Config entries EnterJoint and LeaveJoint (the leader leaves the joint config automatically once it commits), non-voting learners, and joint quorums that require a majority of the old config and a majority of the new. A config takes effect when appended, not when committed. A config change is refused if one is still uncommitted or if the leader has not yet committed an entry in its own term. Snapshots carry the config. A removed leader steps down after LeaveJoint commits and hands leadership off. Removed or unknown servers cannot disrupt the cluster (PreVote, CheckQuorum, and rejecting messages from nodes outside the config).
2. Node lifecycle. NodeId and ClusterId are verified on every message. Bootstrap writes an identical initial config entry at index 1 on every founding node. A node with empty storage and no initial config waits to be added. Adding a node is: add as learner, catch up by log or snapshot, promote when within a configured lag for a configured number of ticks (explicit MemberPromote also exists). Replacing a node whose storage is lost means a new NodeId.
3. specs/anvil\_reconfig.tla: joint consensus and learners. Invariants: ElectionSafety under reconfiguration, serialized config changes, quorum overlap of the joint config with both the old and the new config, LeaderCompleteness. Bounded model of 4 servers (3 plus 1 joining), 2 terms, a short log. Negative config: allow two concurrent config changes and TLC must produce a counterexample. If TLC cannot finish within practical limits, document the bounds and use simulation mode for the larger configs. Say so plainly in the spec README.
4. Repair (protocol-aware recovery). A NeedsRepair node does not vote, does not campaign, and answers appends as if its match index were the last good index. It fetches the missing range from peers, validating term, CRC and log matching, then clears the flag. Apply the same path to a node with a bad snapshot.
5. Reconfiguration nemesis. Random add, remove, replace and transfer under faults; changes during partitions; removing the leader; concurrent change requests rejected with a clear error. The invariant oracle uses the config in effect at each index.
6. Mutant library: 20 planted bugs behind a `mutants` cargo feature, each with a stable id. Use these: (M01) commit previous-term entries by counting replicas; (M02) vote response sent before hard state is persisted; (M03) follower acks an append before it is persisted; (M04) leader counts its own match before it is persisted; (M05) ReadIndex skips leadership confirmation; (M06) lease read ignores the drift bound; (M07) conflicting suffix not truncated correctly; (M08) uncommitted entry applied; (M09) snapshot index off by one; (M10) InstallSnapshot does not restore sessions; (M11) vote not restored after restart; (M12) config change allowed before a current-term commit; (M13) joint quorum uses OR instead of AND; (M14) session dedup cache evicted too early; (M15) torn tail accepted as valid or mid-log corruption silently truncated; (M16) fsync error retried; (M17) leader does not step down without CheckQuorum; (M18) PreVote bypass causes disruption; (M19) removed node keeps campaigning; (M20) repair accepts an unverified entry.
7. Kill matrix. Every mutant must be detected by the farm within 10,000 seeds (target: median under 1,000). Produce docs/mutants.md with seeds-to-detect and which detector caught each (invariant, checker or liveness), reproducible with `cargo xtask mutant ID`. Negative control: with no mutants enabled, 1,000,000 seeds stay green. The `mutants` feature must be impossible to enable in anvild; CI checks the server's dependency graph.
8. Performance. Report membership change latency in the sim and on the node API. No regression against the Week 5 budgets.

**Exit gate**

- All 20 mutants killed within bound; matrix committed.
- Reconfig spec results documented, including the negative config.
- 500,000 seeds clean with the reconfiguration nemesis on.
- anvil-raft coverage at least 95 percent and mutation score at least 88 percent.
- docs/progress/week6.md written.

---

## Week 7 prompt: production runtime

Goal: a real server, `anvild`, that runs the exact consensus core the simulator tests, with a fast network path, a client API, observability, and a way to prove the production core behaves deterministically.

Step 0: read AGENTS.md and docs/progress/week6.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. ADR first: the threading model. One consensus thread owns the anvil-node state and never blocks. One storage thread runs the Device with group commit. One apply thread runs the state machine and snapshot creation. A tokio multi-thread runtime handles only the network and gRPC. Threads talk over bounded channels with explicit backpressure. The consensus thread drains all pending inputs per loop iteration and emits outputs in batches. Optional CPU pinning for the consensus thread through safe rustix wrappers.
2. One code path. The server contains no consensus logic. It supplies a RealEnv (ticks from a monotonic clock, entropy from the OS) to anvil-node. A test fails if anvil-server reaches into anvil-raft internals beyond the public node API.
3. Peer transport. TCP plus rustls with mutual TLS, a custom binary protocol on the anvil-types codec, a handshake Hello carrying cluster id, node id, protocol version and group ids. Length-prefixed frames, several messages per frame, TCP\_NODELAY, bounded per-peer outbound queues where a newer heartbeat supersedes an older one, jittered exponential reconnect backoff. Two connections per peer pair: a priority connection (votes, heartbeats, small appends) and a bulk connection (snapshots), so a snapshot never delays a heartbeat. Every frame header carries a group id; only group 0 is used, which keeps a future multi-group extension cheap.
4. Client API over gRPC (tonic, proto under proto/anvil/v1). KV service: RegisterSession, Put, Get (consistency levels linearizable, follower-linearizable, stale), Delete, Cas, Append, Range. Cluster service: Status, MemberList, MemberAdd, MemberPromote, MemberRemove, TransferLeader, Snapshot. Health. Errors map to gRPC codes with leader hints (NOT\_LEADER carries the leader address), SessionExpired and ResourceExhausted. The leader batches proposals per loop iteration with no timer, caps batch bytes, and rejects when pipeline depth is exceeded.
5. anvil-client library: session management, leader discovery, retries with jittered backoff, idempotent retry through sessions, per-request deadlines. `anvilctl` is a thin CLI over it.
6. Config. TOML with unknown fields denied, environment overrides, documented defaults in docs/ops/config.md, and `anvild --check-config`.
7. Data directory lifecycle. A lock file prevents two processes sharing one directory. Format version check at startup. SIGTERM drains, fsyncs and exits. SIGKILL is a supported shutdown path (crash-only design). Startup reports its recovery time.
8. Observability. Structured tracing logs (JSON option) that contain no keys or values by default. A Prometheus endpoint on the admin listener: proposals, commit latency histogram, fsync latency histogram, batch sizes, apply lag, peer RTT, queue depths, term, role, leader changes, snapshot and recovery and repair stats. Health and ready endpoints (ready means in contact with a quorum). A Grafana dashboard JSON.
9. Record and replay. `--record-inputs DIR` makes the consensus thread log every input in order. `anvil-sim replay-inputs` feeds the log to a fresh node on a SimDevice and asserts the outputs (messages and persist operations) are identical. This proves the core is deterministic on real traffic.
10. Integration tests with real 3-node and 5-node clusters as local processes using certificates generated by rcgen: kill and restart, membership changes through the API, client retry behavior, and a rolling restart during which no linearizable request fails permanently.
11. First benchmark baseline with the Week 8 harness skeleton, written to docs/perf/. Numbers only, no tuning yet.

**Exit gate**

- All integration tests green; record and replay equality holds on a 10-minute loaded run.
- The server dependency graph has no mutants feature.
- Coverage at least 90 percent for anvil-server excluding generated code; no unsafe; no unwrap on external data.
- docs/progress/week7.md written with the baseline numbers.

---

## Week 8 prompt: security hardening and performance engineering

Goal: security and performance reach their final form this week. Both are measured, documented and gated.

Step 0: read AGENTS.md and docs/progress/week7.md. Run `cargo xtask ci`. It must be green first.

**Security deliverables**

1. docs/security/threat-model.md using STRIDE for each component: peer link, client API, admin API, disk, snapshots, build pipeline. List assets, trust boundaries, attacker capabilities and the mitigation for each threat, with a pointer to the test that proves it.
2. mTLS. TLS 1.3 only, rustls with a single crypto provider chosen in an ADR (ring or aws-lc-rs) and no OpenSSL anywhere. Certificate identity through SAN URIs: `anvil://cluster/ID/node/ID` for peers and `anvil://cluster/ID/role/ROLE` for clients (client-read, client-write, admin). The handshake rejects a certificate whose node id differs from the Hello node id, a wrong cluster id, an expired certificate. Hot reload of certificates (file watch and SIGHUP) without dropping healthy connections. A rotation test and a certificate-expiry metric with a warning log.
3. Authorization. Per-RPC role checks happen before any allocation or parsing beyond the header. The admin listener is separate from the client listener.
4. Resource limits. Max frame size, max key and value size, max in-flight requests per connection, per-identity token-bucket rate limits, max sessions per identity, connection limits, handshake and idle timeouts, slow-reader protection through bounded outbound queues, a snapshot transfer rate limit, and a global byte budget on pending proposals with load shedding. Tests: slowloris, oversized frames, malformed handshakes, replayed frames, a certificate from another cluster, an expired certificate, a wrong node id, and a rotated-out certificate.
5. At-rest encryption (first thing to cut if time runs out). XChaCha20-Poly1305 on WAL record payloads and snapshot chunks. Per-segment data keys wrapped by a key-encryption key from a KeyProvider trait (file provider shipped). Nonces are random per record from a NonceSource trait (OS entropy in the server, DetRng in the sim), stored in the frame, never derived from the index, because a truncated and rewritten index must not reuse a key and nonce pair. Associated data is segment id, offset, index and term. CRC is computed over the ciphertext. Key rotation starts a new segment under a new data key. The sim farm runs in both modes.
6. Fuzzing the network surface with structure-aware inputs (arbitrary): handshake parser, frame decoder, peer message decoder, gRPC request validation, config parser. One hour per target nightly.
7. Supply chain. cargo deny with a license allowlist and bans, cargo audit, SBOM per release (cargo-cyclonedx), reproducible release builds (locked, remapped paths, pinned toolchain, SOURCE\_DATE\_EPOCH) built twice in CI with identical hashes required, signed artifacts, build provenance attestations through GitHub, `panic = abort` in release, zero unsafe in first-party code. A minimal non-root container image with a read-only root filesystem and no shell, and a hardened systemd unit (NoNewPrivileges, ProtectSystem=strict, and related options).

**Performance deliverables**

1. docs/perf/methodology.md: hardware and kernel description, CPU governor set to performance, fio and loopback baselines, and reproducible scripts under `cargo xtask bench`. The load generator in anvil-bench is open-loop: constant arrival rate, latency measured from the scheduled send time, hdrhistogram recording, warmup discarded, runs of at least 60 seconds. Workloads: YCSB-style A (50/50), B (95/5), C (read-only), write-only; value sizes 128 B, 1 KiB, 16 KiB; client concurrency sweep. Output throughput-latency curves as CSV and SVG.
2. Profile before tuning. Flamegraphs with samply or perf for every workload, an allocation profile, syscall counts per write, and the fsync-per-commit ratio. Fix the top five hotspots with before and after numbers recorded in docs/perf/.
3. Evaluate each of these with data and adopt only what measurably helps: proposal batching, pipelined AppendEntries, group commit tuning, encoding an entry once and sharing the Bytes across followers, vectored writes, no copy from gRPC buffers to the log, batched apply, one ReadIndex round serving many reads, response batching, thread pinning, an alternative allocator, O\_DIRECT with preallocated segments, and io\_uring for the WAL. io\_uring needs unsafe at its edges, so it requires a demonstrated gain of at least 20 percent, an ADR, and isolation in one small audited crate.
4. Aspirational floors on the 3-node localhost setup with NVMe: at least 50,000 durable writes per second (128 B values, 256 clients) with p99 under 10 ms at 70 percent of maximum load; at least 200,000 linearizable reads per second; failover to serving writes within 2 election timeouts; recovery of a 1 GB WAL in under 3 seconds. Report an efficiency number too: writes per second divided by the fsync ceiling times the average batch size. Explain every miss with evidence.
5. etcd comparison on the same machine, same workload, same fsync settings, a pinned etcd version checked on the day. Report the result honestly in either direction, with caveats, and say where ANVIL is slower and why.
6. Regression gates. Instruction-count benchmarks for hot functions in CI. A nightly wall-clock run on a pinned machine that alerts on a 5 percent regression.
7. Memory. Bounded under overload, RSS versus dataset size, snapshot memory overhead from structural sharing. The 24-hour leak soak is in Week 9.

**Exit gate**

- Threat model complete with every mitigation mapped to a passing test.
- All network fuzz targets ran one hour with no crash; corpora checked in.
- cargo deny and cargo audit green; two reproducible builds with identical hashes; SBOM and provenance generated.
- Performance report with throughput-latency curves, hardware description, etcd comparison, and updated budgets.
- docs/progress/week8.md written.

---

## Week 9 prompt: real-world chaos, formal conformance, mutation testing, soak

Goal: prove that what the simulator says is true holds on the real binary, that the implementation conforms to the spec, and that the system survives long runs.

Step 0: read AGENTS.md and docs/progress/week8.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. Chaos harness under chaos/ driven by `cargo xtask chaos`. A 5-node cluster as processes in Linux network namespaces (bridge plus veth pairs) or Docker Compose, chosen in an ADR; prefer namespaces for speed and tc access. Faults: `tc netem` (delay, jitter, loss, duplication, reorder, corruption, rate limits); nftables partitions including asymmetric and bridge; `kill -9`; SIGSTOP and SIGCONT pauses; restarts; clock offsets through time namespaces (monotonic offsets, Linux 5.6 or newer) and libfaketime for wall clock; disk faults through LazyFS (lose unsynced writes, torn writes, crashes timed around fsync and rename); dm-flakey and dm-delay in a VM with loop devices (document that this needs root); a tiny tmpfs for ENOSPC. The real-world nemesis is generated from a seed. Outcomes are not deterministic, but the schedule is logged and reproducible.
2. Real histories. Clients record invoke and return events with CLOCK\_MONOTONIC\_RAW on a single host. Feed the histories to anvil-check with the register and list-append workloads. Run at least 1,000 chaos runs of 60 to 300 seconds across nemesis profiles with zero violations. Triage any violation and try to reproduce it in the sim by translating the real fault timeline into a Plan.
3. Fidelity study. Take 20 real fault timelines, run equivalent Plans in the sim, and compare observable behavior classes (leader changes, unavailability windows, recovery times). Write the gaps honestly in docs/fidelity.md. Also run the Week 7 input record and replay check on chaos runs.
4. TLA+ trace validation. The sim exports SpecProjection traces (built in Week 2). Generate a Trace.tla per run using the technique MongoDB and Microsoft CCF describe: map implementation states to spec states and let TLC check that the trace is a legal behavior of the spec. Validate at least 1,000 election and replication traces of up to 300 steps and at least 200 reconfiguration traces. Triage every rejection: fix the code, fix the spec, or document the abstraction. Produce a spec action coverage report showing every action was exercised. If you are blocked for two days, ship the pipeline for election and replication only and write down what blocked you.
5. Mutation testing with cargo-mutants on anvil-raft, anvil-storage, anvil-kv, anvil-check and anvil-node. At least 92 percent caught in raft, storage and check and at least 90 percent in the others. Triage every survivor: add a test or justify it in docs/mutation-survivors.md.
6. Soak. At least 24 hours (72 if the hardware allows) of a 3-node real cluster under continuous open-loop load at 50 percent of capacity with periodic faults. Track RSS, file descriptors, disk usage, p99 drift and leader changes. Required: no memory growth trend, bounded disk through snapshots and log compaction, no latency drift. Graphs in docs/perf/soak.md.
7. Compatibility. Freeze format v1. Commit golden WAL, hard-state and snapshot fixtures that every future build must read. Test wire protocol version negotiation. A rolling upgrade test between two builds. Write docs/compat.md.
8. Sanitizers and concurrency checks. TSan and ASan on nightly for the server integration tests (dependencies contain unsafe even though first-party code does not). Miri on the pure crates where feasible. loom tests for any synchronization primitive written in-house; if the channel-based design left none, say so.

**Exit gate**

- 1,000 or more real chaos runs, zero checker violations, every anomaly triaged.
- Trace validation pipeline running in CI nightly; rejection triage written up.
- Mutation scores at or above the floors.
- Soak report with graphs; golden compatibility fixtures committed.
- docs/progress/week9.md written.

---

## Week 10 prompt: visualizer, documentation, final audit, release

Goal: package the project so that a stranger can understand it in ten minutes, trust it in an hour, and so that the only commits allowed after this week are bug fixes with a regression seed.

Step 0: read AGENTS.md and docs/progress/week9.md. Run `cargo xtask ci`. It must be green first.

**Deliverables**

1. anvil-viz. One self-contained HTML file with the trace embedded, no external requests, works from file:// offline. Views: a cluster timeline with a lane per node showing role and term as colored bands, crash, restart and pause markers, message arrows with drop, duplicate and delay glyphs, partition shading and a fault overlay; a scrubbable time slider with play, pause and speed; a per-node inspector at time t (term, vote, log with the committed marker, commit and applied indexes, progress table when leader, config); an event list with search and filters by node, term, index and message kind; a counterexample panel for linearizability failures (per-key operation bars with linearization points, as Porcupine does) and for invariant failures (which invariant, which nodes, the minimal event window). Handles 200,000 events through canvas rendering and virtualized lists. Keyboard shortcuts, light and dark themes, accessible contrast. Deterministic export: the same seed yields identical bytes. CLI: `anvil-sim run --seed N --viz out.html` and `anvil-sim repro plan.json --viz out.html`. A 100,000-event trace loads in under 2 seconds. Smoke test in Chromium and Firefox with Playwright.
2. Documentation site with mdBook: overview; quick start (five commands to run a simulation and a cluster); architecture; determinism guide (the rules, how to add a component, how to debug a nondeterminism failure); simulator guide (fault model tables, Plan format, nemesis, swarm testing, farm, shrinker, sometimes-points); checker guide; consensus design with the spec mapping table; storage design and on-disk format specs; wire protocol spec; API reference from the protos; security (threat model, hardening guide, certificate setup); operations runbook (deploy, config, metrics and alerts, backup and restore through snapshots, replacing a node, upgrades, disaster recovery, troubleshooting by symptom); performance (methodology, results, how to reproduce); testing (every gate explained); limitations and non-goals stated plainly (single group, no multi-key transactions, no watch, no TTL leases, whatever else is true); glossary; ADR index. Doc tests, a link checker in CI, `cargo doc` clean with warnings denied.
3. Final farm. At least 1,000,000 seeds across all profiles on the final commit hash with zero failures, using every fault class including reconfiguration and encryption on and off. Publish seeds run, simulated years, faults injected by class, sometimes-point coverage and the mutant kill matrix; archive the JSON with the release. If a failure appears, fix it and rerun the entire count. No cherry-picking.
4. College report. Ask me for the lab's report template and follow it exactly if I provide one. Otherwise use: abstract, introduction, background (map to the DCS syllabus: election algorithms, logical time, replication and consistency, fault tolerance and recovery, RPC and IPC), related work (FoundationDB, TigerBeetle, Jepsen, etcd, the Raft thesis), design, implementation, testing methodology, evaluation with charts, security, limitations, conclusion, references. Figures are generated by scripts so they reproduce. Build with `cargo xtask report`. Also write docs/demo/script.md: an 8-minute demo (run the farm, a planted bug found by seed, the shrinker, replay in the visualizer, real-cluster chaos with LazyFS, the throughput-latency curve).
5. Release engineering. Tag v1.0.0 with a changelog, signed linux binaries (x86\_64 and aarch64), SBOM, provenance, checksums and the container image. README with CI and coverage badges and a results table filled with the real numbers.
6. Final audit. `cargo xtask audit` checks mechanically: no TODO or FIXME, no unsafe in first-party code, no unwrap or expect on external data (clippy unwrap\_used and expect\_used), every public item documented, every ADR referenced, golden hashes unchanged since Week 9 unless justified, performance budgets met, coverage floors met, all mutants killed, links valid, spec CI green, fuzz corpora committed, dependency checks green. Then write docs/limitations.md and docs/retrospective.md in plain language.
7. Interview kit under docs/interview/. Forty questions with precise answers, for example: why sans-IO, how determinism is enforced and tested, how the shrinker works, how the checker treats indeterminate operations, what fsyncgate is and how ANVIL responds, why joint consensus, how PreVote prevents disruption, where ANVIL is slower than etcd and why, what the simulator cannot catch. Three resume bullet variants, a 2-minute pitch and a 10-minute pitch. Every number comes from the final reports. Invent nothing.
8. Repo polish. README structure, architecture diagram as SVG or Mermaid, issue templates, repository topics, consistent naming.

**Exit gate**

- `cargo xtask audit` green, output pasted.
- Final 1,000,000-seed farm clean on the tagged commit.
- Release artifacts verified (checksum, signature, attestation) from a clean machine.
- Docs build with no broken links; visualizer smoke test passes in both browsers; report PDF built.
- docs/progress/week10.md written.

---

## After Week 10

The project is done when every box above is checked with real output behind it. From then on the only allowed changes are bug fixes, and every bug fix ships with a regression seed or a regression test that failed before the fix.

For your resume, use only numbers from the Week 10 reports: seeds simulated, mutants caught, linearizability violations found and fixed during development, benchmark results with the hardware named, and chaos runs completed. A claim you cannot reproduce with a command is a claim an interviewer will find.
