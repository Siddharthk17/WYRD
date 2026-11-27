# ADR 0027: Week 5 sometimes-points, transfer events, and peer repair

## Status

Accepted.

## Context

Week 5 deliverable 8 asks for a `sometimes!(name)` macro marking rare
protocol paths, with the farm failing when a registered point is never hit
across 100,000 seeds. The same week needs a way to execute leader transfer
under faults (nothing triggers it otherwise) and a liveness gate that
passes on storage-fault profiles. Three latent bugs surfaced while proving
the gate.

## Decision

- `sometimes!(sink, name)` takes an explicit `&mut Vec<&'static str>`
  sink carried through sans-IO outputs, not a bare name. The charter bans
  statics with interior mutability in deterministic crates, so there is no
  global registry to record into; the single-argument form cannot exist
  there. The macro lives in `anvil-types` so every crate shares one.
- The registry is the farm: each seed run drains its hits into the seed
  outcome, and the summary merges them. `SOMETIMES_INVENTORY` lists every
  registered point; a source-scan test fails the build when a site is
  missing from the inventory or vice versa. `anvil-sim farm` reports the
  table and `--require-sometimes-coverage` exits nonzero on a miss. The
  flag is for 100k-plus evidence runs, not small smoke runs.
- Registered now: snapshot send start, snapshot resend, snapshot install
  complete, transfer TimeoutNow sent, transfer aborted, term bump on
  append, client crash, transfer begin, simulated transfer begin,
  storage torn tail, storage mid-log, peer repair. Session expiry, joint
  config, and repair completion join in Week 6 with the code that
  triggers them; registering unhittable points would fail the gate by
  construction.
- Transfers are plan events (`Transfer{node}`, workload runs only, drawn
  from an independent `sim/plan/transfer` stream so consensus plans and
  existing event parameters never shift). About 30 percent of workload
  seeds hand leadership over mid-chaos; aborts on partition are expected
  and also covered.
- Repair is the Week-5 subset of Week-6 D4: rewrite the corrupt WAL frames
  from a committed peer entry (branch truth, never a fork), re-chain CRCs,
  verify the layout by re-scanning, then let the normal restart path
  re-derive a clean node. Membership-driven replacement, the bad-snapshot
  path, and reconfig-time repair stay in Week 6.
- The storage fault atlas now enforces a per-index corruption budget (at
  most N-Q nodes may lose the same index) on top of the committed-copy
  quorum rule, and crash-time fate plus whole-file directory loss consult
  it. Previously the write path could corrupt an entry on two nodes before
  it committed, and crash fate ignored the atlas entirely; both left a
  single surviving copy and no electable quorum.

## Consequences

- Three latent bugs died proving the gate: the follower serving install
  gaps (wait on applied, not just commit), the core appending stale log
  suffixes below the compacted base, and the snapshot supplier labeling a
  stream past a trailing NoOp. Each had existed harmlessly until snapshots
  changed the timing.
- Double-sideline seeds that used to halt the farm now repair and elect
  within the liveness bound. Seeds whose corrupt range no live peer and no
  committed state can supply stay sidelined by design; the liveness
  message carries per-node states for triage.
- Crash-time protection and the repair path are deterministic: no RNG
  draws, only seeded generation and content hashes.
