# ADR 0038: Hand-Authored Mutant Library

## Status

Accepted.

## Context

Week 6 needs twenty planted bugs with stable ids, each killed by a farm or
test within 10,000 seeds. General mutation tooling cannot run on this host:
`cargo-mutants` copies the multi-gigabyte target tree before starting and
has previously been interrupted. The mutants must also stay out of the
production server, and the simulator's determinism must not depend on any
external entropy.

## Decision

- `anvil-types::mutants` owns `MutantId` (M01..M20) and a `mutants` feature.
  Every crate with a planted bug enables `anvil-types/mutants` through its
  own `mutants` feature. `anvil-sim/mutants` enables all of them at once.
- The active mutant is a single `AtomicU8`, set once before farm workers
  start or read once from `ANVIL_MUTANT` for targeted tests. Under the
  feature it is the only static with interior mutability in a deterministic
  crate; it is read-only during a run and `None` in production builds. Every
  planted site calls `is(Mxx)`, which is a constant false without the
  feature.
- Bugs are planted at the real source sites: Raft commit and vote rules,
  node snapshot and read paths, KV session cache, storage recovery and fsync
  handling, simulator repair validation, and the stale-read canary. The
  simulator still runs the same consensus code as the server.
- `cargo xtask mutant ID` re-executes each killer (farm seed or targeted
  test) and its control run, then prints the matrix. A surviving mutant is a
  gate failure.
- `cargo xtask ci` runs `cargo tree -p anvil-server -e features` and fails if
  the `mutants` feature appears. The server has no path to `anvil-sim`.

## Consequences

- Determinism goldens and normal release gates never see a mutation: the
  feature is off and the selector is `None`.
- Feature-powerset CI compiles the mutant-enabled configuration, so a hook
  cannot silently rot.
- M15 and M20 currently survive because the existing farm and tests never
  observe their planted behavior. They are reported as open items in
  `docs/mutants.md` rather than being removed from the library.
