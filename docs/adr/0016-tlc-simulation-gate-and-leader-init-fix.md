# ADR 0016: TLC Smoke Bounds, Simulation Gate, and Leader-Init Fix

## Status
Accepted

## Context
Wiring `cargo xtask tlc` to exhaustive checking never completed on the
development host: the reference 3-server, 3-term, 3-entry bound exceeded
300M generated states and exhausted a 5 GiB disk allocation, and even the
reduced smoke bound kept growing past 250M states without converging. The
charter still requires a TLC check on every change with CI bounds that
finish in 10 minutes. While probing the model, simulation exposed a real
spec defect: leader election unconditionally set
`nextIndex = Len(log) + 2`, which indexes past the end of a full log
(`SendAppendEntries` then reads `log[MaxLogLen + 1]`).

## Options Considered
1. **Exhaustive smoke bound in CI**: still diverges; fails the 10-minute
   requirement and fills the disk.
2. **Seeded simulation gate plus fixed spec**: bounded traces finish in about
   a minute, stay deterministic for fixed seeds, and already caught one
   genuine soundness bug.
3. **Leave TLC unwired**: keeps the gate red and learns nothing from the spec.

## Decision
ANVIL adopts option 2:
- `cargo xtask tlc` runs seeded random simulation (seeds 1, 7, 42, depth
  500) on `specs/anvil_raft_smoke.cfg` (must stay violation-free) and
  `specs/anvil_raft_negative_smoke.cfg` (must produce the Figure 8
  counterexample).
- `cargo xtask tlc-full` keeps the exhaustive reference-bound check for
  larger verification hosts.
- The spec conditions leader `matchIndex`/`nextIndex` initialization on
  whether the no-op append fits a non-full log.

## Consequences
- Simulation is a smoke test, not a proof: it can miss violations that
  exhaustive checking would find. The negative config guards the other
  direction by proving the harness still detects the known counterexample.
- The spec fix is covered by the smoke gate: the previous revision crashes
  simulation with an out-of-bounds access instead of checking invariants.
