# ADR 0014: Materialized Simulation Plans, SimNetwork, and Shrinker

## Status
Accepted

## Context
Fault schedules and workloads were built inline in the farm runner with ad-hoc
integer hashes, and the packet queue lived directly on `SimCluster`. Three gaps
followed: a failing seed could only be described by its number, never replayed
exactly from data; network fault choices were not derived from any named
randomness discipline; and there was no way to reduce a failing run. The
`buggify!` registry compounded this by sharing one global draw sequence across
farm worker threads, so parallel execution perturbed activation order.

## Options Considered
1. **Keep inline schedules**: zero new code, but failures stay seed-number-only
   and fault choices stay outside the RNG discipline.
2. **Serialize closures or hook state**: not portable across processes and
   cannot shrink.
3. **Materialized `Plan` data plus a `SimNetwork` queue plus a greedy
   shrinker**: faults and workloads become versioned text; execution draws no
   generator state; failures replay from the encoding and shrink while the
   failure reproduces.

## Decision
ANVIL adopts option 3:
- `Plan` carries seed, cluster size, ticks, proposals, partitions, crashes,
  and network fault rates. `Plan::generate` is the only place that samples,
  using an isolated `DetRng` stream; `run_plan` executes with no generator
  draws. Network faults are pure hash functions of the plan salt and the
  per-pair message sequence number.
- `SimNetwork` owns the packet queue, partitions, hooks, and per-pair
  sequence counters. `SimCluster` delegates routing, delivery, isolation,
  healing, and draining to it.
- `shrink_plan` applies greedy delta-debugging (drop one fault, shorten the
  run, zero network faults) and keeps each change only while the failure
  reproduces under `catch_unwind`.
- The `buggify!` registry is thread-local, so worker threads and parallel
  test threads evaluate points against their own stream.
- `SimCluster::tick` advances every `SimDevice` clock to the cluster tick
  before stepping nodes, so device completion latencies elapse in lockstep.
- Packet delivery drains in queue order, keeping surviving packets FIFO-stable.

## Consequences
- Every farm failure prints as a `Plan` encoding via `cargo xtask plan` and
  replays via `cargo xtask repro`, which also prints the shrunk plan.
- Farm behavior changed shape (hash-based faults instead of inline hashes);
  invariant coverage is unchanged and golden determinism hashes are unaffected
  because they cover the kernel scenario, not the farm.
- Thread-local buggify means point registries are per-thread; tests that
  reset, evaluate, and list points in one thread are unaffected.
