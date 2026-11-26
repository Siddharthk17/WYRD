# ADR 0017: Week 4 Network Fault Model

## Status
Accepted

## Context
Week 3 left `SimNetwork` with drop, duplicate, delay hooks and symmetric
partitions only. Week 4 requires per-link latency distributions, bandwidth
caps, five partition kinds, connection resets, two transport modes, and
process faults with tick skew. Fault choices must stay deterministic: no
generator draws mid-run.

## Options Considered
1. Extend hooks with more closures: flexible but not serializable and not
   replayable from a plan.
2. Add deterministic fields to `SimNetwork` with pure hash sampling, keep old
   hook and plan APIs working: replayable, testable, backward compatible.
3. Rewrite the network around a full event simulator: more faithful but
   large and risky mid-project.

## Decision
Option 2. `SimNetwork` gains `TransportMode` (ordered-lossy FIFO versus
unordered hash order), `LatencyModel` (constant, uniform, exponential,
bimodal) sampled from pure hashes, per-link overrides, per-tick bandwidth
caps with queueing, directed partitions with `cut_one_way`, `bisect`,
`bridge_hub`, and `heal_directed`, resets that drop in-flight packets and
install backoff, and spike draws. `is_cut` is now directed; symmetric helpers
insert both directions so old behavior is unchanged. `SimCluster` gains
tick skew (500 to 2000 per mille) and pause with stale-timer burst on resume.

## Consequences
- All Week 3 tests pass unchanged. New behavior is covered by
  `network_faults_test`.
- Farm wiring for both transport modes in every configuration is deferred to
  the farm integration subtask. The mechanism exists and is unit tested.
- Benchmark scratch moved from `/tmp` to `target/bench-scratch` because
  `tmpfs` makes `fdatasync` a no-op and invalidates the sync ceiling.
