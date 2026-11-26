# ADR 0023: Week 5 plan-based nemesis and liveness checks

## Status

Accepted.

## Context

Week 5 deliverable 4 needs the simulator to graduate from a fixed fault
loop to a plan-based nemesis: serializable plans, swarm-tested fault
classes, four run phases, and liveness checks after heal. The old workload
ran a hard-coded isolate/crash schedule with faults only in ticks 30-285,
stopped ticking when the client schedule exhausted, and never verified that
a healed cluster could still elect a leader and commit.

## Decision

- Plan v2 replaces the ad-hoc partition/crash fields with a unified
  `FaultEvent` list (partition, asymmetric cut, crash, pause, slow clock,
  delay spike) plus warm-up and heal boundaries. Generation uses swarm
  testing: thirty percent of seeds enable one or two classes, ten percent
  run everything, the rest run a random subset with random intensities.
  Every event draws its parameters from a per-event stream keyed by
  `(seed, event id)`, so removing an event during shrinking never
  reshuffles the others. The plan also carries the workload swarm profile,
  so the client operation mix is materialized data, not a mid-run draw.
- The KV workload executes its plan's events (isolate, cut, crash with
  restart, pause with resume, clock skew with restore-to-base, delay
  spikes folded into the per-tick network model) and realigns every tick
  to the full 400-tick horizon even after the client schedule exhausts,
  since faults live in the chaos window, not in the client burst.
- After heal, liveness requires a leader within 100 ticks (ten times the
  maximum election timeout) and requires a fresh `ClockTick` write to
  commit on every live replica within 200 ticks. The convergence bar is
  the probe's own log index, not the pre-probe commit, and a final drain
  evens out applied indexes before the audit. Violations panic with the
  seed and the stuck state, which the farm records as the failure class.
- Healed runs resume paused nodes, restore base tick rates, restart
  crashed nodes that can restart, and heal all partitions, so the heal
  phase is fault-free by construction rather than by schedule luck. This
  maintenance repeats on every convergence and liveness tick, because
  storage faults can crash nodes at any time and a single post-heal heal
  would leave late crashes unrestarted. Nodes that refuse to start or
  need Week-6 repair stay out; liveness and convergence are judged on the
  nodes that can run.
- A re-probe happens only when the current leader actually lost the probe;
  re-proposing on a timer chases the bar upward under slow commits.
- Committed commands may legitimately fail application (a client can
  pipeline writes against a session whose registration was truncated away
  before committing, and every replica rejects them identically), so
  `SnapshotEqualsReplay` asserts the resulting state hash, never
  per-entry success.
- The nemesis runs one long full-chaos partition on every fourth workload
  seed: without it, followers rarely lag more than the retain window
  behind a compacted leader, so the snapshot-install path would almost
  never execute. Installs stay infrequent by design (they require deep
  lag); the unit, node, and cluster tests cover the mechanism itself.

## Consequences

- Runs are longer (full horizon plus convergence drains), so farm
  throughput per seed drops; the farm summary in a later subtask reports
  the measured number.
- The v1 plan encoding is rejected; golden trace hashes are unaffected
  because the determinism gate covers the kernel scenario, not plans.
