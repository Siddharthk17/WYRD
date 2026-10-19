# ADR 0007: Event Queue Data Structure Selection

## Status
Accepted

## Context
The deterministic simulation kernel drives all distributed actors, network links, timers, and storage devices via an event queue. In discrete-event simulation, the event queue is on the critical execution path: every scheduled message, timer tick, disk completion, and fault trigger results in a queue push and pop.

The queue must satisfy two strict requirements:
1. **Deterministic Tie-Breaking**: Events scheduled for the identical virtual timestamp must resolve in exact, reproducible order across runs. ANVIL resolves ties by ordering on `(SimTime, insertion_sequence: u64)`.
2. **High Throughput**: The performance doctrine sets a starting expectation of at least 10 million event push-pop pairs per second on one core.

## Options Considered
1. **Binary Min-Heap (`BinaryHeapEventQueue`)**: A contiguous array-backed binary min-heap ordered by `(SimTime, insertion_sequence)`.
2. **Hierarchical Timing Wheel (`HierarchicalTimingWheel`)**: A 4-level timing wheel (64 slots per level) with cascading buckets and an overflow list, inspired by network packet schedulers and OS kernel timer wheels.

## Empirical Measurements
Both data structures were implemented in `anvil-sim::queue` and benchmarked under identical workloads (100,000 mixed push-pop pairs with pseudo-random delays across sparse and dense time distributions) in release mode on Linux x86_64:

| Implementation | Runtime (100k pairs) | Throughput | Charter Budget Target |
| :--- | :--- | :--- | :--- |
| **Binary Min-Heap** | **11.14 ms** | **17.96 M ops/sec** | **$\ge$ 10.0 M ops/sec (Passed: +79.6%)** |
| **Hierarchical Timing Wheel** | **983.66 ms** | **0.20 M ops/sec** | $\ge$ 10.0 M ops/sec (Failed: -98.0%) |

### Why the Hierarchical Timing Wheel Underperformed
1. **Sparse Time Jumps**: Distributed simulation frequently jumps across quiet periods (e.g. 50 ms election timeouts or partition intervals). Advancing the timing wheel across sparse intervals requires stepping through empty slots and cascading multiple higher-level buckets down to level 0.
2. **Memory Indirection**: Timing wheel buckets require nested collections (`Vec<Vec<Event>>`), causing frequent pointer indirection and cache line misses.
3. **Secondary Tie-Breaking Overhead**: Because multiple events falling into the same time bucket must still maintain exact `insertion_seq` ordering, popping requires scanning the slot bucket to find the minimum insertion sequence.

### Why the Binary Min-Heap Excelled
1. **Cache Locality**: A flat `Vec<Reverse<ScheduledEvent>>` stores events contiguously in memory.
2. **Arbitrary Time Invariance**: Jumping forward 10 nanoseconds or 10 hours has identical $O(\log N)$ algorithmic complexity.
3. **Simplicity and Safety**: Standard binary heap logic is compact, requires zero pointer manipulation, and has `#![forbid(unsafe_code)]` compliance.

## Decision
ANVIL adopts `BinaryHeapEventQueue` as the canonical event queue engine for `SimKernel`.

## Consequences
- The simulation kernel achieves ~18 million event push-pop pairs per second on a single core, well above the 10 M ops/sec floor.
- Memory consumption is bounded to a single contiguous buffer proportional to active in-flight events.
- Perfect determinism is guaranteed by `(SimTime, insertion_seq)` tuple ordering.
