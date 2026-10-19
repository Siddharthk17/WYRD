# ADR 0004: Crate Layering and Dependency Direction

## Status
Accepted

## Context
Circular dependencies or leaky abstractions between consensus, storage, state machine, and simulator create untestable spaghetti architectures. A clean DAG allows each layer to be verified, fuzzed, and benchmarked independently.

## Options Considered
1. **Monolithic Crate**: Single crate with feature flags separating simulator and production server.
2. **Layered Workspace**: Explicit crates arranged in a strict downward-only directed acyclic graph.

## Decision
The ANVIL workspace is structured into layered crates with dependencies pointing downward only:

```
[ anvil-server, anvilctl, anvil-viz, anvil-bench, anvil-sim ]
                     |
              [ anvil-node ]
             /      |       \
[ anvil-raft ] [ anvil-storage ] [ anvil-kv ]
      |             |                |
      +-------------+----------------+
                    |
              [ anvil-types ]
```

- `anvil-types`: Foundation newtypes, frame formats, binary codecs. Depends on nothing in the workspace.
- `anvil-raft`: Consensus protocol sans-IO. Depends only on `anvil-types`.
- `anvil-kv`: Deterministic state machine and sessions. Depends only on `anvil-types`.
- `anvil-storage`: WAL, hard state, and snapshots over an abstract `Device`. Depends only on `anvil-types`.
- `anvil-node`: Orchestrates consensus, storage, and state machine over generic environment traits.
- `anvil-check`: Linearizability checker and history model.
- `anvil-sim`: Simulation kernel, simulated network, simulated device, and fault injection.
- `anvil-device-fs`: Real filesystem I/O thread device implementation.
- `anvil-server`, `anvil-client`, `anvilctl`, `anvil-viz`, `anvil-bench`: Operational entrypoints and tooling.
- `xtask`: Quality gate and build automation harness.

## Consequences
- Circular dependencies are structurally impossible.
- Deterministic crates remain decoupled from networking runtimes and disk syscalls.
- Clear mental model and compile-time isolation.
