# ADR 0003: RNG Discipline and Stream Derivation

## Status
Accepted

## Context
In a multi-component distributed simulator, sharing a single RNG sequence across all nodes, network channels, and disk actors creates cascading coupling. Adding a new node, adding a logging statement that samples randomness, or altering one actor's schedule alters the pseudorandom numbers drawn by every downstream component. Furthermore, third-party distribution samplers can change output algorithms across version bumps, breaking golden hash regression suites.

## Options Considered
1. **Shared Global ChaCha Generator**: Single RNG instance passed to all callers.
2. **Standard rand::distributions**: Use `rand` crate distribution samplers.
3. **Hierarchical Component Streams + Independent Hand-Crafted Samplers**: Single 64-bit master seed expanded into isolated component streams via SplitMix64 hashing, paired with ANVIL-owned sampling algorithms over raw `u64`.

## Decision
ANVIL uses hierarchical stream derivation:
1. One 64-bit master seed (`u64`) initializes the simulation run.
2. Each subsystem derives its seed using SplitMix64 over the master seed and its unique component path (e.g. `node/3/election`, `network/link/1->2`, `disk/1/latency`).
3. Component RNGs instantiate an exact-version pinned `rand_chacha::ChaCha8Rng`.
4. All distribution sampling (unbiased integer range, Bernoulli trials, exponential decay, bimodal distributions, Fisher-Yates shuffles) is implemented inside `anvil-sim` directly over raw `u64` output.

## Consequences
- Adding or modifying a component's RNG usage does not alter the random sequence observed by any other component.
- Golden trace hashes remain stable across Rust versions and dependency updates.
- Requires maintaining custom distribution math inside `anvil-sim`.
