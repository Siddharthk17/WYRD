# ADR 0002: Determinism Rules and Forbidden APIs

## Status
Accepted

## Context
Replaying distributed race conditions, data corruptions, and split votes requires bitwise-identical execution given the same 64-bit seed. Standard library collections and system interfaces introduce implicit entropy that invalidates trace replay.

## Options Considered
1. **Developer Discipline**: Trust code reviews to prevent forbidden calls.
2. **Compiler Wrapper**: Build a custom rustc frontend or linker script to intercept symbols.
3. **Clippy Disallowed Lists + Dependency Allowlist Test**: Restrict disallowed methods and types via Clippy, and assert crate dependency trees in automated tests.

## Decision
Deterministic crates (`anvil-types`, `anvil-raft`, `anvil-kv`, `anvil-storage`, `anvil-node`, `anvil-check`) forbid the following:
- `std::fs`, `std::net`, `std::thread`
- `std::time::Instant`, `std::time::SystemTime`
- OS randomness and unseeded RNGs
- `std::collections::HashMap`, `std::collections::HashSet`, and `RandomState`
- Floating-point arithmetic
- Statics with interior mutability

All associative storage in deterministic crates must use ordered structures (`BTreeMap`, `BTreeSet`, or `imbl::OrdMap`/`imbl::OrdSet`). Time is measured solely in discrete logical ticks or simulation nanoseconds (`SimTime`).

Restrictions are enforced by:
1. `clippy.toml` with `disallowed-methods` and `disallowed-types` enabled as errors in deterministic crates.
2. An automated xtask check verifying deterministic crates depend only on approved, entropy-free libraries.

## Consequences
- Guarantees trace hash stability across architectures (x86_64 and aarch64), debug/release builds, and address-space layouts (ASLR).
- Requires ordered map lookups ($O(\log N)$) instead of hash map lookups ($O(1)$) in deterministic crates. For the expected collection sizes in core consensus tables, this difference is within performance budgets.
