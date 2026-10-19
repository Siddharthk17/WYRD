# Contributing to ANVIL

## Principles

1. **Correctness first**: Safety is never traded for speed or schedule.
2. **Determinism**: Every run given the same seed and plan must produce a byte-identical trace across debug and release builds.
3. **No Unsafe**: `#![forbid(unsafe_code)]` is enforced in every crate.
4. **Honesty and Quality**: No placeholder implementations, no `TODO` or `FIXME` comments in merged code. Public items must have doc comments.

## Quality Gates

Before opening a pull request, run all CI gates locally:

```bash
cargo xtask ci
```

Every PR must pass format check, clippy with `-D warnings`, unit/integration tests, doc tests, cargo deny, determinism check, and coverage floors.

## Commit Conventions

Use Conventional Commits:
- `feat(scope): ...`
- `fix(scope): ...`
- `test(scope): ...`
- `docs(scope): ...`
- `perf(scope): ...`
- `refactor(scope): ...`

Keep commits small, atomic, and bisectable. Tests must be included with or before implementation code.

## Architecture Decisions

Any non-trivial design choice, trade-off, or structural change requires a numbered Architecture Decision Record in `docs/adr/`.
