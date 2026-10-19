## Summary of Changes
- 

## Verification & Gates
- [ ] `cargo fmt --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `cargo xtask determinism` (if sim/consensus affected)
- [ ] No `unsafe` introduced (`#![forbid(unsafe_code)]` preserved)
- [ ] Numbered ADR created in `docs/adr/` if non-trivial decision made
- [ ] Documentation updated
