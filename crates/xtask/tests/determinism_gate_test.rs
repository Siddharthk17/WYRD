#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Automated test for the determinism gate:
//! 1. Verifies that the gate passes cleanly on golden seeds.
//! 2. Verifies that the gate FAILS when non-deterministic `HashMap` iteration is introduced.

#[test]
fn test_determinism_gate_golden_seeds() {
    let result = xtask::determinism::run_determinism_gate(false, false);
    assert!(
        result.is_ok(),
        "Determinism gate should pass on golden seeds: {:?}",
        result.err()
    );
}

#[test]
fn test_negative_test_hashmap_fixture_fails_gate() {
    let result = xtask::determinism::run_determinism_gate(true, false);
    assert!(
        result.is_err(),
        "Determinism gate MUST fail when HashMap fixture is used"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("Divergence detected in negative test fixture"),
        "Unexpected error: {err}"
    );
}
