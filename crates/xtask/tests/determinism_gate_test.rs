#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

//! Automated test for the determinism gate:
//! 1. Verifies that the gate passes cleanly on golden seeds.
//! 2. Verifies that the gate FAILS when non-deterministic `HashMap` iteration is introduced.

#[test]
fn test_determinism_gate_golden_seeds() {
    let gate = xtask::determinism::run_determinism_gate(false, false);
    assert!(
        gate.is_ok(),
        "Determinism gate should pass on golden seeds: {:?}",
        gate.err()
    );
}

#[test]
fn test_negative_test_hashmap_fixture_fails_gate() {
    let gate = xtask::determinism::run_determinism_gate(true, false);
    assert!(
        gate.is_err(),
        "Determinism gate MUST fail when HashMap fixture is used"
    );
    let gate_err = gate.unwrap_err();
    assert!(
        gate_err.contains("Divergence detected in negative test fixture"),
        "Unexpected error: {gate_err}"
    );
}
