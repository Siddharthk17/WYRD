#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-check`: History model and linearizability checker.
//!
//! Weeks 1-3 validate safety with the online invariant checker in `anvil-sim`.
//! The offline history checker arrives in a later week; this crate reserves
//! the module path so the dependency direction stays fixed.
