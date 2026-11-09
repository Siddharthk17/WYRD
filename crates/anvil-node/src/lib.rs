#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-node`: Orchestrates consensus, storage engine, and state machine over environment traits.
//!
//! Weeks 1-3 run the core and storage directly inside `anvil-sim` harnesses.
//! The shared node glue arrives in a later week; this crate reserves the
//! module path so the dependency direction stays fixed.
