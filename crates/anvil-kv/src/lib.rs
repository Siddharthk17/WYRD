#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-kv`: Deterministic state machine, client sessions, snapshot encode and decode.
//!
//! Weeks 1-3 establish the workspace, codec, Raft core, storage engine, and
//! simulator. The state machine arrives in a later week; this crate reserves
//! the module path so the dependency direction stays fixed.
