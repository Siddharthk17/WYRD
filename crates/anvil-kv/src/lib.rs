#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-kv`: deterministic replicated state machine with client sessions.
//!
//! Log writes replicate through Raft. Reads evaluate against the local store
//! through the read path and never enter the log. Virtual time enters only
//! through `ClockTick` entries the leader proposes periodically.

pub mod command;
pub mod store;

pub use command::{KvCommand, KvResponse, KvRole};
pub use store::{KvConfig, KvError, KvRecord, KvStore, SessionIdView};
