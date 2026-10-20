#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! `anvil-raft`: Sans-IO consensus core for ANVIL.
//!
//! Provides a pure state-machine implementation of Raft consensus with:
//! - `PreVote` protocol preventing partitioned disruption
//! - `CheckQuorum` and leader stickiness
//! - Randomized election timeouts via exact-pinned `ChaCha8Rng`
//! - Strict persistence contract guaranteeing safety across crashes
//! - 1-RTT divergent log repair via term conflict hints
//! - Zero-allocation hot path output buffers
//! - 1:1 state projection to TLA+ formal specification variables

pub mod core;
pub mod log;
pub mod messages;
pub mod progress;
pub mod types;

pub use crate::core::RaftCore;
pub use crate::log::RaftLog;
pub use crate::messages::Message;
pub use crate::progress::{PeerProgress, ProgressState};
pub use crate::types::OutboundMessage;
pub use crate::types::{
    Input, Output, PersistOp, PersistToken, RaftConfig, ReadIndexResult, RecoveredState, Role,
    RoleChangeEvent, SpecProjection,
};
