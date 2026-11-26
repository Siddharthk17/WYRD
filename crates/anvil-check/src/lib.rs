#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]

//! `anvil-check`: history model and deterministic linearizability checker.
//!
//! Single-threaded Wing-Gong-Lowe search with memoization and
//! P-compositionality (partition by key). The farm parallelizes across seeds.

pub mod history;
pub mod list;
pub mod output;
pub mod porcupine;
pub mod register;
pub mod wgl;

pub use history::{ClientOp, History, HistoryOp, OpResult};
pub use output::{CheckFailure, check_history};
pub use wgl::{check_key_history, check_range_history};
