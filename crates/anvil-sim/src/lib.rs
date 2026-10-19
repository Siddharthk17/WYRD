#![forbid(unsafe_code)]

//! `anvil-sim`: Deterministic simulation kernel, `SimDevice`, `SimNetwork`, `DetRng`, and `buggify`.

pub mod buggify;
pub mod kernel;
pub mod queue;
pub mod rng;
pub mod time;
pub mod trace;
pub mod watchdog;

pub use buggify::{
    BuggifyPoint, BuggifyRegistry, list_registered_points, reset_buggify_registry,
    set_buggify_enabled, set_buggify_seed,
};
pub use kernel::{Actor, SimError, SimKernel, SimReport};
pub use queue::{BinaryHeapEventQueue, EventQueue, HierarchicalTimingWheel, ScheduledEvent};
pub use rng::{DetRng, SplitMix64, derive_component_seed};
pub use time::SimTime;
pub use trace::{TraceEvent, TraceRecorder};
pub use watchdog::{Watchdog, WatchdogError};
