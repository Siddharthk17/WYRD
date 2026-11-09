#![forbid(unsafe_code)]

//! `anvil-sim`: Deterministic simulation kernel, `SimDevice`, `SimNetwork`, `DetRng`, and `buggify`.

pub mod buggify;
pub mod cluster;
pub mod device;
pub mod farm;
pub mod kernel;
pub mod network;
pub mod plan;
pub mod queue;
pub mod raft_invariants;
pub mod rng;
pub mod scenario;
pub mod time;
pub mod trace;
pub mod watchdog;

pub use buggify::{
    BuggifyPoint, BuggifyRegistry, list_registered_points, reset_buggify_registry,
    set_buggify_enabled, set_buggify_seed,
};
pub use cluster::{InFlightPacket, PlanNet, SimCluster, SimNodeStorage};
pub use device::{FileState, SimDevice, SimDeviceConfig, SimDeviceStats, StorageFaultAtlas};
pub use farm::{
    FarmResult, run_farm_seed, run_farm_seed_with_storage_faults, run_seed_farm,
    run_seed_farm_with_storage_faults,
};
pub use kernel::{Actor, ActorContext, SimError, SimKernel, SimReport};
pub use network::{NetworkDelayHook, NetworkFilterHook, SimNetwork};
pub use plan::{Plan, PlanCrash, PlanError, PlanPartition, run_plan, shrink_plan};
pub use queue::{BinaryHeapEventQueue, EventQueue, HierarchicalTimingWheel, ScheduledEvent};
pub use raft_invariants::{InvariantViolation, RaftInvariantChecker};
pub use rng::{DetRng, SplitMix64, derive_component_seed};
pub use scenario::{run_golden_scenario, run_scenario_with_hashmap};
pub use time::SimTime;
pub use trace::{TraceEvent, TraceRecorder};
pub use watchdog::{Watchdog, WatchdogError};
