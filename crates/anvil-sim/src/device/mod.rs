#![forbid(unsafe_code)]

//! Simulated storage device (`SimDevice`) and `StorageFaultAtlas` for ANVIL.

pub mod atlas;
pub mod sim_device;

pub use atlas::StorageFaultAtlas;
pub use sim_device::{FileState, SimDevice, SimDeviceConfig, SimDeviceStats};
