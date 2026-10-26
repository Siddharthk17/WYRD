#![forbid(unsafe_code)]

//! `anvil-device-fs`: Real-file Device implementation backed by dedicated I/O threads.

pub mod device;

pub use device::{FsDevice, file_id_to_path};
