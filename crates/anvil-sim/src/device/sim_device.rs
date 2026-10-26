#![forbid(unsafe_code)]
#![allow(
    clippy::cast_possible_truncation,
    clippy::collapsible_if,
    clippy::similar_names
)]

//! Simulated disk storage device (`SimDevice`) for ANVIL.
//!
//! Models physical storage hardware with:
//! - Volatile page cache over durable on-disk images per file
//! - Directory metadata with independent durability (`SyncDir`)
//! - Discrete-time asynchronous completion queue with configurable latency distributions
//! - Crash injection resolving unsynced sectors independently (persisted, lost, or torn)
//! - Write reordering prior to fsync
//! - Read/write bit-rot and latent sector errors (EIO)
//! - Storage Fault Atlas integration enforcing the Quorum Survivability Rule

use super::atlas::StorageFaultAtlas;
use crate::rng::DetRng;
use anvil_storage::{
    CompletionPayload, Device, DeviceCompletion, DeviceOp, FileId, SectorSize, StorageError,
};
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

/// Configuration parameters for `SimDevice`.
#[derive(Debug, Clone)]
pub struct SimDeviceConfig {
    /// Physical sector size (512 or 4096 bytes).
    pub sector_size: SectorSize,
    /// Maximum disk capacity in bytes before ENOSPC.
    pub disk_capacity_bytes: u64,
    /// Rate of random bit-flip corruptions on read (parts per million).
    pub read_corruption_rate_ppm: u32,
    /// Rate of random bit-flip corruptions on write (parts per million).
    pub write_corruption_rate_ppm: u32,
    /// Rate of latent sector errors (EIO) on read (parts per million).
    pub read_eio_rate_ppm: u32,
    /// Rate of fsync failure errors (parts per million).
    pub fsync_failure_rate_ppm: u32,
    /// Rate of unsynced dirty sectors lost on crash (parts per million). Default: 50%.
    pub unsynced_sector_loss_rate_ppm: u32,
    /// Rate of unsynced dirty sectors torn on crash (parts per million). Default: 20%.
    pub torn_write_rate_ppm: u32,
    /// Rate of unsynced directory file creations lost on crash (parts per million). Default: 80%.
    pub unsynced_dir_loss_rate_ppm: u32,
    /// Minimum asynchronous completion latency in logical ticks.
    pub min_latency_ticks: u64,
    /// Maximum asynchronous completion latency in logical ticks.
    pub max_latency_ticks: u64,
    /// Injected delay ticks for fsync operations (gray failure testing).
    pub fsync_stall_ticks: u64,
}

impl Default for SimDeviceConfig {
    fn default() -> Self {
        Self {
            sector_size: SectorSize::Sector512,
            disk_capacity_bytes: 1024 * 1024 * 1024, // 1 GiB
            read_corruption_rate_ppm: 0,
            write_corruption_rate_ppm: 0,
            read_eio_rate_ppm: 0,
            fsync_failure_rate_ppm: 0,
            unsynced_sector_loss_rate_ppm: 500_000, // 50%
            torn_write_rate_ppm: 200_000,           // 20%
            unsynced_dir_loss_rate_ppm: 800_000,    // 80%
            min_latency_ticks: 0,
            max_latency_ticks: 0,
            fsync_stall_ticks: 0,
        }
    }
}

impl SimDeviceConfig {
    /// Creates a test configuration with immediate synchronous completions.
    #[must_use]
    pub fn for_test(sector_size: SectorSize) -> Self {
        Self {
            sector_size,
            ..Default::default()
        }
    }

    /// Creates a configuration with asynchronous completion latency range.
    #[must_use]
    pub fn with_latency(sector_size: SectorSize, min_ticks: u64, max_ticks: u64) -> Self {
        assert!(min_ticks <= max_ticks, "min_ticks must be <= max_ticks");
        Self {
            sector_size,
            min_latency_ticks: min_ticks,
            max_latency_ticks: max_ticks,
            ..Default::default()
        }
    }

    /// Creates a chaotic fault-injection configuration.
    #[must_use]
    pub fn chaotic(
        sector_size: SectorSize,
        read_rot_ppm: u32,
        write_rot_ppm: u32,
        fsync_fail_ppm: u32,
    ) -> Self {
        Self {
            sector_size,
            read_corruption_rate_ppm: read_rot_ppm,
            write_corruption_rate_ppm: write_rot_ppm,
            fsync_failure_rate_ppm: fsync_fail_ppm,
            read_eio_rate_ppm: 1_000, // 0.1%
            ..Default::default()
        }
    }
}

/// Execution statistics and fault counters for `SimDevice`.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SimDeviceStats {
    pub ops_submitted: u64,
    pub writes_completed: u64,
    pub reads_completed: u64,
    pub syncs_completed: u64,
    pub sync_dirs_completed: u64,
    pub fsync_failures_injected: u64,
    pub read_corruptions_injected: u64,
    pub write_corruptions_injected: u64,
    pub read_eios_injected: u64,
    pub torn_sectors_persisted: u64,
    pub lost_sectors_discarded: u64,
    pub crash_count: u64,
}

/// In-memory storage state of a single simulated file.
#[derive(Debug, Clone, Default)]
pub struct FileState {
    /// Data committed to durable non-volatile storage.
    pub durable_data: Vec<u8>,
    /// Volatile page cache representation.
    pub volatile_data: Vec<u8>,
    /// Indices of sectors modified in volatile cache but not yet flushed via `Fdatasync`.
    pub dirty_sectors: BTreeSet<u64>,
}

/// Asynchronous operation completion event queued by logical time.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PendingCompletion {
    completion_tick: u64,
    op_id: u64,
    result: Result<CompletionPayload, StorageError>,
}

impl Ord for PendingCompletion {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap ordering: lowest completion_tick first, then lowest op_id
        other
            .completion_tick
            .cmp(&self.completion_tick)
            .then_with(|| other.op_id.cmp(&self.op_id))
    }
}

impl PartialOrd for PendingCompletion {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Simulated storage device with page cache, crash modeling, and fault injection.
pub struct SimDevice {
    config: SimDeviceConfig,
    rng: DetRng,
    current_tick: u64,

    /// Files that have been made durable via directory sync (`SyncDir`).
    durable_files: BTreeSet<FileId>,
    /// Files currently existing in volatile filesystem namespace.
    volatile_files: BTreeSet<FileId>,
    /// File states indexed by `FileId`.
    files: BTreeMap<FileId, FileState>,

    /// Priority queue of pending asynchronous completions.
    pending_completions: BinaryHeap<PendingCompletion>,

    /// Storage Fault Atlas enforcing the survivability rule.
    atlas: Option<StorageFaultAtlas>,
    /// Node ID associated with this device in a simulated cluster.
    node_id: Option<NodeId>,

    /// Operational statistics and metrics.
    stats: SimDeviceStats,
}

impl SimDevice {
    /// Constructs a new `SimDevice` with the specified configuration and deterministic RNG.
    #[must_use]
    pub fn new(config: SimDeviceConfig, rng: DetRng) -> Self {
        Self {
            config,
            rng,
            current_tick: 0,
            durable_files: BTreeSet::new(),
            volatile_files: BTreeSet::new(),
            files: BTreeMap::new(),
            pending_completions: BinaryHeap::new(),
            atlas: None,
            node_id: None,
            stats: SimDeviceStats::default(),
        }
    }

    /// Attaches a shared `StorageFaultAtlas` and cluster `NodeId` to this device.
    #[must_use]
    pub fn with_atlas(mut self, atlas: StorageFaultAtlas, node_id: NodeId) -> Self {
        self.atlas = Some(atlas);
        self.node_id = Some(node_id);
        self
    }

    /// Returns the current logical time tick.
    #[must_use]
    pub const fn current_tick(&self) -> u64 {
        self.current_tick
    }

    /// Advances the logical time tick by `ticks`.
    pub fn step_time(&mut self, ticks: u64) {
        self.current_tick = self.current_tick.wrapping_add(ticks);
    }

    /// Explicitly sets the logical time tick.
    pub fn set_time(&mut self, tick: u64) {
        self.current_tick = tick;
    }

    /// Returns a reference to the device configuration.
    #[must_use]
    pub const fn config(&self) -> &SimDeviceConfig {
        &self.config
    }

    /// Returns a mutable reference to the device configuration.
    pub fn config_mut(&mut self) -> &mut SimDeviceConfig {
        &mut self.config
    }

    /// Returns current device statistics.
    #[must_use]
    pub const fn stats(&self) -> &SimDeviceStats {
        &self.stats
    }

    /// Returns `true` if there are asynchronous completions queued.
    #[must_use]
    pub fn has_pending_completions(&self) -> bool {
        !self.pending_completions.is_empty()
    }

    /// Checks if a file exists in the volatile namespace.
    #[must_use]
    pub fn file_exists(&self, file_id: FileId) -> bool {
        self.volatile_files.contains(&file_id)
    }

    /// Checks if a file exists in durable disk state.
    #[must_use]
    pub fn is_file_durable(&self, file_id: FileId) -> bool {
        self.durable_files.contains(&file_id)
    }

    /// Returns a reference to durable file bytes if the file exists.
    #[must_use]
    pub fn get_file_durable_bytes(&self, file_id: FileId) -> Option<&[u8]> {
        if self.durable_files.contains(&file_id) {
            self.files.get(&file_id).map(|s| s.durable_data.as_slice())
        } else {
            None
        }
    }

    /// Returns a reference to volatile page cache file bytes if the file exists.
    #[must_use]
    pub fn get_file_volatile_bytes(&self, file_id: FileId) -> Option<&[u8]> {
        if self.volatile_files.contains(&file_id) {
            self.files.get(&file_id).map(|s| s.volatile_data.as_slice())
        } else {
            None
        }
    }

    /// Returns the list of durable files on disk.
    #[must_use]
    pub fn list_durable_files(&self) -> Vec<FileId> {
        self.durable_files.iter().copied().collect()
    }

    /// Returns the list of volatile files in page cache.
    #[must_use]
    pub fn list_volatile_files(&self) -> Vec<FileId> {
        self.volatile_files.iter().copied().collect()
    }

    /// Drains and returns all completions that are ready at `current_tick`.
    pub fn drain_ready_completions(&mut self) -> Vec<DeviceCompletion> {
        let mut ready = Vec::new();
        while let Some(top) = self.pending_completions.peek() {
            if top.completion_tick <= self.current_tick {
                if let Some(comp) = self.pending_completions.pop() {
                    ready.push(DeviceCompletion {
                        op_id: comp.op_id,
                        result: comp.result,
                    });
                }
            } else {
                break;
            }
        }
        ready
    }

    /// Drains all queued completions unconditionally (forcing completion).
    pub fn drain_all_completions_forcing(&mut self) -> Vec<DeviceCompletion> {
        let mut all = Vec::new();
        while let Some(comp) = self.pending_completions.pop() {
            all.push(DeviceCompletion {
                op_id: comp.op_id,
                result: comp.result,
            });
        }
        all
    }

    /// Simulates a crash / power outage.
    ///
    /// Persists each unsynced dirty sector independently:
    /// - **Persisted**: Sector was written to non-volatile disk before power loss.
    /// - **Lost**: Sector was lost from volatile page cache and reverts to old durable content.
    /// - **Torn**: Sector was interrupted mid-write, yielding torn data across the sector boundary.
    ///
    /// Files created without a succeeding `SyncDir` are subject to directory entry loss.
    /// All in-flight pending asynchronous completions are dropped.
    pub fn crash(&mut self) {
        self.stats.crash_count += 1;
        let sector_size = self.config.sector_size.as_u64();

        // 1. Directory entry reconciliation
        let unsynced_creates: Vec<FileId> = self
            .volatile_files
            .difference(&self.durable_files)
            .copied()
            .collect();

        for file_id in unsynced_creates {
            if self
                .rng
                .bernoulli_ppm(self.config.unsynced_dir_loss_rate_ppm)
            {
                self.volatile_files.remove(&file_id);
                self.files.remove(&file_id);
            }
        }

        let unsynced_unlinks: Vec<FileId> = self
            .durable_files
            .difference(&self.volatile_files)
            .copied()
            .collect();

        for file_id in unsynced_unlinks {
            if self
                .rng
                .bernoulli_ppm(self.config.unsynced_dir_loss_rate_ppm)
            {
                self.volatile_files.insert(file_id);
            }
        }

        // 2. Per-file dirty sector crash resolution
        for file_state in self.files.values_mut() {
            for &s in &file_state.dirty_sectors {
                let start = (s * sector_size) as usize;
                let end =
                    ((s + 1) * sector_size).min(file_state.volatile_data.len() as u64) as usize;
                if start >= file_state.volatile_data.len() {
                    continue;
                }

                // Fate decision: Torn, Lost, or Persisted
                let fate_draw = self.rng.gen_range(0..1_000_000);
                if fate_draw < u64::from(self.config.torn_write_rate_ppm) {
                    // Torn write: first half new bytes, second half old/zeroes/corrupted
                    self.stats.torn_sectors_persisted += 1;
                    if file_state.durable_data.len() < end {
                        file_state.durable_data.resize(end, 0);
                    }
                    let mid = start + (end - start) / 2;
                    file_state.durable_data[start..mid]
                        .copy_from_slice(&file_state.volatile_data[start..mid]);
                    // Flip a bit in the torn portion to guarantee invalid checksum/zero padding
                    if mid < end {
                        file_state.durable_data[mid] ^= 0xAA;
                    }
                } else if fate_draw
                    < u64::from(
                        self.config.torn_write_rate_ppm + self.config.unsynced_sector_loss_rate_ppm,
                    )
                {
                    // Lost sector: do not write to durable disk (retains old state)
                    self.stats.lost_sectors_discarded += 1;
                } else {
                    // Persisted sector: made it to durable media before power cut
                    if file_state.durable_data.len() < end {
                        file_state.durable_data.resize(end, 0);
                    }
                    file_state.durable_data[start..end]
                        .copy_from_slice(&file_state.volatile_data[start..end]);
                }
            }

            file_state.dirty_sectors.clear();
            file_state.volatile_data = file_state.durable_data.clone();
        }

        // 3. Drop all pending in-flight completions (process killed)
        self.pending_completions.clear();
    }

    /// Internal helper to check if a corruption of a WAL frame is permitted by the atlas.
    fn can_corrupt_wal_frame(&self, file_id: FileId, offset: u64, data: &[u8]) -> bool {
        if let (Some(atlas), Some(node_id)) = (&self.atlas, self.node_id) {
            if file_id.is_wal_segment() && offset >= self.config.sector_size.as_u64() {
                if data.len() >= 8 {
                    let mut index_bytes = [0u8; 8];
                    index_bytes.copy_from_slice(&data[0..8]);
                    let raw_index = u64::from_le_bytes(index_bytes);
                    if raw_index > 0 {
                        let log_index = LogIndex::from_raw(raw_index);
                        return atlas.can_corrupt_entry(node_id, log_index);
                    }
                }
            }
        }
        true
    }

    /// Computes asynchronous latency for an operation.
    fn calculate_latency(&mut self, is_sync_op: bool) -> u64 {
        let base_latency = if self.config.max_latency_ticks > self.config.min_latency_ticks {
            self.rng
                .gen_range_inclusive(self.config.min_latency_ticks..=self.config.max_latency_ticks)
        } else {
            self.config.min_latency_ticks
        };

        if is_sync_op {
            base_latency.saturating_add(self.config.fsync_stall_ticks)
        } else {
            base_latency
        }
    }

    #[allow(clippy::too_many_lines)]
    fn execute_op(&mut self, op: DeviceOp) -> (bool, Result<CompletionPayload, StorageError>) {
        match op {
            DeviceOp::Create {
                file_id,
                preallocate_bytes,
                ..
            } => {
                if self.volatile_files.contains(&file_id) {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} already exists"
                        ))),
                    );
                }

                let current_used: u64 = self
                    .files
                    .values()
                    .map(|f| f.volatile_data.len() as u64)
                    .sum();
                if current_used + preallocate_bytes > self.config.disk_capacity_bytes {
                    return (
                        false,
                        Err(StorageError::DeviceError(
                            "ENOSPC: disk capacity exceeded".to_string(),
                        )),
                    );
                }

                self.volatile_files.insert(file_id);
                let sector_size = self.config.sector_size;
                let sectors =
                    sector_size.align_up(preallocate_bytes as usize) / sector_size.as_usize();
                let mut dirty = BTreeSet::new();
                for s in 0..sectors {
                    dirty.insert(s as u64);
                }

                self.files.insert(
                    file_id,
                    FileState {
                        durable_data: Vec::new(),
                        volatile_data: vec![0u8; preallocate_bytes as usize],
                        dirty_sectors: dirty,
                    },
                );

                (false, Ok(CompletionPayload::Created))
            }

            DeviceOp::Open { file_id, .. } => {
                if self.volatile_files.contains(&file_id) {
                    (false, Ok(CompletionPayload::Opened))
                } else {
                    (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    )
                }
            }

            DeviceOp::Write {
                file_id,
                offset,
                data,
                ..
            } => {
                if !self.volatile_files.contains(&file_id) {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    );
                }

                let mut write_bytes = data.to_vec();

                // Inject write corruption if configured and permitted by atlas
                if self.config.write_corruption_rate_ppm > 0
                    && self
                        .rng
                        .bernoulli_ppm(self.config.write_corruption_rate_ppm)
                    && self.can_corrupt_wal_frame(file_id, offset, &write_bytes)
                {
                    if !write_bytes.is_empty() {
                        write_bytes[0] ^= 0x5A;
                        self.stats.write_corruptions_injected += 1;
                    }
                }

                let Some(file_state) = self.files.get_mut(&file_id) else {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    );
                };
                let end = (offset as usize) + write_bytes.len();
                if end > file_state.volatile_data.len() {
                    file_state.volatile_data.resize(end, 0);
                }
                file_state.volatile_data[offset as usize..end].copy_from_slice(&write_bytes);

                // Mark touched sectors dirty
                let sector_size = self.config.sector_size.as_u64();
                if !write_bytes.is_empty() {
                    let first_sector = offset / sector_size;
                    let last_sector = (offset + write_bytes.len() as u64 - 1) / sector_size;
                    for s in first_sector..=last_sector {
                        file_state.dirty_sectors.insert(s);
                    }
                }

                self.stats.writes_completed += 1;
                (false, Ok(CompletionPayload::Written(write_bytes.len())))
            }

            DeviceOp::Read {
                file_id,
                offset,
                len,
                ..
            } => {
                if !self.volatile_files.contains(&file_id) {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    );
                }

                // Injected latent sector error (EIO) on read
                if self.config.read_eio_rate_ppm > 0
                    && self.rng.bernoulli_ppm(self.config.read_eio_rate_ppm)
                {
                    self.stats.read_eios_injected += 1;
                    return (
                        false,
                        Err(StorageError::DeviceError(
                            "EIO: latent sector error on read".to_string(),
                        )),
                    );
                }

                let Some(file_state) = self.files.get(&file_id) else {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    );
                };
                if offset as usize >= file_state.volatile_data.len() {
                    return (false, Ok(CompletionPayload::Read(Bytes::new())));
                }

                let end = (offset as usize + len).min(file_state.volatile_data.len());
                let mut read_bytes = file_state.volatile_data[offset as usize..end].to_vec();

                // Injected bit-rot corruption on read path
                if self.config.read_corruption_rate_ppm > 0
                    && self.rng.bernoulli_ppm(self.config.read_corruption_rate_ppm)
                    && self.can_corrupt_wal_frame(file_id, offset, &read_bytes)
                {
                    if !read_bytes.is_empty() {
                        read_bytes[0] ^= 0x3C;
                        self.stats.read_corruptions_injected += 1;
                    }
                }

                self.stats.reads_completed += 1;
                (false, Ok(CompletionPayload::Read(Bytes::from(read_bytes))))
            }

            DeviceOp::Fdatasync { file_id, .. } => {
                // Injected fsync failure
                if self.config.fsync_failure_rate_ppm > 0
                    && self.rng.bernoulli_ppm(self.config.fsync_failure_rate_ppm)
                {
                    self.stats.fsync_failures_injected += 1;
                    return (
                        true,
                        Err(StorageError::FsyncFailed {
                            reason: "injected simulated EIO during fdatasync".to_string(),
                        }),
                    );
                }

                if let Some(file_state) = self.files.get_mut(&file_id) {
                    let sector_size = self.config.sector_size.as_u64();
                    if file_state.durable_data.len() != file_state.volatile_data.len() {
                        file_state
                            .durable_data
                            .resize(file_state.volatile_data.len(), 0);
                    }

                    for &s in &file_state.dirty_sectors {
                        let start = (s * sector_size) as usize;
                        let end = ((s + 1) * sector_size).min(file_state.volatile_data.len() as u64)
                            as usize;
                        if start < file_state.volatile_data.len() {
                            file_state.durable_data[start..end]
                                .copy_from_slice(&file_state.volatile_data[start..end]);
                        }
                    }
                    file_state.dirty_sectors.clear();
                    self.stats.syncs_completed += 1;
                    (true, Ok(CompletionPayload::Synced))
                } else {
                    (
                        true,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    )
                }
            }

            DeviceOp::SyncDir { .. } => {
                // Injected fsync failure on directory
                if self.config.fsync_failure_rate_ppm > 0
                    && self.rng.bernoulli_ppm(self.config.fsync_failure_rate_ppm)
                {
                    self.stats.fsync_failures_injected += 1;
                    return (
                        true,
                        Err(StorageError::FsyncFailed {
                            reason: "injected simulated EIO during syncdir".to_string(),
                        }),
                    );
                }

                self.durable_files = self.volatile_files.clone();
                self.stats.sync_dirs_completed += 1;
                (true, Ok(CompletionPayload::DirSynced))
            }

            DeviceOp::Rename { from, to, .. } => {
                if !self.volatile_files.contains(&from) {
                    return (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {from:?} not found"
                        ))),
                    );
                }

                self.volatile_files.remove(&from);
                self.volatile_files.insert(to);
                if let Some(state) = self.files.remove(&from) {
                    self.files.insert(to, state);
                }
                (false, Ok(CompletionPayload::Renamed))
            }

            DeviceOp::Unlink { file_id, .. } => {
                self.volatile_files.remove(&file_id);
                (false, Ok(CompletionPayload::Unlinked))
            }

            DeviceOp::Truncate { file_id, len, .. } => {
                if let Some(file_state) = self.files.get_mut(&file_id) {
                    file_state.volatile_data.truncate(len as usize);
                    (false, Ok(CompletionPayload::Truncated))
                } else {
                    (
                        false,
                        Err(StorageError::DeviceError(format!(
                            "File {file_id:?} not found"
                        ))),
                    )
                }
            }
        }
    }
}

impl Device for SimDevice {
    fn submit(&mut self, op: DeviceOp) {
        self.stats.ops_submitted += 1;
        let op_id = op.op_id();
        let (is_sync, result) = self.execute_op(op);
        let latency = self.calculate_latency(is_sync);
        let completion_tick = self.current_tick.wrapping_add(latency);

        self.pending_completions.push(PendingCompletion {
            completion_tick,
            op_id,
            result,
        });
    }

    fn poll_completion(&mut self) -> Option<DeviceCompletion> {
        if let Some(top) = self.pending_completions.peek() {
            if top.completion_tick <= self.current_tick {
                if let Some(comp) = self.pending_completions.pop() {
                    return Some(DeviceCompletion {
                        op_id: comp.op_id,
                        result: comp.result,
                    });
                }
            }
        }
        None
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_sim_device_basic_io_lifecycle() {
        let rng = DetRng::new(42, "test_device");
        let config = SimDeviceConfig::for_test(SectorSize::Sector512);
        let mut device = SimDevice::new(config, rng);

        let file_id = FileId::wal_segment(1);

        // 1. Create file with 1024 bytes preallocation
        device.submit(DeviceOp::Create {
            op_id: 1,
            file_id,
            preallocate_bytes: 1024,
        });
        let comp = device.poll_completion().expect("create completion");
        assert_eq!(comp.op_id, 1);
        assert_eq!(comp.result.unwrap(), CompletionPayload::Created);
        assert!(device.file_exists(file_id));
        assert!(!device.is_file_durable(file_id));

        // 2. Write 100 bytes at offset 0
        let payload = Bytes::from_static(b"hello anvil storage simulated disk");
        device.submit(DeviceOp::Write {
            op_id: 2,
            file_id,
            offset: 0,
            data: payload.clone(),
        });
        let comp = device.poll_completion().expect("write completion");
        assert_eq!(comp.op_id, 2);
        assert_eq!(
            comp.result.unwrap(),
            CompletionPayload::Written(payload.len())
        );

        // 3. Read back from volatile cache
        device.submit(DeviceOp::Read {
            op_id: 3,
            file_id,
            offset: 0,
            len: payload.len(),
        });
        let comp = device.poll_completion().expect("read completion");
        assert_eq!(comp.op_id, 3);
        assert_eq!(
            comp.result.unwrap(),
            CompletionPayload::Read(payload.clone())
        );

        // 4. Sync directory to make file creation durable
        device.submit(DeviceOp::SyncDir { op_id: 4 });
        let comp = device.poll_completion().expect("syncdir completion");
        assert_eq!(comp.op_id, 4);
        assert_eq!(comp.result.unwrap(), CompletionPayload::DirSynced);
        assert!(device.is_file_durable(file_id));

        // 5. Fdatasync to make writes durable
        device.submit(DeviceOp::Fdatasync { op_id: 5, file_id });
        let comp = device.poll_completion().expect("sync completion");
        assert_eq!(comp.op_id, 5);
        assert_eq!(comp.result.unwrap(), CompletionPayload::Synced);

        let durable_bytes = device.get_file_durable_bytes(file_id).unwrap();
        assert_eq!(&durable_bytes[..payload.len()], &payload[..]);
    }

    #[test]
    fn test_sim_device_crash_unsynced_data_loss() {
        let rng = DetRng::new(1234, "crash_test");
        let mut config = SimDeviceConfig::for_test(SectorSize::Sector512);
        config.unsynced_sector_loss_rate_ppm = 1_000_000; // 100% loss of unsynced dirty sectors
        config.torn_write_rate_ppm = 0;
        let mut device = SimDevice::new(config, rng);

        let file_id = FileId::wal_segment(1);

        // Create and make durable
        device.submit(DeviceOp::Create {
            op_id: 1,
            file_id,
            preallocate_bytes: 1024,
        });
        let _ = device.poll_completion();
        device.submit(DeviceOp::SyncDir { op_id: 2 });
        let _ = device.poll_completion();
        device.submit(DeviceOp::Fdatasync { op_id: 3, file_id });
        let _ = device.poll_completion();

        // Write without sync
        let dirty_payload = Bytes::from_static(b"unsynced secret message");
        device.submit(DeviceOp::Write {
            op_id: 4,
            file_id,
            offset: 0,
            data: dirty_payload.clone(),
        });
        let _ = device.poll_completion();

        // Crash!
        device.crash();

        // Volatile cache should be wiped and reloaded from durable (all zeroes)
        let bytes_after_crash = device.get_file_volatile_bytes(file_id).unwrap();
        assert_eq!(&bytes_after_crash[..dirty_payload.len()], &[0u8; 23]);
    }

    #[test]
    fn test_sim_device_async_latency_queue() {
        let rng = DetRng::new(999, "latency_test");
        let config = SimDeviceConfig::with_latency(SectorSize::Sector512, 10, 10);
        let mut device = SimDevice::new(config, rng);

        let file_id = FileId::wal_segment(1);
        device.submit(DeviceOp::Create {
            op_id: 10,
            file_id,
            preallocate_bytes: 512,
        });

        // At tick 0, completion is not ready
        assert_eq!(device.poll_completion(), None);

        // Advance to tick 9: still not ready
        device.step_time(9);
        assert_eq!(device.poll_completion(), None);

        // Advance to tick 10: ready!
        device.step_time(1);
        let comp = device.poll_completion().expect("ready at tick 10");
        assert_eq!(comp.op_id, 10);
        assert_eq!(comp.result.unwrap(), CompletionPayload::Created);
    }
}
