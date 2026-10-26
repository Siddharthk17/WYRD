#![forbid(unsafe_code)]

//! Storage Fault Atlas enforcing the ANVIL Quorum Survivability Rule.
//!
//! # The Survivability Rule
//! The fault injector and nemesis never destroy every valid copy of a committed
//! log entry across a quorum (majority) of nodes. At all times, at least one node
//! in any potential majority retains a valid, uncorrupted copy of every committed entry.
//!
//! For a cluster of $N$ nodes where quorum size $Q = \lfloor N/2 \rfloor + 1$,
//! if $K$ nodes hold a valid copy of committed entry $I$, then every potential
//! majority contains at least one valid copy if and only if $K \ge Q$.
//! Therefore, corrupting a copy on node $M$ is allowed if and only if:
//! - Entry $I$ is uncommitted (Raft can overwrite uncommitted entries), or
//! - $K - 1 \ge Q$ (at least $Q$ nodes will still retain valid copies after the corruption).

use anvil_types::{LogIndex, NodeId};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

/// Internal shared state of the `StorageFaultAtlas`.
#[derive(Debug, Default)]
struct AtlasState {
    /// Number of nodes in the simulated cluster.
    cluster_size: usize,
    /// Minimum majority quorum size $Q = \lfloor N/2 \rfloor + 1$.
    quorum_size: usize,
    /// Highest known cluster-wide committed log index.
    commit_index: LogIndex,
    /// Map from `LogIndex` to the set of nodes currently holding a valid, uncorrupted copy.
    valid_copies: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    /// Whether the atlas is in permissive mode (allows all faults without veto).
    permissive: bool,
    /// Counter of vetoed corruption attempts to uphold survivability.
    vetoed_corruptions: u64,
    /// Counter of allowed corruption attempts.
    allowed_corruptions: u64,
}

/// Thread-safe, deterministically queryable Storage Fault Atlas.
#[derive(Debug, Clone)]
pub struct StorageFaultAtlas {
    inner: Arc<Mutex<AtlasState>>,
}

impl StorageFaultAtlas {
    /// Constructs an enforcing `StorageFaultAtlas` for a cluster of `cluster_size` nodes.
    #[must_use]
    pub fn new(cluster_size: usize) -> Self {
        assert!(cluster_size >= 1, "cluster size must be at least 1");
        let quorum_size = (cluster_size / 2) + 1;
        Self {
            inner: Arc::new(Mutex::new(AtlasState {
                cluster_size,
                quorum_size,
                commit_index: LogIndex::from_raw(0),
                valid_copies: BTreeMap::new(),
                permissive: false,
                vetoed_corruptions: 0,
                allowed_corruptions: 0,
            })),
        }
    }

    /// Constructs a permissive `StorageFaultAtlas` that allows all faults without veto.
    ///
    /// Useful for single-node tests and isolated fault injection scenarios.
    #[must_use]
    pub fn permissive() -> Self {
        Self {
            inner: Arc::new(Mutex::new(AtlasState {
                cluster_size: 1,
                quorum_size: 1,
                commit_index: LogIndex::from_raw(0),
                valid_copies: BTreeMap::new(),
                permissive: true,
                vetoed_corruptions: 0,
                allowed_corruptions: 0,
            })),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, AtlasState> {
        match self.inner.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Returns the cluster size.
    #[must_use]
    pub fn cluster_size(&self) -> usize {
        self.lock().cluster_size
    }

    /// Returns the required quorum size $Q = \lfloor N/2 \rfloor + 1$.
    #[must_use]
    pub fn quorum_size(&self) -> usize {
        self.lock().quorum_size
    }

    /// Returns the current highest cluster commit index.
    #[must_use]
    pub fn commit_index(&self) -> LogIndex {
        self.lock().commit_index
    }

    /// Advances the cluster-wide commit index.
    pub fn advance_commit_index(&self, new_commit_index: LogIndex) {
        let mut state = self.lock();
        if new_commit_index > state.commit_index {
            state.commit_index = new_commit_index;
        }
    }

    /// Records that `node_id` holds a valid, uncorrupted copy of `index`.
    pub fn record_persisted(&self, node_id: NodeId, index: LogIndex) {
        let mut state = self.lock();
        state.valid_copies.entry(index).or_default().insert(node_id);
    }

    /// Records that `node_id` lost or corrupted its copy of `index`.
    pub fn record_corruption(&self, node_id: NodeId, index: LogIndex) {
        let mut state = self.lock();
        if let Some(nodes) = state.valid_copies.get_mut(&index) {
            nodes.remove(&node_id);
        }
    }

    /// Returns the number of nodes currently holding a valid copy of `index`.
    #[must_use]
    pub fn valid_copies_count(&self, index: LogIndex) -> usize {
        let state = self.lock();
        state.valid_copies.get(&index).map_or(0, BTreeSet::len)
    }

    /// Checks whether entry `index` is committed.
    #[must_use]
    pub fn is_committed(&self, index: LogIndex) -> bool {
        let state = self.lock();
        index <= state.commit_index
    }

    /// Evaluates whether the injector is permitted to corrupt entry `index` on `node_id`.
    ///
    /// Returns `true` if corruption is allowed under the Quorum Survivability Rule.
    /// Returns `false` (vetoing corruption) if corrupting this entry would leave less than
    /// a quorum of nodes holding valid copies of a committed entry.
    pub fn can_corrupt_entry(&self, node_id: NodeId, index: LogIndex) -> bool {
        let mut state = self.lock();
        if state.permissive {
            state.allowed_corruptions += 1;
            return true;
        }

        // Uncommitted entries can be corrupted: Raft safety allows uncommitted logs to be overwritten
        if index > state.commit_index {
            state.allowed_corruptions += 1;
            return true;
        }

        // For committed entries, ensure at least Q nodes retain valid copies after this corruption
        let current_copies = state.valid_copies.get(&index).map_or(0, |nodes| {
            if nodes.contains(&node_id) {
                nodes.len()
            } else {
                nodes.len() + 1
            }
        });

        let copies_after = current_copies.saturating_sub(1);
        if copies_after >= state.quorum_size {
            state.allowed_corruptions += 1;
            true
        } else {
            state.vetoed_corruptions += 1;
            false
        }
    }

    /// Returns the total number of vetoed corruptions.
    #[must_use]
    pub fn vetoed_corruptions(&self) -> u64 {
        self.lock().vetoed_corruptions
    }

    /// Returns the total number of allowed corruptions.
    #[must_use]
    pub fn allowed_corruptions(&self) -> u64 {
        self.lock().allowed_corruptions
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_atlas_quorum_survivability_3_nodes() {
        let atlas = StorageFaultAtlas::new(3);
        assert_eq!(atlas.cluster_size(), 3);
        assert_eq!(atlas.quorum_size(), 2);

        let node1 = NodeId::new(1).unwrap();
        let node2 = NodeId::new(2).unwrap();
        let node3 = NodeId::new(3).unwrap();
        let idx = LogIndex::from_raw(5);

        // Record entry 5 on all 3 nodes
        atlas.record_persisted(node1, idx);
        atlas.record_persisted(node2, idx);
        atlas.record_persisted(node3, idx);
        assert_eq!(atlas.valid_copies_count(idx), 3);

        // Before commit, uncommitted entries can always be corrupted
        assert!(atlas.can_corrupt_entry(node1, idx));

        // Advance commit index to 5
        atlas.advance_commit_index(idx);
        assert!(atlas.is_committed(idx));

        // With 3 copies, corrupting 1 leaves 2 copies (>= quorum size 2): ALLOWED
        assert!(atlas.can_corrupt_entry(node1, idx));
        atlas.record_corruption(node1, idx);
        assert_eq!(atlas.valid_copies_count(idx), 2);

        // With 2 copies, corrupting another leaves 1 copy (< quorum size 2): VETOED!
        assert!(!atlas.can_corrupt_entry(node2, idx));
        assert!(!atlas.can_corrupt_entry(node3, idx));
        assert_eq!(atlas.vetoed_corruptions(), 2);
    }

    #[test]
    fn test_atlas_permissive_mode() {
        let atlas = StorageFaultAtlas::permissive();
        let node1 = NodeId::new(1).unwrap();
        let idx = LogIndex::from_raw(1);

        atlas.advance_commit_index(idx);
        assert!(atlas.can_corrupt_entry(node1, idx));
        assert_eq!(atlas.allowed_corruptions(), 1);
        assert_eq!(atlas.vetoed_corruptions(), 0);
    }
}
