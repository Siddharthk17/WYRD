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
    /// Map from `LogIndex` to the set of nodes whose copy was corrupted or
    /// lost. Each index may be destroyed on at most `N - Q` nodes, so at
    /// least a quorum of valid copies always survives, whether or not the
    /// entry has committed yet.
    corrupted: BTreeMap<LogIndex, BTreeSet<NodeId>>,
    /// Nodes whose hard state is unrecoverably lost (refused restart).
    /// Hard state has no redundant copies elsewhere, so at most `N - Q`
    /// nodes may lose it: a second permanent loss would leave no electable
    /// quorum for any future term. Recorded at refusal time, when the
    /// loss is certain, never speculatively at fault time.
    hard_state_dead: BTreeSet<NodeId>,
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
                corrupted: BTreeMap::new(),
                hard_state_dead: BTreeSet::new(),
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
                corrupted: BTreeMap::new(),
                hard_state_dead: BTreeSet::new(),
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
        state.corrupted.entry(index).or_default().insert(node_id);
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

    /// Records that `node_id` refused restart: its hard state is
    /// unrecoverably lost. Later hard-state faults on other nodes are
    /// vetoed once the survivor budget is spent, so a second permanent
    /// loss cannot take the last electable quorum with it.
    pub fn record_hard_state_dead(&self, node_id: NodeId) {
        self.lock().hard_state_dead.insert(node_id);
    }

    /// Returns whether hard state on `node_id` may be destroyed. At most
    /// `N - Q` nodes may lose it; beyond that no quorum could ever elect
    /// again. Nodes with no other dead peer are always allowed, so fault
    /// pressure is unchanged until the first permanent loss.
    #[must_use]
    pub fn can_lose_hard_state(&self, node_id: NodeId) -> bool {
        let state = self.lock();
        if state.permissive {
            return true;
        }
        let others_dead = state
            .hard_state_dead
            .iter()
            .filter(|&&n| n != node_id)
            .count();
        let budget = state.cluster_size.saturating_sub(state.quorum_size).max(1);
        others_dead < budget
    }

    /// Shared survivability rule. Corrupting `index` on `node_id` keeps
    /// every quorum able to elect and recover: an entry may be destroyed on
    /// at most `N - Q` nodes, and a committed entry additionally needs a
    /// quorum of other recorded valid copies. The budget half covers
    /// entries corrupted before they commit: without it, two nodes could
    /// each destroy an uncommitted entry that later commits through the
    /// survivors, leaving one copy and no electable quorum.
    fn corruption_allowed(state: &AtlasState, node_id: NodeId, index: LogIndex) -> bool {
        if state.permissive {
            return true;
        }
        let already_destroyed = state
            .corrupted
            .get(&index)
            .map_or(0, |nodes| nodes.iter().filter(|&&n| n != node_id).count());
        let budget = state.cluster_size.saturating_sub(state.quorum_size).max(1);
        if already_destroyed >= budget {
            return false;
        }
        if index > state.commit_index {
            return true;
        }
        let others_valid = state
            .valid_copies
            .get(&index)
            .map_or(0, |nodes| nodes.iter().filter(|&&n| n != node_id).count());
        others_valid >= state.quorum_size
    }

    /// Returns whether corrupting entry `index` on `node_id` keeps a quorum
    /// of valid copies of every committed entry. Pure query without
    /// touching the allow/veto counters; crash-time fate uses it to decide
    /// without accounting a corruption that never happens.
    #[must_use]
    pub fn entry_survives_corruption(&self, node_id: NodeId, index: LogIndex) -> bool {
        Self::corruption_allowed(&self.lock(), node_id, index)
    }

    /// Evaluates whether the injector is permitted to corrupt entry `index` on `node_id`.
    ///
    /// Returns `true` if corruption is allowed under the Quorum Survivability Rule.
    /// Returns `false` (vetoing corruption) if corrupting this entry would leave less than
    /// a quorum of nodes holding valid copies of a committed entry.
    pub fn can_corrupt_entry(&self, node_id: NodeId, index: LogIndex) -> bool {
        let mut state = self.lock();
        let decision = Self::corruption_allowed(&state, node_id, index);
        if decision {
            state.allowed_corruptions += 1;
        } else {
            state.vetoed_corruptions += 1;
        }
        decision
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
