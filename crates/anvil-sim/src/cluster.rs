#![forbid(unsafe_code)]

//! In-memory multi-node `SimCluster` test harness for Raft consensus.
//!
//! Models 3, 5, and 7-node clusters with:
//! - In-memory network message routing with drop, duplicate, and delay hooks
//! - Simulated storage persistence completions
//! - Per-step online invariant verification across all nodes

use crate::raft_invariants::RaftInvariantChecker;
use crate::rng::derive_component_seed;
use anvil_raft::{Input, Message, Output, RaftConfig, RaftCore, RecoveredState, Role};
use anvil_types::{LogIndex, NodeId};
use bytes::Bytes;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::collections::{BTreeMap, BTreeSet};

/// Hook to drop selected packets `(from, to, message) -> bool`.
pub type NetworkFilterHook = Box<dyn Fn(NodeId, NodeId, &Message) -> bool>;

/// Hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
pub type NetworkDelayHook = Box<dyn Fn(NodeId, NodeId, &Message) -> u64>;

/// In-flight network packet queued for discrete logical time delivery.
#[derive(Debug, Clone)]
pub struct InFlightPacket {
    /// Originating sender node ID.
    pub from: NodeId,
    /// Destination recipient node ID.
    pub to: NodeId,
    /// Consensus message payload.
    pub message: Message,
    /// Logical tick at which packet becomes deliverable.
    pub deliver_at_tick: u64,
}

/// Simulated Raft cluster harness for deterministic consensus testing.
pub struct SimCluster {
    /// Active Raft consensus cores indexed by node ID.
    pub nodes: BTreeMap<NodeId, RaftCore>,
    /// Global cluster logical tick counter.
    pub current_tick: u64,
    /// In-flight message queue.
    pub in_flight: Vec<InFlightPacket>,
    /// Network partition pairs `(from, to)` where traffic is unconditionally dropped.
    pub partitions: BTreeSet<(NodeId, NodeId)>,
    /// Global online invariant checker called after every node transition.
    pub checker: RaftInvariantChecker,
    /// Scratch output buffer reused across steps to eliminate allocations.
    output_scratch: Output,
    /// Optional hook to drop selected packets `(from, to, message) -> bool`.
    pub drop_hook: Option<NetworkFilterHook>,
    /// Optional hook to duplicate packets `(from, to, message) -> bool`.
    pub duplicate_hook: Option<NetworkFilterHook>,
    /// Optional hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
    pub delay_hook: Option<NetworkDelayHook>,
}

impl SimCluster {
    /// Constructs a new `SimCluster` with `size` nodes (typically 3, 5, or 7) seeded from `master_seed`.
    #[must_use]
    pub fn new(size: usize, master_seed: u64) -> Self {
        assert!(
            (1..=9).contains(&size),
            "cluster size must be between 1 and 9 nodes"
        );

        let mut node_ids = Vec::with_capacity(size);
        for i in 1..=size {
            let id_raw = u64::try_from(i).unwrap_or(1);
            if let Some(id) = NodeId::new(id_raw) {
                node_ids.push(id);
            }
        }

        let mut nodes = BTreeMap::new();
        for &id in &node_ids {
            let mut peers = BTreeSet::new();
            for &peer_id in &node_ids {
                if peer_id != id {
                    peers.insert(peer_id);
                }
            }

            let config = RaftConfig {
                node_id: id,
                peers,
                election_timeout_ticks: 10,
                heartbeat_interval_ticks: 3,
                max_inflight_messages: 256,
                max_bytes_per_message: 64 * 1024,
                pre_vote_enabled: true,
                check_quorum: true,
            };

            let path = format!("cluster/node/{}", id.get());
            let seed = derive_component_seed(master_seed, &path);
            let rng = ChaCha8Rng::from_seed(seed);

            let core = RaftCore::new(config, RecoveredState::default(), rng);
            nodes.insert(id, core);
        }

        Self {
            nodes,
            current_tick: 0,
            in_flight: Vec::new(),
            partitions: BTreeSet::new(),
            checker: RaftInvariantChecker::new(),
            output_scratch: Output::new(),
            drop_hook: None,
            duplicate_hook: None,
            delay_hook: None,
        }
    }

    /// Advances the cluster by one logical tick.
    ///
    /// 1. Steps `Input::Tick` on all live nodes.
    /// 2. Delivers packets scheduled for `current_tick`.
    /// 3. Evaluates online invariants across all nodes.
    pub fn tick(&mut self) {
        self.current_tick = self.current_tick.saturating_add(1);

        // 1. Tick every node
        let node_ids: Vec<NodeId> = self.nodes.keys().copied().collect();
        for id in node_ids {
            self.step_node_internal(id, Input::Tick);
        }

        // 2. Deliver all in-flight packets scheduled for now or earlier
        self.deliver_due_messages();
    }

    /// Advances the cluster by `n` ticks.
    pub fn tick_n(&mut self, n: usize) {
        for _ in 0..n {
            self.tick();
        }
    }

    /// Steps a specific node with an `Input` event, processes all emitted side-effects,
    /// and immediately validates online invariants.
    pub fn step_node(&mut self, id: NodeId, input: Input) {
        self.step_node_internal(id, input);
    }

    fn step_node_internal(&mut self, id: NodeId, input: Input) {
        self.output_scratch.clear();

        // Step the core
        if let Some(core) = self.nodes.get_mut(&id) {
            core.step(input, &mut self.output_scratch);
        }

        // Process persistence operations immediately in simulator (simulating durable storage sync)
        let persist_tokens: Vec<_> = self
            .output_scratch
            .persist_ops
            .iter()
            .map(anvil_raft::PersistOp::token)
            .collect();

        // Enqueue outbound network messages into simulated network
        let outbound = std::mem::take(&mut self.output_scratch.messages);
        for msg in outbound {
            self.route_message(id, msg.to, msg.message);
        }

        // Check invariants immediately after step
        self.checker.check_invariants(&self.nodes);

        // Complete persistence callbacks back to node
        for token in persist_tokens {
            self.output_scratch.clear();
            if let Some(core) = self.nodes.get_mut(&id) {
                core.step(Input::Persisted { token }, &mut self.output_scratch);
            }
            let follow_up = std::mem::take(&mut self.output_scratch.messages);
            for msg in follow_up {
                self.route_message(id, msg.to, msg.message);
            }
            self.checker.check_invariants(&self.nodes);
        }
    }

    /// Routes an outbound message through network hooks, partitions, and delay scheduling.
    fn route_message(&mut self, from: NodeId, to: NodeId, message: Message) {
        // 1. Partition filter
        if self.partitions.contains(&(from, to)) || self.partitions.contains(&(to, from)) {
            return;
        }

        // 2. Drop hook
        if self
            .drop_hook
            .as_ref()
            .is_some_and(|h| h(from, to, &message))
        {
            return;
        }

        // 3. Delay calculation
        let delay = self
            .delay_hook
            .as_ref()
            .map_or(0, |h| h(from, to, &message));
        let deliver_at = self.current_tick.saturating_add(delay);

        // 4. Duplicate hook
        let duplicate = self
            .duplicate_hook
            .as_ref()
            .is_some_and(|h| h(from, to, &message));

        self.in_flight.push(InFlightPacket {
            from,
            to,
            message: message.clone(),
            deliver_at_tick: deliver_at,
        });

        if duplicate {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at.saturating_add(1),
            });
        }
    }

    /// Delivers all messages whose `deliver_at_tick <= current_tick`.
    pub fn deliver_due_messages(&mut self) {
        let mut loop_count = 0usize;
        loop {
            loop_count += 1;
            assert!(
                loop_count < 10_000,
                "message storm detected: delivered over 10,000 messages in one tick"
            );

            let mut due_indices = Vec::new();
            for (i, pkt) in self.in_flight.iter().enumerate() {
                if pkt.deliver_at_tick <= self.current_tick {
                    due_indices.push(i);
                }
            }

            if due_indices.is_empty() {
                break;
            }

            // Extract due packets in FIFO order
            let mut packets_to_deliver = Vec::new();
            for &idx in due_indices.iter().rev() {
                packets_to_deliver.push(self.in_flight.swap_remove(idx));
            }
            packets_to_deliver.reverse();

            for pkt in packets_to_deliver {
                // Drop if partition was created in the interim
                if self.partitions.contains(&(pkt.from, pkt.to))
                    || self.partitions.contains(&(pkt.to, pkt.from))
                {
                    continue;
                }
                self.step_node_internal(
                    pkt.to,
                    Input::Recv {
                        from: pkt.from,
                        message: pkt.message,
                    },
                );
            }
        }
    }

    /// Drains all in-flight messages by advancing logical time until the network is quiescent.
    pub fn drain_network(&mut self, max_ticks: usize) {
        for _ in 0..max_ticks {
            if self.in_flight.is_empty() {
                break;
            }
            self.tick();
        }
    }

    /// Proposes a client command payload to a target node (typically the leader).
    pub fn propose(&mut self, node_id: NodeId, payload: Bytes) {
        self.step_node_internal(node_id, Input::Propose { payload });
    }

    /// Returns the currently elected leader node ID, or `None` if no single leader exists.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        let mut leader = None;
        for (&id, core) in &self.nodes {
            if core.role() == Role::Leader {
                if leader.is_some() {
                    // Invariant checker would already catch split brain, but safeguard here
                    return None;
                }
                leader = Some(id);
            }
        }
        leader
    }

    /// Isolates a node completely by cutting bidirectional network traffic to all peers.
    pub fn isolate_node(&mut self, isolated: NodeId) {
        for &peer in self.nodes.keys() {
            if peer != isolated {
                self.partitions.insert((isolated, peer));
                self.partitions.insert((peer, isolated));
            }
        }
    }

    /// Partitions the cluster into two disconnected components.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partitions.insert((a, b));
                self.partitions.insert((b, a));
            }
        }
    }

    /// Heals all network partitions, restoring full peer connectivity.
    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    /// Helper to tick the cluster until a leader is elected, up to `max_ticks`.
    pub fn elect_leader(&mut self, max_ticks: usize) -> NodeId {
        for _ in 0..max_ticks {
            if let Some(leader) = self.leader() {
                return leader;
            }
            self.tick();
        }
        let leader = self.leader();
        assert!(
            leader.is_some(),
            "failed to elect leader within {max_ticks} ticks"
        );
        leader.unwrap_or(NodeId::from_nonzero(core::num::NonZeroU64::MIN))
    }

    /// Returns the highest committed log index agreed across all live replicas.
    #[must_use]
    pub fn highest_commit_index(&self) -> LogIndex {
        self.nodes
            .values()
            .map(anvil_raft::RaftCore::commit_index)
            .max()
            .unwrap_or(LogIndex::from_raw(0))
    }
}
