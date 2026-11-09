#![forbid(unsafe_code)]

//! Deterministic in-memory network for `SimCluster`.
//!
//! `SimNetwork` owns the packet queue, partition set, and drop, duplicate,
//! and delay hooks. It never draws from a generator: hook decisions either
//! come from caller-installed closures or, for plan execution, from pure
//! hash functions of the plan salt and per-pair message sequence numbers.

use anvil_raft::Message;
use anvil_types::NodeId;
use std::collections::{BTreeMap, BTreeSet};

/// Hook to drop selected packets `(from, to, message) -> bool`.
pub type NetworkFilterHook = Box<dyn Fn(NodeId, NodeId, &Message) -> bool>;

/// Hook to inject delivery delays `(from, to, message) -> u64 delay_ticks`.
pub type NetworkDelayHook = Box<dyn Fn(NodeId, NodeId, &Message) -> u64>;

/// Plan-hash network fault parameters.
#[derive(Debug, Clone, Copy)]
pub struct PlanNet {
    /// Salt isolating one plan's fault decisions.
    pub salt: u64,
    /// Drop percentage in `[0, 100]`.
    pub drop_pct: u64,
    /// Duplicate percentage in `[0, 100]`.
    pub dup_pct: u64,
    /// Maximum extra delivery delay in ticks.
    pub max_delay: u64,
}

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

/// Deterministic packet queue with partitions and fault hooks.
pub struct SimNetwork {
    /// In-flight message queue in send order.
    pub in_flight: Vec<InFlightPacket>,
    /// Network partition pairs `(from, to)` where traffic is dropped.
    pub partitions: BTreeSet<(NodeId, NodeId)>,
    /// Optional hook to drop selected packets.
    pub drop_hook: Option<NetworkFilterHook>,
    /// Optional hook to duplicate packets.
    pub duplicate_hook: Option<NetworkFilterHook>,
    /// Optional hook to inject delivery delays.
    pub delay_hook: Option<NetworkDelayHook>,
    /// Per-pair message sequence numbers for plan-hash fault decisions.
    seqs: BTreeMap<(u64, u64), u64>,
}

impl SimNetwork {
    /// Creates an empty network with no partitions or hooks.
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_flight: Vec::new(),
            partitions: BTreeSet::new(),
            drop_hook: None,
            duplicate_hook: None,
            delay_hook: None,
            seqs: BTreeMap::new(),
        }
    }

    /// Returns true when no packets are queued.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }

    /// Returns the number of queued packets.
    #[must_use]
    pub fn len(&self) -> usize {
        self.in_flight.len()
    }

    /// Returns true when traffic between the pair is partitioned.
    #[must_use]
    pub fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.contains(&(from, to)) || self.partitions.contains(&(to, from))
    }

    /// Queues a message, applying partition, drop, duplicate, and delay hooks.
    /// Returns false when the packet was partitioned or dropped.
    pub fn schedule(&mut self, from: NodeId, to: NodeId, message: Message, now_tick: u64) -> bool {
        if self.is_cut(from, to) {
            return false;
        }
        if self
            .drop_hook
            .as_ref()
            .is_some_and(|hook| hook(from, to, &message))
        {
            return false;
        }
        let delay = self
            .delay_hook
            .as_ref()
            .map_or(0, |hook| hook(from, to, &message));
        let deliver_at = now_tick.saturating_add(delay);
        let duplicate = self
            .duplicate_hook
            .as_ref()
            .is_some_and(|hook| hook(from, to, &message));
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
        true
    }

    /// Queues a message under plan-hash faults: drop, duplicate, and delay
    /// come from pure hash functions of the plan salt and the per-pair
    /// message sequence number, so execution draws no generator state.
    pub fn schedule_plan(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: Message,
        now_tick: u64,
        net: &PlanNet,
    ) -> bool {
        if self.is_cut(from, to) {
            return false;
        }
        let seq = self.seqs.entry((from.get(), to.get())).or_insert(0);
        *seq = seq.saturating_add(1);
        let seq_no = *seq;
        if hash_pct(
            net.salt,
            from.get(),
            to.get(),
            seq_no,
            0x9E37_79B9_7F4A_7C15,
        ) < net.drop_pct
        {
            return false;
        }
        let delay = if net.max_delay == 0 {
            0
        } else {
            hash_pct(
                net.salt,
                from.get(),
                to.get(),
                seq_no,
                0xBF58_476D_1CE4_E5B9,
            ) % (net.max_delay + 1)
        };
        let deliver_at = now_tick.saturating_add(delay);
        self.in_flight.push(InFlightPacket {
            from,
            to,
            message: message.clone(),
            deliver_at_tick: deliver_at,
        });
        if hash_pct(
            net.salt,
            from.get(),
            to.get(),
            seq_no,
            0x94D0_49BB_1331_11EB,
        ) < net.dup_pct
        {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at.saturating_add(1),
            });
        }
        true
    }

    /// Removes and returns due packets in queue order, keeping the rest stable.
    pub fn take_due(&mut self, now_tick: u64) -> Vec<InFlightPacket> {
        let mut due = Vec::new();
        let mut kept = Vec::new();
        for packet in self.in_flight.drain(..) {
            if packet.deliver_at_tick <= now_tick {
                due.push(packet);
            } else {
                kept.push(packet);
            }
        }
        self.in_flight = kept;
        due
    }

    /// Returns true when any packet is due at `now_tick`.
    #[must_use]
    pub fn has_due(&self, now_tick: u64) -> bool {
        self.in_flight
            .iter()
            .any(|packet| packet.deliver_at_tick <= now_tick)
    }

    /// Removes and returns the packet at `index % len`, or `None` when empty.
    pub fn take_at(&mut self, index: usize) -> Option<InFlightPacket> {
        if self.in_flight.is_empty() {
            return None;
        }
        let actual = index % self.in_flight.len();
        Some(self.in_flight.remove(actual))
    }

    /// Drops the packet at `index % len`. Returns false when empty.
    pub fn drop_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() {
            return false;
        }
        let actual = index % self.in_flight.len();
        self.in_flight.remove(actual);
        true
    }

    /// Duplicates the packet at `index % len`, bounding the queue at 512.
    /// Returns false when empty or at the bound.
    pub fn duplicate_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() || self.in_flight.len() >= 512 {
            return false;
        }
        let actual = index % self.in_flight.len();
        let packet = self.in_flight[actual].clone();
        self.in_flight.push(packet);
        true
    }

    /// Cuts all traffic to and from `isolated`.
    pub fn isolate(&mut self, isolated: NodeId, peers: &[NodeId]) {
        for &peer in peers {
            if peer != isolated {
                self.partitions.insert((isolated, peer));
                self.partitions.insert((peer, isolated));
            }
        }
    }

    /// Cuts all traffic between two groups.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partitions.insert((a, b));
                self.partitions.insert((b, a));
            }
        }
    }

    /// Heals every partition.
    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }
}

impl Default for SimNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic percentage draw in `[0, 100)` from hashed inputs.
fn hash_pct(salt: u64, from: u64, to: u64, seq: u64, tag: u64) -> u64 {
    let mut x = salt
        .wrapping_add(from.wrapping_mul(0x517C_C1B7_2722_0A95))
        .wrapping_add(to.wrapping_mul(0x6C62_272E_07BB_0142))
        .wrapping_add(seq.wrapping_mul(0x4B66_F4D9_E213_5A2D))
        .wrapping_add(tag);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    (x ^ (x >> 31)) % 100
}
