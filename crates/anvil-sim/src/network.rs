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

/// Transport mode for a link or an entire run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportMode {
    /// TCP-like: per-connection FIFO. A reset drops in-flight packets for the
    /// pair and forces reconnect backoff. Reordering never happens inside one
    /// connection.
    OrderedLossy,
    /// UDP-like: no ordering guarantee. Due packets are released in a
    /// deterministic hash order so the protocol is stressed by reordering.
    Unordered,
}

/// Per-link one-way latency distribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyModel {
    /// Fixed delay in ticks.
    Constant(u64),
    /// Uniform delay in `[min, max]`.
    Uniform { min: u64, max: u64 },
    /// Exponential tail with integer mean in ticks.
    Exponential { mean: u64 },
    /// Two modes: fast path and slow tail (for example disk stall).
    Bimodal {
        fast: u64,
        fast_spread: u64,
        slow: u64,
        slow_spread: u64,
        fast_ppm: u32,
    },
}

impl LatencyModel {
    /// Samples a delay from pure hash inputs. No generator state is drawn.
    #[must_use]
    pub fn sample_hash(self, salt: u64, from: u64, to: u64, seq: u64) -> u64 {
        match self {
            Self::Constant(ticks) => ticks,
            Self::Uniform { min, max } => {
                if max <= min {
                    return min;
                }
                let span = max - min + 1;
                min + hash_pct_wide(salt, from, to, seq, 0xC2B2_AE3D_27D4_EB4F) % span
            }
            Self::Exponential { mean } => {
                if mean == 0 {
                    return 0;
                }
                let u = hash_u32(salt, from, to, seq, 0x1656_25B7_7629_3AED);
                let delay = exp_delay_fixed(mean, u);
                delay.min(mean.saturating_mul(8))
            }
            Self::Bimodal {
                fast,
                fast_spread,
                slow,
                slow_spread,
                fast_ppm,
            } => {
                let pick = hash_pct(salt, from, to, seq, 0x9E37_79B9_7F4A_7C15);
                let fast_pick = pick * 10_000 < u64::from(fast_ppm) / 100;
                let (mean, spread) = if fast_pick {
                    (fast, fast_spread)
                } else {
                    (slow, slow_spread)
                };
                if spread == 0 {
                    return mean;
                }
                let span = spread.saturating_mul(2) + 1;
                let delta = hash_pct_wide(salt, from, to, seq, 0xD6E8_FEB8_66F1_2D57) % span;
                mean.saturating_sub(spread).saturating_add(delta)
            }
        }
    }
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

/// Maximum in-flight packets retained by `duplicate_at` before it refuses.
pub const MAX_IN_FLIGHT_PACKETS: usize = 512;

/// Maximum backoff ticks after a connection reset.
pub const MAX_RESET_BACKOFF_TICKS: u64 = 32;

/// Upper bound on bandwidth catch-up scan when a link is saturated.
const MAX_BW_CATCH_UP_TICKS: u64 = 10_000;

/// Deterministic packet queue with partitions and fault hooks.
pub struct SimNetwork {
    /// In-flight message queue in send order.
    pub in_flight: Vec<InFlightPacket>,
    /// Directed network partition pairs `(from, to)` where traffic is dropped.
    pub partitions: BTreeSet<(NodeId, NodeId)>,
    /// Optional hook to drop selected packets.
    pub drop_hook: Option<NetworkFilterHook>,
    /// Optional hook to duplicate packets.
    pub duplicate_hook: Option<NetworkFilterHook>,
    /// Optional hook to inject delivery delays.
    pub delay_hook: Option<NetworkDelayHook>,
    /// Per-pair message sequence numbers for plan-hash fault decisions.
    seqs: BTreeMap<(u64, u64), u64>,
    /// Transport mode for the whole network.
    transport: TransportMode,
    /// Default latency model applied when a link has no override.
    base_latency: LatencyModel,
    /// Per-link one-way latency overrides keyed by `(from.get(), to.get())`.
    link_latency: BTreeMap<(u64, u64), LatencyModel>,
    /// Per-link bandwidth cap in messages per tick. Zero means unlimited.
    bandwidth_per_tick: u64,
    /// Consumed bandwidth per `((from, to), tick)`.
    bw_usage: BTreeMap<((u64, u64), u64), u64>,
    /// Salt for deterministic latency, reorder, spike, and reset draws.
    fault_salt: u64,
    /// Reorder percentage in `[0, 100]` for unordered delivery stress.
    reorder_pct: u64,
    /// Delay-spike percentage in `[0, 100]`.
    spike_pct: u64,
    /// Extra ticks added when a spike fires.
    spike_extra_ticks: u64,
    /// Connection-reset percentage in `[0, 100]` evaluated per message.
    reset_pct: u64,
    /// Reconnect backoff deadline per directed pair.
    backoff_until: BTreeMap<(u64, u64), u64>,
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
            transport: TransportMode::OrderedLossy,
            base_latency: LatencyModel::Constant(0),
            link_latency: BTreeMap::new(),
            bandwidth_per_tick: 0,
            bw_usage: BTreeMap::new(),
            fault_salt: 0,
            reorder_pct: 0,
            spike_pct: 0,
            spike_extra_ticks: 0,
            reset_pct: 0,
            backoff_until: BTreeMap::new(),
        }
    }

    /// Sets the transport mode for the whole network.
    pub fn set_transport(&mut self, mode: TransportMode) {
        self.transport = mode;
    }

    /// Returns the current transport mode.
    #[must_use]
    pub const fn transport(&self) -> TransportMode {
        self.transport
    }

    /// Sets the default latency model for links without an override.
    pub fn set_latency_model(&mut self, model: LatencyModel) {
        self.base_latency = model;
    }

    /// Sets a one-way latency model for a single directed link.
    pub fn set_link_latency(&mut self, from: NodeId, to: NodeId, model: LatencyModel) {
        self.link_latency.insert((from.get(), to.get()), model);
    }

    /// Sets the per-link bandwidth cap in messages per tick. Zero disables it.
    pub fn set_bandwidth_per_tick(&mut self, cap: u64) {
        self.bandwidth_per_tick = cap;
    }

    /// Sets the salt used for deterministic latency, reorder, spike, and
    /// reset draws on the hook path.
    pub fn set_fault_salt(&mut self, salt: u64) {
        self.fault_salt = salt;
    }

    /// Sets deterministic fault rates. Each value is a percentage in
    /// `[0, 100]` except `spike_extra`, which is a tick count.
    pub fn set_fault_rates(
        &mut self,
        reorder_pct: u64,
        spike_pct: u64,
        spike_extra: u64,
        reset_pct: u64,
    ) {
        self.reorder_pct = reorder_pct.min(100);
        self.spike_pct = spike_pct.min(100);
        self.spike_extra_ticks = spike_extra;
        self.reset_pct = reset_pct.min(100);
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

    /// Returns true when traffic from `from` to `to` is partitioned.
    ///
    /// The check is directed so one-way (asymmetric) partitions work: cutting
    /// `(a, b)` does not cut `(b, a)`. Symmetric helpers insert both
    /// directions, so old two-way behavior is unchanged.
    #[must_use]
    pub fn is_cut(&self, from: NodeId, to: NodeId) -> bool {
        self.partitions.contains(&(from, to))
    }

    /// Returns true while a reset backoff suppresses sends on the pair.
    #[must_use]
    pub fn is_backing_off(&self, from: NodeId, to: NodeId, now_tick: u64) -> bool {
        self.backoff_until
            .get(&(from.get(), to.get()))
            .is_some_and(|until| now_tick < *until)
    }

    /// Queues a message, applying partition, drop, duplicate, and delay hooks.
    /// Returns false when the packet was partitioned or dropped.
    pub fn schedule(&mut self, from: NodeId, to: NodeId, message: Message, now_tick: u64) -> bool {
        if self.is_cut(from, to) {
            return false;
        }
        if self.is_backing_off(from, to, now_tick) {
            return false;
        }
        if self
            .drop_hook
            .as_ref()
            .is_some_and(|hook| hook(from, to, &message))
        {
            return false;
        }
        let seq = self.next_seq(from, to);
        if self.fires(self.reset_pct, from, to, seq, 0x5B21_2E73_9C3A_7F11) {
            self.reset_connection(from, to, now_tick, 8);
            return false;
        }
        let mut deliver_at = now_tick.saturating_add(self.link_delay(from, to, seq));
        deliver_at = deliver_at.saturating_add(self.spike_delay(from, to, seq));
        if let Some(hook) = &self.delay_hook {
            deliver_at = deliver_at.saturating_add(hook(from, to, &message));
        }
        deliver_at = self.apply_bandwidth(from, to, deliver_at);
        let duplicate = self
            .duplicate_hook
            .as_ref()
            .is_some_and(|hook| hook(from, to, &message));
        if duplicate {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message: message.clone(),
                deliver_at_tick: deliver_at,
            });
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at.saturating_add(1),
            });
        } else {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at,
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
        let seq = self.next_seq(from, to);
        if hash_pct(net.salt, from.get(), to.get(), seq, 0x9E37_79B9_7F4A_7C15) < net.drop_pct {
            return false;
        }
        let delay = if net.max_delay == 0 {
            0
        } else {
            hash_pct(net.salt, from.get(), to.get(), seq, 0xBF58_476D_1CE4_E5B9)
                % (net.max_delay + 1)
        };
        let deliver_at = self.apply_bandwidth(from, to, now_tick.saturating_add(delay));
        self.in_flight.push(InFlightPacket {
            from,
            to,
            message: message.clone(),
            deliver_at_tick: deliver_at,
        });
        if hash_pct(net.salt, from.get(), to.get(), seq, 0x94D0_49BB_1331_11EB) < net.dup_pct {
            self.in_flight.push(InFlightPacket {
                from,
                to,
                message,
                deliver_at_tick: deliver_at.saturating_add(1),
            });
        }
        true
    }

    /// Removes and returns due packets at `now_tick`.
    ///
    /// Ordered-lossy mode keeps queue order so per-connection FIFO holds.
    /// Unordered mode releases due packets in deterministic hash order, which
    /// stresses the protocol with reordering while staying replayable.
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
        if self.transport == TransportMode::Unordered && self.reorder_pct > 0 {
            due.sort_by_key(|packet| {
                hash_wide(
                    self.fault_salt,
                    packet.from.get(),
                    packet.to.get(),
                    packet.deliver_at_tick,
                )
            });
        }
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

    /// Duplicates the packet at `index % len`, bounding the queue.
    /// Returns false when empty or at the bound.
    pub fn duplicate_at(&mut self, index: usize) -> bool {
        if self.in_flight.is_empty() || self.in_flight.len() >= MAX_IN_FLIGHT_PACKETS {
            return false;
        }
        let actual = index % self.in_flight.len();
        let mut packet = self.in_flight[actual].clone();
        packet.deliver_at_tick = packet.deliver_at_tick.saturating_add(1);
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

    /// Cuts all traffic between two groups in both directions.
    pub fn partition(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.partitions.insert((a, b));
                self.partitions.insert((b, a));
            }
        }
    }

    /// Random bisection helper: cuts every cross edge between the halves.
    pub fn bisect(&mut self, first_half: &[NodeId], second_half: &[NodeId]) {
        self.partition(first_half, second_half);
    }

    /// Bridge-node partition: `hub` keeps both sides reachable, but the sides
    /// cannot reach each other directly.
    pub fn bridge_hub(&mut self, hub: NodeId, side_a: &[NodeId], side_b: &[NodeId]) {
        self.partition(side_a, side_b);
        for &node in side_a.iter().chain(side_b.iter()) {
            if node != hub {
                self.partitions.remove(&(hub, node));
                self.partitions.remove(&(node, hub));
            }
        }
    }

    /// Cuts one direction only, leaving the reverse path live.
    pub fn cut_one_way(&mut self, from: NodeId, to: NodeId) {
        self.partitions.insert((from, to));
    }

    /// Drops every in-flight packet on the directed pair and suppresses new
    /// sends until `now_tick + backoff_ticks`. Corruption is modeled the same
    /// way: TLS would reject the frame, so the link drops and resets.
    pub fn reset_connection(
        &mut self,
        from: NodeId,
        to: NodeId,
        now_tick: u64,
        backoff_ticks: u64,
    ) {
        self.in_flight
            .retain(|packet| !(packet.from == from && packet.to == to));
        let backoff = backoff_ticks.min(MAX_RESET_BACKOFF_TICKS);
        self.backoff_until
            .insert((from.get(), to.get()), now_tick.saturating_add(backoff));
    }

    /// Heals every partition, restoring full peer connectivity.
    pub fn heal_all(&mut self) {
        self.partitions.clear();
    }

    /// Heals the partition between one pair, keeping other partitions intact.
    pub fn heal_pair(&mut self, first: NodeId, second: NodeId) {
        self.partitions.remove(&(first, second));
        self.partitions.remove(&(second, first));
    }

    /// Heals one direction only.
    pub fn heal_directed(&mut self, from: NodeId, to: NodeId) {
        self.partitions.remove(&(from, to));
    }

    fn next_seq(&mut self, from: NodeId, to: NodeId) -> u64 {
        let seq = self.seqs.entry((from.get(), to.get())).or_insert(0);
        *seq = seq.saturating_add(1);
        *seq
    }

    fn link_delay(&self, from: NodeId, to: NodeId, seq: u64) -> u64 {
        let model = self
            .link_latency
            .get(&(from.get(), to.get()))
            .copied()
            .unwrap_or(self.base_latency);
        model.sample_hash(self.fault_salt, from.get(), to.get(), seq)
    }

    fn spike_delay(&self, from: NodeId, to: NodeId, seq: u64) -> u64 {
        if self.spike_pct == 0 || self.spike_extra_ticks == 0 {
            return 0;
        }
        if self.fires(self.spike_pct, from, to, seq, 0xA24B_A3A9_7C5D_11E7) {
            self.spike_extra_ticks
        } else {
            0
        }
    }

    fn fires(&self, pct: u64, from: NodeId, to: NodeId, seq: u64, tag: u64) -> bool {
        pct > 0 && hash_pct(self.fault_salt, from.get(), to.get(), seq, tag) < pct
    }

    /// Enforces the per-link bandwidth cap by pushing the delivery tick
    /// forward until the tick has budget. Queueing is what a capped link
    /// does; no artificial delay is added while budget remains.
    fn apply_bandwidth(&mut self, from: NodeId, to: NodeId, mut deliver_at: u64) -> u64 {
        if self.bandwidth_per_tick == 0 {
            return deliver_at;
        }
        let pair = (from.get(), to.get());
        for _ in 0..MAX_BW_CATCH_UP_TICKS {
            let used = self.bw_usage.get(&(pair, deliver_at)).copied().unwrap_or(0);
            if used < self.bandwidth_per_tick {
                self.bw_usage.insert((pair, deliver_at), used + 1);
                return deliver_at;
            }
            deliver_at = deliver_at.saturating_add(1);
        }
        self.bw_usage.insert((pair, deliver_at), 1);
        deliver_at
    }
}

impl Default for SimNetwork {
    fn default() -> Self {
        Self::new()
    }
}

/// Deterministic percentage draw in `[0, 100)` from hashed inputs.
fn hash_pct(salt: u64, from: u64, to: u64, seq: u64, tag: u64) -> u64 {
    hash_wide(salt, from, to, seq.wrapping_add(tag)) % 100
}

fn hash_pct_wide(salt: u64, from: u64, to: u64, seq: u64, tag: u64) -> u64 {
    hash_wide(salt, from, to, seq.wrapping_add(tag))
}

fn hash_wide(salt: u64, from: u64, to: u64, seq: u64) -> u64 {
    let mut x = salt
        .wrapping_add(from.wrapping_mul(0x517C_C1B7_2722_0A95))
        .wrapping_add(to.wrapping_mul(0x6C62_272E_07BB_0142))
        .wrapping_add(seq.wrapping_mul(0x4B66_F4D9_E213_5A2D));
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn hash_u32(salt: u64, from: u64, to: u64, seq: u64, tag: u64) -> u32 {
    let wide = hash_wide(
        salt ^ tag,
        from.wrapping_add(seq),
        to,
        seq.wrapping_mul(0x9E37_79B9_7F4A_7C15),
    );
    (wide >> 32) as u32
}

/// Integer exponential delay with the given mean and a 32-bit uniform draw.
///
/// Uses fixed-point `-ln(U)` so the result is deterministic across machines
/// with no floating point in the crate.
#[allow(clippy::cast_possible_truncation)]
fn exp_delay_fixed(mean: u64, mut u: u32) -> u64 {
    if u == 0 {
        u = 1;
    }
    let ln = integer_ln_fixed(u);
    let neg_ln = 1_453_598u64.saturating_sub(ln);
    let product = u128::from(mean) * u128::from(neg_ln);
    (product >> 16) as u64
}

/// Integer natural log of `x` scaled by `2^16`.
#[allow(clippy::cast_possible_truncation)]
fn integer_ln_fixed(x: u32) -> u64 {
    if x <= 1 {
        return 0;
    }
    let leading = x.leading_zeros();
    let k = 31 - leading;
    let k_ln2 = u64::from(k) * 45_426;
    let rem = x - (1 << k);
    let f = ((u64::from(rem) << 16) >> k) as u32;
    let f_term = u64::from(f);
    let f_sq_half = (u64::from(f) * u64::from(f)) >> 17;
    k_ln2.saturating_add(f_term.saturating_sub(f_sq_half))
}
