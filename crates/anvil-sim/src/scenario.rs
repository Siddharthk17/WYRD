#![forbid(unsafe_code)]

//! Standard simulation scenarios for determinism gates, seed farms, and regression tests.

use crate::kernel::{Actor, ActorContext, SimKernel};
use crate::rng::DetRng;
use crate::time::SimTime;
use std::any::Any;
use std::collections::{BTreeMap, BTreeSet};

const MSG_CLIENT_PROPOSE: u8 = 1;
const MSG_REPLICATE: u8 = 2;
const MSG_REPLICATE_ACK: u8 = 3;
const MSG_COMMIT: u8 = 4;
const MSG_CLIENT_RESPONSE: u8 = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ProtocolMsg {
    kind: u8,
    sender: u8,
    id_or_idx: u16,
    value: u32,
}

impl ProtocolMsg {
    fn pack(self) -> u64 {
        (u64::from(self.kind) << 56)
            | (u64::from(self.sender) << 48)
            | (u64::from(self.id_or_idx) << 32)
            | u64::from(self.value)
    }

    #[allow(clippy::cast_possible_truncation)]
    fn unpack(raw: u64) -> Self {
        Self {
            kind: (raw >> 56) as u8,
            sender: (raw >> 48) as u8,
            id_or_idx: (raw >> 32) as u16,
            value: raw as u32,
        }
    }
}

struct NodeActor {
    id: u8,
    peers: Vec<u64>,
    is_leader: bool,
    counter: u32,
    last_log_index: u16,
    commit_index: u16,
    proposal_values: BTreeMap<u16, u32>,
    acks: BTreeMap<u16, BTreeSet<u8>>,
    rng: DetRng,
}

impl NodeActor {
    fn new(id: u8, peers: Vec<u64>, is_leader: bool, seed: u64) -> Self {
        Self {
            id,
            peers,
            is_leader,
            counter: 0,
            last_log_index: 0,
            commit_index: 0,
            proposal_values: BTreeMap::new(),
            acks: BTreeMap::new(),
            rng: DetRng::new(seed, &format!("node_{id}")),
        }
    }
}

impl Actor for NodeActor {
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64) {
        let now = ctx.now();
        let msg = ProtocolMsg::unpack(payload);

        match msg.kind {
            MSG_CLIENT_PROPOSE => {
                if !self.is_leader {
                    return;
                }
                self.last_log_index = self.last_log_index.saturating_add(1);
                let idx = self.last_log_index;
                self.proposal_values.insert(idx, msg.value);

                let mut self_acks = BTreeSet::new();
                self_acks.insert(self.id);
                self.acks.insert(idx, self_acks);

                for &peer in &self.peers {
                    let delay_nanos = self.rng.gen_range(5_000_000..15_000_000);
                    let replicate_msg = ProtocolMsg {
                        kind: MSG_REPLICATE,
                        sender: self.id,
                        id_or_idx: idx,
                        value: msg.value,
                    };
                    ctx.schedule(
                        now.saturating_add_nanos(delay_nanos),
                        peer,
                        replicate_msg.pack(),
                    );
                }
            }
            MSG_REPLICATE => {
                if self.is_leader {
                    return;
                }
                let reply_delay = self.rng.gen_range(2_000_000..8_000_000);
                let ack_msg = ProtocolMsg {
                    kind: MSG_REPLICATE_ACK,
                    sender: self.id,
                    id_or_idx: msg.id_or_idx,
                    value: msg.value,
                };
                ctx.schedule(
                    now.saturating_add_nanos(reply_delay),
                    u64::from(msg.sender),
                    ack_msg.pack(),
                );
            }
            MSG_REPLICATE_ACK => {
                if !self.is_leader {
                    return;
                }
                let idx = msg.id_or_idx;
                let acks = self.acks.entry(idx).or_default();
                acks.insert(msg.sender);

                if acks.len() >= 2 && idx > self.commit_index {
                    self.commit_index = idx;
                    if let Some(val) = self.proposal_values.get(&idx) {
                        self.counter = self.counter.saturating_add(*val);
                    }
                    ctx.record_progress();

                    for &peer in &self.peers {
                        let delay = self.rng.gen_range(2_000_000..5_000_000);
                        let commit_msg = ProtocolMsg {
                            kind: MSG_COMMIT,
                            sender: self.id,
                            id_or_idx: idx,
                            value: self.counter,
                        };
                        ctx.schedule(now.saturating_add_nanos(delay), peer, commit_msg.pack());
                    }

                    let client_delay = self.rng.gen_range(1_000_000..3_000_000);
                    let resp = ProtocolMsg {
                        kind: MSG_CLIENT_RESPONSE,
                        sender: self.id,
                        id_or_idx: idx,
                        value: self.counter,
                    };
                    ctx.schedule(now.saturating_add_nanos(client_delay), 100, resp.pack());
                }
            }
            MSG_COMMIT => {
                if self.is_leader {
                    return;
                }
                self.commit_index = msg.id_or_idx;
                self.counter = msg.value;
                ctx.record_progress();
            }
            _ => {}
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

struct ClientActor {
    leader_id: u64,
    target_proposals: u32,
    confirmed_proposals: u32,
    rng: DetRng,
}

impl ClientActor {
    fn new(leader_id: u64, target_proposals: u32, seed: u64) -> Self {
        Self {
            leader_id,
            target_proposals,
            confirmed_proposals: 0,
            rng: DetRng::new(seed, "client"),
        }
    }
}

impl Actor for ClientActor {
    fn on_event(&mut self, ctx: &mut ActorContext<'_>, payload: u64) {
        let now = ctx.now();
        let msg = ProtocolMsg::unpack(payload);

        if msg.kind == MSG_CLIENT_RESPONSE {
            self.confirmed_proposals = self.confirmed_proposals.saturating_add(1);
            ctx.record_progress();

            if self.confirmed_proposals < self.target_proposals {
                let think_time = self.rng.gen_range(1_000_000..5_000_000);
                #[allow(clippy::cast_possible_truncation)]
                let next_req = ProtocolMsg {
                    kind: MSG_CLIENT_PROPOSE,
                    sender: 100,
                    id_or_idx: self.confirmed_proposals as u16,
                    value: 1,
                };
                ctx.schedule(
                    now.saturating_add_nanos(think_time),
                    self.leader_id,
                    next_req.pack(),
                );
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Runs the standard replicated-counter simulation scenario for a given master seed.
///
/// Returns the total event count and streaming BLAKE3 trace hash, or a `SimError` on failure.
pub fn run_golden_scenario(seed: u64) -> Result<(u64, blake3::Hash), crate::kernel::SimError> {
    const NUM_PROPOSALS: u32 = 25;

    let mut kernel = SimKernel::new(seed);
    let node1 = NodeActor::new(1, vec![2, 3], true, seed);
    let node2 = NodeActor::new(2, vec![1, 3], false, seed);
    let node3 = NodeActor::new(3, vec![1, 2], false, seed);
    let client = ClientActor::new(1, NUM_PROPOSALS, seed);

    kernel.register_actor(1, Box::new(node1));
    kernel.register_actor(2, Box::new(node2));
    kernel.register_actor(3, Box::new(node3));
    kernel.register_actor(100, Box::new(client));

    let first_req = ProtocolMsg {
        kind: MSG_CLIENT_PROPOSE,
        sender: 100,
        id_or_idx: 0,
        value: 1,
    };
    kernel.schedule(SimTime::from_millis(1), 1, first_req.pack());

    let report = kernel.run()?;
    Ok((report.events_processed, report.trace_hash))
}

/// Negative test fixture intentionally introducing non-determinism via `std::collections::HashMap`
/// iteration.
///
/// This function deliberately allows `disallowed_types` to prove that iterating a `HashMap`
/// breaks cross-process determinism and triggers the determinism gate's failure path.
#[allow(clippy::disallowed_types)]
pub fn run_scenario_with_hashmap(
    seed: u64,
) -> Result<(u64, blake3::Hash), crate::kernel::SimError> {
    const NUM_PROPOSALS: u32 = 25;

    // Populate a HashMap with keys whose iteration order depends on process RandomState
    let mut non_det_map: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for i in 0..100 {
        non_det_map.insert(i, (i * 37) ^ seed);
    }

    let mut kernel = SimKernel::new(seed);
    let node1 = NodeActor::new(1, vec![2, 3], true, seed);
    let node2 = NodeActor::new(2, vec![1, 3], false, seed);
    let node3 = NodeActor::new(3, vec![1, 2], false, seed);
    let client = ClientActor::new(1, NUM_PROPOSALS, seed);

    kernel.register_actor(1, Box::new(node1));
    kernel.register_actor(2, Box::new(node2));
    kernel.register_actor(3, Box::new(node3));
    kernel.register_actor(100, Box::new(client));

    // Schedule events in HashMap iteration order (non-deterministic across processes)
    let mut offset_nanos = 1_000_000;
    for &k in non_det_map.keys() {
        let perturb_msg = ProtocolMsg {
            kind: MSG_CLIENT_PROPOSE,
            sender: 100,
            id_or_idx: 0,
            value: 1,
        };
        // Schedule slight jitter to cause event reordering based on map order
        offset_nanos += (k % 5) * 100;
        kernel.schedule(SimTime::from_nanos(offset_nanos), 1, perturb_msg.pack());
    }

    let report = kernel.run()?;
    Ok((report.events_processed, report.trace_hash))
}
