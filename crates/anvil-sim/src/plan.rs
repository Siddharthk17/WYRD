#![forbid(unsafe_code)]

//! Serializable fault plans and the plan shrinker.
//!
//! A `Plan` materializes every fault schedule and workload choice as data
//! before a run starts. Network faults come from pure hash functions of the
//! plan salt and per-pair message sequence numbers, and all timing comes from
//! the plan's tick schedule. Consensus election jitter and storage fault
//! draws use seeded RNG streams derived from the master seed, so the same
//! seed replays identically. Any failure replays exactly from the plan
//! encoding, and the shrinker reduces a failing plan while preserving the
//! failure.
//!
//! Fault generation uses swarm testing: each seed enables a random subset of
//! fault classes at random intensities. At least thirty percent of seeds
//! enable only one or two classes, and some seeds run everything at once.
//! Every event carries a stable id and draws its parameters from a
//! per-event stream keyed by that id, so removing one event never reshuffles
//! the others and delta-debugging stays sound.

use crate::cluster::{PlanNet, SimCluster};
use crate::rng::DetRng;
use anvil_types::{LogIndex, sometimes};
use bytes::Bytes;
use std::fmt::Write as _;
use std::panic::AssertUnwindSafe;

/// Current plan encoding version.
pub const PLAN_VERSION: u32 = 2;

/// Upper bound accepted for tick counts and entry lists on decode.
const PLAN_LIMIT: u64 = 100_000;

/// Upper bound on fault events in one plan.
const MAX_EVENTS: usize = 64;

/// One timed fault. `id` is stable across shrinks; all parameters come from
/// the per-event stream keyed by `(seed, id)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FaultEvent {
    /// Stable per-plan event id.
    pub id: u32,
    /// First tick the fault is active.
    pub start: u64,
    /// First tick the fault is lifted.
    pub end: u64,
    /// What the fault does.
    pub fault: FaultKind,
}

/// Fault vocabulary: network, process, clock, and disk faults.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FaultKind {
    /// Cut both directions of one member's links.
    Partition { member: usize },
    /// Cut one direction of a link.
    Asymmetric { from: usize, to: usize },
    /// Crash a node; it restarts at the event end.
    Crash { node: usize },
    /// Pause a node with stale timers firing on resume.
    Pause { node: usize },
    /// Skew one node's tick rate in ticks per mille (500 to 2000).
    SlowClock { node: usize, rate_per_mille: u64 },
    /// Hand leadership to one node through `TimeoutNow`. The transfer runs
    /// inside the chaos window and aborts on its own when the target is
    /// unreachable, so liveness after heal is unaffected.
    Transfer { node: usize },
    /// Add delivery delay in ticks while active.
    DelaySpike { extra_ticks: u64 },
}

/// Swarm client-profile parameters. The client operation list expands
/// deterministically from `(seed, profile)` before the run starts, so the
/// profile is the materialized form of the workload choice. The fields
/// mirror the workload's swarm profile one to one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkloadSpec {
    /// Number of clients.
    pub clients: usize,
    /// Key space size.
    pub keys: usize,
    /// Operations per client.
    pub ops_per_client: usize,
    /// Percentage of list-append keys.
    pub list_pct: u64,
    /// Think ticks between operations.
    pub think_ticks: u64,
    /// Timeout ticks before an operation is indeterminate.
    pub timeout_ticks: u64,
    /// Whether clients crash mid-run.
    pub client_crashes: bool,
    /// Whether the unordered transport is used.
    pub unordered: bool,
}

/// A materialized fault schedule and workload for one deterministic run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// Encoding version.
    pub version: u32,
    /// Master seed this plan was generated from.
    pub seed: u64,
    /// Cluster size (3, 5, or 7).
    pub cluster_size: usize,
    /// Total run ticks before draining.
    pub ticks: u64,
    /// Ticks carrying a client proposal when a leader exists.
    pub proposals: Vec<u64>,
    /// Ticks at or below this carry no faults (warm-up).
    pub warmup_end: u64,
    /// Ticks at or above this carry no faults (heal).
    pub heal_start: u64,
    /// Timed fault events, sorted by `(start, id)`.
    pub events: Vec<FaultEvent>,
    /// Network drop percentage in `[0, 100]`.
    pub drop_pct: u64,
    /// Network duplicate percentage in `[0, 100]`.
    pub dup_pct: u64,
    /// Maximum extra delivery delay in ticks.
    pub max_delay: u64,
    /// Salt isolating this plan's network fault decisions.
    pub salt: u64,
    /// Storage fault injection enabled for the run.
    pub storage_faults: bool,
    /// Swarm workload parameters, if this plan drives a KV run.
    pub workload: Option<WorkloadSpec>,
}

/// Failure to decode a plan encoding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// Encoding does not start with the expected `anvil-plan/` prefix.
    #[error("not a plan encoding")]
    BadPrefix,
    /// Encoding version is newer than this build understands.
    #[error("unsupported plan version {0}")]
    BadVersion(u32),
    /// A field failed to parse or violates plan bounds.
    #[error("malformed plan field: {0}")]
    BadField(&'static str),
}

/// Fault classes the swarm sampler enables per seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FaultClass {
    Partition,
    Asymmetric,
    Crash,
    Pause,
    Clock,
    Delay,
    Loss,
}

impl FaultClass {
    const ALL: [Self; 7] = [
        Self::Partition,
        Self::Asymmetric,
        Self::Crash,
        Self::Pause,
        Self::Clock,
        Self::Delay,
        Self::Loss,
    ];
}

impl Plan {
    /// Generates the canonical plan for a seed: a short consensus run with
    /// no workload profile. All sampling happens here, through isolated
    /// `DetRng` streams, before the run starts.
    #[must_use]
    pub fn generate(seed: u64, storage_faults: bool) -> Self {
        let mut rng = DetRng::new(seed, "sim/plan/size");
        let sizes = [3, 5, 7];
        let size_idx = usize::try_from(rng.gen_range(0..3)).unwrap_or(0);
        let cluster_size = sizes[size_idx % sizes.len()];
        Self::generate_inner(seed, storage_faults, 30, None, cluster_size)
    }

    /// Generates a workload plan: a longer run whose client operations expand
    /// from the embedded swarm profile.
    #[must_use]
    pub fn generate_for_workload(seed: u64, storage_faults: bool, workload: WorkloadSpec) -> Self {
        // The KV workload always runs three nodes.
        Self::generate_inner(seed, storage_faults, 400, Some(workload), 3)
    }

    fn generate_inner(
        seed: u64,
        storage_faults: bool,
        ticks: u64,
        workload: Option<WorkloadSpec>,
        cluster_size: usize,
    ) -> Self {
        let mut rng = DetRng::new(seed, "sim/plan");
        let warmup_end = (ticks / 8).max(5);
        let heal_start = ticks.saturating_sub(ticks / 8).max(warmup_end + 1);

        let classes = Self::pick_classes(&mut rng);
        let lossy = classes.contains(&FaultClass::Loss);
        let (drop_pct, dup_pct, max_delay) = if lossy {
            (
                rng.gen_range(0..16),
                rng.gen_range(0..10),
                rng.gen_range(0..6),
            )
        } else {
            (0, 0, rng.gen_range(0..3))
        };

        let mut events = Vec::new();
        let mut next_id = 0u32;
        for class in &classes {
            if *class == FaultClass::Loss {
                continue;
            }
            let count = match class {
                FaultClass::Crash => rng.gen_range(1..3),
                _ => rng.gen_range(0..3),
            };
            for _ in 0..count {
                if events.len() >= MAX_EVENTS {
                    break;
                }
                let id = next_id;
                next_id = next_id.saturating_add(1);
                let mut erng = DetRng::new(seed, &format!("sim/plan/event/{id}"));
                let span = heal_start.saturating_sub(warmup_end + 1).max(1);
                let start = warmup_end + 1 + erng.gen_range(0..span) % span;
                let len = 2 + erng.gen_range(0..span / 2 + 2);
                let end = (start + len).min(heal_start);
                let fault = Self::sample_fault(*class, cluster_size, &mut erng);
                events.push(FaultEvent {
                    id,
                    start,
                    end,
                    fault,
                });
            }
        }
        // Every fourth workload seed runs a partition spanning the whole
        // chaos window. Short faults rarely push a follower more than the
        // retain window behind a compacted leader, so without a long
        // partition the snapshot-install path would almost never execute.
        // It is an ordinary event: serializable, shrinkable, and drawn at
        // generation time like everything else.
        if workload.is_some() && seed.is_multiple_of(4) {
            let id = next_id;
            next_id = next_id.saturating_add(1);
            let member =
                usize::try_from(rng.gen_range(0..cluster_size as u64)).unwrap_or(0) % cluster_size;
            events.push(FaultEvent {
                id,
                start: warmup_end + 1,
                end: heal_start,
                fault: FaultKind::Partition { member },
            });
        }
        Self::push_transfer_events(
            seed,
            workload.is_some(),
            warmup_end,
            heal_start,
            cluster_size,
            &mut events,
            &mut next_id,
        );
        events.sort_by_key(|event| (event.start, event.id));

        let mut proposals = Vec::new();
        let mut tick = warmup_end + 1;
        while tick <= ticks {
            proposals.push(tick);
            tick += 6;
        }

        let salt = rng.next_u64();
        Self {
            version: PLAN_VERSION,
            seed,
            cluster_size,
            ticks,
            proposals,
            warmup_end,
            heal_start,
            events,
            drop_pct,
            dup_pct,
            max_delay,
            salt,
            storage_faults,
            workload,
        }
    }

    /// Adds `TimeoutNow` transfer events to workload plans so the transfer
    /// path executes under faults. The presence draw uses an independent
    /// stream, so consensus plans and existing event parameters never
    /// shift; per-event parameters still come from the stream keyed by
    /// event id.
    fn push_transfer_events(
        seed: u64,
        is_workload: bool,
        warmup_end: u64,
        heal_start: u64,
        cluster_size: usize,
        events: &mut Vec<FaultEvent>,
        next_id: &mut u32,
    ) {
        if !is_workload {
            return;
        }
        let mut trng = DetRng::new(seed, "sim/plan/transfer");
        if trng.gen_range(0..100) >= 30 {
            return;
        }
        let count = 1 + trng.gen_range(0..2);
        for _ in 0..count {
            if events.len() >= MAX_EVENTS {
                break;
            }
            let id = *next_id;
            *next_id = next_id.saturating_add(1);
            let mut erng = DetRng::new(seed, &format!("sim/plan/event/{id}"));
            let span = heal_start.saturating_sub(warmup_end + 1).max(1);
            let start = warmup_end + 1 + erng.gen_range(0..span) % span;
            let end = (start + 1).min(heal_start);
            let node =
                usize::try_from(erng.gen_range(0..cluster_size as u64)).unwrap_or(0) % cluster_size;
            events.push(FaultEvent {
                id,
                start,
                end,
                fault: FaultKind::Transfer { node },
            });
        }
    }

    /// Swarm class selection: at least thirty percent of seeds run one or
    /// two classes, a few run everything, the rest run a random subset.
    fn pick_classes(rng: &mut DetRng) -> Vec<FaultClass> {
        let roll = rng.gen_range(0..100);
        if roll < 30 {
            let a = usize::try_from(rng.gen_range(0..7)).unwrap_or(0);
            let mut out = vec![FaultClass::ALL[a % 7]];
            if rng.gen_range(0..2) == 0 {
                let b = usize::try_from(rng.gen_range(0..7)).unwrap_or(0);
                let class = FaultClass::ALL[b % 7];
                if !out.contains(&class) {
                    out.push(class);
                }
            }
            out
        } else if roll < 40 {
            FaultClass::ALL.to_vec()
        } else {
            let mut out = Vec::new();
            for class in FaultClass::ALL {
                if rng.gen_range(0..2) == 0 {
                    out.push(class);
                }
            }
            if out.is_empty() {
                out.push(FaultClass::Crash);
            }
            out
        }
    }

    fn sample_fault(class: FaultClass, cluster_size: usize, rng: &mut DetRng) -> FaultKind {
        let member = |rng: &mut DetRng| {
            usize::try_from(rng.gen_range(0..cluster_size as u64)).unwrap_or(0) % cluster_size
        };
        match class {
            FaultClass::Partition => FaultKind::Partition {
                member: member(rng),
            },
            FaultClass::Asymmetric => {
                let from = member(rng);
                let mut to = member(rng);
                if to == from {
                    to = (to + 1) % cluster_size;
                }
                FaultKind::Asymmetric { from, to }
            }
            FaultClass::Crash => FaultKind::Crash { node: member(rng) },
            FaultClass::Pause => FaultKind::Pause { node: member(rng) },
            FaultClass::Clock => {
                let rates = [500u64, 750, 1000, 1500, 2000];
                let pick = usize::try_from(rng.gen_range(0..5)).unwrap_or(0);
                FaultKind::SlowClock {
                    node: member(rng),
                    rate_per_mille: rates[pick % rates.len()],
                }
            }
            FaultClass::Delay => FaultKind::DelaySpike {
                extra_ticks: rng.gen_range(2..12),
            },
            FaultClass::Loss => FaultKind::DelaySpike { extra_ticks: 0 },
        }
    }

    /// Encodes the plan as versioned text for logs, files, and repro commands.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut out = format!(
            "anvil-plan/{};seed={};size={};ticks={};warm={};heal={};net={},{},{};salt={};storage={}",
            self.version,
            self.seed,
            self.cluster_size,
            self.ticks,
            self.warmup_end,
            self.heal_start,
            self.drop_pct,
            self.dup_pct,
            self.max_delay,
            self.salt,
            u8::from(self.storage_faults),
        );
        if !self.proposals.is_empty() {
            out.push_str(";propose=");
            out.push_str(
                &self
                    .proposals
                    .iter()
                    .map(u64::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
            );
        }
        for event in &self.events {
            let _ = write!(out, ";ev={}", encode_event(event));
        }
        if let Some(workload) = &self.workload {
            let _ = write!(
                out,
                ";workload={},{},{},{},{},{},{},{}",
                workload.clients,
                workload.keys,
                workload.ops_per_client,
                workload.list_pct,
                workload.think_ticks,
                workload.timeout_ticks,
                u8::from(workload.client_crashes),
                u8::from(workload.unordered),
            );
        }
        out
    }

    /// Decodes a plan encoding, rejecting unknown versions and out-of-range fields.
    pub fn decode(input: &str) -> Result<Self, PlanError> {
        let body = input
            .strip_prefix("anvil-plan/")
            .ok_or(PlanError::BadPrefix)?;
        let (version_raw, mut rest) = body.split_once(';').ok_or(PlanError::BadField("version"))?;
        let version: u32 = version_raw
            .parse()
            .map_err(|_| PlanError::BadField("version"))?;
        if version != PLAN_VERSION {
            return Err(PlanError::BadVersion(version));
        }
        let mut state = DecodedPlan::default();
        if rest.is_empty() {
            return Err(PlanError::BadField("empty"));
        }
        loop {
            let (field, next) = rest.split_once(';').map_or((rest, ""), |(a, b)| (a, b));
            let (key, value) = field.split_once('=').ok_or(PlanError::BadField("pair"))?;
            state.apply(key, value)?;
            if next.is_empty() {
                break;
            }
            rest = next;
        }
        state.build(version)
    }
}

/// Encodes one fault event as `id:start-end:kind:params`.
fn encode_event(event: &FaultEvent) -> String {
    let (kind, params) = match event.fault {
        FaultKind::Partition { member } => ("part", member.to_string()),
        FaultKind::Asymmetric { from, to } => ("asym", format!("{from}-{to}")),
        FaultKind::Crash { node } => ("crash", node.to_string()),
        FaultKind::Pause { node } => ("pause", node.to_string()),
        FaultKind::SlowClock {
            node,
            rate_per_mille,
        } => ("clock", format!("{node}-{rate_per_mille}")),
        FaultKind::Transfer { node } => ("xfer", node.to_string()),
        FaultKind::DelaySpike { extra_ticks } => ("delay", extra_ticks.to_string()),
    };
    format!("{}:{}-{}:{kind}:{params}", event.id, event.start, event.end)
}

/// Decodes one `id:start-end:kind:params` event.
fn decode_event(raw: &str) -> Result<FaultEvent, PlanError> {
    let (id_raw, rest) = raw.split_once(':').ok_or(PlanError::BadField("event"))?;
    let (range, rest) = rest.split_once(':').ok_or(PlanError::BadField("event"))?;
    let (kind, params) = rest.split_once(':').ok_or(PlanError::BadField("event"))?;
    let id: u32 = id_raw.parse().map_err(|_| PlanError::BadField("event"))?;
    let (start_raw, end_raw) = range.split_once('-').ok_or(PlanError::BadField("event"))?;
    let start = parse_bounded(start_raw)?;
    let end = parse_bounded(end_raw)?;
    if end < start || u64::from(id) > PLAN_LIMIT {
        return Err(PlanError::BadField("event"));
    }
    let fault = match kind {
        "part" => FaultKind::Partition {
            member: parse_idx(params)?,
        },
        "asym" => {
            let (from_raw, to_raw) = params.split_once('-').ok_or(PlanError::BadField("event"))?;
            FaultKind::Asymmetric {
                from: parse_idx(from_raw)?,
                to: parse_idx(to_raw)?,
            }
        }
        "crash" => FaultKind::Crash {
            node: parse_idx(params)?,
        },
        "pause" => FaultKind::Pause {
            node: parse_idx(params)?,
        },
        "clock" => {
            let (node_raw, rate_raw) =
                params.split_once('-').ok_or(PlanError::BadField("event"))?;
            let rate = parse_bounded(rate_raw)?;
            if !(500..=2000).contains(&rate) {
                return Err(PlanError::BadField("event"));
            }
            FaultKind::SlowClock {
                node: parse_idx(node_raw)?,
                rate_per_mille: rate,
            }
        }
        "xfer" => FaultKind::Transfer {
            node: parse_idx(params)?,
        },
        "delay" => FaultKind::DelaySpike {
            extra_ticks: parse_bounded(params)?,
        },
        _ => return Err(PlanError::BadField("event")),
    };
    Ok(FaultEvent {
        id,
        start,
        end,
        fault,
    })
}

/// Partially decoded plan fields, assembled one `key=value` pair at a time.
#[derive(Default)]
struct DecodedPlan {
    seed: Option<u64>,
    size: Option<usize>,
    ticks: Option<u64>,
    warmup_end: Option<u64>,
    heal_start: Option<u64>,
    faults: (u64, u64, u64),
    salt: Option<u64>,
    storage: bool,
    proposals: Vec<u64>,
    events: Vec<FaultEvent>,
    workload: Option<WorkloadSpec>,
}

impl DecodedPlan {
    fn apply(&mut self, key: &str, value: &str) -> Result<(), PlanError> {
        match key {
            "seed" => self.seed = Some(parse_u64(value)?),
            "size" => {
                let raw = parse_idx(value)?;
                if raw == 0 || raw > 9 {
                    return Err(PlanError::BadField("size"));
                }
                self.size = Some(raw);
            }
            "ticks" => self.ticks = Some(parse_bounded(value)?),
            "warm" => self.warmup_end = Some(parse_bounded(value)?),
            "heal" => self.heal_start = Some(parse_bounded(value)?),
            "net" => {
                let parts: Vec<&str> = value.split(',').collect();
                if parts.len() != 3 {
                    return Err(PlanError::BadField("net"));
                }
                self.faults = (
                    parse_pct(parts[0])?,
                    parse_pct(parts[1])?,
                    parse_bounded(parts[2])?,
                );
            }
            "salt" => self.salt = Some(parse_u64(value)?),
            "storage" => {
                self.storage = match value {
                    "0" => false,
                    "1" => true,
                    _ => return Err(PlanError::BadField("storage")),
                };
            }
            "propose" => {
                if !value.is_empty() {
                    for raw in value.split(',') {
                        self.proposals.push(parse_bounded(raw)?);
                    }
                }
            }
            "ev" => {
                if self.events.len() >= MAX_EVENTS {
                    return Err(PlanError::BadField("too many entries"));
                }
                self.events.push(decode_event(value)?);
            }
            "workload" => {
                let parts: Vec<&str> = value.split(',').collect();
                if parts.len() != 8 {
                    return Err(PlanError::BadField("workload"));
                }
                let clients = parse_idx(parts[0])?;
                let keys = parse_idx(parts[1])?;
                let ops = parse_idx(parts[2])?;
                let list_pct = parse_pct(parts[3])?;
                let think = parse_bounded(parts[4])?;
                let timeout = parse_bounded(parts[5])?;
                let crashes = match parts[6] {
                    "0" => false,
                    "1" => true,
                    _ => return Err(PlanError::BadField("workload")),
                };
                let unordered = match parts[7] {
                    "0" => false,
                    "1" => true,
                    _ => return Err(PlanError::BadField("workload")),
                };
                if clients == 0 || keys == 0 || ops == 0 || think == 0 || timeout == 0 {
                    return Err(PlanError::BadField("workload"));
                }
                self.workload = Some(WorkloadSpec {
                    clients,
                    keys,
                    ops_per_client: ops,
                    list_pct,
                    think_ticks: think,
                    timeout_ticks: timeout,
                    client_crashes: crashes,
                    unordered,
                });
            }
            _ => return Err(PlanError::BadField("key")),
        }
        if u64::try_from(self.proposals.len() + self.events.len()).is_ok_and(|n| n > PLAN_LIMIT) {
            return Err(PlanError::BadField("too many entries"));
        }
        Ok(())
    }

    fn build(mut self, version: u32) -> Result<Plan, PlanError> {
        let size = self.size.ok_or(PlanError::BadField("size"))?;
        let ticks = self.ticks.ok_or(PlanError::BadField("ticks"))?;
        let warmup_end = self.warmup_end.ok_or(PlanError::BadField("warm"))?;
        let heal_start = self.heal_start.ok_or(PlanError::BadField("heal"))?;
        if warmup_end > heal_start || heal_start > ticks {
            return Err(PlanError::BadField("phase"));
        }
        if self.events.iter().any(|event| event_member(event) >= size) {
            return Err(PlanError::BadField("member"));
        }
        let mut ids: Vec<u32> = self.events.iter().map(|event| event.id).collect();
        ids.sort_unstable();
        ids.dedup();
        if ids.len() != self.events.len() {
            return Err(PlanError::BadField("event"));
        }
        self.proposals.sort_unstable();
        self.proposals.dedup();
        self.events.sort_by_key(|event| (event.start, event.id));
        Ok(Plan {
            version,
            seed: self.seed.ok_or(PlanError::BadField("seed"))?,
            cluster_size: size,
            ticks,
            proposals: self.proposals,
            warmup_end,
            heal_start,
            events: self.events,
            drop_pct: self.faults.0,
            dup_pct: self.faults.1,
            max_delay: self.faults.2,
            salt: self.salt.ok_or(PlanError::BadField("salt"))?,
            storage_faults: self.storage,
            workload: self.workload,
        })
    }
}

/// Highest member or node index referenced by an event.
fn event_member(event: &FaultEvent) -> usize {
    match event.fault {
        FaultKind::Partition { member }
        | FaultKind::Crash { node: member }
        | FaultKind::Pause { node: member }
        | FaultKind::Transfer { node: member }
        | FaultKind::SlowClock { node: member, .. } => member,
        FaultKind::Asymmetric { from, to } => from.max(to),
        FaultKind::DelaySpike { .. } => 0,
    }
}

fn parse_bounded(raw: &str) -> Result<u64, PlanError> {
    let value: u64 = raw.parse().map_err(|_| PlanError::BadField("number"))?;
    if value > PLAN_LIMIT {
        return Err(PlanError::BadField("range"));
    }
    Ok(value)
}

fn parse_u64(raw: &str) -> Result<u64, PlanError> {
    raw.parse().map_err(|_| PlanError::BadField("number"))
}

fn parse_pct(raw: &str) -> Result<u64, PlanError> {
    let value: u64 = raw.parse().map_err(|_| PlanError::BadField("percent"))?;
    if value > 100 {
        return Err(PlanError::BadField("percent"));
    }
    Ok(value)
}

fn parse_idx(raw: &str) -> Result<usize, PlanError> {
    let value: u64 = raw.parse().map_err(|_| PlanError::BadField("number"))?;
    if value > PLAN_LIMIT {
        return Err(PlanError::BadField("range"));
    }
    usize::try_from(value).map_err(|_| PlanError::BadField("range"))
}

/// Outcome counters for one consensus-plan run. The farm aggregates these.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlanRunStats {
    /// Invariant evaluation rounds performed.
    pub invariant_checks: u64,
    /// Virtual ticks advanced.
    pub virtual_ticks: u64,
    /// Rare-path hits tallied by point name.
    pub sometimes: Vec<(String, u64)>,
}

/// Executes a plan: warm-up with no faults, chaos from the event schedule,
/// heal with no faults, then a bounded convergence drain and audit.
/// Network faults are pure hash functions of the plan salt. Consensus
/// election jitter and storage fault draws remain seeded RNG streams, so the
/// same seed replays identically.
pub fn run_plan(plan: &Plan) -> PlanRunStats {
    use crate::buggify::set_buggify_seed;
    use crate::device::SimDeviceConfig;
    use anvil_storage::SectorSize;

    set_buggify_seed(plan.seed ^ 0x9E37_79B9_7F4A_7C15);
    let mut cluster = if plan.storage_faults {
        let dev_config = SimDeviceConfig::chaotic(SectorSize::Sector512, 10_000, 5_000, 200);
        SimCluster::new_with_storage_faults(plan.cluster_size, plan.seed, &dev_config)
    } else {
        SimCluster::new(plan.cluster_size, plan.seed)
    };
    cluster.plan_net = Some(PlanNet {
        salt: plan.salt,
        drop_pct: plan.drop_pct,
        dup_pct: plan.dup_pct,
        max_delay: plan.max_delay,
    });

    let node_ids: Vec<anvil_types::NodeId> = cluster.nodes.keys().copied().collect();

    for tick in 1..=plan.ticks {
        for event in &plan.events {
            if event.start == tick && tick > plan.warmup_end && tick < plan.heal_start {
                begin_event(&node_ids, event, &mut cluster);
            }
            if event.end == tick {
                end_event(&node_ids, plan, tick, event, &mut cluster);
            }
        }
        if tick == plan.heal_start {
            cluster.heal_all();
            for id in &node_ids {
                if cluster.is_paused(*id) {
                    cluster.resume_node(*id);
                }
                cluster.set_tick_skew(*id, 1000);
            }
        }
        let spike: u64 = plan
            .events
            .iter()
            .filter(|event| {
                matches!(event.fault, FaultKind::DelaySpike { .. })
                    && event.start <= tick
                    && tick < event.end
            })
            .map(|event| match event.fault {
                FaultKind::DelaySpike { extra_ticks } => extra_ticks,
                _ => 0,
            })
            .sum();
        cluster.plan_net = Some(PlanNet {
            salt: plan.salt,
            drop_pct: plan.drop_pct,
            dup_pct: plan.dup_pct,
            max_delay: plan.max_delay.saturating_add(spike),
        });

        cluster.tick();

        if plan.proposals.contains(&tick)
            && let Some(leader) = cluster.leader()
        {
            let payload = plan_proposal_payload(plan.seed, tick);
            cluster.propose(leader, payload);
        }
    }

    cluster.plan_net = None;
    cluster.heal_all();
    cluster.repair_step(None);
    cluster.drain_network(20);

    let highest_commit = cluster.highest_commit_index();
    for (&node_id, core) in &cluster.nodes {
        assert!(
            core.commit_index() <= core.last_log_index(),
            "commit index exceeds last log index on node {node_id}"
        );
    }

    for idx in 1..=highest_commit.as_u64() {
        let log_idx = LogIndex::from_raw(idx);
        let mut committed_term = None;
        for core in cluster.nodes.values() {
            if core.commit_index() >= log_idx
                && let Some(term) = core.log().term_at(log_idx)
            {
                if let Some(first_term) = committed_term {
                    assert_eq!(
                        first_term, term,
                        "seed {}: conflicting terms at committed index {idx}",
                        plan.seed
                    );
                } else {
                    committed_term = Some(term);
                }
            }
        }
    }

    PlanRunStats {
        invariant_checks: cluster.checker.checks(),
        virtual_ticks: cluster.current_tick,
        sometimes: crate::count_sometimes(std::mem::take(&mut cluster.sometimes_hits)),
    }
}

/// Starts one fault event.
fn begin_event(node_ids: &[anvil_types::NodeId], event: &FaultEvent, cluster: &mut SimCluster) {
    let at = |member: usize| node_ids[member % node_ids.len()];
    match event.fault {
        FaultKind::Partition { member } => {
            cluster.isolate_node(at(member));
        }
        FaultKind::Asymmetric { from, to } => {
            cluster.network.cut_one_way(at(from), at(to));
        }
        FaultKind::Crash { node } => {
            let victim = at(node);
            if !cluster.is_node_crashed(victim) {
                cluster.crash_node(victim);
            }
        }
        FaultKind::Pause { node } => {
            cluster.pause_node(at(node));
        }
        FaultKind::SlowClock {
            node,
            rate_per_mille,
        } => {
            cluster.set_tick_skew(at(node), rate_per_mille);
        }
        FaultKind::Transfer { node } => {
            let target = at(node);
            if let Some(leader) = cluster.leader()
                && leader != target
            {
                sometimes!(cluster.sometimes_hits, "sim.transfer.begin");
                cluster.step_node(leader, anvil_raft::Input::TransferLeader { to: target });
            }
        }
        FaultKind::DelaySpike { .. } => {
            // Delay spikes compose through the per-tick plan_net override in
            // the run loop, so overlapping spikes add instead of clobbering.
        }
    }
}

/// Ends one fault event, restoring the pre-event condition. Partition heals
/// wait until no other partition event is active so overlapping cuts keep
/// their full duration.
fn end_event(
    node_ids: &[anvil_types::NodeId],
    plan: &Plan,
    tick: u64,
    event: &FaultEvent,
    cluster: &mut SimCluster,
) {
    let at = |member: usize| node_ids[member % node_ids.len()];
    match event.fault {
        FaultKind::Partition { .. } | FaultKind::Asymmetric { .. } => {
            let other_cuts = plan.events.iter().any(|other| {
                other.id != event.id
                    && matches!(
                        other.fault,
                        FaultKind::Partition { .. } | FaultKind::Asymmetric { .. }
                    )
                    && other.start <= tick
                    && tick < other.end
            });
            if !other_cuts {
                cluster.heal_all();
            }
        }
        FaultKind::Crash { node } => {
            let victim = at(node);
            // Restart every crashed node that is not refused: recovery
            // re-derives NeedsRepair from the current disk, so a repaired
            // node rejoins and an unrepaired one stays sidelined.
            if cluster.is_node_crashed(victim) && !cluster.is_node_refused_start(victim) {
                let _ = cluster.restart_node(victim);
            }
        }
        FaultKind::Pause { node } => {
            if cluster.is_paused(at(node)) {
                cluster.resume_node(at(node));
            }
        }
        FaultKind::SlowClock { node, .. } => {
            cluster.set_tick_skew(at(node), 1000);
        }
        FaultKind::Transfer { .. } | FaultKind::DelaySpike { .. } => {}
    }
}

/// Collapses digit runs in raw failure text to `#`, capped at a fixed
/// length. Indexes, ticks, and counts vary between runs of one violation
/// kind; the surrounding words identify the kind.
#[must_use]
pub fn normalize_failure_class(raw: &str) -> String {
    let mut out = String::new();
    let mut in_digits = false;
    for ch in raw.chars() {
        if ch.is_ascii_digit() {
            if !in_digits {
                out.push('#');
                in_digits = true;
            }
        } else {
            in_digits = false;
            out.push(ch);
        }
        if out.len() >= 240 {
            break;
        }
    }
    out
}

/// Names the failure kind carried by a caught panic payload.
#[must_use]
pub fn panic_failure_class(payload: &(dyn std::any::Any + Send)) -> String {
    let raw = if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else {
        String::from("non-string panic payload")
    };
    format!("panic:{}", normalize_failure_class(&raw))
}

/// Failure class of one consensus-plan run: `None` on success, otherwise
/// the normalized panic text. Callers compare classes for equality, so one
/// violation kind stays one class while the shrinker removes events.
#[must_use]
pub fn consensus_failure_class(plan: &Plan) -> Option<String> {
    match std::panic::catch_unwind(AssertUnwindSafe(|| run_plan(plan))) {
        Ok(_) => None,
        Err(payload) => Some(panic_failure_class(&payload)),
    }
}

/// Shrinks a failing plan with greedy delta debugging while preserving the
/// failure class: each candidate is re-run through `run`, which returns
/// `Some(class)` on failure, and only candidates reporting the original
/// class are kept. A candidate that passes or fails differently is
/// rejected, so the minimal plan still demonstrates the same violation.
///
/// Soundness comes from per-event RNG streams keyed by event id: removing
/// one event never reshuffles another's parameters, and runtime draws are
/// per-node streams and salt hashes, never positional, so a smaller plan
/// replays its shared prefix exactly.
pub fn shrink_plan_same_class(plan: &Plan, run: &dyn Fn(&Plan) -> Option<String>) -> Plan {
    let Some(expected) = run(plan) else {
        return plan.clone();
    };
    let keep = |candidate: &Plan| run(candidate).is_some_and(|class| class == expected);
    let mut best = plan.clone();
    loop {
        let mut improved = false;
        if let Some(shrunk) = shrink_events(&best, &keep) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_event_windows(&best, &keep) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_cluster_size(&best, &keep) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_net(&best, &keep) {
            best = shrunk;
            improved = true;
        }
        if best.workload.is_some() {
            if let Some(shrunk) = shrink_dead_proposals(&best, &keep) {
                best = shrunk;
                improved = true;
            }
            if let Some(shrunk) = shrink_workload(&best, &keep) {
                best = shrunk;
                improved = true;
            }
        } else {
            if let Some(shrunk) = shrink_proposals(&best, &keep) {
                best = shrunk;
                improved = true;
            }
            if let Some(shrunk) = shrink_ticks(&best, &keep) {
                best = shrunk;
                improved = true;
            }
        }
        if !improved {
            return best;
        }
    }
}

/// Shrinks a failing plan with greedy delta debugging: each candidate
/// removes one event, shortens one event, zeroes network faults, or shortens
/// the run, and is kept only when the failure still reproduces. Events draw
/// from per-event streams keyed by id, so removing one never reshuffles the
/// others.
pub fn shrink_plan(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Plan {
    let mut best = plan.clone();
    loop {
        let mut improved = false;
        if let Some(shrunk) = shrink_events(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_event_windows(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_proposals(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_net(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_ticks(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if !improved {
            return best;
        }
    }
}

fn still_fails(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> bool {
    std::panic::catch_unwind(AssertUnwindSafe(|| fails(plan))).unwrap_or(true)
}

fn shrink_events(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    for i in 0..plan.events.len() {
        let mut candidate = plan.clone();
        candidate.events.remove(i);
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    None
}

fn shrink_event_windows(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    for i in 0..plan.events.len() {
        let event = &plan.events[i];
        if event.end > event.start + 1 {
            let mut candidate = plan.clone();
            candidate.events[i].end = event.start + 1;
            if still_fails(&candidate, fails) {
                return Some(candidate);
            }
        }
    }
    None
}

/// Shrinks the cluster to three nodes where allowed: every event member
/// reference must already fit, so no fault is silently retargeted.
fn shrink_cluster_size(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    if plan.cluster_size <= 3 {
        return None;
    }
    if plan.events.iter().any(|event| event_member(event) >= 3) {
        return None;
    }
    let mut candidate = plan.clone();
    candidate.cluster_size = 3;
    if still_fails(&candidate, fails) {
        return Some(candidate);
    }
    None
}

/// Shrinks a workload plan's client count, then its operations per client.
/// Client operations expand deterministically from the profile, so dropping
/// them shortens durations without reshuffling the survivors.
fn shrink_workload(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    let workload = plan.workload.clone()?;
    if workload.clients > 1 {
        let mut candidate = plan.clone();
        if let Some(entry) = candidate.workload.as_mut() {
            entry.clients = workload.clients - 1;
        }
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    if workload.ops_per_client > 1 {
        let mut candidate = plan.clone();
        if let Some(entry) = candidate.workload.as_mut() {
            entry.ops_per_client = workload.ops_per_client / 2;
        }
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    None
}

/// Drops client proposals a workload run never consumes: the KV schedule
/// expands from the embedded profile, so proposals are inert text. One
/// re-run confirms the failure survives without them.
fn shrink_dead_proposals(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    if plan.workload.is_none() || plan.proposals.is_empty() {
        return None;
    }
    let mut candidate = plan.clone();
    candidate.proposals.clear();
    if still_fails(&candidate, fails) {
        return Some(candidate);
    }
    None
}

fn shrink_proposals(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    if plan.proposals.len() <= 1 {
        return None;
    }
    for i in 0..plan.proposals.len() {
        let mut candidate = plan.clone();
        candidate.proposals.remove(i);
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    None
}

fn shrink_net(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    if plan.drop_pct == 0 && plan.dup_pct == 0 && plan.max_delay == 0 {
        return None;
    }
    let mut candidate = plan.clone();
    candidate.drop_pct = 0;
    candidate.dup_pct = 0;
    candidate.max_delay = 0;
    if still_fails(&candidate, fails) {
        return Some(candidate);
    }
    None
}

fn shrink_ticks(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    let last = plan
        .proposals
        .iter()
        .copied()
        .filter(|p| *p <= plan.ticks)
        .chain(plan.events.iter().map(|event| event.end))
        .max()
        .unwrap_or(plan.ticks);
    if last < plan.ticks && last >= plan.warmup_end {
        let mut candidate = plan.clone();
        candidate.ticks = last;
        candidate.heal_start = candidate.heal_start.min(last).max(candidate.warmup_end);
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    None
}

fn plan_proposal_payload(seed: u64, tick: u64) -> Bytes {
    let mut buf = [0u8; 64];
    let mut len = 0usize;
    len += write_static(&mut buf[len..], b"seed_");
    len += write_u64(&mut buf[len..], seed);
    len += write_static(&mut buf[len..], b"_tick_");
    len += write_u64(&mut buf[len..], tick);
    Bytes::copy_from_slice(&buf[..len])
}

fn write_static(out: &mut [u8], text: &[u8]) -> usize {
    let len = text.len().min(out.len());
    out[..len].copy_from_slice(&text[..len]);
    len
}

fn write_u64(out: &mut [u8], mut value: u64) -> usize {
    if out.is_empty() {
        return 0;
    }
    if value == 0 {
        out[0] = b'0';
        return 1;
    }
    let mut digits = [0u8; 20];
    let mut count = 0usize;
    while value > 0 && count < digits.len() {
        digits[count] = b'0' + (value % 10) as u8;
        value /= 10;
        count += 1;
    }
    debug_assert!(count <= out.len(), "proposal buffer fits u64");
    let len = count.min(out.len());
    for i in 0..len {
        out[i] = digits[count - 1 - i];
    }
    len
}
