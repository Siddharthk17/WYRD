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

use crate::cluster::{PlanNet, SimCluster};
use crate::rng::DetRng;
use anvil_types::LogIndex;
use bytes::Bytes;
use std::fmt::Write as _;
use std::panic::AssertUnwindSafe;

/// Current plan encoding version.
pub const PLAN_VERSION: u32 = 1;

/// Upper bound accepted for tick counts and entry lists on decode.
const PLAN_LIMIT: u64 = 100_000;

/// A scheduled network partition of one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanPartition {
    /// First tick with traffic cut.
    pub start: u64,
    /// First tick with traffic restored.
    pub end: u64,
    /// Index into the cluster's initial node ordering.
    pub member: usize,
}

/// A scheduled crash and restart of one member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanCrash {
    /// Tick on which the node crashes.
    pub crash_tick: u64,
    /// Tick on which the node restarts.
    pub restart_tick: u64,
    /// Index into the cluster's initial node ordering.
    pub node_idx: usize,
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
    /// Scheduled partitions.
    pub partitions: Vec<PlanPartition>,
    /// Scheduled crashes.
    pub crashes: Vec<PlanCrash>,
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

impl Plan {
    /// Generates the canonical plan for a seed. All sampling happens here,
    /// through an isolated `DetRng` stream, before the run starts.
    #[must_use]
    pub fn generate(seed: u64, storage_faults: bool) -> Self {
        let mut rng = DetRng::new(seed, "sim/plan");
        let sizes = [3, 5, 7];
        let size_idx = usize::try_from(rng.gen_range(0..3)).unwrap_or(0);
        let cluster_size = sizes[size_idx % sizes.len()];
        let ticks = 30;
        let mut proposals = Vec::new();
        let mut tick = 6;
        while tick <= ticks {
            proposals.push(tick);
            tick += 6;
        }
        let mut partitions = Vec::new();
        if cluster_size > 3 {
            let member_raw = rng.gen_range(0..cluster_size as u64);
            let member = usize::try_from(member_raw).unwrap_or(0) % cluster_size;
            partitions.push(PlanPartition {
                start: 12,
                end: 20,
                member,
            });
        }
        let victim_raw = rng.gen_range(0..cluster_size as u64);
        let victim = usize::try_from(victim_raw).unwrap_or(0) % cluster_size;
        let crash_tick = rng.gen_range(10..16);
        let restart_tick = crash_tick + rng.gen_range(2..6);
        let crashes = vec![PlanCrash {
            crash_tick,
            restart_tick,
            node_idx: victim,
        }];
        let drop_pct = rng.gen_range(0..16);
        let dup_pct = rng.gen_range(0..10);
        let max_delay = rng.gen_range(0..6);
        let salt = rng.next_u64();
        Self {
            version: PLAN_VERSION,
            seed,
            cluster_size,
            ticks,
            proposals,
            partitions,
            crashes,
            drop_pct,
            dup_pct,
            max_delay,
            salt,
            storage_faults,
        }
    }

    /// Encodes the plan as versioned text for logs, files, and repro commands.
    #[must_use]
    pub fn encode(&self) -> String {
        let mut out = format!(
            "anvil-plan/{};seed={};size={};ticks={};net={},{},{};salt={};storage={}",
            self.version,
            self.seed,
            self.cluster_size,
            self.ticks,
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
        for part in &self.partitions {
            let _ = write!(out, ";part={}-{}:{}", part.start, part.end, part.member);
        }
        for crash in &self.crashes {
            let _ = write!(
                out,
                ";crash={}-{}:{}",
                crash.crash_tick, crash.restart_tick, crash.node_idx
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

/// Partially decoded plan fields, assembled one `key=value` pair at a time.
#[derive(Default)]
struct DecodedPlan {
    seed: Option<u64>,
    size: Option<usize>,
    ticks: Option<u64>,
    faults: (u64, u64, u64),
    salt: Option<u64>,
    storage: bool,
    proposals: Vec<u64>,
    partitions: Vec<PlanPartition>,
    crashes: Vec<PlanCrash>,
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
            "part" => {
                let (range, member) = value.split_once(':').ok_or(PlanError::BadField("part"))?;
                let (start, end) = range.split_once('-').ok_or(PlanError::BadField("part"))?;
                let start = parse_bounded(start)?;
                let end = parse_bounded(end)?;
                let member = parse_idx(member)?;
                if end < start {
                    return Err(PlanError::BadField("part"));
                }
                self.partitions.push(PlanPartition { start, end, member });
            }
            "crash" => {
                let (range, node) = value.split_once(':').ok_or(PlanError::BadField("crash"))?;
                let (crash_tick, restart_tick) =
                    range.split_once('-').ok_or(PlanError::BadField("crash"))?;
                let crash_tick = parse_bounded(crash_tick)?;
                let restart_tick = parse_bounded(restart_tick)?;
                let node_idx = parse_idx(node)?;
                if restart_tick < crash_tick {
                    return Err(PlanError::BadField("crash"));
                }
                self.crashes.push(PlanCrash {
                    crash_tick,
                    restart_tick,
                    node_idx,
                });
            }
            _ => return Err(PlanError::BadField("key")),
        }
        if u64::try_from(self.proposals.len() + self.partitions.len() + self.crashes.len())
            .is_ok_and(|n| n > PLAN_LIMIT)
        {
            return Err(PlanError::BadField("too many entries"));
        }
        Ok(())
    }

    fn build(mut self, version: u32) -> Result<Plan, PlanError> {
        let size = self.size.ok_or(PlanError::BadField("size"))?;
        if self.partitions.iter().any(|part| part.member >= size)
            || self.crashes.iter().any(|crash| crash.node_idx >= size)
        {
            return Err(PlanError::BadField("member"));
        }
        self.proposals.sort_unstable();
        self.proposals.dedup();
        Ok(Plan {
            version,
            seed: self.seed.ok_or(PlanError::BadField("seed"))?,
            cluster_size: size,
            ticks: self.ticks.ok_or(PlanError::BadField("ticks"))?,
            proposals: self.proposals,
            partitions: self.partitions,
            crashes: self.crashes,
            drop_pct: self.faults.0,
            dup_pct: self.faults.1,
            max_delay: self.faults.2,
            salt: self.salt.ok_or(PlanError::BadField("salt"))?,
            storage_faults: self.storage,
        })
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
    let value: u64 = raw.parse().map_err(|_| PlanError::BadField("number"))?;
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

/// Executes a plan: fault schedule and workload come from the plan, and
/// network faults are pure hash functions of the plan salt. Consensus
/// election jitter and storage fault draws remain seeded RNG streams, so the
/// same seed replays identically.
pub fn run_plan(plan: &Plan) {
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
        for part in &plan.partitions {
            if part.start == tick {
                let member = node_ids[part.member % node_ids.len()];
                cluster.network.isolate(member, &node_ids);
            }
            if part.end == tick {
                cluster.network.heal_all();
            }
        }
        for crash in &plan.crashes {
            if crash.crash_tick == tick {
                let victim = node_ids[crash.node_idx % node_ids.len()];
                if !cluster.is_node_crashed(victim) {
                    cluster.crash_node(victim);
                }
            }
            if crash.restart_tick == tick {
                let victim = node_ids[crash.node_idx % node_ids.len()];
                if cluster.is_node_crashed(victim)
                    && !cluster.is_node_refused_start(victim)
                    && !cluster.is_node_needs_repair(victim)
                {
                    let _ = cluster.restart_node(victim);
                }
            }
        }

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
}

/// Shrinks a failing plan with greedy delta debugging: each candidate
/// removes one fault, shortens the run, or zeroes network faults, and is
/// kept only when the failure still reproduces.
pub fn shrink_plan(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Plan {
    let mut best = plan.clone();
    loop {
        let mut improved = false;
        if let Some(shrunk) = shrink_crashes(&best, fails) {
            best = shrunk;
            improved = true;
        }
        if let Some(shrunk) = shrink_partitions(&best, fails) {
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

fn shrink_crashes(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    for i in 0..plan.crashes.len() {
        let mut candidate = plan.clone();
        candidate.crashes.remove(i);
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
    }
    None
}

fn shrink_partitions(plan: &Plan, fails: &dyn Fn(&Plan) -> bool) -> Option<Plan> {
    for i in 0..plan.partitions.len() {
        let mut candidate = plan.clone();
        candidate.partitions.remove(i);
        if still_fails(&candidate, fails) {
            return Some(candidate);
        }
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
        .chain(plan.partitions.iter().map(|p| p.end))
        .chain(plan.crashes.iter().map(|c| c.restart_tick))
        .max()
        .unwrap_or(plan.ticks);
    if last < plan.ticks {
        let mut candidate = plan.clone();
        candidate.ticks = last;
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
