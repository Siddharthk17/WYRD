#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Wing-Gong-Lowe search with memoization and P-compositionality.
//! Single-threaded and deterministic; the farm parallelizes across seeds.

use crate::history::{ClientOp, History, HistoryOp};
use crate::list::ListState;
use crate::register::RegisterState;
use std::collections::{BTreeMap, BTreeSet};

/// Outcome of checking one key partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyVerdict {
    Linearizable,
    NonLinearizable { failing_ids: Vec<usize> },
}

/// Checks one key partition with WGL search.
pub fn check_key_history(ops: &[HistoryOp], is_list: bool) -> KeyVerdict {
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_by_key(|&i| (ops[i].invoke, ops[i].id));
    let failing = order.clone();
    let (pred_count, successors) = build_precedence(ops);
    let mut search = KeySearch {
        ops,
        order,
        is_list,
        register: RegisterState::default(),
        list: ListState::default(),
        linearized: vec![false; ops.len()],
        linearized_count: 0,
        memo: BTreeSet::new(),
        pred_count,
        successors,
    };
    if search.dfs() {
        KeyVerdict::Linearizable
    } else {
        KeyVerdict::NonLinearizable {
            failing_ids: failing,
        }
    }
}

/// Builds happens-before predecessor counts and successor lists once, so
/// the search advances minimal sets incrementally instead of rescanning
/// pairs at every level.
fn build_precedence(ops: &[HistoryOp]) -> (Vec<usize>, Vec<Vec<usize>>) {
    let n = ops.len();
    let mut pred_count = vec![0usize; n];
    let mut successors: Vec<Vec<usize>> = vec![Vec::new(); n];
    for first in 0..n {
        for second in 0..n {
            if first != second && ops[first].happens_before(&ops[second]) {
                pred_count[second] += 1;
                successors[first].push(second);
            }
        }
    }
    (pred_count, successors)
}

/// Checks a whole-store range history on bounded inputs.
pub fn check_range_history(history: &History) -> bool {
    if history.len() > 300 {
        return false;
    }
    let mut state: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut order: Vec<usize> = (0..history.len()).collect();
    order.sort_by_key(|&i| (history.ops[i].invoke, history.ops[i].id));
    let mut memo = std::collections::BTreeSet::new();
    let mut steps = 0u64;
    check_range_dfs(
        history,
        &order,
        &mut state,
        &mut vec![false; history.len()],
        0,
        &mut memo,
        &mut steps,
    )
}

/// Upper bound on range-model search steps. Valid histories linearize in
/// roughly one pass; the cap only stops pathological failure searches from
/// hanging the farm. It never turns a linearizable history non-linearizable
/// in practice: every farm history that reaches the cap is reported, and the
/// cap is raised if one ever does.
const RANGE_SEARCH_STEP_CAP: u64 = 20_000_000;

fn check_range_dfs(
    history: &History,
    order: &[usize],
    state: &mut BTreeMap<Vec<u8>, Vec<u8>>,
    linearized: &mut [bool],
    done: usize,
    memo: &mut std::collections::BTreeSet<(u64, u64)>,
    steps: &mut u64,
) -> bool {
    if done == order.len() {
        return true;
    }
    *steps = steps.saturating_add(1);
    if *steps > RANGE_SEARCH_STEP_CAP {
        return false;
    }
    let memo_key = (linearized_hash(linearized), range_state_hash(state));
    if !memo.insert(memo_key) {
        return false;
    }
    for &pos in order {
        if linearized[pos] {
            continue;
        }
        let op = &history.ops[pos];
        let mut blocked = false;
        #[allow(clippy::needless_range_loop)]
        for &other in order {
            if other == pos || linearized[other] {
                continue;
            }
            if history.ops[other].happens_before(op) {
                blocked = true;
                break;
            }
        }
        if blocked {
            continue;
        }
        if op.indeterminate {
            linearized[pos] = true;
            if check_range_dfs(history, order, state, linearized, done + 1, memo, steps) {
                return true;
            }
            let snapshot = state.clone();
            apply_range_effect(state, op);
            if check_range_dfs(history, order, state, linearized, done + 1, memo, steps) {
                return true;
            }
            *state = snapshot;
            linearized[pos] = false;
            continue;
        }
        let snapshot = state.clone();
        if apply_range_op(state, op) {
            linearized[pos] = true;
            if check_range_dfs(history, order, state, linearized, done + 1, memo, steps) {
                return true;
            }
            linearized[pos] = false;
        }
        *state = snapshot;
    }
    false
}

fn linearized_hash(linearized: &[bool]) -> u64 {
    let mut x: u64 = 0xCBF2_9CE4_8422_2325;
    for (i, done) in linearized.iter().enumerate() {
        if *done {
            x ^= (i as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
            x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        }
    }
    x ^ (x >> 29)
}

fn range_state_hash(state: &BTreeMap<Vec<u8>, Vec<u8>>) -> u64 {
    let mut x: u64 = 0xD6E8_FEB8_66F1_2D57;
    for (key, value) in state {
        for chunk in key.iter().chain(value.iter()).collect::<Vec<_>>().chunks(8) {
            let mut word = [0u8; 8];
            for (i, byte) in chunk.iter().enumerate() {
                word[i] = **byte;
            }
            x = x.wrapping_add(u64::from_le_bytes(word));
            x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
            x ^= x >> 29;
        }
        x = x.wrapping_add(state.len() as u64);
    }
    x ^ (x >> 31)
}

fn apply_range_effect(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: &HistoryOp) {
    match &op.op {
        ClientOp::Put { value } | ClientOp::Append { value } => {
            state.insert(op.key.to_vec(), value.to_vec());
        }
        ClientOp::Delete => {
            state.remove(&op.key.to_vec());
        }
        ClientOp::Cas { value, .. } => {
            state.insert(op.key.to_vec(), value.to_vec());
        }
        ClientOp::Get | ClientOp::Range { .. } => {}
    }
}

fn apply_range_op(state: &mut BTreeMap<Vec<u8>, Vec<u8>>, op: &HistoryOp) -> bool {
    use crate::history::OpResult;
    match (&op.op, &op.result) {
        (ClientOp::Put { value } | ClientOp::Append { value }, _) => {
            state.insert(op.key.to_vec(), value.to_vec());
            true
        }
        (ClientOp::Delete, _) => {
            state.remove(&op.key.to_vec());
            true
        }
        (
            ClientOp::Cas { value, .. },
            Some(OpResult::Value {
                value: observed, ..
            }),
        ) => {
            if observed.as_ref() != Some(value) {
                return false;
            }
            state.insert(op.key.to_vec(), value.to_vec());
            true
        }
        (ClientOp::Cas { .. }, Some(OpResult::CasConflict { .. })) => true,
        (ClientOp::Get, Some(OpResult::Value { value, .. })) => {
            state.get(op.key.as_ref()).cloned() == value.as_ref().map(|b| b.to_vec())
        }
        (ClientOp::Get, Some(OpResult::List { values, .. })) => {
            values.last().map(|b| b.to_vec()) == state.get(&op.key.to_vec()).cloned()
        }
        (ClientOp::Range { limit }, Some(OpResult::Range { pairs })) => {
            let mut expected: Vec<(Vec<u8>, Vec<u8>)> = state
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .take(*limit)
                .collect();
            let mut observed: Vec<(Vec<u8>, Vec<u8>)> = pairs
                .iter()
                .map(|(k, v)| (k.to_vec(), v.to_vec()))
                .collect();
            expected.sort();
            observed.sort();
            expected == observed
        }
        _ => true,
    }
}

struct KeySearch<'a> {
    ops: &'a [HistoryOp],
    order: Vec<usize>,
    is_list: bool,
    register: RegisterState,
    list: ListState,
    linearized: Vec<bool>,
    linearized_count: usize,
    memo: BTreeSet<(u64, u64)>,
    /// Unlinearized predecessor counts for incremental minimal tracking.
    /// `pred_count[pos]` is the number of unlinearized ops that happen
    /// before `pos`. Built once from the happens-before DAG.
    pred_count: Vec<usize>,
    /// Successor lists mirroring the predecessor counts.
    successors: Vec<Vec<usize>>,
}

impl KeySearch<'_> {
    fn dfs(&mut self) -> bool {
        if self.linearized_count == self.ops.len() {
            return true;
        }
        let memo_key = (self.linearized_hash(), self.state_hash());
        if !self.memo.insert(memo_key) {
            return false;
        }
        let candidates = self.minimal_candidates();
        for pos in candidates {
            if self.try_linearize(pos) {
                return true;
            }
        }
        false
    }

    fn minimal_candidates(&self) -> Vec<usize> {
        let mut out = Vec::new();
        for &pos in &self.order {
            if !self.linearized[pos] && self.pred_count[pos] == 0 {
                out.push(pos);
            }
        }
        out
    }

    fn try_linearize(&mut self, pos: usize) -> bool {
        let op = self.ops[pos].clone();
        self.linearized[pos] = true;
        self.linearized_count += 1;
        for i in 0..self.successors[pos].len() {
            let next = self.successors[pos][i];
            self.pred_count[next] = self.pred_count[next].saturating_sub(1);
        }
        let saved_register = self.register.clone();
        let saved_list = self.list.clone();
        let ok = if op.indeterminate {
            if self.dfs() {
                return true;
            }
            if op.result.is_none() {
                self.apply_indeterminate(&op);
                self.dfs()
            } else {
                self.apply_checked(&op) && self.dfs()
            }
        } else {
            self.apply_checked(&op) && self.dfs()
        };
        if ok {
            return true;
        }
        self.linearized[pos] = false;
        self.linearized_count -= 1;
        for i in 0..self.successors[pos].len() {
            let next = self.successors[pos][i];
            self.pred_count[next] = self.pred_count[next].saturating_add(1);
        }
        self.register = saved_register;
        self.list = saved_list;
        false
    }

    fn apply_checked(&mut self, op: &HistoryOp) -> bool {
        if self.is_list {
            self.list.apply(op)
        } else {
            self.register.apply(op)
        }
    }

    fn apply_indeterminate(&mut self, op: &HistoryOp) {
        if self.is_list {
            self.list.apply_indeterminate(op);
        } else {
            self.register.apply_indeterminate(op);
        }
    }

    fn state_hash(&self) -> u64 {
        if self.is_list {
            self.list.state_hash()
        } else {
            self.register.state_hash()
        }
    }

    fn linearized_hash(&self) -> u64 {
        let mut x: u64 = 0xCBF2_9CE4_8422_2325;
        for (i, done) in self.linearized.iter().enumerate() {
            if *done {
                x ^= (i as u64).wrapping_add(0x9E37_79B9_7F4A_7C15);
                x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
            }
        }
        x ^ (x >> 29)
    }
}
