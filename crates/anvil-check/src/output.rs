#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Top-level history check with P-compositionality and JSON failure output.

use crate::history::{ClientOp, History, HistoryOp, OpResult};
use crate::wgl::{KeyVerdict, check_key_history, check_range_history};
use bytes::Bytes;
use std::collections::BTreeMap;

/// A linearizability failure with diagnostics for the visualizer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckFailure {
    /// Minimal non-linearizable sub-history operation ids.
    pub failing_ids: Vec<usize>,
    /// Longest linearizable prefix operation ids.
    pub longest_prefix: Vec<usize>,
    /// JSON encoding for the Week 10 visualizer.
    pub json: String,
}

fn index_by_id(history: &History) -> BTreeMap<usize, usize> {
    history
        .ops
        .iter()
        .enumerate()
        .map(|(pos, op)| (op.id, pos))
        .collect()
}

fn op_by_id<'a>(history: &'a History, by_id: &BTreeMap<usize, usize>, id: usize) -> &'a HistoryOp {
    let pos = by_id.get(&id).copied().unwrap_or(0);
    &history.ops[pos.min(history.ops.len().saturating_sub(1))]
}

fn is_vacuous(op: &HistoryOp) -> bool {
    op.indeterminate
        && op.result.is_none()
        && matches!(op.op, ClientOp::Get | ClientOp::Range { .. })
}

/// Drops vacuous operations before checking. An indeterminate read with no
/// observed result changes no state, verifies nothing, and (since
/// indeterminate operations impose no out-edges) constrains nothing that
/// follows. It can always linearize right after its last predecessor, so
/// removing it preserves the verdict in both directions while keeping the
/// exponential search away from timeout reads that carry no information.
fn strip_vacuous(history: &History) -> History {
    History {
        ops: history
            .ops
            .iter()
            .filter(|op| !is_vacuous(op))
            .cloned()
            .collect(),
    }
}

/// Checks a full history. Range-only histories use the whole-store model on
/// bounded inputs; all other histories compose per key.
pub fn check_history(history: &History) -> Result<(), CheckFailure> {
    let history = strip_vacuous(history);
    let history = &history;
    let has_range = history
        .ops
        .iter()
        .any(|op| matches!(op.op, ClientOp::Range { .. }));
    if has_range {
        if check_range_history(history) {
            return Ok(());
        }
        return Err(minimize_failure(history));
    }
    let by_id = index_by_id(history);
    let partitions = history.partition_by_key();
    let mut failing: Vec<usize> = Vec::new();
    for ids in partitions.values() {
        let ops: Vec<_> = ids
            .iter()
            .map(|id| op_by_id(history, &by_id, *id).clone())
            .collect();
        let is_list = ops.iter().any(|op| {
            matches!(op.op, ClientOp::Append { .. })
                || matches!(op.result, Some(OpResult::List { .. }))
        });
        match check_key_history(&ops, is_list) {
            KeyVerdict::Linearizable => {}
            KeyVerdict::NonLinearizable { failing_ids } => {
                for pos in failing_ids {
                    if let Some(op) = ops.get(pos) {
                        failing.push(op.id);
                    }
                }
            }
        }
    }
    if failing.is_empty() {
        Ok(())
    } else {
        failing.sort_unstable();
        failing.dedup();
        Err(minimize_failure_ids(history, &failing))
    }
}

fn minimize_failure(history: &History) -> CheckFailure {
    let all: Vec<usize> = history.ops.iter().map(|op| op.id).collect();
    minimize_failure_ids(history, &all)
}

fn minimize_failure_ids(history: &History, ids: &[usize]) -> CheckFailure {
    let mut current: Vec<usize> = ids.to_vec();
    let mut changed = true;
    while changed {
        changed = false;
        let mut i = 0;
        while i < current.len() {
            let mut candidate = current.clone();
            candidate.remove(i);
            if candidate.is_empty() {
                i += 1;
                continue;
            }
            // Never orphan a read: dropping the only writer of an observed
            // value yields a dangling read that fails for a trivial reason
            // and hides the real stale read. Only closed candidates count.
            if !is_closed(history, &candidate) {
                i += 1;
                continue;
            }
            if sub_history_fails(history, &candidate) {
                current = candidate;
                changed = true;
            } else {
                i += 1;
            }
        }
    }
    let longest_prefix = longest_linearizable_prefix(history);
    let json = render_json(history, &current, &longest_prefix);
    CheckFailure {
        failing_ids: current,
        longest_prefix,
        json,
    }
}

/// What a remaining read needs from the candidate sub-history.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Need {
    /// No writer required.
    None,
    /// A writer of exactly this value (`None` means a tombstone).
    Value(Option<Bytes>),
    /// Any list-append on the key.
    AnyAppend,
}

/// Whether every completed read in the candidate still has a writer that
/// could have produced its observed value: a completed write of the same
/// value, an indeterminate write that may have taken effect, or any append
/// for a list read. Vacant reads, indeterminate reads without results,
/// range reads, and failed CAS checks impose no constraint. Without this,
/// delta debugging orphans reads whose writers were minimized away and
/// reports a dangling read instead of the real stale one.
fn is_closed(history: &History, ids: &[usize]) -> bool {
    use std::collections::BTreeSet;
    let present: BTreeSet<usize> = ids.iter().copied().collect();
    let by_id = index_by_id(history);
    for &id in &present {
        let Some(&pos) = by_id.get(&id) else {
            continue;
        };
        let op = &history.ops[pos];
        if op.indeterminate && op.result.is_none() {
            continue;
        }
        let need = match (&op.op, &op.result) {
            (ClientOp::Get, Some(OpResult::Value { value, .. }))
            | (ClientOp::Cas { .. }, Some(OpResult::Value { value, .. })) => {
                Need::Value(value.clone())
            }
            (ClientOp::Get, Some(OpResult::List { .. })) => Need::AnyAppend,
            _ => Need::None,
        };
        if matches!(need, Need::None) {
            continue;
        }
        let mut explained = false;
        for &other in &present {
            if other == id {
                continue;
            }
            let Some(&opos) = by_id.get(&other) else {
                continue;
            };
            let writer = &history.ops[opos];
            if writer.key != op.key {
                continue;
            }
            let covers = match (&writer.op, &writer.result, writer.indeterminate, &need) {
                // Any indeterminate write, or any append for a list read,
                // may explain the observation.
                (ClientOp::Put { .. } | ClientOp::Delete | ClientOp::Cas { .. }, _, true, _)
                | (ClientOp::Append { .. }, _, _, Need::AnyAppend) => true,
                (ClientOp::Put { value }, _, false, Need::Value(want)) => {
                    Some(value) == want.as_ref()
                }
                (ClientOp::Delete, _, false, Need::Value(want)) => want.is_none(),
                (
                    ClientOp::Cas { .. },
                    Some(OpResult::Value { value, .. }),
                    false,
                    Need::Value(want),
                ) => value == want,
                _ => false,
            };
            if covers {
                explained = true;
                break;
            }
        }
        if !explained {
            return false;
        }
    }
    true
}

fn sub_history_fails(history: &History, ids: &[usize]) -> bool {
    let subset: std::collections::BTreeSet<usize> = ids.iter().copied().collect();
    let filtered: History = History {
        ops: history
            .ops
            .iter()
            .filter(|op| subset.contains(&op.id))
            .cloned()
            .collect(),
    };
    check_history_inner(&filtered).is_err()
}

fn check_history_inner(history: &History) -> Result<(), Vec<usize>> {
    let history = strip_vacuous(history);
    let history = &history;
    if history
        .ops
        .iter()
        .any(|op| matches!(op.op, ClientOp::Range { .. }))
    {
        if check_range_history(history) {
            Ok(())
        } else {
            Err(history.ops.iter().map(|op| op.id).collect())
        }
    } else {
        check_history_registers(history)
    }
}

fn check_history_registers(history: &History) -> Result<(), Vec<usize>> {
    let by_id = index_by_id(history);
    let partitions = history.partition_by_key();
    let mut failing = Vec::new();
    for ids in partitions.values() {
        let ops: Vec<_> = ids
            .iter()
            .map(|id| op_by_id(history, &by_id, *id).clone())
            .collect();
        let is_list = ops.iter().any(|op| {
            matches!(op.op, ClientOp::Append { .. })
                || matches!(op.result, Some(OpResult::List { .. }))
        });
        if matches!(
            check_key_history(&ops, is_list),
            KeyVerdict::NonLinearizable { .. }
        ) {
            failing.extend(ids.iter().copied());
        }
    }
    if failing.is_empty() {
        Ok(())
    } else {
        Err(failing)
    }
}

fn longest_linearizable_prefix(history: &History) -> Vec<usize> {
    let mut prefix = Vec::new();
    for op in &history.ops {
        let mut trial = History {
            ops: history
                .ops
                .iter()
                .filter(|o| prefix.contains(&o.id) || o.id == op.id)
                .cloned()
                .collect(),
        };
        trial.ops.sort_by_key(|o| o.id);
        if check_history_inner(&trial).is_ok() {
            prefix.push(op.id);
        } else {
            break;
        }
    }
    prefix
}

fn render_json(history: &History, failing: &[usize], prefix: &[usize]) -> String {
    use std::fmt::Write as _;
    let by_id = index_by_id(history);
    let mut out = String::from("{\"failing\":[");
    for (i, id) in failing.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let op = op_by_id(history, &by_id, *id);
        let _ = write!(
            out,
            "{{\"id\":{},\"client\":{},\"invoke\":{},\"ret\":{},\"result\":\"{}\"}}",
            op.id,
            op.client,
            op.invoke,
            op.ret.unwrap_or(u64::MAX),
            result_label(op.result.as_ref()),
        );
    }
    out.push_str("],\"prefix\":[");
    for (i, id) in prefix.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let _ = write!(out, "{id}");
    }
    out.push_str("]}");
    out
}

fn result_label(result: Option<&OpResult>) -> &'static str {
    match result {
        None => "indeterminate",
        Some(OpResult::Value { value: Some(_), .. }) => "value",
        Some(OpResult::Value { value: None, .. }) => "none",
        Some(OpResult::List { .. }) => "list",
        Some(OpResult::Range { .. }) => "range",
        Some(OpResult::Ok) => "ok",
        Some(OpResult::CasConflict { .. }) => "conflict",
        Some(OpResult::SessionExpired) => "expired",
    }
}
