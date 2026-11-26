#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::assert_is_empty,
    clippy::manual_is_multiple_of,
    clippy::collapsible_if,
    clippy::explicit_iter_loop,
    clippy::type_complexity
)]

use anvil_check::porcupine::export_porcupine;
use anvil_check::{ClientOp, History, HistoryOp, OpResult, check_history};
use bytes::Bytes;

fn put_op(
    id: usize,
    client: u64,
    key: &[u8],
    value: &[u8],
    invoke: u64,
    ret: u64,
    version: u64,
) -> HistoryOp {
    HistoryOp {
        id,
        client,
        key: Bytes::copy_from_slice(key),
        op: ClientOp::Put {
            value: Bytes::copy_from_slice(value),
        },
        invoke,
        ret: Some(ret),
        result: Some(OpResult::Value {
            value: Some(Bytes::copy_from_slice(value)),
            version,
        }),
        indeterminate: false,
    }
}

fn get_op(
    id: usize,
    client: u64,
    key: &[u8],
    value: Option<&[u8]>,
    version: u64,
    invoke: u64,
    ret: u64,
) -> HistoryOp {
    HistoryOp {
        id,
        client,
        key: Bytes::copy_from_slice(key),
        op: ClientOp::Get,
        invoke,
        ret: Some(ret),
        result: Some(OpResult::Value {
            value: value.map(Bytes::copy_from_slice),
            version,
        }),
        indeterminate: false,
    }
}

#[test]
fn test_checker_accepts_linearizable_register() {
    let mut history = History::new();
    history.push(put_op(0, 1, b"k", b"v1", 0, 5, 10));
    history.push(get_op(1, 2, b"k", Some(b"v1"), 10, 6, 9));
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_checker_rejects_stale_read() {
    let mut history = History::new();
    history.push(put_op(0, 1, b"k", b"v1", 0, 5, 10));
    history.push(put_op(1, 1, b"k", b"v2", 6, 10, 20));
    history.push(get_op(2, 2, b"k", Some(b"v1"), 10, 11, 14));
    let failure = check_history(&history).expect_err("stale read must fail");
    assert!(!failure.failing_ids.is_empty());
    assert!(failure.json.contains("failing"));
}

#[test]
fn test_checker_p_composition_two_keys() {
    let mut history = History::new();
    history.push(put_op(0, 1, b"a", b"1", 0, 3, 5));
    history.push(put_op(1, 2, b"b", b"2", 1, 4, 6));
    history.push(get_op(2, 1, b"a", Some(b"1"), 5, 5, 7));
    history.push(get_op(3, 2, b"b", Some(b"2"), 6, 6, 8));
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_checker_indeterminate_may_skip() {
    let mut history = History::new();
    history.push(put_op(0, 1, b"k", b"v1", 0, 5, 10));
    history.push(HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Put {
            value: Bytes::from_static(b"v2"),
        },
        invoke: 6,
        ret: Some(20),
        result: None,
        indeterminate: true,
    });
    history.push(get_op(2, 3, b"k", Some(b"v1"), 10, 21, 25));
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_checker_list_append() {
    use anvil_check::{ClientOp as C, HistoryOp as H, OpResult as R};
    let mut history = History::new();
    history.push(H {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: C::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 0,
        ret: Some(4),
        result: Some(R::List {
            values: vec![Bytes::from_static(b"a")],
            version: 7,
        }),
        indeterminate: false,
    });
    history.push(H {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"l"),
        op: C::Get,
        invoke: 5,
        ret: Some(8),
        result: Some(R::List {
            values: vec![Bytes::from_static(b"a")],
            version: 7,
        }),
        indeterminate: false,
    });
    assert!(check_history(&history).is_ok());
}

#[test]
fn test_checker_brute_force_agrees_on_tiny_histories() {
    let mut verified = 0u64;
    for seed in 0..2000u64 {
        let history = tiny_history(seed);
        let fast = check_history(&history).is_ok();
        let brute = brute_force(&history);
        assert_eq!(fast, brute, "disagreement on seed {seed}");
        verified += 1;
    }
    assert_eq!(verified, 2000);
}

/// Million-history brute-force agreement. Ignored by default: it takes
/// seconds in release and minutes in debug. Run explicitly with
/// `cargo test -p anvil-check --release --test checker_test
/// -- --ignored --nocapture`.
#[test]
#[ignore = "million-history agreement run, execute explicitly in release"]
fn test_checker_brute_force_agrees_on_million_histories() {
    let mut verified = 0u64;
    for seed in 0..1_000_000u64 {
        let history = tiny_history(seed);
        let fast = check_history(&history).is_ok();
        let brute = brute_force(&history);
        assert_eq!(fast, brute, "disagreement on seed {seed}");
        verified += 1;
    }
    assert_eq!(verified, 1_000_000);
}

#[test]
fn test_checker_mutated_histories_fail() {
    for seed in 0..200u64 {
        let mut history = linearizable_history(seed);
        if history.ops.is_empty() {
            continue;
        }
        corrupt_one_read(&mut history, seed);
        assert!(check_history(&history).is_err(), "mutant {seed} must fail");
    }
}

#[test]
fn test_porcupine_export_shape() {
    let mut history = History::new();
    history.push(put_op(0, 1, b"k", b"v", 0, 3, 9));
    let json = export_porcupine(&history);
    assert!(json.starts_with('['));
    assert!(json.contains("put"));
    assert!(json.contains("\"result\""));
    assert!(json.contains("\"version\":9"));
}

fn tiny_history(seed: u64) -> History {
    let mut history = History::new();
    let mut state: Option<(Vec<u8>, u64)> = None;
    let mut tick = 0u64;
    let n = 2 + (seed % 4) as usize;
    for i in 0..n {
        let client = (seed + i as u64) % 2 + 1;
        if (seed >> (i * 2)) % 3 == 0 {
            let value = format!("v{seed}-{i}").into_bytes();
            let version = 100 + seed * 10 + i as u64;
            // Some writes time out with no result. The checker must allow
            // their effects at any later point, with versions adopted from
            // the observations they explain.
            let timeout = (seed >> (i + 7)) % 5 == 0;
            history.push(HistoryOp {
                id: i,
                client,
                key: Bytes::from_static(b"k"),
                op: ClientOp::Put {
                    value: Bytes::from(value.clone()),
                },
                invoke: tick,
                ret: Some(tick + 2),
                result: if timeout {
                    None
                } else {
                    Some(OpResult::Value {
                        value: Some(Bytes::from(value.clone())),
                        version,
                    })
                },
                indeterminate: timeout,
            });
            if !timeout {
                state = Some((value, version));
            }
        } else {
            let (value, version) = state.clone().map_or((None, 0), |(v, x)| (Some(v), x));
            history.push(HistoryOp {
                id: i,
                client,
                key: Bytes::from_static(b"k"),
                op: ClientOp::Get,
                invoke: tick,
                ret: Some(tick + 2),
                result: if (seed >> (i + 5)) % 4 == 0 {
                    None
                } else {
                    Some(OpResult::Value {
                        value: value.map(Bytes::from),
                        version,
                    })
                },
                indeterminate: (seed >> (i + 5)) % 4 == 0,
            });
        }
        tick += 3;
    }
    if seed % 7 == 0 {
        if let Some(op) = history.ops.last_mut() {
            if let Some(OpResult::Value { value: Some(_), .. }) = &op.result {
                op.result = Some(OpResult::Value {
                    value: Some(Bytes::from_static(b"corrupt")),
                    version: 9999,
                });
            }
        }
    }
    history
}

fn linearizable_history(seed: u64) -> History {
    let mut history = History::new();
    let mut tick = 0u64;
    let mut version = 1u64;
    for i in 0..4usize {
        let value = format!("w{seed}-{i}").into_bytes();
        history.push(HistoryOp {
            id: i * 2,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Put {
                value: Bytes::from(value.clone()),
            },
            invoke: tick,
            ret: Some(tick + 2),
            result: Some(OpResult::Value {
                value: Some(Bytes::from(value)),
                version,
            }),
            indeterminate: false,
        });
        tick += 3;
        version += 1;
        history.push(HistoryOp {
            id: i * 2 + 1,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Get,
            invoke: tick,
            ret: Some(tick + 1),
            result: Some(OpResult::Value {
                value: history.ops[i * 2].result.as_ref().and_then(|r| match r {
                    OpResult::Value { value, .. } => value.clone(),
                    _ => None,
                }),
                version: version - 1,
            }),
            indeterminate: false,
        });
        tick += 2;
    }
    history
}

fn corrupt_one_read(history: &mut History, _seed: u64) {
    for op in history.ops.iter_mut() {
        if matches!(op.op, ClientOp::Get) {
            op.result = Some(OpResult::Value {
                value: Some(Bytes::from_static(b"bogus")),
                version: 0,
            });
            return;
        }
    }
}

fn brute_force(history: &History) -> bool {
    let n = history.ops.len();
    if n > 8 {
        return check_history(history).is_ok();
    }
    let mut perm: Vec<usize> = (0..n).collect();
    brute_perms(&mut perm, 0, history)
}

fn brute_perms(perm: &mut [usize], start: usize, history: &History) -> bool {
    if start == perm.len() {
        return respects_order(perm, history) && simulates(perm, history);
    }
    for i in start..perm.len() {
        perm.swap(start, i);
        if brute_perms(perm, start + 1, history) {
            return true;
        }
        perm.swap(start, i);
    }
    false
}

fn respects_order(perm: &[usize], history: &History) -> bool {
    let mut position = vec![0usize; history.ops.len()];
    for (rank, &id) in perm.iter().enumerate() {
        position[history.ops[id].id] = rank;
    }
    for a in &history.ops {
        for b in &history.ops {
            if a.happens_before(b) && position[a.id] >= position[b.id] {
                return false;
            }
        }
    }
    true
}

fn simulates(perm: &[usize], history: &History) -> bool {
    use std::collections::BTreeMap;
    fn step(
        perm: &[usize],
        at: usize,
        history: &History,
        states: &mut BTreeMap<Vec<u8>, (Option<Vec<u8>>, Option<u64>)>,
    ) -> bool {
        if at == perm.len() {
            return true;
        }
        let op = &history.ops[perm[at]];
        if op.indeterminate && op.result.is_none() {
            if step(perm, at + 1, history, states) {
                return true;
            }
            let snapshot = states.clone();
            apply_effect(states, op);
            if step(perm, at + 1, history, states) {
                return true;
            }
            *states = snapshot;
            return false;
        }
        if !apply_checked(states, op) {
            return false;
        }
        step(perm, at + 1, history, states)
    }
    step(perm, 0, history, &mut BTreeMap::new())
}

fn apply_effect(
    states: &mut std::collections::BTreeMap<Vec<u8>, (Option<Vec<u8>>, Option<u64>)>,
    op: &HistoryOp,
) {
    let entry = states.entry(op.key.to_vec()).or_insert((None, None));
    match &op.op {
        ClientOp::Put { value } => *entry = (Some(value.to_vec()), None),
        ClientOp::Delete => *entry = (None, None),
        _ => {}
    }
}

fn apply_checked(
    states: &mut std::collections::BTreeMap<Vec<u8>, (Option<Vec<u8>>, Option<u64>)>,
    op: &HistoryOp,
) -> bool {
    let entry = states.entry(op.key.to_vec()).or_insert((None, None));
    match (&op.op, &op.result) {
        (
            ClientOp::Put { value },
            Some(OpResult::Value {
                value: observed,
                version,
            }),
        ) => {
            if observed.as_ref() != Some(value) {
                return false;
            }
            *entry = (Some(value.to_vec()), Some(*version));
        }
        (ClientOp::Get, Some(OpResult::Value { value, version })) => {
            if value.as_ref().map(|b| b.to_vec()) != entry.0 {
                return false;
            }
            match entry.1 {
                Some(known) if known != *version => return false,
                _ => entry.1 = Some(*version),
            }
        }
        (ClientOp::Delete, Some(OpResult::Value { version, .. })) => {
            *entry = (None, Some(*version));
        }
        (ClientOp::Delete, _) => *entry = (None, entry.1.map(|v| v.wrapping_add(1))),
        _ => {}
    }
    true
}
