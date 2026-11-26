#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::disallowed_methods,
    clippy::disallowed_types,
    clippy::explicit_iter_loop
)]

use anvil_check::{ClientOp, History, HistoryOp, OpResult, check_history};
use bytes::Bytes;
use std::time::Instant;

fn big_linearizable_history(total_ops: usize, keys: usize) -> History {
    let mut history = History::new();
    let mut versions = vec![0u64; keys];
    let mut tick = 0u64;
    for i in 0..total_ops {
        let key_idx = i % keys;
        let key = Bytes::from(format!("k{key_idx:04}").into_bytes());
        let value = Bytes::from(format!("v{i:06}").into_bytes());
        if i % 5 == 4 {
            history.push(HistoryOp {
                id: i,
                client: (i % 8) as u64,
                key,
                op: ClientOp::Get,
                invoke: tick,
                ret: Some(tick + 1),
                result: Some(OpResult::Value {
                    value: versions[key_idx].checked_sub(1).map(|_| value.clone()),
                    version: versions[key_idx],
                }),
                indeterminate: false,
            });
            let _ = value;
        } else {
            versions[key_idx] += 1;
            let version = versions[key_idx];
            history.push(HistoryOp {
                id: i,
                client: (i % 8) as u64,
                key,
                op: ClientOp::Put {
                    value: value.clone(),
                },
                invoke: tick,
                ret: Some(tick + 1),
                result: Some(OpResult::Value {
                    value: Some(value),
                    version,
                }),
                indeterminate: false,
            });
        }
        tick += 2;
    }
    fix_gets(history, keys)
}

fn fix_gets(mut history: History, keys: usize) -> History {
    let mut current: Vec<Option<(Bytes, u64)>> = vec![None; keys];
    for op in &mut history.ops {
        let key_idx: usize = String::from_utf8_lossy(&op.key)[1..].parse().unwrap_or(0);
        let slot = &mut current[key_idx % keys];
        match (&op.op, &mut op.result) {
            (
                ClientOp::Put { value },
                Some(OpResult::Value {
                    value: observed,
                    version,
                }),
            ) => {
                *observed = Some(value.clone());
                *slot = Some((value.clone(), *version));
            }
            (ClientOp::Get, Some(OpResult::Value { value, version })) => {
                let (v, x) = slot.clone().map_or((None, 0), |(a, b)| (Some(a), b));
                *value = v;
                *version = x;
            }
            _ => {}
        }
    }
    history
}

#[test]
fn test_checker_perf_100k_100_keys() {
    let history = big_linearizable_history(100_000, 100);
    assert_eq!(history.len(), 100_000);
    let start = Instant::now();
    let verdict = check_history(&history);
    let elapsed = start.elapsed();
    eprintln!("100k-op 100-key check took {elapsed:.2?}");
    assert!(verdict.is_ok());
    if cfg!(not(debug_assertions)) {
        assert!(
            elapsed.as_secs_f64() < 2.0,
            "checker exceeded 2s budget: {elapsed:.2?}"
        );
    }
}

#[test]
#[ignore = "1M bounded-memory run, execute explicitly in release"]
fn test_checker_1m_bounded_memory() {
    let history = big_linearizable_history(1_000_000, 1000);
    assert_eq!(history.len(), 1_000_000);
    let start = Instant::now();
    let verdict = check_history(&history);
    let elapsed = start.elapsed();
    eprintln!("1M-op check took {elapsed:.2?}");
    assert!(verdict.is_ok());
}
