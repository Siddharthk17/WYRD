#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#![allow(
    clippy::assert_is_empty,
    clippy::cast_possible_truncation,
    clippy::too_many_lines
)]

use anvil_check::list::ListState;
use anvil_check::register::RegisterState;
use anvil_check::{ClientOp, History, HistoryOp, OpResult, check_history};
use bytes::Bytes;

fn op(
    id: usize,
    op: ClientOp,
    invoke: u64,
    ret: Option<u64>,
    result: Option<OpResult>,
    ind: bool,
) -> HistoryOp {
    HistoryOp {
        id,
        client: 1,
        key: Bytes::from_static(b"k"),
        op,
        invoke,
        ret,
        result,
        indeterminate: ind,
    }
}
fn val(v: &[u8], version: u64) -> OpResult {
    OpResult::Value {
        value: Some(Bytes::copy_from_slice(v)),
        version,
    }
}
fn lst(v: &[&[u8]], version: u64) -> OpResult {
    OpResult::List {
        values: v.iter().map(|b| Bytes::copy_from_slice(b)).collect(),
        version,
    }
}

#[test]
fn test_register_every_apply_arm() {
    let mut s = RegisterState::default();
    // Put mismatch then match.
    assert!(!s.apply(&op(
        0,
        ClientOp::Put {
            value: Bytes::from_static(b"a")
        },
        0,
        Some(1),
        Some(OpResult::Value {
            value: Some(Bytes::from_static(b"b")),
            version: 1
        }),
        false
    )));
    assert!(s.apply(&op(
        0,
        ClientOp::Put {
            value: Bytes::from_static(b"a")
        },
        0,
        Some(1),
        Some(val(b"a", 1)),
        false
    )));
    // Completed op with no result is accepted.
    assert!(s.apply(&op(
        1,
        ClientOp::Put {
            value: Bytes::from_static(b"x")
        },
        2,
        Some(3),
        None,
        false
    )));
    assert!(s.apply(&op(2, ClientOp::Get, 4, Some(5), None, false)));
    // Completed Delete reports its version.
    assert!(s.apply(&HistoryOp {
        id: 3,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Delete,
        invoke: 6,
        ret: Some(7),
        result: Some(OpResult::Value {
            value: None,
            version: 9
        }),
        indeterminate: false
    }));
    // Cas success, conflict, and mismatched conflict version.
    assert!(s.apply(&op(
        4,
        ClientOp::Cas {
            expected_version: 9,
            value: Bytes::from_static(b"c")
        },
        8,
        Some(9),
        Some(val(b"c", 10)),
        false
    )));
    let conflict = HistoryOp {
        id: 5,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 99,
            value: Bytes::from_static(b"x"),
        },
        invoke: 10,
        ret: Some(11),
        result: Some(OpResult::CasConflict { version: 10 }),
        indeterminate: false,
    };
    assert!(s.apply(&conflict));
    let bad_conflict = HistoryOp {
        id: 6,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 99,
            value: Bytes::from_static(b"x"),
        },
        invoke: 12,
        ret: Some(13),
        result: Some(OpResult::CasConflict { version: 11 }),
        indeterminate: false,
    };
    assert!(!s.apply(&bad_conflict));
    // Mismatched kinds fall through.
    let mismatch = HistoryOp {
        id: 7,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Put {
            value: Bytes::from_static(b"a"),
        },
        invoke: 14,
        ret: Some(15),
        result: Some(OpResult::Ok),
        indeterminate: false,
    };
    assert!(!s.apply(&mismatch));
    // Indeterminate Cas with an unknown version cannot be ruled out.
    let mut t = RegisterState::default();
    t.apply_indeterminate(&op(
        0,
        ClientOp::Cas {
            expected_version: 5,
            value: Bytes::from_static(b"z"),
        },
        0,
        None,
        None,
        true,
    ));
    assert_eq!(t.value, Some(Bytes::from_static(b"z")));
    // Indeterminate Cas that provably misses leaves state alone.
    let mut u = RegisterState::default();
    u.apply(&op(
        9,
        ClientOp::Put {
            value: Bytes::from_static(b"q"),
        },
        8,
        Some(9),
        Some(val(b"q", 4)),
        false,
    ));
    u.apply_indeterminate(&op(
        10,
        ClientOp::Cas {
            expected_version: 5,
            value: Bytes::from_static(b"z"),
        },
        10,
        None,
        None,
        true,
    ));
    assert_eq!(u.value, Some(Bytes::from_static(b"q")));
    t.apply_indeterminate(&op(
        2,
        ClientOp::Append {
            value: Bytes::from_static(b"z"),
        },
        4,
        None,
        None,
        true,
    ));
    t.apply_indeterminate(&op(3, ClientOp::Range { limit: 1 }, 6, None, None, true));
    assert_eq!(t.value, Some(Bytes::from_static(b"z")));
    // Empty-value hashing paths.
    let empty = RegisterState::default();
    let _ = empty.state_hash();
    let mut long = RegisterState::default();
    long.apply(&op(
        0,
        ClientOp::Put {
            value: Bytes::from(vec![7u8; 100]),
        },
        0,
        Some(1),
        Some(val(&[7u8; 100], 3)),
        false,
    ));
    let _ = long.state_hash();
}

#[test]
fn test_list_every_apply_arm() {
    let mut s = ListState::default();
    assert!(s.apply(&op(0, ClientOp::Get, 0, Some(1), None, false)));
    assert!(!s.apply(&op(
        1,
        ClientOp::Put {
            value: Bytes::from_static(b"a")
        },
        2,
        Some(3),
        Some(val(b"a", 1)),
        false
    )));
    assert!(s.apply(&HistoryOp {
        id: 2,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a")
        },
        invoke: 4,
        ret: Some(5),
        result: Some(lst(&[b"a"], 6)),
        indeterminate: false
    }));
    assert!(!s.apply(&HistoryOp {
        id: 3,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"b")
        },
        invoke: 6,
        ret: Some(7),
        result: Some(lst(&[b"a"], 6)),
        indeterminate: false
    }));
    assert!(s.apply(&HistoryOp {
        id: 4,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 8,
        ret: Some(9),
        result: Some(lst(&[b"a"], 6)),
        indeterminate: false
    }));
    let mut t = ListState::default();
    t.apply_indeterminate(&op(0, ClientOp::Get, 0, None, None, true));
    t.apply_indeterminate(&op(1, ClientOp::Range { limit: 2 }, 2, None, None, true));
    t.apply_indeterminate(&op(2, ClientOp::Delete, 4, None, None, true));
    t.apply_indeterminate(&op(
        3,
        ClientOp::Cas {
            expected_version: 1,
            value: Bytes::from_static(b"x"),
        },
        6,
        None,
        None,
        true,
    ));
    assert!(t.items.is_empty());
    let _ = t.state_hash();
    let mut big = ListState::default();
    big.apply(&HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Append {
            value: Bytes::from(vec![9u8; 100]),
        },
        invoke: 0,
        ret: Some(1),
        result: Some(lst(&[&[9u8; 100]], 2)),
        indeterminate: false,
    });
    let _ = big.state_hash();
}

fn hist(op_list: Vec<HistoryOp>) -> History {
    let mut h = History::new();
    for op in op_list {
        h.push(op);
    }
    h
}

fn wput(id: u64, key: &[u8], v: &[u8], ver: u64, inv: u64, ret: u64) -> HistoryOp {
    HistoryOp {
        id: id as usize,
        client: 1,
        key: Bytes::copy_from_slice(key),
        op: ClientOp::Put {
            value: Bytes::copy_from_slice(v),
        },
        invoke: inv,
        ret: Some(ret),
        result: Some(OpResult::Value {
            value: Some(Bytes::copy_from_slice(v)),
            version: ver,
        }),
        indeterminate: false,
    }
}

#[test]
fn test_search_indeterminate_with_result() {
    let mut h = hist(vec![
        wput(0, b"k", b"a", 1, 0, 2),
        HistoryOp {
            id: 1,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Put {
                value: Bytes::from_static(b"b"),
            },
            invoke: 3,
            ret: Some(9),
            result: Some(OpResult::Value {
                value: Some(Bytes::from_static(b"b")),
                version: 7,
            }),
            indeterminate: true,
        },
        HistoryOp {
            id: 2,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Get,
            invoke: 4,
            ret: Some(5),
            result: Some(OpResult::Value {
                value: Some(Bytes::from_static(b"b")),
                version: 7,
            }),
            indeterminate: false,
        },
    ]);
    assert!(check_history(&h).is_ok());
    // A read of a value no write ever produced cannot linearize anywhere.
    h.ops[2].result = Some(OpResult::Value {
        value: Some(Bytes::from_static(b"zzz")),
        version: 7,
    });
    assert!(check_history(&h).is_err());
}

#[test]
fn test_search_range_with_indet_effects() {
    let range_of = |pairs: Vec<(&[u8], &[u8])>| OpResult::Range {
        pairs: pairs
            .into_iter()
            .map(|(k, v)| (Bytes::copy_from_slice(k), Bytes::copy_from_slice(v)))
            .collect(),
    };
    let h = hist(vec![
        wput(0, b"k", b"a", 1, 0, 2),
        HistoryOp {
            id: 1,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Delete,
            invoke: 3,
            ret: Some(9),
            result: None,
            indeterminate: true,
        },
        HistoryOp {
            id: 2,
            client: 2,
            key: Bytes::new(),
            op: ClientOp::Range { limit: 8 },
            invoke: 4,
            ret: Some(5),
            result: Some(range_of(vec![])),
            indeterminate: false,
        },
    ]);
    assert!(check_history(&h).is_ok());
    let h2 = hist(vec![
        wput(0, b"k", b"a", 1, 0, 2),
        HistoryOp {
            id: 1,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Cas {
                expected_version: 1,
                value: Bytes::from_static(b"b"),
            },
            invoke: 3,
            ret: Some(9),
            result: None,
            indeterminate: true,
        },
        HistoryOp {
            id: 2,
            client: 2,
            key: Bytes::new(),
            op: ClientOp::Range { limit: 8 },
            invoke: 4,
            ret: Some(5),
            result: Some(range_of(vec![(b"k", b"b")])),
            indeterminate: false,
        },
    ]);
    assert!(check_history(&h2).is_ok());
    let h3 = hist(vec![
        wput(0, b"k", b"a", 1, 0, 2),
        HistoryOp {
            id: 1,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Delete,
            invoke: 3,
            ret: Some(4),
            result: Some(OpResult::Value {
                value: None,
                version: 5,
            }),
            indeterminate: false,
        },
        HistoryOp {
            id: 2,
            client: 2,
            key: Bytes::new(),
            op: ClientOp::Range { limit: 8 },
            invoke: 5,
            ret: Some(6),
            result: Some(range_of(vec![])),
            indeterminate: false,
        },
    ]);
    assert!(check_history(&h3).is_ok());
}

#[test]
fn test_search_range_with_list_get() {
    let h = hist(vec![
        HistoryOp {
            id: 0,
            client: 1,
            key: Bytes::from_static(b"l"),
            op: ClientOp::Append {
                value: Bytes::from_static(b"x"),
            },
            invoke: 0,
            ret: Some(2),
            result: Some(OpResult::List {
                values: vec![Bytes::from_static(b"x")],
                version: 3,
            }),
            indeterminate: false,
        },
        HistoryOp {
            id: 1,
            client: 2,
            key: Bytes::from_static(b"l"),
            op: ClientOp::Get,
            invoke: 3,
            ret: Some(4),
            result: Some(OpResult::List {
                values: vec![Bytes::from_static(b"x")],
                version: 3,
            }),
            indeterminate: false,
        },
        HistoryOp {
            id: 2,
            client: 3,
            key: Bytes::new(),
            op: ClientOp::Range { limit: 8 },
            invoke: 5,
            ret: Some(6),
            result: Some(OpResult::Range {
                pairs: vec![(Bytes::from_static(b"l"), Bytes::from_static(b"x"))],
            }),
            indeterminate: false,
        },
    ]);
    // Append lands in the whole-store map; the range observes it.
    assert!(check_history(&h).is_ok());
}

#[test]
fn test_failure_json_covers_result_kinds() {
    // Conflict label.
    let h = hist(vec![
        wput(0, b"k", b"a", 10, 0, 2),
        HistoryOp {
            id: 1,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Cas {
                expected_version: 1,
                value: Bytes::from_static(b"b"),
            },
            invoke: 3,
            ret: Some(4),
            result: Some(OpResult::CasConflict { version: 99 }),
            indeterminate: false,
        },
    ]);
    let err = check_history(&h).expect_err("conflict version must mismatch");
    assert!(err.json.contains("conflict"));
    // Expired label.
    let h2 = hist(vec![
        wput(0, b"k", b"a", 10, 0, 2),
        HistoryOp {
            id: 1,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Get,
            invoke: 3,
            ret: Some(4),
            result: Some(OpResult::SessionExpired),
            indeterminate: false,
        },
    ]);
    let err2 = check_history(&h2).expect_err("expired read must fail");
    assert!(err2.json.contains("expired"));
    // Ok label via a failing delete-dependent history.
    let h3 = hist(vec![
        wput(0, b"k", b"a", 10, 0, 2),
        HistoryOp {
            id: 1,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Delete,
            invoke: 3,
            ret: Some(4),
            result: Some(OpResult::Ok),
            indeterminate: false,
        },
        HistoryOp {
            id: 2,
            client: 3,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Get,
            invoke: 5,
            ret: Some(6),
            result: Some(OpResult::Value {
                value: Some(Bytes::from_static(b"a")),
                version: 10,
            }),
            indeterminate: false,
        },
    ]);
    let err3 = check_history(&h3).expect_err("read-after-delete must fail");
    assert!(err3.json.contains("\"ok\"") || err3.json.contains("value"));
    // None label and missing ret path.
    let h4 = hist(vec![
        wput(0, b"k", b"a", 10, 0, 2),
        HistoryOp {
            id: 1,
            client: 2,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Get,
            invoke: 3,
            ret: None,
            result: Some(OpResult::Value {
                value: None,
                version: 0,
            }),
            indeterminate: false,
        },
    ]);
    let err4 = check_history(&h4).expect_err("none mismatch must fail");
    assert!(err4.json.contains("none") || err4.json.contains("18446744073709551615"));
}

#[test]
fn test_list_prefix_mismatch_and_range_variants() {
    let mut state = ListState::default();
    let app = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 0,
        ret: Some(1),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"a")],
            version: 1,
        }),
        indeterminate: false,
    };
    assert!(state.apply(&app));
    let mismatch = HistoryOp {
        id: 1,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 2,
        ret: Some(3),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"x"), Bytes::from_static(b"a")],
            version: 2,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&mismatch));
    // Get with mismatched prefix panics nothing and returns false.
    let get_bad = HistoryOp {
        id: 2,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Get,
        invoke: 4,
        ret: Some(5),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"a"), Bytes::from_static(b"extra")],
            version: 1,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&get_bad));
}

#[test]
fn test_porcupine_all_kinds_and_range_bounds() {
    use anvil_check::porcupine::export_porcupine;
    let mut h = History::new();
    let ops = vec![
        (
            ClientOp::Put {
                value: Bytes::from_static(b"v"),
            },
            0u64,
        ),
        (ClientOp::Get, 1),
        (ClientOp::Delete, 2),
        (
            ClientOp::Cas {
                expected_version: 7,
                value: Bytes::from_static(b"w"),
            },
            3,
        ),
        (
            ClientOp::Append {
                value: Bytes::from_static(b"a"),
            },
            4,
        ),
        (ClientOp::Range { limit: 9 }, 5),
    ];
    for (i, (op, t)) in ops.into_iter().enumerate() {
        h.push(HistoryOp {
            id: i,
            client: i as u64,
            key: Bytes::copy_from_slice(b"k"),
            op,
            invoke: t,
            ret: Some(t + 1),
            result: Some(OpResult::Ok),
            indeterminate: false,
        });
    }
    let json = export_porcupine(&h);
    assert!(json.contains("cas") && json.contains("append") && json.contains("\"limit\":9"));
}

#[test]
fn test_failing_history_long_prefix_and_all_labels() {
    // Three clean puts then a stale read: longest prefix keeps all three.
    let mut h = History::new();
    for i in 0..3usize {
        h.push(HistoryOp {
            id: i,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Put {
                value: Bytes::from(vec![i as u8]),
            },
            invoke: (i * 2) as u64,
            ret: Some(i as u64 * 2 + 1),
            result: Some(OpResult::Value {
                value: Some(Bytes::from(vec![i as u8])),
                version: 10 + i as u64,
            }),
            indeterminate: false,
        });
    }
    h.push(HistoryOp {
        id: 9,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 20,
        ret: Some(21),
        result: Some(OpResult::Value {
            value: Some(Bytes::from_static(b"zzz")),
            version: 99,
        }),
        indeterminate: false,
    });
    let err = check_history(&h).expect_err("stale read must fail");
    assert!(
        err.longest_prefix.len() >= 2,
        "prefix should keep the clean puts"
    );
    assert!(err.json.contains("\"prefix\":[0,1,2]") || err.json.contains("\"prefix\""));
}
