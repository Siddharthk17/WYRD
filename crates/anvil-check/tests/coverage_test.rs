#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_check::list::ListState;
use anvil_check::porcupine::export_porcupine;
use anvil_check::register::RegisterState;
use anvil_check::{ClientOp, History, HistoryOp, OpResult, check_history};
use bytes::Bytes;

fn put(id: usize, key: &[u8], value: &[u8], version: u64, invoke: u64, ret: u64) -> HistoryOp {
    HistoryOp {
        id,
        client: 1,
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

fn get(
    id: usize,
    key: &[u8],
    value: Option<&[u8]>,
    version: u64,
    invoke: u64,
    ret: u64,
) -> HistoryOp {
    HistoryOp {
        id,
        client: 2,
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

fn cas(
    id: usize,
    key: &[u8],
    expected: u64,
    value: &[u8],
    version: u64,
    invoke: u64,
    ret: u64,
) -> HistoryOp {
    HistoryOp {
        id,
        client: 3,
        key: Bytes::copy_from_slice(key),
        op: ClientOp::Cas {
            expected_version: expected,
            value: Bytes::copy_from_slice(value),
        },
        invoke,
        ret: Some(ret),
        result: Some(OpResult::Value {
            value: Some(Bytes::from(value.to_vec())),
            version,
        }),
        indeterminate: false,
    }
}

#[test]
fn test_register_cas_success_and_conflict() {
    let mut history = History::new();
    assert!(history.is_empty());
    history.push(put(0, b"k", b"a", 5, 0, 2));
    history.push(cas(1, b"k", 5, b"b", 9, 3, 5));
    history.push(get(2, b"k", Some(b"b"), 9, 6, 8));
    assert_eq!(history.len(), 3);
    assert!(check_history(&history).is_ok());

    let mut conflict = History::new();
    conflict.push(put(0, b"k", b"a", 5, 0, 2));
    conflict.push(HistoryOp {
        id: 1,
        client: 3,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 99,
            value: Bytes::from_static(b"b"),
        },
        invoke: 3,
        ret: Some(5),
        result: Some(OpResult::CasConflict { version: 5 }),
        indeterminate: false,
    });
    conflict.push(get(2, b"k", Some(b"a"), 5, 6, 8));
    assert!(check_history(&conflict).is_ok());
}

#[test]
fn test_register_cas_unknown_version_paths() {
    let mut state = RegisterState::default();
    assert_eq!(state.state_hash(), RegisterState::default().state_hash());
    let mut indet = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Put {
            value: Bytes::from_static(b"v"),
        },
        invoke: 0,
        ret: Some(4),
        result: None,
        indeterminate: true,
    };
    state.apply_indeterminate(&indet);
    let cas_op = HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 7,
            value: Bytes::from_static(b"w"),
        },
        invoke: 5,
        ret: Some(6),
        result: Some(OpResult::Value {
            value: Some(Bytes::from_static(b"w")),
            version: 8,
        }),
        indeterminate: false,
    };
    assert!(state.apply(&cas_op));
    assert!(indet.is_completed() == false);
    indet.result = Some(OpResult::Value {
        value: None,
        version: 0,
    });
    assert!(indet.is_completed());
    let mut conflict_state = RegisterState::default();
    conflict_state.apply_indeterminate(&HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Delete,
        invoke: 0,
        ret: Some(1),
        result: None,
        indeterminate: true,
    });
    let conflict = HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 3,
            value: Bytes::from_static(b"x"),
        },
        invoke: 2,
        ret: Some(3),
        result: Some(OpResult::CasConflict { version: 9 }),
        indeterminate: false,
    };
    assert!(conflict_state.apply(&conflict));
    let mismatch = HistoryOp {
        id: 2,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Cas {
            expected_version: 9,
            value: Bytes::from_static(b"x"),
        },
        invoke: 4,
        ret: Some(5),
        result: Some(OpResult::CasConflict { version: 9 }),
        indeterminate: false,
    };
    assert!(!conflict_state.apply(&mismatch));
}

#[test]
fn test_register_delete_unknown_and_mismatch() {
    let mut state = RegisterState::default();
    let del = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Delete,
        invoke: 0,
        ret: Some(1),
        result: None,
        indeterminate: true,
    };
    state.apply_indeterminate(&del);
    let get = HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 2,
        ret: Some(3),
        result: Some(OpResult::Value {
            value: None,
            version: 11,
        }),
        indeterminate: false,
    };
    assert!(state.apply(&get));
    let wrong_get = HistoryOp {
        id: 2,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 4,
        ret: Some(5),
        result: Some(OpResult::Value {
            value: Some(Bytes::from_static(b"z")),
            version: 11,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&wrong_get));
    let wrong_version = HistoryOp {
        id: 3,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 6,
        ret: Some(7),
        result: Some(OpResult::Value {
            value: None,
            version: 12,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&wrong_version));
}

#[test]
fn test_list_indeterminate_and_adoption() {
    let mut state = ListState::default();
    let append = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 0,
        ret: Some(3),
        result: None,
        indeterminate: true,
    };
    state.apply_indeterminate(&append);
    let get = HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Get,
        invoke: 4,
        ret: Some(5),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"a")],
            version: 21,
        }),
        indeterminate: false,
    };
    assert!(state.apply(&get));
    let wrong = HistoryOp {
        id: 2,
        client: 2,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Get,
        invoke: 6,
        ret: Some(7),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"b")],
            version: 21,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&wrong));
    let wrong_version = HistoryOp {
        id: 3,
        client: 2,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Get,
        invoke: 8,
        ret: Some(9),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"a")],
            version: 22,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&wrong_version));
    let _ = state.state_hash();
}

#[test]
fn test_list_append_mismatch_and_unknown_ops() {
    let mut state = ListState::default();
    let bad_append = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 0,
        ret: Some(1),
        result: Some(OpResult::List {
            values: vec![Bytes::from_static(b"b")],
            version: 1,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&bad_append));
    let short = HistoryOp {
        id: 1,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Append {
            value: Bytes::from_static(b"a"),
        },
        invoke: 2,
        ret: Some(3),
        result: Some(OpResult::List {
            values: vec![],
            version: 1,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&short));
    let put_on_list = HistoryOp {
        id: 2,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Put {
            value: Bytes::from_static(b"a"),
        },
        invoke: 4,
        ret: Some(5),
        result: Some(OpResult::Value {
            value: Some(Bytes::from_static(b"a")),
            version: 1,
        }),
        indeterminate: false,
    };
    assert!(!state.apply(&put_on_list));
    let indet_get = HistoryOp {
        id: 3,
        client: 1,
        key: Bytes::from_static(b"l"),
        op: ClientOp::Get,
        invoke: 6,
        ret: None,
        result: None,
        indeterminate: true,
    };
    state.apply_indeterminate(&indet_get);
    assert!(state.items.is_empty());
}

#[test]
fn test_history_helpers_and_porcupine_variants() {
    let mut history = History::new();
    assert!(history.is_empty());
    for (i, (op, key)) in [
        (
            ClientOp::Put {
                value: Bytes::from_static(b"v"),
            },
            "k",
        ),
        (ClientOp::Delete, "k"),
        (
            ClientOp::Cas {
                expected_version: 1,
                value: Bytes::from_static(b"w"),
            },
            "k",
        ),
        (
            ClientOp::Append {
                value: Bytes::from_static(b"a"),
            },
            "l",
        ),
        (ClientOp::Range { limit: 3 }, ""),
    ]
    .into_iter()
    .enumerate()
    {
        history.push(HistoryOp {
            id: 100 + i,
            client: i as u64,
            key: Bytes::copy_from_slice(key.as_bytes()),
            op,
            invoke: i as u64,
            ret: Some(i as u64 + 1),
            result: Some(OpResult::Ok),
            indeterminate: false,
        });
    }
    assert_eq!(history.len(), 5);
    assert!(!history.is_empty());
    assert_eq!(history.ops[0].id, 0);
    let json = export_porcupine(&history);
    assert!(
        json.contains("delete")
            && json.contains("cas")
            && json.contains("append")
            && json.contains("range")
    );
    let empty = History::new();
    assert_eq!(export_porcupine(&empty), "[]");
}

#[test]
fn test_happens_before_edges() {
    let mut first = HistoryOp {
        id: 0,
        client: 1,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 0,
        ret: Some(5),
        result: None,
        indeterminate: false,
    };
    let second = HistoryOp {
        id: 1,
        client: 2,
        key: Bytes::from_static(b"k"),
        op: ClientOp::Get,
        invoke: 6,
        ret: Some(7),
        result: None,
        indeterminate: false,
    };
    assert!(first.happens_before(&second));
    assert!(!second.happens_before(&first));
    first.indeterminate = true;
    assert!(!first.happens_before(&second));
    first.ret = None;
    first.indeterminate = false;
    assert!(!first.happens_before(&second));
}

#[test]
fn test_range_bounded_rejection_and_conflict_paths() {
    let mut history = History::new();
    for i in 0..4usize {
        history.push(HistoryOp {
            id: i,
            client: 1,
            key: Bytes::from_static(b"k"),
            op: ClientOp::Put {
                value: Bytes::from(vec![i as u8; 4]),
            },
            invoke: i as u64 * 2,
            ret: Some(i as u64 * 2 + 1),
            result: Some(OpResult::Value {
                value: Some(Bytes::from(vec![i as u8; 4])),
                version: i as u64,
            }),
            indeterminate: false,
        });
    }
    history.push(HistoryOp {
        id: 99,
        client: 2,
        key: Bytes::new(),
        op: ClientOp::Range { limit: 2 },
        invoke: 20,
        ret: Some(21),
        result: Some(OpResult::Range { pairs: vec![] }),
        indeterminate: false,
    });
    assert!(check_history(&history).is_err());
}
