#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_possible_truncation
)]

use anvil_check::porcupine::export_porcupine;
use anvil_check::{ClientOp, History, HistoryOp, OpResult, check_history};
use bytes::Bytes;
use std::collections::BTreeMap;
use std::io::Write as _;
use std::process::Command;

/// Deterministic 64-bit LCG for history generation. No external RNG: the
/// cross-check fixtures must build identically on every machine.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        if bound == 0 {
            return 0;
        }
        (self.0 >> 33) % bound
    }
}

struct ReferenceRegister {
    values: BTreeMap<Vec<u8>, Vec<u8>>,
    versions: BTreeMap<Vec<u8>, u64>,
    next_version: u64,
}

impl ReferenceRegister {
    fn new() -> Self {
        Self {
            values: BTreeMap::new(),
            versions: BTreeMap::new(),
            next_version: 1,
        }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> OpResult {
        let version = self.next_version;
        self.next_version += 1;
        self.values.insert(key.to_vec(), value.to_vec());
        self.versions.insert(key.to_vec(), version);
        OpResult::Value {
            value: Some(Bytes::copy_from_slice(value)),
            version,
        }
    }

    fn get(&self, key: &[u8]) -> OpResult {
        OpResult::Value {
            value: self
                .values
                .get(key.as_ref())
                .map(|v| Bytes::copy_from_slice(v)),
            version: self.versions.get(key.as_ref()).copied().unwrap_or(0),
        }
    }

    fn delete(&mut self, key: &[u8]) -> OpResult {
        let version = self.next_version;
        self.next_version += 1;
        self.values.remove(key);
        self.versions.insert(key.to_vec(), version);
        OpResult::Value {
            value: None,
            version,
        }
    }

    fn cas(&mut self, key: &[u8], expected: u64, value: &[u8]) -> OpResult {
        let current = self.versions.get(key.as_ref()).copied().unwrap_or(0);
        if current == expected {
            let version = self.next_version;
            self.next_version += 1;
            self.values.insert(key.to_vec(), value.to_vec());
            self.versions.insert(key.to_vec(), version);
            OpResult::Value {
                value: Some(Bytes::copy_from_slice(value)),
                version,
            }
        } else {
            OpResult::CasConflict { version: current }
        }
    }
}

/// Builds one deterministic register history from `seed`. Times increase
/// monotonically, so the generation order is always a valid linearization.
/// About a third of histories get one corrupted read, making them
/// non-linearizable. Both properties hold by construction for any seed.
fn cross_history(seed: u64) -> History {
    let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(1));
    let mut reference = ReferenceRegister::new();
    let mut history = History::new();
    let keys = 1 + rng.next(3);
    let clients = 1 + rng.next(3);
    let ops = 2 + rng.next(9);
    let mut tick = 0u64;
    for _ in 0..ops {
        let client = 1 + rng.next(clients);
        let key_idx = rng.next(keys);
        let key = Bytes::from(format!("k{key_idx}").into_bytes());
        let roll = rng.next(100);
        let invoke = tick + rng.next(4);
        let ret = invoke + 1 + rng.next(10);
        tick = invoke + 1;
        let value = Bytes::from(format!("v{seed}-{invoke}").into_bytes());
        let (op, result) = if roll < 40 {
            let result = reference.put(&key, &value);
            (ClientOp::Put { value }, result)
        } else if roll < 80 {
            let result = reference.get(&key);
            (ClientOp::Get, result)
        } else if roll < 90 {
            let result = reference.delete(&key);
            (ClientOp::Delete, result)
        } else {
            let expected = reference.versions.get(key.as_ref()).copied().unwrap_or(0);
            let attempt_succeeds = rng.next(2) == 0;
            let expected_arg = if attempt_succeeds {
                expected
            } else {
                expected.wrapping_add(1 + rng.next(3))
            };
            let result = reference.cas(&key, expected_arg, &value);
            (
                ClientOp::Cas {
                    expected_version: expected_arg,
                    value,
                },
                result,
            )
        };
        history.push(HistoryOp {
            id: 0,
            client,
            key,
            op,
            invoke,
            ret: Some(ret),
            result: Some(result),
            indeterminate: false,
        });
    }
    if rng.next(3) == 0 {
        corrupt_one_get(&mut history, seed);
    }
    history
}

fn corrupt_one_get(history: &mut History, seed: u64) {
    let mut rng = Lcg(seed.wrapping_add(0xD6E8_FEB8_66F1_2D57));
    let gets: Vec<usize> = history
        .ops
        .iter()
        .enumerate()
        .filter(|(_, op)| matches!(op.op, ClientOp::Get))
        .map(|(i, _)| i)
        .collect();
    if gets.is_empty() {
        return;
    }
    let pick = gets[rng.next(gets.len() as u64) as usize];
    let bogus = Bytes::from(format!("corrupt-{seed}").into_bytes());
    if let Some(OpResult::Value { value, .. }) = history.ops[pick].result.as_mut() {
        *value = Some(bogus);
    }
}

/// Porcupine cross-check on 10,000 deterministic histories.
///
/// Requires Go on PATH. Ignored by default; runs in release as the nightly
/// CI job `porcupine-cross-check`. Fails loudly when Go is missing or any
/// verdict disagrees.
#[test]
#[ignore = "needs Go toolchain, runs as the nightly porcupine-cross-check job"]
fn test_porcupine_cross_check_ten_thousand_histories() {
    const HISTORIES: u64 = 10_000;
    let dir = std::env::temp_dir().join(format!("anvil-porcupine-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create porcupine scratch dir");
    let path = dir.join("histories.jsonl");
    let mut file = std::fs::File::create(&path).expect("create histories file");
    let mut expected = Vec::with_capacity(HISTORIES as usize);
    for seed in 0..HISTORIES {
        let history = cross_history(seed);
        expected.push(check_history(&history).is_ok());
        let line = export_porcupine(&history);
        writeln!(file, "{line}").expect("write history");
    }
    drop(file);

    let manifest = std::env::var("CARGO_MANIFEST_DIR").expect("manifest dir");
    let go_dir = format!("{manifest}/tests/porcupine");
    let output = Command::new("go")
        .args(["run", ".", &path.to_string_lossy()])
        .current_dir(&go_dir)
        .output()
        .expect("Go toolchain must be installed for the porcupine cross-check");
    assert!(
        output.status.success(),
        "porcupine checker failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("porcupine output is utf8");
    let verdicts: Vec<bool> = stdout
        .lines()
        .map(|line| match line.trim() {
            "true" => true,
            "false" => false,
            other => panic!("unexpected porcupine verdict: {other}"),
        })
        .collect();
    assert_eq!(
        verdicts.len(),
        expected.len(),
        "porcupine verdict count must match history count"
    );
    let mut linearizable = 0usize;
    for (seed, (got, want)) in verdicts.iter().zip(expected.iter()).enumerate() {
        if *want {
            linearizable += 1;
        }
        assert_eq!(
            got, want,
            "porcupine disagrees with anvil-check on history {seed}"
        );
    }
    assert!(
        (1000..9000).contains(&linearizable),
        "expected a mix of verdicts, saw {linearizable} linearizable of {HISTORIES}"
    );
    std::fs::remove_dir_all(&dir).expect("remove porcupine scratch dir");
}
