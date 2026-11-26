#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_sim::SOMETIMES_INVENTORY;
use std::collections::{BTreeMap, BTreeSet};

/// Collects every `sometimes!(sink, "name")` point in a crate's sources.
/// Occurrences without the two-argument call shape are doc mentions and are
/// skipped; every real site keeps its name on one line.
fn collect_points(dir: &std::path::Path, out: &mut BTreeMap<String, String>) {
    for entry in std::fs::read_dir(dir).expect("read src dir") {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            collect_points(&path, out);
            continue;
        }
        if path.extension().is_none_or(|ext| ext != "rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("read rust file");
        for (line_no, line) in text.lines().enumerate() {
            let mut rest = line;
            while let Some(pos) = rest.find("sometimes!(") {
                let after = &rest[pos + "sometimes!(".len()..];
                let Some(comma) = after.find(',') else {
                    rest = after;
                    continue;
                };
                let name_part = after[comma + 1..].trim_start();
                let Some(quoted) = name_part.strip_prefix('"') else {
                    rest = &after[comma + 1..];
                    continue;
                };
                let Some(end) = quoted.find('"') else {
                    panic!(
                        "unterminated sometimes name at {}:{}",
                        path.display(),
                        line_no + 1
                    );
                };
                out.insert(
                    quoted[..end].to_string(),
                    format!("{}:{}", path.display(), line_no + 1),
                );
                rest = &quoted[end + 1..];
            }
        }
    }
}

#[test]
fn test_sometimes_inventory_matches_source() {
    let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found: BTreeMap<String, String> = BTreeMap::new();
    for krate in [
        "anvil-raft",
        "anvil-sim",
        "anvil-kv",
        "anvil-storage",
        "anvil-node",
    ] {
        collect_points(&manifest.join("..").join(krate).join("src"), &mut found);
    }
    let scanned: BTreeSet<&str> = found.keys().map(String::as_str).collect();
    let registered: BTreeSet<&str> = SOMETIMES_INVENTORY.iter().copied().collect();
    assert_eq!(
        SOMETIMES_INVENTORY.len(),
        registered.len(),
        "inventory has duplicates"
    );
    let mut sorted = SOMETIMES_INVENTORY.to_vec();
    sorted.sort_unstable();
    assert_eq!(sorted, SOMETIMES_INVENTORY, "inventory must stay sorted");
    assert!(
        registered == scanned,
        "inventory/scanner mismatch: missing={:?} unregistered={:?} ({:?})",
        scanned.difference(&registered).collect::<Vec<_>>(),
        registered.difference(&scanned).collect::<Vec<_>>(),
        found,
    );
}

#[test]
fn test_sometimes_missed_lists_zero_hit_points() {
    let hits = vec![("raft.snapshot.send.start".to_string(), 3u64)];
    let missed = anvil_sim::sometimes_missed(&hits);
    assert!(!missed.contains(&"raft.snapshot.send.start".to_string()));
    assert!(missed.contains(&"storage.torn.tail".to_string()));
    assert_eq!(
        anvil_sim::sometimes_missed(&[]).len(),
        SOMETIMES_INVENTORY.len()
    );
    let full: Vec<(String, u64)> = SOMETIMES_INVENTORY
        .iter()
        .map(|name| ((*name).to_string(), 1u64))
        .collect();
    assert_eq!(anvil_sim::sometimes_missed(&full).len(), 0);
}
