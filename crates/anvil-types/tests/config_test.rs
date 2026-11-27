#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use anvil_types::{
    ClusterConfig, ClusterId, EntryKind, LogIndex, MembershipError, NodeId, Term, config_entry,
    decode_config_entry, ensure_cluster,
};

fn node(n: u64) -> NodeId {
    NodeId::new(n).expect("valid node id")
}

#[test]
fn test_config_new_sorts_and_validates() {
    let cfg =
        ClusterConfig::new(vec![node(3), node(1), node(2)], vec![node(9)]).expect("valid config");
    assert_eq!(cfg.voters(), &[node(1), node(2), node(3)]);
    assert_eq!(cfg.learners(), &[node(9)]);
    assert!(cfg.is_voter(node(1)));
    assert!(cfg.is_learner(node(9)));
    assert!(cfg.is_member(node(9)));
    assert!(!cfg.is_member(node(7)));
}

#[test]
fn test_config_rejects_empty_overlap_and_caps() {
    assert_eq!(
        ClusterConfig::new(Vec::new(), Vec::new()),
        Err(MembershipError::EmptyVoters)
    );
    assert_eq!(
        ClusterConfig::new(vec![node(1)], vec![node(1)]),
        Err(MembershipError::Overlap(node(1)))
    );
    let many: Vec<NodeId> = (1..=10).map(node).collect();
    assert_eq!(
        ClusterConfig::new(many, Vec::new()),
        Err(MembershipError::TooManyMembers)
    );
}

#[test]
fn test_config_quorum_math() {
    assert_eq!(ClusterConfig::majority_of(1), 1);
    assert_eq!(ClusterConfig::majority_of(2), 2);
    assert_eq!(ClusterConfig::majority_of(3), 2);
    assert_eq!(ClusterConfig::majority_of(4), 3);
    let three = ClusterConfig::new(vec![node(1), node(2), node(3)], Vec::new()).expect("valid");
    assert_eq!(three.quorum_size(), 2);
    assert!(three.has_quorum(2));
    assert!(!three.has_quorum(1));
    let five = ClusterConfig::new(
        vec![node(1), node(2), node(3), node(4), node(5)],
        Vec::new(),
    )
    .expect("valid");
    assert!(ClusterConfig::has_joint_quorum(&three, &five, 2, 3));
    assert!(!ClusterConfig::has_joint_quorum(&three, &five, 1, 3));
    assert!(!ClusterConfig::has_joint_quorum(&three, &five, 2, 2));
}

#[test]
fn test_config_wire_roundtrip() {
    let cfg = ClusterConfig::new(vec![node(1), node(2), node(3)], vec![node(4)]).expect("valid");
    let mut buf = Vec::new();
    cfg.encode(&mut buf);
    let back = ClusterConfig::decode(&mut &buf[..]).expect("decode");
    assert_eq!(cfg, back);
}

#[test]
fn test_config_entry_roundtrip_and_rejects_non_config() {
    let cfg = ClusterConfig::single(node(1)).expect("valid");
    for kind in [EntryKind::EnterJoint, EntryKind::LeaveJoint] {
        let entry = config_entry(kind, LogIndex::from_raw(1), Term::from_raw(2), &cfg);
        assert!(entry.kind.is_config_change());
        assert_eq!(decode_config_entry(&entry).expect("decode"), cfg);
    }
    let plain = anvil_types::Entry::noop(LogIndex::from_raw(1), Term::from_raw(1));
    assert_eq!(
        decode_config_entry(&plain),
        Err(MembershipError::NotConfigEntry)
    );
}

#[test]
fn test_config_entry_kinds_on_wire() {
    assert_eq!(EntryKind::from_u8(3), Some(EntryKind::EnterJoint));
    assert_eq!(EntryKind::from_u8(4), Some(EntryKind::LeaveJoint));
    assert_eq!(EntryKind::EnterJoint.as_u8(), 3);
    assert_eq!(EntryKind::LeaveJoint.as_u8(), 4);
    assert!(EntryKind::EnterJoint.is_config_change());
    assert!(!EntryKind::Normal.is_config_change());
}

#[test]
fn test_ensure_cluster() {
    let a = ClusterId::from_raw(7);
    assert_eq!(ensure_cluster(a, a), Ok(()));
    let err = ensure_cluster(a, ClusterId::from_raw(8)).expect_err("mismatch");
    assert_eq!(
        err,
        MembershipError::WrongCluster {
            expected: a,
            actual: ClusterId::from_raw(8)
        }
    );
}
