#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Client history model with virtual-time invoke and return events.
//! Timed-out operations are recorded as indeterminate.

use bytes::Bytes;

/// A client operation submitted to the store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientOp {
    Put { value: Bytes },
    Get,
    Delete,
    Cas { expected_version: u64, value: Bytes },
    Append { value: Bytes },
    Range { limit: usize },
}

/// Observed result of a completed operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OpResult {
    Value { value: Option<Bytes>, version: u64 },
    List { values: Vec<Bytes>, version: u64 },
    Range { pairs: Vec<(Bytes, Bytes)> },
    Ok,
    CasConflict { version: u64 },
    SessionExpired,
}

/// One invoke and return event pair in virtual time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoryOp {
    /// Position in the history.
    pub id: usize,
    /// Client that issued the operation.
    pub client: u64,
    /// Key under test. Empty for whole-store range.
    pub key: Bytes,
    /// Operation kind.
    pub op: ClientOp,
    /// Virtual tick of the invoke event.
    pub invoke: u64,
    /// Virtual tick of the return event. `None` means no return observed.
    pub ret: Option<u64>,
    /// Result for completed operations.
    pub result: Option<OpResult>,
    /// True when the operation timed out: it may linearize at any point
    /// after its invoke, or never.
    pub indeterminate: bool,
}

impl HistoryOp {
    /// Returns true when the operation completed with a known result.
    #[must_use]
    pub const fn is_completed(&self) -> bool {
        self.result.is_some()
    }

    /// Returns true when `self` returns before `other` is invoked.
    /// Indeterminate operations impose no out-edges: a timed-out operation
    /// may take effect at any point after its invoke, or never, so its
    /// return time constrains nothing that follows.
    #[must_use]
    pub fn happens_before(&self, other: &Self) -> bool {
        if self.indeterminate {
            return false;
        }
        match (self.ret, other.ret) {
            (Some(end), _) => end < other.invoke,
            (None, _) => false,
        }
    }
}

/// A recorded client history in virtual time.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct History {
    /// Operations in invoke order.
    pub ops: Vec<HistoryOp>,
}

impl History {
    /// Creates an empty history.
    #[must_use]
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Appends an operation, assigning its id.
    pub fn push(&mut self, mut op: HistoryOp) {
        op.id = self.ops.len();
        self.ops.push(op);
    }

    /// Returns the number of operations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns true when empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Groups operation ids by key for P-compositionality.
    #[must_use]
    pub fn partition_by_key(&self) -> std::collections::BTreeMap<Vec<u8>, Vec<usize>> {
        let mut groups = std::collections::BTreeMap::new();
        for op in &self.ops {
            groups
                .entry(op.key.to_vec())
                .or_insert_with(Vec::new)
                .push(op.id);
        }
        groups
    }
}
