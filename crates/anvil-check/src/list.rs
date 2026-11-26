#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! List-append model (Elle-style): `Append` adds one element, `Get` returns
//! the full list in apply order. Versions follow the same adoption rule as
//! registers: indeterminate effects fix contents while leaving the version
//! free until a completed observation pins it down.

use crate::history::{ClientOp, HistoryOp, OpResult};
use bytes::Bytes;

/// List state for one key.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ListState {
    /// Elements in linearization order.
    pub items: Vec<Bytes>,
    /// Version observed from the last linearized write.
    pub version: u64,
    /// False while the version comes from an indeterminate effect.
    pub version_known: bool,
}

impl ListState {
    /// Applies a linearized operation, checking the observed result.
    pub fn apply(&mut self, op: &HistoryOp) -> bool {
        match (&op.op, &op.result) {
            (ClientOp::Append { .. } | ClientOp::Get, None) => true,
            (ClientOp::Append { value }, Some(OpResult::List { values, version })) => {
                if values.len() != self.items.len() + 1 {
                    return false;
                }
                if values[..self.items.len()] != self.items[..] {
                    return false;
                }
                if values.last() != Some(value) {
                    return false;
                }
                self.items.push(value.clone());
                self.version = *version;
                self.version_known = true;
                true
            }
            (ClientOp::Get, Some(OpResult::List { values, version })) => {
                if values != &self.items {
                    return false;
                }
                if self.version_known {
                    if *version != self.version {
                        return false;
                    }
                } else {
                    self.version = *version;
                    self.version_known = true;
                }
                true
            }
            _ => false,
        }
    }

    /// State hash for memoization.
    #[must_use]
    pub fn state_hash(&self) -> u64 {
        // The known flag selects the seed, never an added one: adding it
        // after the value sum lets a one-byte value difference cancel the
        // flag exactly, which would memo-prune distinct states as one.
        let mut x = self.version.wrapping_add(if self.version_known {
            0xD6E8_FEB8_66F1_2D57
        } else {
            0x94D0_49BB_1331_11EB
        });
        x = x.wrapping_add(self.items.len() as u64);
        for item in &self.items {
            for chunk in item.chunks(8) {
                let mut word = [0u8; 8];
                word[..chunk.len()].copy_from_slice(chunk);
                x = x.wrapping_add(u64::from_le_bytes(word));
                x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
                x ^= x >> 29;
            }
        }
        x ^ (x >> 31)
    }

    /// Applies an indeterminate append without checking a result.
    pub fn apply_indeterminate(&mut self, op: &HistoryOp) {
        if let ClientOp::Append { value } = &op.op {
            self.items.push(value.clone());
            self.version_known = false;
        }
    }
}
