#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Register model: `Put`, `Get`, `Delete`, and value-compared `Cas`.
//! Versions are opaque observations, never predictions. A completed write
//! carries the version its response observed. An indeterminate write that
//! takes effect fixes the value but leaves the version free: the version is
//! adopted from the next completed observation of the same value. Two
//! writers never share a value in practice (values embed client, sequence,
//! and tick), so adoption attributes each observation to its only possible
//! writer instead of rejecting the history on a guessed counter.

use crate::history::{ClientOp, HistoryOp, OpResult};
use bytes::Bytes;

/// Register state for one key.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RegisterState {
    /// Current value, or `None` when deleted or never written.
    pub value: Option<Bytes>,
    /// Version observed from the last linearized write.
    pub version: u64,
    /// False while the version comes from an indeterminate effect and no
    /// completed observation has pinned it down yet.
    pub version_known: bool,
}

impl RegisterState {
    /// Applies a linearized operation, checking the observed result.
    /// Returns false when the result cannot match this state.
    pub fn apply(&mut self, op: &HistoryOp) -> bool {
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
                self.value = Some(value.clone());
                self.version = *version;
                self.version_known = true;
                true
            }
            (ClientOp::Put { .. } | ClientOp::Get | ClientOp::Cas { .. }, None) => true,
            (ClientOp::Get, Some(OpResult::Value { value, version })) => {
                if value != &self.value {
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
            (
                ClientOp::Delete,
                Some(OpResult::Value {
                    value: None,
                    version,
                }),
            ) => {
                self.value = None;
                self.version = *version;
                self.version_known = true;
                true
            }
            (ClientOp::Delete, None) => {
                self.value = None;
                self.version = self.version.wrapping_add(1);
                self.version_known = false;
                true
            }
            (
                ClientOp::Cas {
                    expected_version,
                    value,
                },
                Some(OpResult::Value {
                    value: observed,
                    version,
                }),
            ) => {
                if self.version_known {
                    if *expected_version != self.version {
                        return false;
                    }
                } else {
                    self.version = *expected_version;
                    self.version_known = true;
                }
                if observed.as_ref() != Some(value) {
                    return false;
                }
                self.value = Some(value.clone());
                self.version = *version;
                self.version_known = true;
                true
            }
            (
                ClientOp::Cas {
                    expected_version, ..
                },
                Some(OpResult::CasConflict { version }),
            ) => {
                if self.version_known {
                    *expected_version != self.version && *version == self.version
                } else {
                    self.version = *version;
                    self.version_known = true;
                    *expected_version != self.version
                }
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
            0x9E37_79B9_7F4A_7C15
        } else {
            0xD6E8_FEB8_66F1_2D57
        });
        if let Some(value) = &self.value {
            for chunk in value.chunks(8) {
                let mut word = [0u8; 8];
                word[..chunk.len()].copy_from_slice(chunk);
                x = x.wrapping_add(u64::from_le_bytes(word));
                x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                x ^= x >> 27;
            }
            x = x.wrapping_add(value.len() as u64);
        }
        x ^= x >> 30;
        x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^ (x >> 31)
    }

    /// Applies an indeterminate operation without checking a result.
    /// Effects fix values; versions stay free until an observation pins
    /// them down.
    pub fn apply_indeterminate(&mut self, op: &HistoryOp) {
        match &op.op {
            ClientOp::Put { value } => {
                self.value = Some(value.clone());
                self.version_known = false;
            }
            ClientOp::Delete => {
                self.value = None;
                self.version_known = false;
            }
            ClientOp::Cas {
                expected_version,
                value,
            } => {
                if !self.version_known || *expected_version == self.version {
                    self.value = Some(value.clone());
                    self.version_known = false;
                }
            }
            ClientOp::Get | ClientOp::Append { .. } | ClientOp::Range { .. } => {}
        }
    }
}
