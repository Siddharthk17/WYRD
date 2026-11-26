#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Porcupine-compatible JSON export for cross-checking against the Go
//! checker.

use crate::history::{ClientOp, History};
use std::fmt::Write as _;

/// Exports a history in Porcupine JSON format.
#[must_use]
pub fn export_porcupine(history: &History) -> String {
    let mut out = String::from("[");
    for (i, op) in history.ops.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let (kind, value) = porcupine_call(op);
        let ret = op.ret.unwrap_or(op.invoke);
        let _ = write!(
            out,
            "{{\"client\":{},\"invoke\":{{\"op\":{}}},\"value\":{},\"invoke_time\":{},\"ret_time\":{}}}",
            op.client, kind, value, op.invoke, ret,
        );
    }
    out.push(']');
    out
}

fn porcupine_call(op: &crate::history::HistoryOp) -> (String, String) {
    match &op.op {
        ClientOp::Put { value } => (
            format!("{{\"kind\":\"put\",\"key\":\"{}\"}}", hex(&op.key)),
            format!("{{\"value\":\"{}\"}}", hex(value)),
        ),
        ClientOp::Get => (
            format!("{{\"kind\":\"get\",\"key\":\"{}\"}}", hex(&op.key)),
            "\"ok\"".to_string(),
        ),
        ClientOp::Delete => (
            format!("{{\"kind\":\"delete\",\"key\":\"{}\"}}", hex(&op.key)),
            "\"ok\"".to_string(),
        ),
        ClientOp::Cas {
            expected_version,
            value,
        } => (
            format!(
                "{{\"kind\":\"cas\",\"key\":\"{}\",\"expected\":{expected_version}}}",
                hex(&op.key),
            ),
            format!("{{\"value\":\"{}\"}}", hex(value)),
        ),
        ClientOp::Append { value } => (
            format!("{{\"kind\":\"append\",\"key\":\"{}\"}}", hex(&op.key)),
            format!("{{\"value\":\"{}\"}}", hex(value)),
        ),
        ClientOp::Range { limit } => (
            format!("{{\"kind\":\"range\",\"limit\":{limit}}}"),
            "\"ok\"".to_string(),
        ),
    }
}

fn hex(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        let _ = write!(out, "{byte:02x}");
    }
    out
}
