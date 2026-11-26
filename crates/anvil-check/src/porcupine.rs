#![forbid(unsafe_code)]
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

//! Porcupine-compatible JSON export for cross-checking against the Go
//! checker.

use crate::history::{ClientOp, History};
use std::fmt::Write as _;

/// Exports a history in Porcupine JSON format.
///
/// Each operation carries its call, result, and virtual-time interval, so the
/// Go checker judges the same history the Rust checker judges. Indeterminate
/// operations (timed-out with no result) export with a null result; the Go
/// harness skips them when building Porcupine operations because Porcupine
/// only judges completed operations.
#[must_use]
pub fn export_porcupine(history: &History) -> String {
    let mut out = String::from("[");
    for (i, op) in history.ops.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let (kind, value) = porcupine_call(op);
        let result = porcupine_result(op);
        let ret = op.ret.unwrap_or(op.invoke);
        let _ = write!(
            out,
            "{{\"client\":{},\"invoke\":{{\"op\":{}}},\"value\":{},\"result\":{},\"invoke_time\":{},\"ret_time\":{}}}",
            op.client, kind, value, result, op.invoke, ret,
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

fn hex_opt(data: Option<&bytes::Bytes>) -> String {
    data.map_or("null".to_string(), |b| format!("\"{}\"", hex(b)))
}

fn porcupine_result(op: &crate::history::HistoryOp) -> String {
    use crate::history::OpResult;
    match (&op.op, &op.result) {
        (_, None) => "null".to_string(),
        (_, Some(OpResult::Value { value, version })) => format!(
            "{{\"value\":{},\"version\":{version}}}",
            hex_opt(value.as_ref())
        ),
        (_, Some(OpResult::CasConflict { version })) => {
            format!("{{\"conflict\":true,\"version\":{version}}}")
        }
        (_, Some(OpResult::List { .. })) => "\"list\"".to_string(),
        (_, Some(OpResult::Range { .. })) => "\"range\"".to_string(),
        (_, Some(OpResult::Ok)) => "\"ok\"".to_string(),
        (_, Some(OpResult::SessionExpired)) => "\"expired\"".to_string(),
    }
}
