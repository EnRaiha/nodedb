// SPDX-License-Identifier: BUSL-1.1

//! Shared response encoder for array dispatch handlers.
//!
//! Emits a top-level msgpack array where each element is a `Value` written
//! via `value_to_msgpack` — the native (untagged) writer. This is the
//! shape the pgwire `msgpack_to_json_string` transcoder expects: a named
//! slice row is a plain map, and a `Value::ArrayCell` is a clean
//! `{"coords": [...], "attrs": [...]}` map.
//!
//! Using the tagged zerompk codec instead leaks the internal
//! `[18, <bin>]` shape to clients, which is unreadable.

use nodedb_types::Value;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::ArraySliceResponse;
use crate::data::executor::task::ExecutionTask;

pub(super) fn encode_value_rows(core: &CoreLoop, task: &ExecutionTask, rows: &[Value]) -> Response {
    match rows_msgpack(rows) {
        Ok(buf) => core.response_with_payload(task, buf),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("array response encode: {e}"),
            },
        ),
    }
}

/// Encode already-encoded slice rows into an `ArraySliceResponse` envelope
/// carrying the below-horizon flag.
pub(super) fn encode_slice_rows(
    core: &CoreLoop,
    task: &ExecutionTask,
    rows: Vec<Vec<u8>>,
    truncated_before_horizon: bool,
) -> Response {
    let resp = ArraySliceResponse {
        rows_msgpack: concat_rows(&rows),
        truncated_before_horizon,
    };
    match zerompk::to_msgpack_vec(&resp) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: format!("array slice response encode: {e}"),
            },
        ),
    }
}

/// One msgpack array holding each row's native encoding.
fn rows_msgpack(rows: &[Value]) -> zerompk::Result<Vec<u8>> {
    let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 64);
    write_array_header(&mut buf, rows.len());
    for row in rows {
        buf.extend_from_slice(&nodedb_types::value_to_msgpack(row)?);
    }
    Ok(buf)
}

/// One msgpack array holding the already-encoded `rows`.
fn concat_rows(rows: &[Vec<u8>]) -> Vec<u8> {
    let mut buf: Vec<u8> = Vec::with_capacity(5 + rows.iter().map(Vec::len).sum::<usize>());
    write_array_header(&mut buf, rows.len());
    for row in rows {
        buf.extend_from_slice(row);
    }
    buf
}

fn write_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}
