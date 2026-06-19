// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated raw document scan for the clone materializer.
//!
//! Returns raw `(doc_id_hex, surrogate_u32, value_bytes)` triples plus the
//! next-cursor in a single response so the Control Plane materializer can
//! drive the scan to completion in O(N / count) round-trips.
//!
//! The `doc_id` returned here is the **hex-encoded surrogate** (the redb storage
//! key, e.g. `"0000002a"`).  The Control Plane materializer recovers the
//! user-visible PK via `catalog.get_pk_for_surrogate`.
//!
//! ## Response payload (msgpack)
//! ```text
//! [ next_cursor: bin,
//!   entries: [ [doc_id: str, surrogate: u32, value_bytes: bin], ... ] ]
//! ```
//! `next_cursor` is empty when the scan is complete.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::doc_id_to_surrogate;
use crate::engine::sparse::btree::DOCUMENTS;

impl CoreLoop {
    /// Execute a cursor-paginated raw document scan for the clone materializer.
    pub(in crate::data::executor) fn execute_document_materialize_scan(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        cursor: &[u8],
        count: usize,
        _system_as_of_ms: Option<i64>,
    ) -> Response {
        // Quiesce gate: same contract as the standard scan.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let prefix = crate::engine::sparse::btree::coll_prefix(
            task.request.database_id.as_u64(),
            tid,
            collection,
        );
        let prefix_end = format!("{prefix}\u{ffff}");

        // Cursor is the last doc_id_hex seen; resume AFTER it.
        let range_start = if cursor.is_empty() {
            prefix.clone()
        } else {
            // cursor bytes are the UTF-8 doc_id_hex string; advance by one
            // character to make the scan exclusive.
            let cursor_str = String::from_utf8_lossy(cursor);
            format!("{prefix}{cursor_str}\x00")
        };

        let read_txn = match self.sparse.db().begin_read() {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan begin_read: {e}"),
                    },
                );
            }
        };

        let table = match read_txn.open_table(DOCUMENTS) {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan open_table: {e}"),
                    },
                );
            }
        };

        let range = match table.range(range_start.as_str()..prefix_end.as_str()) {
            Ok(r) => r,
            Err(e) => {
                return self.response_error(
                    task,
                    crate::bridge::envelope::ErrorCode::Internal {
                        detail: format!("materialize_scan range: {e}"),
                    },
                );
            }
        };

        let mut entries: Vec<(String, u32, Vec<u8>)> = Vec::with_capacity(count.min(256));
        let mut last_doc_id = String::new();

        for row in range {
            if entries.len() >= count {
                break;
            }
            let row = match row {
                Ok(r) => r,
                Err(e) => {
                    return self.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("materialize_scan row: {e}"),
                        },
                    );
                }
            };
            let full_key = row.0.value().to_string();
            let doc_id = full_key
                .strip_prefix(&prefix)
                .unwrap_or(&full_key)
                .to_string();
            let value = row.1.value().to_vec();

            let surrogate = match doc_id_to_surrogate(&doc_id) {
                Some(s) => s.as_u32(),
                None => {
                    // Skip non-surrogate keys (legacy or corrupted rows).
                    continue;
                }
            };

            last_doc_id.clone_from(&doc_id);
            entries.push((doc_id, surrogate, value));
        }

        // Next-cursor is the last doc_id_hex seen; empty = scan complete.
        let next_cursor: Vec<u8> = if entries.len() < count {
            Vec::new()
        } else {
            last_doc_id.into_bytes()
        };

        // Encode response: [next_cursor: bin, entries: [[str, u32, bin], ...]]
        let mut payload = Vec::with_capacity(
            entries
                .iter()
                .map(|(d, _, v)| d.len() + 4 + v.len() + 12)
                .sum::<usize>()
                + next_cursor.len()
                + 16,
        );
        nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
        write_bin(&mut payload, &next_cursor);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, entries.len());
        for (doc_id, surrogate, value) in &entries {
            nodedb_query::msgpack_scan::write_array_header(&mut payload, 3);
            write_str(&mut payload, doc_id.as_bytes());
            write_u32(&mut payload, *surrogate);
            write_bin(&mut payload, value);
        }

        self.response_with_payload(task, payload)
    }
}

/// Append a msgpack `bin` value to `out`.
fn write_bin(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= u8::MAX as usize {
        out.push(0xc4);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xc5);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Append a msgpack `str` value to `out`.
fn write_str(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= 31 {
        out.push(0xa0 | len as u8);
    } else if len <= u8::MAX as usize {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

/// Append a msgpack `u32` value to `out`.
fn write_u32(out: &mut Vec<u8>, v: u32) {
    out.push(0xce);
    out.extend_from_slice(&v.to_be_bytes());
}
