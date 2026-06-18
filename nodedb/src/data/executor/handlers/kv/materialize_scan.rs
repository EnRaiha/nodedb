// SPDX-License-Identifier: BUSL-1.1

//! Cursor-paginated raw KV scan used by the clone materializer.
//!
//! Returns the engine's `(key, value)` pairs verbatim (no map wrapping or
//! key-injection — the materializer needs the raw stored value bytes to
//! re-`Put` them on target) plus the next-cursor in a single response so the
//! caller can drive the scan to completion in O(N / count) round-trips.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::kv::KvScanParams;
use crate::engine::kv::current_ms;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_kv_materialize_scan(
        &self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        collection: &str,
        cursor: &[u8],
        count: usize,
    ) -> Response {
        // Quiesce gate: same contract as the standard scan so the purge
        // handler can drain readers safely.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        let now_ms = current_ms();
        let (entries, next_cursor) = self.kv_engine.scan(KvScanParams {
            database_id: did,
            tenant_id: tid,
            collection,
            cursor,
            count,
            now_ms,
            match_pattern: None,
            filter_field: None,
            filter_value: None,
            surrogate_ceiling: None,
        });

        // Encode response payload as msgpack:
        //   [next_cursor: bytes, entries: [[key, value], ...]]
        let mut payload = Vec::with_capacity(
            entries
                .iter()
                .map(|(k, v)| k.len() + v.len() + 6)
                .sum::<usize>()
                + next_cursor.len()
                + 16,
        );
        nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
        write_bin(&mut payload, &next_cursor);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, entries.len());
        for (k, v) in &entries {
            nodedb_query::msgpack_scan::write_array_header(&mut payload, 2);
            write_bin(&mut payload, k);
            write_bin(&mut payload, v);
        }

        if let Some(ref m) = self.metrics {
            m.record_kv_scan();
        }
        self.response_with_payload(task, payload)
    }
}

/// Append a msgpack `bin` value (raw byte string) to `out`.
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
