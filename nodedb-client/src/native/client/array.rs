// SPDX-License-Identifier: Apache-2.0

//! Array bulk-load implementation for `NativeClient`: `ArrayPutBatch`
//! requests over one connection.

use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::protocol::{ArrayCellWrite, ArrayPutBatch, MAX_FRAME_SIZE, OpCode, TextFields};

use super::core::NativeClient;
use crate::native::connection::check_error;

/// The payload size one request targets: three quarters of the frame cap,
/// leaving room for the request envelope.
const TARGET_PAYLOAD_BYTES: usize = (MAX_FRAME_SIZE as usize) * 3 / 4;

/// How many cells the first request encodes to measure the cell size.
const PROBE_CELLS: usize = 1024;

impl NativeClient {
    /// Send `cells` as `ArrayPutBatch` requests on one connection, each
    /// under one frame. Outside a transaction block each chunk of a request
    /// commits on its own. A bad cell stops the load: the requests and
    /// chunks before it stay committed, and the error names the cell's row
    /// in its request and that request's committed row count. Inside a
    /// block a refusal aborts the block. Returns the cells loaded.
    pub(super) async fn array_put_cells_impl(
        &self,
        array: &str,
        cells: Vec<ArrayCellWrite>,
    ) -> NodeDbResult<u64> {
        if cells.is_empty() {
            return Ok(0);
        }
        let mut conn = self.pool.acquire().await?;
        let per_request = cells_per_request(&cells)?;
        let mut loaded = 0u64;
        let mut pending: Vec<&[ArrayCellWrite]> = cells.chunks(per_request).rev().collect();
        while let Some(slice) = pending.pop() {
            let payload =
                zerompk::to_msgpack_vec(&ArrayPutBatch::from_cells(slice)?).map_err(|e| {
                    NodeDbError::serialization("msgpack", format!("ArrayPutBatch: {e}"))
                })?;
            // Cells vary in size. A slice over the target splits in two, and
            // the halves go first, in order.
            if payload.len() > TARGET_PAYLOAD_BYTES && slice.len() > 1 {
                let (head, tail) = slice.split_at(slice.len() / 2);
                pending.push(tail);
                pending.push(head);
                continue;
            }
            let request = TextFields {
                collection: Some(array.to_string()),
                payload: Some(payload),
                ..Default::default()
            };
            let resp = conn.send(OpCode::ArrayPutBatch, request).await?;
            check_error(&resp)?;
            loaded += resp.rows_affected.ok_or_else(|| {
                NodeDbError::internal("ArrayPutBatch: the server answered with no row count")
            })?;
        }
        Ok(loaded)
    }
}

/// How many cells one request carries, from the encoded size of a probe.
fn cells_per_request(cells: &[ArrayCellWrite]) -> NodeDbResult<usize> {
    let probe = &cells[..cells.len().min(PROBE_CELLS)];
    let bytes = zerompk::to_msgpack_vec(&ArrayPutBatch::from_cells(probe)?)
        .map_err(|e| NodeDbError::serialization("msgpack", format!("ArrayPutBatch: {e}")))?
        .len();
    let per_cell = bytes.div_ceil(probe.len()).max(1);
    Ok((TARGET_PAYLOAD_BYTES / per_cell).max(1))
}
