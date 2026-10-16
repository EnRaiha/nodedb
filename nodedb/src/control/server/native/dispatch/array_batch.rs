// SPDX-License-Identifier: BUSL-1.1

//! Native transport for `ArrayPutBatch`: decode the columnar cells and hand
//! them to the shared array bulk-load core, which checks, chunks and
//! dispatches them as `COPY <array> FROM STDIN` does. The frame cap bounds
//! one request.
//!
//! Outside a transaction block each chunk of the request commits on its
//! own, and a bad cell stops the request with the committed row count.
//! Inside a block the request stages whole or aborts the block.

use nodedb_types::protocol::{ArrayPutBatch, NativeResponse, TextFields};

use crate::control::server::native::sqlstate_code::sqlstate_error;
use crate::control::server::shared::ddl::DdlResult;
use crate::control::server::shared::ddl::neutral::collection::copy_from::{
    ArrayLoadCtx, ArrayLoader, LoadInput,
};
use crate::control::server::shared::session::DmlTxnCtx;

use super::{DispatchCtx, ddl_result_to_native};

/// Load the request's cells into the array `collection` names. Answers the
/// row count as a `COPY` status.
pub(crate) async fn handle_array_put_batch(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let Some(array) = fields.collection.as_deref() else {
        return sqlstate_error(seq, "42601", "ArrayPutBatch: missing 'collection' field");
    };
    let Some(payload) = fields.payload.as_deref() else {
        return sqlstate_error(seq, "42601", "ArrayPutBatch: missing 'payload' field");
    };
    let batch: ArrayPutBatch = match zerompk::from_msgpack(payload) {
        Ok(batch) => batch,
        Err(e) => {
            return sqlstate_error(
                seq,
                "22P02",
                format!("ArrayPutBatch: payload is not an ArrayPutBatch: {e}"),
            );
        }
    };
    let txn_ctx = DmlTxnCtx {
        sessions: ctx.sessions,
        session_id: ctx.peer_addr.into(),
    };
    let load = ArrayLoadCtx {
        state: ctx.state,
        identity: ctx.identity,
        database_id: ctx.database_id(),
        txn_ctx: &txn_ctx,
    };
    let _request = ctx.state.tenant_request_guard(ctx.tenant_id());
    let result = async {
        let mut loader = ArrayLoader::open(&load, array, LoadInput::Cells)?;
        loader.feed_cells(&load, &batch.dims, &batch.attrs).await?;
        loader.finish(&load).await
    }
    .await
    .map(|rows| {
        vec![DdlResult::Status {
            command: "COPY".to_string(),
            rows_affected: Some(rows),
        }]
    });
    ddl_result_to_native(seq, result)
}
