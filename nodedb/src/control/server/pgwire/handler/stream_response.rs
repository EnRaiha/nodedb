// SPDX-License-Identifier: BUSL-1.1

//! Lazy streaming response shaping for the single-node pgwire SELECT path.
//!
//! Turns a [`ResultStream`] of row batches into a pgwire `QueryResponse` whose
//! `DataRow`s are pulled by the framework AFTER `do_query` returns, so a large
//! scan flows to the client incrementally instead of being materialized and
//! merged on the coordinator first.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, QueryResponse, Response};
use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::server::result_stream::ResultStream;
use crate::data::executor::response_codec::decode_payload_to_json;

use super::super::types::{error_to_sqlstate, text_field};

/// Build a streaming multi-row pgwire `Response` whose `DataRow`s are pulled
/// lazily from a [`ResultStream`].
///
/// Schema is a single TEXT column `result`, matching the non-streaming
/// `MultiRow` shape in `plan::payload_to_response`: each row's JSON object is
/// rendered as one text field.
///
/// A global take-N is enforced when `limit < usize::MAX`: the stream stops
/// emitting after `limit` rows across the whole union. A mid-stream
/// `crate::Error` (over-budget, dispatch failure) is mapped to a pgwire
/// `ErrorResponse` via `error_to_sqlstate`, so the client sees a proper error
/// instead of a silently-truncated result.
pub(crate) fn streaming_multirow_response(stream: ResultStream, limit: usize) -> Response {
    use futures::StreamExt;

    let schema = Arc::new(vec![text_field("result")]);
    let row_schema = schema.clone();

    let row_stream = async_stream::try_stream! {
        let mut emitted: usize = 0;
        let mut batches = stream;
        while let Some(batch) = batches.next().await {
            let batch = batch.map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            if emitted >= limit {
                break;
            }

            // Each batch payload is a standalone msgpack array of rows; decode
            // to a JSON array and stream each element as its own pgwire row.
            let text = decode_payload_to_json(&batch.payload);
            if let Ok(serde_json::Value::Array(items)) =
                sonic_rs::from_str::<serde_json::Value>(&text)
            {
                for item in items {
                    if emitted >= limit {
                        break;
                    }
                    let mut encoder = DataRowEncoder::new(row_schema.clone());
                    encoder.encode_field(&item.to_string()).map_err(|e| {
                        PgWireError::UserError(Box::new(ErrorInfo::new(
                            "ERROR".to_owned(),
                            "XX000".to_owned(),
                            format!("failed to encode streamed row: {e}"),
                        )))
                    })?;
                    emitted += 1;
                    yield encoder.take_row();
                }
            }
        }
    };

    Response::Query(QueryResponse::new(schema, row_stream))
}
