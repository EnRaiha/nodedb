// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch.
//!
//! Presents the same 4-arg signature as the underlying router but yields a
//! protocol-neutral [`DdlResult`] / [`DdlError`] instead of pgwire `Response`
//! types, so native and http entrypoints do not depend on pgwire.

use futures::StreamExt;
use serde_json::{Map, Value as JsonValue};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::ShapedRows;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::result::{DdlError, DdlResult};

/// Try to handle a SQL statement as a Control Plane DDL command, returning a
/// protocol-neutral result.
///
/// Returns `None` when the statement is not a recognized DDL command (the
/// caller falls through to the SQL planner).
pub async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    database_id: DatabaseId,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    // transitional: the neutral module delegates to the pgwire router until the
    // leaf DDL handlers are ported to produce `DdlResult` directly.
    let pg_result =
        crate::control::server::pgwire::ddl::dispatch(state, identity, sql, database_id).await?;

    match pg_result {
        Ok(responses) => Some(responses_to_ddl_results(responses).await),
        Err(err) => Some(Err(pgwire_error_to_ddl_error(&err))),
    }
}

/// Translate a vec of pgwire `Response`s into protocol-neutral [`DdlResult`]s.
///
/// A row-stream error aborts the whole translation with a `DdlError`, mirroring
/// the previous native bridge behavior.
async fn responses_to_ddl_results(
    responses: Vec<pgwire::api::results::Response>,
) -> Result<Vec<DdlResult>, DdlError> {
    use pgwire::api::results::Response as PgResponse;

    let mut results = Vec::with_capacity(responses.len());
    for resp in responses {
        match resp {
            PgResponse::Execution(tag) => {
                // Tag has no Display impl; extract command from Debug.
                let debug = format!("{tag:?}");
                let command = debug
                    .strip_prefix("Tag { command: \"")
                    .and_then(|s| s.split('"').next())
                    .unwrap_or("OK")
                    .to_string();
                results.push(DdlResult::Status {
                    command,
                    rows_affected: None,
                });
            }
            PgResponse::Query(mut query_resp) => {
                let schema = query_resp.row_schema();
                let columns: Vec<String> = schema.iter().map(|f| f.name().to_string()).collect();
                let ncols = columns.len();

                let mut rows: Vec<Map<String, JsonValue>> = Vec::new();
                let row_stream = query_resp.data_rows();
                while let Some(row_result) = row_stream.next().await {
                    match row_result {
                        Ok(data_row) => {
                            rows.push(parse_data_row_fields(&data_row.data, &columns, ncols));
                        }
                        Err(e) => {
                            return Err(DdlError {
                                sqlstate: "XX000".to_string(),
                                message: format!("row stream error: {e}"),
                            });
                        }
                    }
                }

                results.push(DdlResult::Rows(ShapedRows {
                    columns,
                    rows,
                    notice: None,
                }));
            }
            PgResponse::EmptyQuery => {
                results.push(DdlResult::Empty);
            }
            _ => {}
        }
    }
    Ok(results)
}

/// Extract a protocol-neutral SQLSTATE + message from a `PgWireError`.
///
/// `UserError` carries an explicit SQLSTATE + message; every other variant
/// falls back to `XX000` + the error's `Display` text (mirroring the previous
/// native bridge's error arm).
fn pgwire_error_to_ddl_error(err: &pgwire::error::PgWireError) -> DdlError {
    match err {
        pgwire::error::PgWireError::UserError(info) => DdlError {
            sqlstate: info.code.clone(),
            message: info.message.clone(),
        },
        other => DdlError {
            sqlstate: "XX000".to_string(),
            message: other.to_string(),
        },
    }
}

/// Parse raw pgwire DataRow field bytes into a column-keyed JSON row.
///
/// Each field is encoded as: `i32 len` (-1 = NULL), then `len` bytes of
/// text-encoded data. This matches pgwire text format encoding. Each field is
/// emitted as `JsonValue::String(text)`, or `JsonValue::Null` for the -1 NULL
/// length prefix, keyed by the corresponding column name.
fn parse_data_row_fields(
    data: &[u8],
    columns: &[String],
    expected_fields: usize,
) -> Map<String, JsonValue> {
    let mut map = Map::new();
    let mut offset = 0;

    for column in columns.iter().take(expected_fields) {
        if offset + 4 > data.len() {
            map.insert(column.clone(), JsonValue::Null);
            continue;
        }
        let len = i32::from_be_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);
        offset += 4;

        if len < 0 {
            map.insert(column.clone(), JsonValue::Null);
        } else {
            let len = len as usize;
            if offset + len > data.len() {
                map.insert(column.clone(), JsonValue::Null);
                break;
            }
            let text = String::from_utf8_lossy(&data[offset..offset + len]).into_owned();
            map.insert(column.clone(), JsonValue::String(text));
            offset += len;
        }
    }

    map
}
