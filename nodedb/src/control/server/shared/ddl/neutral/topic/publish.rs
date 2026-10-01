// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral `PUBLISH TO` handler — thin wrapper over the unified SQL
//! dispatcher.
//!
//! Parsing, escape handling, and cluster-aware forwarding are delegated to the
//! protocol-agnostic `sql_dispatch::dispatch_sql`. The success tag, the
//! per-variant SQLSTATE mapping, and the unrecognized-syntax error use the
//! protocol-neutral [`DdlResult`] / [`DdlError`].
//!
//! Syntax: `PUBLISH TO <topic> '<payload>'`
//!
//! Inside a transaction block the message is checked at the statement and
//! held for COMMIT: it commits in the transaction's redo record and the Event
//! Plane delivers it from there. ROLLBACK drops it.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::session::{DmlTxnCtx, TransactionState};
use crate::control::sql_dispatch::{dispatch_sql_in_database, prepare_publish};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};
use super::super::auth_support::status;

/// The owner a client's own `PUBLISH TO` names in its retry and DLQ records.
const CLIENT_PUBLISH_OWNER: &str = "client";

/// Handle `PUBLISH TO <topic> '<payload>'`.
///
/// Delegates parsing, escape handling, and cluster-aware forwarding to the
/// protocol-agnostic `sql_dispatch::dispatch_sql`.
pub async fn handle_publish(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    if txn_ctx.sessions.transaction_state(txn_ctx.session_id) == TransactionState::InBlock {
        let publish =
            prepare_publish(state, identity, database_id, sql).map_err(|e| publish_error(&e))?;
        txn_ctx.sessions.buffer_publishes(
            txn_ctx.session_id,
            vec![crate::wal::RedoPublish {
                owner: CLIENT_PUBLISH_OWNER.to_owned(),
                database_id: publish.database_id,
                tenant_id: publish.tenant_id,
                topic: publish.topic,
                payload: publish.payload,
                metadata_floor: publish.metadata_floor,
                position: None,
            }],
        );
        return Ok(status("PUBLISH"));
    }
    match dispatch_sql_in_database(state, identity, database_id, sql).await {
        Ok(Some(_)) => Ok(status("PUBLISH")),
        Err(e) => Err(publish_error(&e)),
        Ok(None) => Err(DdlError::new(
            "42601",
            "expected PUBLISH TO <topic> '<payload>'",
        )),
    }
}

/// The client error for a refused `PUBLISH TO`.
fn publish_error(e: &crate::Error) -> DdlError {
    match e {
        // The named topic does not exist.
        crate::Error::CollectionNotFound { .. } => DdlError::new("42704", e.to_string()),
        crate::Error::BadRequest { .. } => DdlError::new("42601", e.to_string()),
        crate::Error::Dispatch { .. } => DdlError::new("58000", e.to_string()),
        // Any other error keeps the class the SQLSTATE table gives it.
        other => DdlError::from_error(other),
    }
}
