// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral DDL dispatch.
//!
//! Presents a 4-arg entrypoint that yields a protocol-neutral [`DdlResult`] /
//! [`DdlError`] instead of pgwire `Response` types, so native, http, and pgwire
//! entrypoints share one router with no pgwire dependency in the routing layer.

use crate::control::security::identity::AuthenticatedIdentity;
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
    super::neutral::try_dispatch(state, identity, sql, database_id).await
}
