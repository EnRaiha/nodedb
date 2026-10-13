// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral routing for the temporal / audit query functions.
//!
//! These are `SELECT <FUNC>(...)` statements that never parse into a typed
//! DDL AST statement. `nodedb_sql` recognizes them from the parsed SQL
//! statement, so a function name inside a string literal, a quoted
//! identifier, or a comment never routes here. The parent neutral router
//! calls this only from its `None` (non-DDL-parse) branch.

use nodedb_sql::parser::query_function::{QueryFunctionCall, parse_query_function};

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Try to handle `sql` as one of the temporal / audit query functions.
///
/// Returns `Some(result)` when `sql` is a query-function statement, or names
/// a query function in a form it does not accept. Returns `None` otherwise,
/// so the caller falls through to the SQL planner.
pub async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let call = match parse_query_function(sql)? {
        Ok(call) => call,
        Err(e) => return Some(Err(DdlError::from_sql_error(e, identity.tenant_id))),
    };
    Some(match &call {
        QueryFunctionCall::VerifyAuditChain(args) => {
            super::verify_audit_chain(state, identity, database_id, args).await
        }
        QueryFunctionCall::VerifyHashChain(args) => {
            super::verify_hash_chain(state, identity, database_id, args).await
        }
        QueryFunctionCall::BalanceAsOf(args) => {
            super::balance_as_of(state, identity, database_id, args).await
        }
        QueryFunctionCall::TemporalLookup(args) => {
            super::temporal_lookup(state, identity, database_id, args).await
        }
        QueryFunctionCall::VerifyBalance(args) => {
            super::verify_balance(state, identity, database_id, args).await
        }
        QueryFunctionCall::ConvertCurrencyLookup(args) => {
            super::convert_currency_lookup(state, identity, database_id, args).await
        }
    })
}
