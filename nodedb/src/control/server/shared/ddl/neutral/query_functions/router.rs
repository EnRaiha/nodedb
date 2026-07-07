// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral routing for the temporal / audit query functions.
//!
//! These are `SELECT <FUNC>(...)` calls that never parse into a typed DDL AST
//! statement — the pgwire router recognized them by substring (`upper.contains`)
//! in its `router::function::dispatch`, after the typed-AST parse gate and the
//! auth family. Replicate that exactly: this router is invoked only from the
//! `None` (non-DDL-parse) branch of the parent neutral router, so any typed DDL
//! statement (or parse error) whose body happens to contain one of these
//! substrings is handled by the typed path first, byte-identically to before.
//! The substring recognition order is preserved verbatim.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::super::super::result::{DdlError, DdlResult};

/// Try to handle `sql` as one of the temporal / audit query functions.
///
/// Returns `Some(result)` when a substring matches (mirroring the pgwire
/// `router::function::dispatch` contains-checks in the same order), `None`
/// otherwise so the caller falls back to the transitional pgwire delegation.
pub async fn try_dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    sql: &str,
) -> Option<Result<Vec<DdlResult>, DdlError>> {
    let upper = sql.to_uppercase();

    if upper.contains("VERIFY_AUDIT_CHAIN") {
        return Some(super::verify_audit_chain(state, identity, database_id, sql).await);
    }
    if upper.contains("VERIFY_HASH_CHAIN") {
        return Some(super::verify_hash_chain(state, identity, database_id, sql).await);
    }
    if upper.contains("BALANCE_AS_OF") {
        return Some(super::balance_as_of(state, identity, database_id, sql).await);
    }
    if upper.contains("TEMPORAL_LOOKUP") {
        return Some(super::temporal_lookup(state, identity, database_id, sql).await);
    }
    if upper.contains("VERIFY_BALANCE") {
        return Some(super::verify_balance(state, identity, database_id, sql).await);
    }
    if upper.contains("CONVERT_CURRENCY_LOOKUP") {
        return Some(super::convert_currency_lookup(state, identity, database_id, sql).await);
    }

    None
}
