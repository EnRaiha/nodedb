// SPDX-License-Identifier: BUSL-1.1

use pgwire::api::results::Response;
use pgwire::error::PgWireResult;

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

pub(super) async fn dispatch(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
    upper: &str,
    parts: &[&str],
) -> Option<PgWireResult<Vec<Response>>> {
    // Stored procedures.
    if upper.starts_with("CREATE OR REPLACE PROCEDURE ") || upper.starts_with("CREATE PROCEDURE ") {
        return Some(super::super::procedure::create_procedure(
            state, identity, sql,
        ));
    }
    if upper.starts_with("DROP PROCEDURE ") {
        return Some(super::super::procedure::drop_procedure(
            state, identity, parts,
        ));
    }
    if upper == "SHOW PROCEDURES" || upper.starts_with("SHOW PROCEDURES") {
        return Some(super::super::procedure::show_procedures(state, identity));
    }
    if upper.starts_with("CALL ") {
        return Some(super::super::procedure::call_procedure(state, identity, sql).await);
    }

    // Query functions.
    if upper.contains("VERIFY_AUDIT_CHAIN") {
        return Some(super::super::query_functions::verify_audit_chain(state, identity, sql).await);
    }
    if upper.contains("VERIFY_HASH_CHAIN") {
        return Some(super::super::query_functions::verify_hash_chain(state, identity, sql).await);
    }
    if upper.contains("BALANCE_AS_OF") {
        return Some(super::super::query_functions::balance_as_of(state, identity, sql).await);
    }
    if upper.contains("TEMPORAL_LOOKUP") {
        return Some(super::super::query_functions::temporal_lookup(state, identity, sql).await);
    }
    if upper.contains("VERIFY_BALANCE") {
        return Some(super::super::query_functions::verify_balance(state, identity, sql).await);
    }
    if upper.contains("CONVERT_CURRENCY_LOOKUP") {
        return Some(
            super::super::query_functions::convert_currency_lookup(state, identity, sql).await,
        );
    }

    None
}
