// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SPARSE INDEX` DSL handler.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CREATE SPARSE INDEX [name] ON <collection> (<field>)
pub fn create_sparse_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 6 {
        return Err(ddl_err(
            "42601",
            "syntax: CREATE SPARSE INDEX [name] ON <collection> (<field>)",
        ));
    }

    let (index_name, on_idx) = if parts[3].eq_ignore_ascii_case("ON") {
        ("_auto_sparse".to_string(), 3)
    } else {
        if parts.len() < 7 || !parts[4].eq_ignore_ascii_case("ON") {
            return Err(ddl_err("42601", "expected ON after index name"));
        }
        (parts[3].to_string(), 4)
    };

    let collection = parts
        .get(on_idx + 1)
        .ok_or_else(|| ddl_err("42601", "expected collection name after ON"))?;

    let field = parts
        .get(on_idx + 2)
        .map(|s| s.trim_matches(|c| c == '(' || c == ')'))
        .unwrap_or("_sparse");

    let tenant_id = identity.tenant_id;

    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        "sparse_index",
        tenant_id,
        &index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created sparse index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE SPARSE INDEX".to_string(),
        rows_affected: None,
    }])
}
