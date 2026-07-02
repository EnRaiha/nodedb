// SPDX-License-Identifier: BUSL-1.1

//! `CREATE FULLTEXT INDEX` DSL handler.

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CREATE FULLTEXT INDEX <name> ON <collection> (<field>)
pub fn create_fulltext_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 7 {
        return Err(ddl_err(
            "42601",
            "syntax: CREATE FULLTEXT INDEX <name> ON <collection> (<field>)",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(ddl_err("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let field = parts[6].trim_matches(|c| c == '(' || c == ')');
    let tenant_id = identity.tenant_id;

    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        "fulltext_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!("created fulltext index '{index_name}' on '{collection}' ({field})"),
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE FULLTEXT INDEX".to_string(),
        rows_affected: None,
    }])
}
