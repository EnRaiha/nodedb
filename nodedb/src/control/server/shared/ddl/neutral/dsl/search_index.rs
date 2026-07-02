// SPDX-License-Identifier: BUSL-1.1

//! `CREATE SEARCH INDEX` DSL handler (higher-level alias for fulltext).

use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CREATE SEARCH INDEX ON <collection> FIELDS <field1>[, <field2>...] [ANALYZER '<name>'] [FUZZY true|false]
pub fn create_search_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    sql: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let upper = sql.to_uppercase();

    let on_pos = upper.find(" ON ").ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;
    let after_on = sql[on_pos + 4..].trim_start();
    let fields_pos = upper.find(" FIELDS ").ok_or_else(|| {
        ddl_err(
            "42601",
            "syntax: CREATE SEARCH INDEX ON <collection> FIELDS <field> [ANALYZER 'name'] [FUZZY true]",
        )
    })?;

    let collection = after_on[..fields_pos - on_pos - 4].trim().to_lowercase();
    if collection.is_empty() {
        return Err(ddl_err("42601", "missing collection name"));
    }

    let after_fields = &sql[fields_pos + 8..];
    let fields_end = upper[fields_pos + 8..]
        .find(" ANALYZER ")
        .or_else(|| upper[fields_pos + 8..].find(" FUZZY "))
        .unwrap_or(after_fields.len());
    let fields_str = after_fields[..fields_end].trim();
    let fields: Vec<&str> = fields_str.split(',').map(|s| s.trim()).collect();

    if fields.is_empty() || fields[0].is_empty() {
        return Err(ddl_err("42601", "missing field list"));
    }

    let tenant_id = identity.tenant_id;

    for field in &fields {
        let index_name = format!("fts_{}_{}", collection, field);

        crate::control::server::shared::ddl::owner::propose_owner(
            state,
            "fulltext_index",
            tenant_id,
            &index_name,
            &identity.username,
        )?;

        state.audit_record(
            crate::control::security::audit::AuditEvent::AdminAction,
            Some(tenant_id),
            &identity.username,
            &format!("created search index '{index_name}' on '{collection}' ({field})"),
        );
    }

    Ok(vec![DdlResult::Status {
        command: "CREATE SEARCH INDEX".to_string(),
        rows_affected: None,
    }])
}
