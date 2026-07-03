// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION accounts ADD COLUMN balance DECIMAL DEFAULT 0 AS MATERIALIZED_SUM ...`
//! — ADD COLUMN variant that binds a computed balance to another collection's
//! per-row contribution. Atomically maintained on INSERT into the source side.
//!
//! Ported verbatim from the pgwire `ddl::collection::alter::materialized_sum`
//! handler; only the result type changed to the protocol-neutral
//! [`DdlResult`] / [`DdlError`]. The value-expression validation, duplicate
//! binding guard, `materialized_sums` push, `PutCollection` propose,
//! `schema_version` bump, and audit are unchanged, as is the `ALTER
//! COLLECTION` command tag.

use nodedb_types::DatabaseId;

use crate::bridge::expr_eval::SqlExpr;
use crate::control::security::audit::AuditEvent;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::{err, status};

pub(super) fn add_materialized_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    target_collection: &str,
    target_column: &str,
    source_collection: &str,
    join_column: &str,
    value_expr: &str,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();

    let expr = parse_value_expression(value_expr)?;

    let def = crate::control::security::catalog::types::MaterializedSumDef {
        target_collection: target_collection.to_string(),
        target_column: target_column.to_string(),
        source_collection: source_collection.to_string(),
        join_column: join_column.to_string(),
        value_expr: expr,
    };

    let Some(catalog) = state.credentials.catalog() else {
        return Err(err("XX000", "no catalog available"));
    };

    let mut coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, target_collection)
        .map_err(|e| err("XX000", e.to_string()))?
        .ok_or_else(|| {
            err(
                "42P01",
                format!("collection '{target_collection}' not found"),
            )
        })?;

    if coll
        .materialized_sums
        .iter()
        .any(|m| m.target_column == target_column)
    {
        return Err(err(
            "42710",
            format!("materialized sum already defined for column '{target_column}'"),
        ));
    }

    coll.materialized_sums.push(def);
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    super::support::propose_and_apply(state, &entry)?;

    state.schema_version.bump();

    state.audit_record(
        AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD MATERIALIZED_SUM {target_column} on {target_collection}"),
    );

    Ok(status("ALTER COLLECTION"))
}

/// Convert a pre-validated value expression string into [`SqlExpr`].
fn parse_value_expression(value_expr: &str) -> Result<SqlExpr, DdlError> {
    if value_expr.chars().all(|c| c.is_alphanumeric() || c == '_') {
        Ok(SqlExpr::Column(value_expr.to_string()))
    } else {
        Err(err(
            "0A000",
            format!(
                "complex VALUE expressions not yet supported; use a pre-computed column. Got: '{value_expr}'"
            ),
        ))
    }
}
