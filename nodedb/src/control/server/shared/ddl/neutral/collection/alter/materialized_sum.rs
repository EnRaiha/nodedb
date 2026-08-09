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
use crate::control::security::catalog::types::MaterializedSumDef;
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

    let def = MaterializedSumDef {
        target_collection: target_collection.to_string(),
        target_column: target_column.to_string(),
        source_collection: source_collection.to_string(),
        join_column: join_column.to_string(),
        value_expr: expr,
    };

    let catalog = state.credentials.catalog();

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

    let existing_bindings: Vec<MaterializedSumDef> = catalog
        .load_collections_for_tenant(DatabaseId::DEFAULT, tenant_id)
        .map_err(|e| err("XX000", e.to_string()))?
        .into_iter()
        .flat_map(|c| c.materialized_sums)
        .collect();
    validate_binding_depth(&existing_bindings, target_collection, source_collection)?;

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

/// Refuse a binding that would make some collection both a materialized-sum
/// source and a materialized-sum target.
///
/// Maintenance of a materialized sum writes the target row through a plain
/// document write that deliberately does not re-enter enforcement — that is
/// what makes the recursion floor structural rather than depth-limited. The
/// consequence is that propagation stops after exactly one hop: in a chain
/// `A -> B -> C`, a write to `A` updates `B` but never reaches `C`, and the
/// sum on `C` silently drifts. A cycle is the same defect closed on itself.
///
/// Every stored binding is an edge `source -> target`. Requiring that no
/// collection carries both an inbound and an outbound edge bounds every path
/// at length one, which rules out chains and cycles of any length alike.
fn validate_binding_depth(
    existing: &[MaterializedSumDef],
    target_collection: &str,
    source_collection: &str,
) -> Result<(), DdlError> {
    let chain_error = |downstream: &str, upstream: &str| {
        err(
            "0A000",
            format!(
                "materialized sum from '{source_collection}' into '{target_collection}' would \
                 chain through '{upstream}' -> '{downstream}': a materialized-sum target cannot \
                 also be a source for another materialized sum, because maintenance writes do \
                 not propagate past the first hop"
            ),
        )
    };

    if target_collection.eq_ignore_ascii_case(source_collection) {
        return Err(chain_error(target_collection, source_collection));
    }

    // The new target already feeds another collection: it would become both a
    // sink (of this binding) and a source (of that one).
    if let Some(downstream) = existing
        .iter()
        .find(|m| m.source_collection.eq_ignore_ascii_case(target_collection))
    {
        return Err(chain_error(
            &downstream.target_collection,
            target_collection,
        ));
    }

    // The new source is already fed by another collection: same defect, one
    // hop upstream.
    if let Some(upstream) = existing
        .iter()
        .find(|m| m.target_collection.eq_ignore_ascii_case(source_collection))
    {
        return Err(chain_error(source_collection, &upstream.source_collection));
    }

    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `entries -> accounts`: writes to `entries` maintain `accounts.balance`.
    fn binding(source: &str, target: &str) -> MaterializedSumDef {
        MaterializedSumDef {
            target_collection: target.to_string(),
            target_column: "balance".to_string(),
            source_collection: source.to_string(),
            join_column: "account_id".to_string(),
            value_expr: SqlExpr::Column("amount".to_string()),
        }
    }

    #[test]
    fn independent_binding_is_accepted() {
        let existing = vec![binding("entries", "accounts")];
        assert!(validate_binding_depth(&existing, "budgets", "line_items").is_ok());
    }

    #[test]
    fn first_binding_is_accepted() {
        assert!(validate_binding_depth(&[], "accounts", "entries").is_ok());
    }

    #[test]
    fn second_binding_onto_the_same_target_is_accepted() {
        // Two sources fanning into one target is still depth 1.
        let existing = vec![binding("entries", "accounts")];
        assert!(validate_binding_depth(&existing, "accounts", "adjustments").is_ok());
    }

    #[test]
    fn extending_an_existing_target_into_a_source_is_rejected() {
        // entries -> accounts already exists; accounts -> ledger would chain.
        let existing = vec![binding("entries", "accounts")];
        let error = validate_binding_depth(&existing, "ledger", "accounts")
            .expect_err("a two-hop chain must be refused");
        assert_eq!(error.sqlstate, "0A000");
        assert!(error.message.contains("accounts"), "{}", error.message);
        assert!(error.message.contains("ledger"), "{}", error.message);
        assert!(
            error
                .message
                .contains("a materialized-sum target cannot also be a source"),
            "{}",
            error.message
        );
    }

    #[test]
    fn feeding_an_existing_source_is_rejected() {
        // entries -> accounts already exists; raw_events -> entries would chain.
        let existing = vec![binding("entries", "accounts")];
        let error = validate_binding_depth(&existing, "entries", "raw_events")
            .expect_err("a two-hop chain must be refused");
        assert_eq!(error.sqlstate, "0A000");
        assert!(error.message.contains("entries"), "{}", error.message);
        assert!(error.message.contains("raw_events"), "{}", error.message);
    }

    #[test]
    fn self_binding_is_rejected() {
        let error = validate_binding_depth(&[], "accounts", "accounts")
            .expect_err("a self-referential binding must be refused");
        assert_eq!(error.sqlstate, "0A000");
        assert!(error.message.contains("accounts"), "{}", error.message);
    }

    #[test]
    fn cycle_closing_an_existing_binding_is_rejected() {
        // entries -> accounts already exists; accounts -> entries closes a cycle.
        let existing = vec![binding("entries", "accounts")];
        let error = validate_binding_depth(&existing, "entries", "accounts")
            .expect_err("a cycle must be refused");
        assert_eq!(error.sqlstate, "0A000");
    }

    #[test]
    fn longer_chain_is_rejected_at_every_extension_point() {
        let existing = vec![binding("a", "b"), binding("c", "d")];
        // b -> c would join the two independent edges into a -> b -> c -> d.
        assert!(validate_binding_depth(&existing, "c", "b").is_err());
    }
}
