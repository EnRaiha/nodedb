// SPDX-License-Identifier: BUSL-1.1

//! `ALTER COLLECTION accounts ADD COLUMN balance DECIMAL DEFAULT 0 AS MATERIALIZED_SUM ...`
//! — ADD COLUMN variant that binds a computed balance to another collection's
//! per-row contribution. Atomically maintained on INSERT into the source side.
//!
//! The value-expression validation, duplicate binding guard,
//! `materialized_sums` push, `PutCollection` propose, `schema_version` bump,
//! and audit all live here, as does the `ALTER COLLECTION` command tag.
//!
//! The statement declares a real column as well as a binding, and both halves
//! are applied. Declaring only the binding leaves a strict target with no
//! `balance` field, and every maintenance write into it is a full document
//! write that the Binary Tuple encoder rejects on a field the schema does not
//! carry — the balance would never land, and neither would the source row that
//! caused it. Column and binding are mutated into one `StoredCollection` and
//! proposed as a single `PutCollection`, so no committed state ever holds a
//! binding whose column does not exist.

use nodedb_types::columnar::StrictSchema;
use nodedb_types::{CollectionType, DatabaseId};

use crate::bridge::expr_eval::SqlExpr;
use crate::control::security::audit::AuditEvent;
use crate::control::security::catalog::StoredCollection;
use crate::control::security::catalog::types::MaterializedSumDef;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::shared::ddl::neutral::collection::helpers::parse_origin_column_def;
use crate::control::server::shared::ddl::result::{DdlError, DdlResult};
use crate::control::state::SharedState;

use super::support::{err, status};

/// The fully parsed `ADD COLUMN ... MATERIALIZED_SUM ...` statement.
pub(super) struct MaterializedSumRequest<'a> {
    /// Collection whose column holds the running total.
    pub target_collection: &'a str,
    /// Column that holds the running total.
    pub target_column: &'a str,
    /// Declared type of that column, as written.
    pub target_column_type: &'a str,
    /// Collection whose rows contribute to the total.
    pub source_collection: &'a str,
    /// Column on the source side that names the target row.
    pub join_column: &'a str,
    /// Source column whose value each row contributes.
    pub value_expr: &'a str,
}

pub(super) async fn add_materialized_sum(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    req: &MaterializedSumRequest<'_>,
) -> Result<Vec<DdlResult>, DdlError> {
    let tenant_id = identity.tenant_id.as_u64();
    let MaterializedSumRequest {
        target_collection,
        target_column,
        target_column_type,
        source_collection,
        join_column,
        value_expr,
    } = *req;

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

    declare_target_column(&mut coll, target_column, target_column_type)?;
    coll.materialized_sums.push(def);
    let entry = crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(coll.clone()));
    super::support::propose_and_apply_async(state, entry).await?;

    // The Data Plane's in-memory shape is what the Binary Tuple encoder
    // consults, so it must learn the new column before the first source write
    // arrives; without this the binding is durable and the very next
    // maintenance write is still rejected on an unknown field.
    super::super::register::dispatch_register_from_stored(state, &coll)
        .await
        .map_err(|e| err("XX000", e.to_string()))?;

    // The SOURCE has to be re-registered too, and it is the half that decides
    // whether anything is folded at all: the binding is stored here on the
    // target, but the Data-Plane config that drives the write-path fold is
    // derived for the source. Registering only the target leaves the source
    // asserting it drives no binding, so every co-resident write into it folds
    // nothing and the total silently stays where it was.
    super::super::register::dispatch_register_for_sum_sources(state, &coll)
        .await
        .map_err(|e| err("XX000", e.to_string()))?;

    state.schema_version.bump();

    state.audit_record(
        AuditEvent::ConfigChange,
        Some(identity.tenant_id),
        &identity.username,
        &format!("ADD MATERIALIZED_SUM {target_column} on {target_collection}"),
    );

    Ok(status("ALTER COLLECTION"))
}

/// Append the declared total column to a strict target's schema, in place.
///
/// A schemaless target carries no column list to append to, and its writes
/// accept any field, so it needs no declaration. A strict target does: its
/// encoder rejects fields the schema does not carry, and the maintenance write
/// is an ordinary document write through that encoder.
///
/// Mirrors `ALTER ... ADD COLUMN`'s multi-version add — `added_at_version`
/// stamp plus `schema.version` bump — so rows written before this statement
/// keep reading back at their own version, and records the *declared* type
/// alongside the resolved one so the column reports the same width on the wire
/// as an identical column declared at CREATE time.
fn declare_target_column(
    coll: &mut StoredCollection,
    column: &str,
    declared_type: &str,
) -> Result<(), DdlError> {
    if !coll.collection_type.is_strict() {
        return Ok(());
    }

    let config_json = coll.timeseries_config.as_deref().ok_or_else(|| {
        err(
            "XX000",
            format!("strict collection '{}' has no stored schema", coll.name),
        )
    })?;
    let mut schema: StrictSchema = sonic_rs::from_str(config_json)
        .map_err(|e| err("XX000", format!("strict schema decode: {e}")))?;

    if schema.columns.iter().any(|c| c.name == column) {
        return Err(err(
            "42P07",
            format!("column '{column}' already exists on '{}'", coll.name),
        ));
    }

    let mut col = parse_origin_column_def(&format!("{column} {declared_type}"))
        .map_err(|e| err("42601", e.to_string()))?;
    let new_version = schema.version.saturating_add(1);
    col.added_at_version = new_version;
    schema.columns.push(col);
    schema.version = new_version;

    coll.collection_type = CollectionType::strict(schema.clone());
    coll.timeseries_config = sonic_rs::to_string(&schema).ok();
    super::strict_schema::add_field(coll, column, declared_type);
    Ok(())
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

    /// One source driving TWO bindings that read the SAME join column into
    /// DIFFERENT targets is legal, and must stay legal.
    ///
    /// It is still depth 1 — neither target feeds anything and the source is fed
    /// by nothing — so the chain rule has nothing to say about it. Nothing else
    /// needs to: the resolution a write carries is keyed on the
    /// `(target collection, join value)` PAIR, so each binding resolves, defers
    /// and folds against its own target row independently. There is nothing left
    /// for a DDL-time guard to protect, and refusing the shape here would reject
    /// a schema the engine now maintains correctly.
    #[test]
    fn a_second_binding_on_the_same_source_and_join_column_is_accepted() {
        let existing = vec![binding("entries", "accounts")];
        assert!(validate_binding_depth(&existing, "audit_totals", "entries").is_ok());
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
