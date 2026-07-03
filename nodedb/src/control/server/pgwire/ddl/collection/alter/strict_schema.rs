// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for strict-schema-altering DDL.
//!
//! `ALTER COLUMN TYPE`, `DROP COLUMN`, and `RENAME COLUMN` all open
//! with the same six-step prelude — look up the catalog, fetch the
//! active strict collection, deserialize its `StrictSchema` blob —
//! and close with the same three-step coda — package the mutated
//! `StoredCollection` into a `PutCollection` entry, replicate it
//! through the metadata raft group, refresh the Data Plane register,
//! and bump the schema version.
//!
//! Inlining either bookend in every handler bloated each file to ~110
//! lines of mostly boilerplate and made it easy to silently drift on
//! the propose / register / version-bump ordering. The helpers below
//! collapse the bookends to a single call each; per-statement
//! handlers only own the schema mutation in between.

use nodedb_types::DatabaseId;
use pgwire::error::PgWireResult;

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

use super::super::super::super::types::sqlstate_error;

/// Look up the active strict collection `name` for `tenant_id` and
/// return it together with its deserialized `StrictSchema`. Returns
/// the appropriate pgwire error if the catalog is missing, the
/// collection is absent / inactive, the engine is not strict, or
/// the embedded `timeseries_config` JSON fails to parse.
pub fn load_strict_collection(
    state: &SharedState,
    tenant_id: u64,
    name: &str,
    operation: &str,
) -> PgWireResult<(StoredCollection, nodedb_types::columnar::StrictSchema)> {
    let Some(catalog) = state.credentials.catalog() else {
        return Err(sqlstate_error("XX000", "no catalog available"));
    };

    let coll = catalog
        .get_collection(DatabaseId::DEFAULT, tenant_id, name)
        .map_err(|e| sqlstate_error("XX000", &e.to_string()))?
        .filter(|c| c.is_active)
        .ok_or_else(|| sqlstate_error("42P01", &format!("collection '{name}' does not exist")))?;

    if !coll.collection_type.is_strict() {
        return Err(sqlstate_error(
            "0A000",
            &format!("{operation} is only supported on strict document collections"),
        ));
    }

    let schema: nodedb_types::columnar::StrictSchema = coll
        .timeseries_config
        .as_deref()
        .and_then(|s| sonic_rs::from_str(s).ok())
        .ok_or_else(|| sqlstate_error("XX000", "strict schema missing or malformed"))?;

    Ok((coll, schema))
}

/// Re-serialize `schema` into `coll.timeseries_config` and set
/// `coll.collection_type` to the matching `Strict(...)` variant.
/// Centralised so the two-field invariant — JSON blob in
/// `timeseries_config` mirrors typed schema in `collection_type` —
/// can't drift across the three column-mutating handlers.
pub fn write_schema_back(
    coll: &mut StoredCollection,
    schema: nodedb_types::columnar::StrictSchema,
) {
    coll.collection_type = nodedb_types::CollectionType::strict(schema.clone());
    coll.timeseries_config = sonic_rs::to_string(&schema).ok();
}

/// Replicate the mutated collection through the metadata raft group,
/// refresh this node's Data Plane register so the in-memory shape
/// catches up with the new schema, then bump `schema_version`. Every
/// strict-schema-altering handler ends with this exact three-step
/// dance; centralising it keeps the ordering uniform and prevents a
/// missed `dispatch_register` from leaving a node serving stale rows.
pub async fn persist_schema_change(
    state: &SharedState,
    updated: &StoredCollection,
) -> PgWireResult<()> {
    let entry =
        crate::control::catalog_entry::CatalogEntry::PutCollection(Box::new(updated.clone()));
    super::super::super::catalog_propose::propose_and_apply(state, &entry)?;

    crate::control::server::shared::ddl::neutral::collection::dispatch_register_from_stored(
        state, updated,
    )
    .await
    .map_err(|e| sqlstate_error("XX000", &e.to_string()))?;
    state.schema_version.bump();
    Ok(())
}
