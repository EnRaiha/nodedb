// SPDX-License-Identifier: BUSL-1.1

//! Shared target-identity helpers for the MERGE orchestrator.
//!
//! Both the autocommit orchestrator ([`super::orchestrator::run_merge`]) and the
//! COMMIT-time in-transaction expander ([`super::expand_staged_merge`]) resolve a
//! merge's arms through the SAME Data-Plane RESOLVE pass and then assign a fresh,
//! catalog-registered surrogate per inserted row on the TARGET's primary key.
//! These primitives — primary-key classification, surrogate assignment, PK
//! extraction, the collection-name de-qualifier, and the RESOLVE-payload decode —
//! are factored here so the two drivers cannot diverge on identity derivation.

use nodedb_types::columnar::DocumentMode;
use nodedb_types::{CollectionType, DatabaseId, Surrogate, TenantId, Value};

use crate::control::security::catalog::StoredCollection;
use crate::control::state::SharedState;

/// How the target collection's primary key drives surrogate assignment for a
/// merge-inserted row (mirrors the plain-`INSERT` identity path).
pub(crate) enum TargetPk {
    /// Auto-generated `_rowid` (no declared PK): every inserted row gets a
    /// fresh, distinct surrogate.
    AutoRowId,
    /// A declared / built-in primary-key field: the fresh surrogate is
    /// content-addressed on this field's value so a later point-get /
    /// cross-engine resolve lands on the same identity.
    Field(String),
}

/// The three resolved arms of a MERGE, decoded from the Data-Plane RESOLVE
/// pass. `updates` / `deletes` carry the EXISTING target row's storage key
/// (`doc_id`), its registered `surrogate` (`None` only for a legacy
/// non-surrogate-keyed row — unreachable for any surrogate-keyed collection),
/// and the arm's resolved body (post-image for updates, the deleted row for
/// deletes so its PK can be extracted). `inserts` carry `(join_key, body)`.
#[derive(Default)]
pub(crate) struct ResolvedMergeArms {
    pub(crate) updates: Vec<(String, Option<u32>, Vec<u8>)>,
    pub(crate) deletes: Vec<(String, Option<u32>, Vec<u8>)>,
    pub(crate) inserts: Vec<(String, Vec<u8>)>,
}

/// Decode the RESOLVE pass payload (a msgpack 3-tuple `(updates, deletes,
/// inserts)`; see `execute_merge_resolve`) into [`ResolvedMergeArms`].
pub(crate) fn decode_resolve(payload: &[u8]) -> crate::Result<ResolvedMergeArms> {
    if payload.is_empty() {
        return Ok(ResolvedMergeArms::default());
    }
    type Wire = (
        Vec<(String, Option<u32>, Vec<u8>)>,
        Vec<(String, Option<u32>, Vec<u8>)>,
        Vec<(String, Vec<u8>)>,
    );
    let (updates, deletes, inserts): Wire =
        zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("merge resolve rows: {e}"),
        })?;
    Ok(ResolvedMergeArms {
        updates,
        deletes,
        inserts,
    })
}

/// Assign a fresh, registered surrogate for one merge-inserted row on the
/// TARGET's primary key.
pub(crate) fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    body: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection)
        }
        TargetPk::Field(field) => match extract_pk_value(body, field) {
            Some(pk) if !pk.is_empty() => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value: mint a fresh unique surrogate rather than
            // collapsing every keyless inserted row onto one binding.
            _ => state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection),
        },
    }
}

/// Resolve how the target collection's primary key maps an inserted row to a
/// surrogate, mirroring the plain-`INSERT` identity path.
pub(crate) fn resolve_target_pk(target: &StoredCollection) -> crate::Result<TargetPk> {
    match &target.collection_type {
        CollectionType::Document(DocumentMode::Strict(schema)) => {
            match schema.columns.iter().find(|c| c.primary_key) {
                Some(col) if col.name == "_rowid" => Ok(TargetPk::AutoRowId),
                Some(col) => Ok(TargetPk::Field(col.name.clone())),
                None => Ok(TargetPk::AutoRowId),
            }
        }
        CollectionType::Document(DocumentMode::Schemaless) => Ok(TargetPk::Field(
            target
                .declared_primary_key
                .clone()
                .unwrap_or_else(|| "id".to_string()),
        )),
        CollectionType::KeyValue(_) | CollectionType::Columnar(_) => Err(crate::Error::PlanError {
            detail: format!(
                "MERGE target '{}' must be a document collection",
                target.name
            ),
        }),
    }
}

/// The user-visible primary key (`document_id`) for a merge row on this target,
/// mirroring the plain-`INSERT` identity path (`insert.rs`): an auto-`_rowid`
/// row's PK is the decimal surrogate the Data Plane also writes into `_rowid`;
/// a declared-PK row's PK is the field value extracted from the body.
pub(crate) fn derive_document_id(
    target_pk: &TargetPk,
    body: &[u8],
    surrogate: Surrogate,
) -> String {
    match target_pk {
        TargetPk::AutoRowId => surrogate.as_u32().to_string(),
        TargetPk::Field(field) => {
            extract_pk_value(body, field).unwrap_or_else(|| surrogate.as_u32().to_string())
        }
    }
}

/// Extract a stringified primary-key value from a MessagePack row body.
fn extract_pk_value(body: &[u8], field: &str) -> Option<String> {
    let Value::Object(obj) = nodedb_types::value_from_msgpack(body).ok()? else {
        return None;
    };
    value_to_pk_string(obj.get(field)?)
}

/// Stringify a scalar value into its primary-key byte form (mirrors the
/// `sql_value_to_string` convention used by the plain-INSERT identity path).
fn value_to_pk_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Integer(n) => Some(n.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Decimal(d) => Some(d.to_string()),
        _ => None,
    }
}

/// Strip the `{database_id}/` qualifier from a db-qualified collection name to
/// recover the bare name the catalog keys collections by.
pub(crate) fn bare_collection_name(database_id: DatabaseId, qualified: &str) -> String {
    if database_id == DatabaseId::DEFAULT {
        return qualified.to_string();
    }
    let prefix = format!("{}/", database_id.as_u64());
    qualified
        .strip_prefix(&prefix)
        .unwrap_or(qualified)
        .to_string()
}
