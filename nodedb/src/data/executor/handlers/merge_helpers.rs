// SPDX-License-Identifier: BUSL-1.1

//! Pure helper functions for MERGE statement execution (arm selection, action application).

use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind as MergeClauseKindOp, MergeClauseOp,
};

/// Find the first clause of the given kind whose extra_predicate is satisfied
/// against `context_doc`.
///
/// A MERGE clause's `AND <condition>` extra predicate is WHERE-shaped, so a
/// division/modulo-by-zero inside it fails the whole statement — the same
/// behavior-flip rule 4 applies to WHERE/projection — rather than silently
/// skipping to the next clause.
pub(super) fn find_arm<'a>(
    clauses: &'a [MergeClauseOp],
    kind: MergeClauseKindOp,
    context_doc: &serde_json::Value,
) -> crate::Result<Option<&'a MergeClauseOp>> {
    let context_bytes = doc_format::encode_to_msgpack(context_doc);
    for c in clauses {
        if c.kind != kind {
            continue;
        }
        if c.extra_predicate.is_empty() {
            return Ok(Some(c));
        }
        let filters: Vec<ScanFilter> =
            zerompk::from_msgpack(&c.extra_predicate).unwrap_or_default();
        let mut all_match = true;
        for f in &filters {
            if !f.matches_binary(&context_bytes)? {
                all_match = false;
                break;
            }
        }
        if all_match {
            return Ok(Some(c));
        }
    }
    Ok(None)
}

/// Parameters for [`apply_action`].
pub(super) struct ApplyActionParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub doc_id: &'a str,
    pub target_doc: &'a serde_json::Value,
    pub source_doc: &'a serde_json::Value,
    pub source_alias: &'a str,
    pub clause: &'a MergeClauseOp,
    pub strict_schema: &'a Option<nodedb_types::columnar::StrictSchema>,
    /// Whether the target collection has a secondary vector index. Gated once
    /// by the caller so a non-vector collection pays nothing; when set, the
    /// UPDATE / DELETE branches maintain the row's HNSW vectors (the merge path
    /// otherwise never touches the vector index).
    pub has_vectors: bool,
}

/// Apply a MATCHED / NOT MATCHED BY SOURCE arm (UPDATE or DELETE) to a target row.
/// Returns `Ok(true)` when a write was performed.
pub(super) fn apply_action(
    core: &mut CoreLoop,
    params: ApplyActionParams<'_>,
) -> crate::Result<bool> {
    let ApplyActionParams {
        database_id,
        tid,
        collection,
        doc_id,
        target_doc,
        source_doc,
        source_alias,
        clause,
        strict_schema,
        has_vectors,
    } = params;
    match &clause.action {
        MergeActionOp::DoNothing => Ok(false),
        MergeActionOp::Delete => {
            core.sparse
                .delete(database_id, tid, collection, doc_id)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("merge delete {doc_id}: {e}"),
                })?;
            // Soft-delete the row's HNSW vectors + drop the reverse-map entry,
            // or the leaked node keeps scoring in KNN search. No-op unless the
            // collection has a vector field (gated by the caller).
            if has_vectors {
                core.remove_document_vector_indexes(database_id, tid, collection, doc_id);
            }
            Ok(true)
        }
        MergeActionOp::Update { updates } => {
            let updated = build_update_doc(target_doc, source_doc, source_alias, updates)?;

            let updated_bytes = if let Some(schema) = strict_schema {
                let ndb_val: nodedb_types::Value = updated.clone().into();
                super::super::strict_format::value_to_binary_tuple(&ndb_val, schema).map_err(
                    |e| crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!("merge strict re-encode: {e}"),
                    },
                )?
            } else {
                doc_format::encode_to_msgpack(&updated)
            };

            core.sparse
                .put(database_id, tid, collection, doc_id, &updated_bytes)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("merge update {doc_id}: {e}"),
                })?;
            core.doc_cache
                .put(database_id, tid, collection, doc_id, &updated_bytes);
            // Re-index the merged row's vectors (soft-delete the old HNSW node +
            // insert the new one, keyed by the stable surrogate), or KNN search
            // keeps returning the pre-merge embedding. No-op unless the
            // collection has a vector field (gated by the caller).
            if has_vectors
                && let Some(surrogate) = crate::engine::document::store::doc_id_to_surrogate(doc_id)
            {
                core.update_reindex_vector_indexes(
                    crate::data::executor::handlers::point::update_reindex_vector::UpdateVectorReindex {
                        database_id,
                        tid,
                        collection,
                        row_key: doc_id,
                        surrogate,
                        new_body: &updated_bytes,
                        is_strict: strict_schema.is_some(),
                        has_vectors,
                    },
                );
            }
            Ok(true)
        }
        MergeActionOp::Insert { .. } => {
            // INSERT in a MATCHED arm is unusual — ignore it.
            Ok(false)
        }
    }
}

/// Parameters for [`apply_insert_action`].
pub(super) struct ApplyInsertActionParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub source_doc: &'a serde_json::Value,
    pub source_alias: &'a str,
    pub clause: &'a MergeClauseOp,
    pub strict_schema: &'a Option<nodedb_types::columnar::StrictSchema>,
}

/// Apply a NOT MATCHED arm (INSERT) using the source document.
pub(super) fn apply_insert_action(
    core: &mut CoreLoop,
    params: ApplyInsertActionParams<'_>,
) -> crate::Result<bool> {
    let ApplyInsertActionParams {
        database_id,
        tid,
        collection,
        source_doc,
        source_alias,
        clause,
        strict_schema,
    } = params;
    match &clause.action {
        MergeActionOp::DoNothing => Ok(false),
        MergeActionOp::Delete | MergeActionOp::Update { .. } => {
            // DELETE / UPDATE in a NOT MATCHED arm is a no-op (no target row exists).
            Ok(false)
        }
        MergeActionOp::Insert { columns, values } => {
            let json_doc = build_insert_doc(columns, values, source_doc, source_alias)?;
            let doc_id = json_doc
                .get("id")
                .map(json_to_str)
                .unwrap_or_else(uuid_v4_str);

            let encoded = if let Some(schema) = strict_schema {
                let ndb_val: nodedb_types::Value = json_doc.clone().into();
                super::super::strict_format::value_to_binary_tuple(&ndb_val, schema).map_err(
                    |e| crate::Error::Storage {
                        engine: "sparse".into(),
                        detail: format!("merge insert strict encode: {e}"),
                    },
                )?
            } else {
                doc_format::encode_to_msgpack(&json_doc)
            };

            core.sparse
                .put(database_id, tid, collection, &doc_id, &encoded)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("merge insert {doc_id}: {e}"),
                })?;
            Ok(true)
        }
    }
}

/// Build the JSON document a NOT-MATCHED `INSERT` arm produces from a source
/// row. Empty `columns` copies all source fields; an explicit column list
/// evaluates each `UpdateValue` against the qualified source document (source
/// fields keyed as `"<alias>.<field>"`) — literals decode directly, expressions
/// (`s.new_embedding`, `s.qty * 2`) evaluate against the merged doc — and stores
/// the result under the *target* column name. Mirrors [`build_update_doc`].
/// Shared by the legacy per-row insert path and the orchestrated resolve/apply
/// passes so both derive byte-identical bodies.
pub(in crate::data::executor) fn build_insert_doc(
    columns: &[String],
    values: &[UpdateValue],
    source_doc: &serde_json::Value,
    source_alias: &str,
) -> crate::Result<serde_json::Value> {
    let mut new_doc = serde_json::Map::new();
    if columns.is_empty() {
        if let Some(obj) = source_doc.as_object() {
            for (k, v) in obj {
                new_doc.insert(k.clone(), v.clone());
            }
        }
    } else {
        // There is no target row for an insert, so the merged document is the
        // qualified source alone (target side is an empty object).
        let merged = build_merged(
            &serde_json::Value::Object(Default::default()),
            source_doc,
            source_alias,
        );
        let merged_ndb: nodedb_types::Value = merged.into();
        for (col, val) in columns.iter().zip(values.iter()) {
            new_doc.insert(col.clone(), resolve_update_value(val, &merged_ndb)?);
        }
    }
    Ok(serde_json::Value::Object(new_doc))
}

/// Build the post-update JSON document a MATCHED / NOT-MATCHED-BY-SOURCE
/// `UPDATE` arm produces. Assignment expressions evaluate against the merged
/// document (target fields at top level, source fields as `<alias>.<field>`),
/// then overwrite fields on a clone of the target. Shared by the legacy per-row
/// update path and the orchestrated resolve/apply passes.
pub(in crate::data::executor) fn build_update_doc(
    target_doc: &serde_json::Value,
    source_doc: &serde_json::Value,
    source_alias: &str,
    updates: &[(String, UpdateValue)],
) -> crate::Result<serde_json::Value> {
    let merged = build_merged(target_doc, source_doc, source_alias);
    let merged_ndb: nodedb_types::Value = merged.into();
    let mut updated = target_doc.clone();
    if let Some(obj) = updated.as_object_mut() {
        for (field, update_val) in updates {
            obj.insert(
                field.clone(),
                resolve_update_value(update_val, &merged_ndb)?,
            );
        }
    }
    Ok(updated)
}

/// Resolve one `UpdateValue` to JSON: a literal decodes directly from its
/// msgpack encoding, an expression evaluates against the merged document.
/// Shared by [`build_insert_doc`] and [`build_update_doc`]. An assignment
/// expression is write-path-shaped, so a division/modulo-by-zero fails the
/// whole MERGE statement.
fn resolve_update_value(
    val: &UpdateValue,
    merged_ndb: &nodedb_types::Value,
) -> crate::Result<serde_json::Value> {
    Ok(match val {
        UpdateValue::Literal(bytes) => {
            nodedb_types::json_from_msgpack(bytes).unwrap_or(serde_json::Value::Null)
        }
        UpdateValue::Expr(expr) => expr.eval(merged_ndb)?.into(),
    })
}

/// Build merged document: target fields at top level, source fields as
/// `"alias.field"` qualified entries.
pub(super) fn build_merged(
    target: &serde_json::Value,
    source: &serde_json::Value,
    source_alias: &str,
) -> serde_json::Value {
    let mut merged = target.clone();
    if let (Some(m), Some(src)) = (merged.as_object_mut(), source.as_object()) {
        for (k, v) in src {
            m.insert(format!("{source_alias}.{k}"), v.clone());
        }
    }
    merged
}

pub(super) fn json_to_str(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub(super) fn uuid_v4_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    format!("merge-{nanos}")
}
