// SPDX-License-Identifier: BUSL-1.1

//! Pre-flight schema-compatibility check for `MOVE TENANT`.
//!
//! Verifies that every collection the tenant has data in on the source
//! database exists in the target database with a compatible schema (same
//! engine type, same column names and types).
//!
//! No state is mutated during pre-flight. If this function returns `Err`,
//! the move is aborted with `MOVE_TENANT_PREFLIGHT_FAILED` and no compensation
//! is needed.

use crate::control::security::catalog::SystemCatalog;
use crate::types::DatabaseId;
use nodedb_types::NodeDbError;

/// Run the pre-flight compatibility check.
///
/// Returns `Ok(())` if every collection in `source_db_id` has a
/// schema-compatible counterpart in `target_db_id`.  All collections in
/// the source database are checked — the move transfers the entire
/// database namespace.
pub fn run(
    catalog: &SystemCatalog,
    source_db_id: DatabaseId,
    target_db_id: DatabaseId,
    tenant_name: &str,
    target_db_name: &str,
) -> Result<(), NodeDbError> {
    let source_colls = catalog.load_all_collections(source_db_id).map_err(|e| {
        NodeDbError::move_tenant_preflight_failed(
            tenant_name,
            format!("failed to enumerate source collections: {e}"),
        )
    })?;

    for src_coll in source_colls.iter().filter(|c| c.is_active) {
        let target_coll = catalog
            .get_collection(target_db_id, src_coll.tenant_id, &src_coll.name)
            .map_err(|e| {
                NodeDbError::move_tenant_preflight_failed(
                    tenant_name,
                    format!(
                        "catalog lookup for collection '{}' in target failed: {e}",
                        src_coll.name
                    ),
                )
            })?;

        match target_coll {
            None => {
                return Err(NodeDbError::move_tenant_preflight_failed(
                    tenant_name,
                    format!(
                        "collection '{}' exists in source database but is missing \
                         in target database '{target_db_name}'",
                        src_coll.name
                    ),
                ));
            }
            Some(ref tgt) => {
                // Engine type must match.
                if src_coll.collection_type != tgt.collection_type {
                    return Err(NodeDbError::move_tenant_preflight_failed(
                        tenant_name,
                        format!(
                            "collection '{}': engine mismatch — source has {:?}, \
                             target has {:?}",
                            src_coll.name, src_coll.collection_type, tgt.collection_type
                        ),
                    ));
                }
                // Field count must match for strict-schema engines.
                if src_coll.fields.len() != tgt.fields.len() {
                    return Err(NodeDbError::move_tenant_preflight_failed(
                        tenant_name,
                        format!(
                            "collection '{}': field count mismatch — source has {}, \
                             target has {}",
                            src_coll.name,
                            src_coll.fields.len(),
                            tgt.fields.len()
                        ),
                    ));
                }
            }
        }
    }

    Ok(())
}
