// SPDX-License-Identifier: BUSL-1.1

//! Shared "apply a PointDelete" helper — manages its own doc-store write
//! transaction internally (see doc comment on `apply_point_delete`).
//!
//! Reused by the autocommit PointDelete path and (in a later unit) by the
//! transactional `tx_point_delete` path.

use tracing::warn;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::doc_format;
use crate::data::executor::enforcement::{append_only, period_lock, retention};
use nodedb_types::Surrogate;

use crate::data::executor::handlers::point::apply_put::map_enforcement_error;

/// Parameters for [`CoreLoop::apply_point_delete`].
pub(in crate::data::executor) struct PointDeleteParams<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    /// Roles held by the authenticated user. Currently unused by DELETE
    /// enforcement (no role-gated delete checks exist yet), but threaded
    /// through for symmetry with `PointPutParams` and future-proofing.
    pub user_roles: &'a [String],
    /// Whether to run stateless DELETE enforcement (append-only, period
    /// lock, retention/legal-hold).
    ///
    /// `true` for user-DML callers (autocommit PointDelete, and the
    /// transactional path in a later unit). `false` for system-sourced
    /// deletes (e.g. CRDT-sync materialization) whose admission already
    /// happened on their origin replica.
    pub enforce: bool,
    /// Whether to apply the EXTRA delete cascades that have no undo variant
    /// yet: the spatial R-tree removal and `mark_node_deleted` bookkeeping.
    ///
    /// `true` for autocommit user-DML callers (autocommit PointDelete),
    /// which own the full delete. `false` for the transactional path
    /// (`tx_point_delete`): those two side-effects have no rollback
    /// reversal, so enabling them inside a transaction would leave a
    /// rollback hole. The CORE side-effects (bitemporal tombstone or plain
    /// delete — including versioned index tombstones, FTS/inverted index
    /// removal, secondary-index cascade, graph-edge cascade, doc_cache
    /// invalidation) run regardless.
    pub enable_extra_cascades: bool,
}

/// Capture of the mutations an [`CoreLoop::apply_point_delete`] performed, so
/// a transactional caller can build an undo entry that fully reverses it.
pub(in crate::data::executor) struct PointDeleteOutcome {
    /// Prior stored bytes when a row was actually removed, else `None`.
    pub prior_value: Option<Vec<u8>>,
    /// System-time key the bitemporal tombstone row (and its versioned index
    /// tombstones) were appended at. `Some(t)` on the bitemporal branch,
    /// `None` on the plain delete branch.
    pub bitemporal_sys_from_ms: Option<i64>,
    /// `(field, value)` pairs whose versioned index tombstones this op wrote
    /// at `bitemporal_sys_from_ms`. Empty when not bitemporal / none written.
    pub bitemporal_index_tuples: Vec<(String, String)>,
    /// `(index_key, vector_id)` pairs this delete soft-deleted from HNSW vector
    /// indexes. Populated unconditionally (autocommit and transactional) so the
    /// owning document's vectors never orphan; a transactional caller pushes an
    /// `UndoEntry::DeleteVector` per entry so a rolled-back delete restores them.
    pub vector_deletes: Vec<(
        (nodedb_types::DatabaseId, crate::types::TenantId, String),
        u32,
    )>,
}

impl CoreLoop {
    /// Apply a PointDelete, managing its own doc-store write transaction.
    ///
    /// Handles the bitemporal-aware tombstone/versioned-index-tombstone
    /// branch, the non-bitemporal overwrite-delete branch, and all cascades
    /// (inverted index, secondary indexes, graph edges, spatial R-tree,
    /// node-deleted bookkeeping, doc cache invalidation).
    ///
    /// The doc-store write transaction (bitemporal: an explicit
    /// `begin_write`/`commit` pair; non-bitemporal: the self-committing
    /// `SparseEngine::delete`) is committed BEFORE any cascade runs. Several
    /// cascades (`delete_indexes_for_document`, the inverted index removal)
    /// open their own internal write transactions, and redb allows only one
    /// write transaction at a time — keeping the doc-store txn open across
    /// those calls would deadlock every delete.
    ///
    /// Does NOT emit WriteEvents, mark checkpoints dirty, or build
    /// RETURNING payloads — those stay with the caller.
    ///
    /// Returns a [`PointDeleteOutcome`] capturing the prior stored bytes
    /// (present when a row was actually removed) plus the bitemporal system
    /// time and versioned index tombstone tuples written, so a transactional
    /// caller can build a fully-reversible undo entry. Autocommit callers
    /// read only `prior_value`.
    pub(in crate::data::executor) fn apply_point_delete(
        &mut self,
        params: PointDeleteParams<'_>,
    ) -> crate::Result<PointDeleteOutcome> {
        let PointDeleteParams {
            database_id,
            tid,
            collection,
            document_id,
            surrogate,
            user_roles,
            enforce,
            enable_extra_cascades,
        } = params;
        let _ = user_roles;

        let row_key = crate::engine::document::store::surrogate_to_doc_id(surrogate);
        let row_key = row_key.as_str();
        let bitemporal = self.is_bitemporal(tid, collection);
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());

        // On bitemporal collections: append a doc tombstone + versioned
        // index tombstones for every current field value. `prior` is the
        // pre-delete body so the Event Plane sees `old_value` correctly.
        // Current-state-only indexes (text, graph, spatial, vector) are
        // still cascaded below — they track "what exists now" regardless
        // of bitemporal history.
        let mut bitemporal_sys_from_ms: Option<i64> = None;
        let mut bitemporal_index_tuples: Vec<(String, String)> = Vec::new();
        let prior = if bitemporal {
            let prior = self
                .sparse
                .versioned_get_current(database_id, tid, collection, row_key)?;
            if let Some(ref body) = prior {
                if enforce && let Some(config) = self.doc_configs.get(&config_key) {
                    run_delete_enforcement(
                        &self.sparse,
                        database_id,
                        tid,
                        collection,
                        config,
                        Some(body),
                    )?;
                }
                let sys_from = self.bitemporal_now_ms();
                bitemporal_sys_from_ms = Some(sys_from);
                let txn = self.sparse.begin_write()?;
                self.sparse.versioned_tombstone_in_txn(
                    &txn,
                    database_id,
                    tid,
                    collection,
                    row_key,
                    sys_from,
                )?;
                // Index tombstones: reflect every current value so
                // `index_lookup_as_of` at or after `sys_from` skips this
                // doc_id.
                if let Some(config) = self.doc_configs.get(&config_key)
                    && let Some(doc) = doc_format::decode_document(body)
                {
                    for path in config.index_paths.clone() {
                        for v in crate::engine::document::store::extract_index_values(
                            &doc,
                            &path.path,
                            path.is_array,
                        ) {
                            let value = if path.case_insensitive {
                                v.to_lowercase()
                            } else {
                                v
                            };
                            self.sparse.versioned_index_tombstone_in_txn(
                                &txn,
                                crate::engine::sparse::btree_versioned::VersionedIndexEntry {
                                    database_id,
                                    tenant: tid,
                                    coll: collection,
                                    field: &path.path,
                                    value: &value,
                                    doc_id: row_key,
                                    sys_from_ms: sys_from,
                                },
                            )?;
                            bitemporal_index_tuples.push((path.path.clone(), value));
                        }
                    }
                }
                txn.commit().map_err(|e| crate::Error::Storage {
                    engine: "sparse".into(),
                    detail: format!("commit: {e}"),
                })?;
            }
            prior
        } else {
            if enforce && let Some(config) = self.doc_configs.get(&config_key) {
                let old_value = self.sparse.get(database_id, tid, collection, row_key)?;
                run_delete_enforcement(
                    &self.sparse,
                    database_id,
                    tid,
                    collection,
                    config,
                    old_value.as_deref(),
                )?;
            }
            self.sparse.delete(database_id, tid, collection, row_key)?
        };

        // Cascade 1: Remove from full-text inverted index. The inverted
        // index was populated by `apply_point_put` with the substrate row
        // key (hex surrogate), not the user-visible PK — keep the cascade
        // keyed the same way so a delete actually wipes the term postings.
        if let Err(e) = self.inverted.remove_document(
            database_id,
            crate::types::TenantId::new(tid),
            collection,
            surrogate,
        ) {
            warn!(core = self.core_id, %collection, %document_id, error = %e, "inverted index removal failed");
        }

        // Cascade 2: Remove secondary index entries for this document.
        // Secondary indexes use key format "{tenant}:{collection}:{field}:{value}:{doc_id}".
        // We scan and delete all entries ending with this doc_id.
        if let Err(e) =
            self.sparse
                .delete_indexes_for_document(database_id, tid, collection, row_key)
        {
            warn!(core = self.core_id, %collection, %document_id, error = %e, "secondary index cascade failed");
        }

        // Cascade 3: Remove graph edges where this document is src or dst.
        let edges_removed = self
            .csr_partition_mut(database_id, tid)
            .remove_node_edges(document_id);
        if edges_removed > 0 {
            // Also tombstone in persistent edge store.
            let cascade_ord = self.hlc.next_ordinal();
            if let Err(e) = self.edge_store.delete_edges_for_node(
                database_id,
                nodedb_types::TenantId::new(tid),
                document_id,
                cascade_ord,
            ) {
                warn!(core = self.core_id, %document_id, error = %e, "edge cascade failed");
            }
            tracing::trace!(core = self.core_id, %document_id, edges_removed, "EDGE_CASCADE_DELETE");
        }

        // Cascade 4 (SIDE): Remove from spatial R-tree indexes + reverse map,
        // and record the node deletion for edge referential integrity. Both
        // have no undo reversal yet, so they run only for autocommit callers
        // (`enable_extra_cascades == true`); the transactional path skips them
        // to avoid a rollback hole.
        //
        // `apply_point_put` hashes the substrate row key as the R-tree entry
        // id, so delete must hash the same key to find the entry. Hashing the
        // user PK would leak ghost bbox entries that survive the row's removal.
        if enable_extra_cascades {
            let entry_id = crate::util::fnv1a_hash(row_key.as_bytes());
            let db_id = nodedb_types::DatabaseId::new(database_id);
            let tid_id = crate::types::TenantId::new(tid);
            let spatial_fields: Vec<String> = self
                .spatial_indexes
                .keys()
                .filter(|(d, t, c, _)| *d == db_id && *t == tid_id && c == collection)
                .map(|(_, _, _, f)| f.clone())
                .collect();
            for field in spatial_fields {
                let skey = (db_id, tid_id, collection.to_string(), field.clone());
                if let Some(rtree) = self.spatial_indexes.get_mut(&skey) {
                    rtree.delete(entry_id);
                }
                self.spatial_doc_map.remove(&(
                    db_id,
                    tid_id,
                    collection.to_string(),
                    field,
                    entry_id,
                ));
            }

            // Record deletion for edge referential integrity.
            self.mark_node_deleted(database_id, tid, document_id);
        }

        // Cascade 5 (CORE, UNCONDITIONAL): soft-delete any HNSW vector entries
        // this document produced. Runs for BOTH autocommit and transactional
        // callers — leaving them behind orphans the vector index forever (a
        // deleted doc keeps scoring in KNN). The reverse map `vector_doc_map`
        // was populated by `apply_point_put_vector_indexes` under the same hex
        // surrogate row key used here. Soft-delete (not hard) so a rolled-back
        // transactional delete can `undelete` the exact vector id.
        //
        // The candidate fields are known from the same schema/vector_params
        // enumeration the put path uses, so each `vector_doc_map` entry is
        // looked up by its exact key rather than scanning the whole map on
        // every delete.
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let tid_id = crate::types::TenantId::new(tid);
        let strict_fields = self.strict_vector_fields(tid, collection);
        let candidate_fields: Vec<String> = if !strict_fields.is_empty() {
            strict_fields.into_iter().map(|(name, _dim)| name).collect()
        } else {
            self.schemaless_vector_field_names(database_id, tid, collection)
        };
        let mut vector_deletes = Vec::with_capacity(candidate_fields.len());
        for field in candidate_fields {
            let doc_key = (
                db_id,
                tid_id,
                collection.to_string(),
                field.clone(),
                row_key.to_string(),
            );
            let Some(vector_id) = self.vector_doc_map.remove(&doc_key) else {
                continue;
            };
            let index_key = Self::vector_index_key(database_id, tid, collection, &field);
            if let Some(coll) = self.vector_collections.get_mut(&index_key) {
                coll.delete(vector_id);
            }
            vector_deletes.push((index_key, vector_id));
        }

        // Invalidate document cache.
        self.doc_cache
            .invalidate(database_id, tid, collection, row_key);

        Ok(PointDeleteOutcome {
            prior_value: prior,
            bitemporal_sys_from_ms,
            bitemporal_index_tuples,
            vector_deletes,
        })
    }
}

/// Stateless DELETE enforcement, unified across the autocommit
/// (`apply_point_delete`) and transactional (`tx_point_delete`) paths.
/// These checks have no persistent side effect, so a violation here
/// simply aborts before the write.
fn run_delete_enforcement(
    sparse: &crate::engine::sparse::btree::SparseEngine,
    database_id: u64,
    tid: u64,
    collection: &str,
    config: &crate::engine::document::store::CollectionConfig,
    old_value: Option<&[u8]>,
) -> crate::Result<()> {
    append_only::check_point_delete(collection, &config.enforcement)
        .map_err(map_enforcement_error)?;
    if let Some(ref pl) = config.enforcement.period_lock
        && let Some(old_bytes) = old_value
    {
        period_lock::check_period_lock(sparse, database_id, tid, collection, old_bytes, pl)
            .map_err(map_enforcement_error)?;
    }
    let created_at = old_value.and_then(retention::extract_created_at_secs);
    retention::check_delete_allowed(collection, &config.enforcement, created_at)
        .map_err(map_enforcement_error)?;
    Ok(())
}
