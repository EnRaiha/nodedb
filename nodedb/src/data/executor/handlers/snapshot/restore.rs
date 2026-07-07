// SPDX-License-Identifier: BUSL-1.1

//! Tenant snapshot restoration: import Data Plane state for all engines.

use tracing::{info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Restore a tenant's data across ALL engines from a snapshot.
    ///
    /// `documents_bytes` carries a MessagePack-serialized
    /// `TenantDataSnapshot` — the full per-tenant snapshot with
    /// documents, indexes, edges, vectors, KV, CRDT, and timeseries.
    pub(in crate::data::executor) fn execute_restore_tenant_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        snapshot_bytes: &[u8],
        replace_mode: bool,
        // Carried for symmetry with the applier; the per-collection list below
        // drives the actual clear. The applier populates this from the catalog.
        _clear_vshards: &[u32],
        collections_to_clear: &[(u64, String)],
    ) -> Response {
        info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

        // Clear-then-install: drop stale state for the listed collections before
        // installing, so keys deleted before the snapshot index and dropped
        // collections do not linger on a lagging follower. Empty list = no-op.
        for (tid_raw, coll) in collections_to_clear {
            // Preserve the collection definition: clear-then-install replaces row
            // data from the snapshot, but the snapshot does not carry the schema,
            // so the reinstalled rows must land in the still-defined collection.
            // Fail-closed: if stale state cannot be cleared, abort the restore
            // rather than install the snapshot over rows that survived — those
            // would linger as un-owned data on this follower.
            if let Err(e) = self.clear_collection_all_engines(
                nodedb_types::DatabaseId::DEFAULT,
                crate::types::TenantId::new(*tid_raw),
                coll,
                true,
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("clear-then-install purge failed for '{coll}': {e}"),
                    },
                );
            }
        }

        let snap: crate::types::TenantDataSnapshot = match zerompk::from_msgpack(snapshot_bytes) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("malformed tenant snapshot: {e}"),
                    },
                );
            }
        };

        let (docs_written, indexes_written) =
            self.restore_sparse(tenant_id, &snap.documents, &snap.indexes);

        let mut edges_written = 0u64;
        let mut vectors_written = 0u64;
        let mut kv_written = 0u64;
        let mut crdt_written = 0u64;
        let mut crdt_constraints_written = 0u64;
        let mut ts_written = 0u64;

        {
            // Restore graph edges. Keys are the versioned form
            // `"{collection}\x00{src}\x00{label}\x00{dst}\x00{system_from:020}"`;
            // tenant is supplied from context.
            let tid = crate::types::TenantId::new(tenant_id);
            let database_id = task.request.database_id.as_u64();
            for (key, props) in &snap.edges {
                if let Err(e) = self.edge_store.put_edge_raw(database_id, tid, key, props) {
                    warn!(key, error = %e, "failed to restore edge");
                    continue;
                }
                edges_written += 1;
            }
            // Restore tenant-aware edges from the multi-tenant merged Raft
            // snapshot. The edge key does NOT carry the tenant, so each entry
            // carries its owning `tid` explicitly — install it under THAT tenant
            // rather than the dispatch-context tenant (which is 0 for the merged
            // group snapshot). Shares `edges_written` with the legacy loop so the
            // CSR rebuild below runs if EITHER source installed edges.
            for (tid_raw, key, props) in &snap.tenant_edges {
                let edge_tid = crate::types::TenantId::new(*tid_raw);
                if let Err(e) = self
                    .edge_store
                    .put_edge_raw(database_id, edge_tid, key, props)
                {
                    warn!(key, error = %e, "failed to restore tenant edge");
                    continue;
                }
                edges_written += 1;
            }
            // Rebuild CSR from restored edges. A rebuild failure is fatal to the
            // whole restore: leaving the stale CSR in place would make graph
            // traversals silently return wrong results over the just-installed
            // edges — the same silent-corruption class the durable-section
            // failures above treat as fatal.
            if edges_written > 0 {
                match crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(
                    &self.edge_store,
                ) {
                    Ok(rebuilt) => self.csr = rebuilt,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!(
                                    "restore: CSR rebuild after edge install failed: {e}"
                                ),
                            },
                        );
                    }
                }
            }

            // Restore vector_params: re-populate HnswParams before the vector
            // collection restore so `restore_vector_collection` finds real params
            // instead of falling back to `HnswParams::default()`.
            for (key, bytes) in &snap.vector_params {
                let params: crate::engine::vector::hnsw::HnswParams =
                    match zerompk::from_msgpack(bytes) {
                        Ok(p) => p,
                        Err(e) => {
                            warn!(key, error = %e, "failed to decode vector_params snapshot entry");
                            continue;
                        }
                    };
                let (vp_db, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(vp_db),
                    crate::types::TenantId::new(tenant_id),
                    coll_key.to_string(),
                );
                self.vector_params.insert(map_key, params);
            }

            // Restore index_configs: re-populate IndexConfig before the vector
            // collection restore so index routing uses the correct type.
            for (key, bytes) in &snap.index_configs {
                let cfg: crate::engine::vector::index_config::IndexConfig =
                    match zerompk::from_msgpack(bytes) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!(key, error = %e, "failed to decode index_configs snapshot entry");
                            continue;
                        }
                    };
                let (ic_db, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                let map_key = (
                    nodedb_types::DatabaseId::new(ic_db),
                    crate::types::TenantId::new(tenant_id),
                    coll_key.to_string(),
                );
                self.index_configs.insert(map_key, cfg);
            }

            // Restore vector collections.
            // Snapshot keys are `"{db}:{tid}:{coll_key}"` (new format) or, for
            // legacy snapshots, `"{tid}:{coll_key}"`. Parse the leading numeric
            // components back-compatibly; `coll_key` may itself contain `:`.
            for (key, bytes) in &snap.vectors {
                let vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)> =
                    match zerompk::from_msgpack(bytes) {
                        Ok(v) => v,
                        Err(e) => {
                            warn!(key, error = %e, "failed to decode vector snapshot");
                            continue;
                        }
                    };
                let count = vectors.len() as u64;
                let (database_id, coll_key) = parse_vector_snapshot_key(key, tenant_id);
                self.restore_vector_collection(
                    database_id,
                    tenant_id,
                    coll_key,
                    vectors,
                    replace_mode,
                );
                vectors_written += count;
            }

            // Restore KV tables.
            for (collection_name, bytes) in &snap.kv_tables {
                let entries: Vec<(Vec<u8>, Vec<u8>, u64)> = match zerompk::from_msgpack(bytes) {
                    Ok(e) => e,
                    Err(e) => {
                        warn!(collection_name, error = %e, "failed to decode kv snapshot");
                        continue;
                    }
                };
                let count = entries.len() as u64;
                self.restore_kv_table(tenant_id, collection_name, entries);
                kv_written += count;
            }

            // Restore CRDT state per collection (tenant carried explicitly so
            // both the per-group Raft snapshot, whose merged blob dispatches
            // with tenant 0, and the per-tenant user RESTORE path route the same
            // way). Loro import is a monotonic CRDT merge, so no replace_mode
            // handling is needed: the snapshot is >= the follower's committed
            // state and the merge converges to the correct result.
            for (tid_raw, collection, bytes) in &snap.crdt_state {
                if let Err(e) = self.restore_crdt_state(*tid_raw, collection, bytes) {
                    warn!(tid_raw, %collection, error = %e, "failed to restore crdt state");
                } else {
                    crdt_written += 1;
                }
            }

            // Restore CRDT constraint state per collection: reconstructs the
            // validator's installed constraint set + `installed_constraint_version`
            // so a snapshot-installed follower does not come up empty and
            // retry-fence every peer delta on constrained collections. Fail-safe
            // on error — warn and continue, matching the `crdt_state` loop, since
            // a failed reconstruction only reverts to the pre-fix (over-rejecting)
            // behavior rather than corrupting state.
            for (tid_raw, collection, version, encoded) in &snap.crdt_constraints {
                if let Err(e) =
                    self.restore_crdt_constraints(*tid_raw, collection, *version, encoded)
                {
                    warn!(tid_raw, %collection, error = %e, "failed to restore crdt constraints");
                } else {
                    crdt_constraints_written += 1;
                }
            }

            // Restore timeseries memtables and flush each to an on-disk segment
            // for durability. A flush failure is fatal to the whole restore —
            // consistent with how `restore_flushed_ts_segments` treats durability
            // errors — because partial restore with non-durable data is worse than
            // a clean failure the operator can retry.
            for (key, bytes) in &snap.timeseries {
                if let Err(e) = self.restore_timeseries(key, bytes) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("restore: timeseries collection {key} failed: {e}"),
                        },
                    );
                }
                ts_written += 1;
            }

            // Restore flushed on-disk timeseries segments.
            if !snap.flushed_ts_segments.is_empty()
                && let Err(e) =
                    self.restore_flushed_ts_segments(&snap.flushed_ts_segments, replace_mode)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: flushed ts segment restore failed: {e}"),
                    },
                );
            }

            // Restore plain-columnar engines.
            if !snap.columnar_engines.is_empty()
                && let Err(e) = self.restore_columnar_engines(&snap.columnar_engines, replace_mode)
            {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("restore: columnar engine restore failed: {e}"),
                    },
                );
            }
        }

        info!(
            tenant_id,
            docs_written,
            indexes_written,
            edges_written,
            vectors_written,
            kv_written,
            crdt_written,
            crdt_constraints_written,
            ts_written,
            flushed_ts_collections = snap.flushed_ts_segments.len(),
            columnar_engines = snap.columnar_engines.len(),
            "full tenant snapshot restored"
        );

        let result = serde_json::json!({
            "tenant_id": tenant_id,
            "documents_restored": docs_written,
            "indexes_restored": indexes_written,
            "edges_restored": edges_written,
            "vectors_restored": vectors_written,
            "kv_entries_restored": kv_written,
            "crdt_restored": crdt_written,
            "crdt_constraints_restored": crdt_constraints_written,
            "timeseries_restored": ts_written,
            "columnar_engines_restored": snap.columnar_engines.len(),
        });
        match crate::data::executor::response_codec::encode_json(&result) {
            Ok(p) => self.response_with_payload(task, p),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("result serialization failed: {e}"),
                },
            ),
        }
    }

    fn restore_sparse(
        &self,
        _tenant_id: u64,
        documents: &[(String, Vec<u8>)],
        indexes: &[(String, Vec<u8>)],
    ) -> (u64, u64) {
        let mut docs_written = 0u64;
        for (key, value) in documents {
            if let Err(e) = self.sparse.put_raw(key, value) {
                warn!(key, error = %e, "failed to restore document");
                continue;
            }
            docs_written += 1;
        }
        let mut indexes_written = 0u64;
        for (key, value) in indexes {
            if let Err(e) = self.sparse.put_index_raw(key, value) {
                warn!(key, error = %e, "failed to restore index");
                continue;
            }
            indexes_written += 1;
        }
        (docs_written, indexes_written)
    }

    fn restore_vector_collection(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        coll_key: &str,
        vectors: Vec<(u32, Vec<f32>, Option<nodedb_types::Surrogate>)>,
        replace_mode: bool,
    ) {
        if vectors.is_empty() {
            return;
        }
        let dim = vectors[0].1.len();
        let map_key = (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tenant_id),
            coll_key.to_string(),
        );
        let params = self
            .vector_params
            .get(&map_key)
            .cloned()
            .unwrap_or_default();
        // Raft InstallSnapshot apply (`replace_mode`) must REPLACE the local
        // collection so the snapshot's vectors are not appended on top of stale
        // entries. User RESTORE (`!replace_mode`) keeps the prior insert-into-
        // existing-or-create behavior.
        if replace_mode {
            self.vector_collections.insert(
                map_key.clone(),
                crate::engine::vector::collection::VectorCollection::new(dim, params.clone()),
            );
        }
        let coll = self.vector_collections.entry(map_key).or_insert_with(|| {
            crate::engine::vector::collection::VectorCollection::new(dim, params)
        });
        for (_, data, surrogate) in vectors {
            coll.insert_with_surrogate(data, surrogate.unwrap_or(nodedb_types::Surrogate::ZERO));
        }
    }

    fn restore_kv_table(
        &mut self,
        tenant_id: u64,
        collection: &str,
        entries: Vec<(Vec<u8>, Vec<u8>, u64)>,
    ) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        // The snapshot stores the db-qualified collection name (e.g. "2/orders"
        // for database 2; bare name for the default database). Recover the
        // database id from that prefix so the restored hash key matches the one
        // live reads compute from the same (database_id, qualified collection).
        let database_id = database_id_from_qualified(collection);
        for (key, value, expire_at) in entries {
            let ttl_ms = if expire_at > now_ms {
                expire_at - now_ms
            } else if expire_at == 0 {
                0
            } else {
                continue; // Already expired.
            };
            self.kv_engine.put(crate::engine::kv::KvPutParams {
                database_id,
                tenant_id,
                collection,
                key: &key,
                value: &value,
                ttl_ms,
                now_ms,
                surrogate: nodedb_types::Surrogate::ZERO,
            });
        }
    }

    fn restore_crdt_state(
        &mut self,
        tenant_id: u64,
        collection: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        // Lazily create the tenant engine if absent, then import into the
        // target collection's per-collection LoroDoc.
        let engine = self.get_crdt_engine(tid)?;
        engine.import_snapshot_bytes(collection, bytes)
    }

    /// Reconstructs a collection's installed constraint set + version from a
    /// snapshot entry. Version-fenced via `set_collection_constraints`
    /// (`>=`), so this is idempotent against later replay/reconcile.
    fn restore_crdt_constraints(
        &mut self,
        tenant_id: u64,
        collection: &str,
        constraint_version: u64,
        encoded: &[Vec<u8>],
    ) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        let engine = self.get_crdt_engine(tid)?;
        let mut constraints = Vec::with_capacity(encoded.len());
        for blob in encoded {
            let c: nodedb_crdt::Constraint =
                zerompk::from_msgpack(blob).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: e.to_string(),
                })?;
            constraints.push(c);
        }
        engine.set_collection_constraints(collection, constraint_version, constraints);
        Ok(())
    }

    fn restore_timeseries(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
        use crate::engine::timeseries::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig, MemtableSnapshot,
        };

        let snap: MemtableSnapshot =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: e.to_string(),
            })?;

        // Parse key: "{database_id}:{tenant_id}:{collection}" (canonical).
        // Legacy 2-part key ("{tenant_id}:{collection}") and bare keys are
        // handled by `parse_timeseries_snapshot_key`.
        let (database_id, tenant_id, collection) = parse_timeseries_snapshot_key(key);

        let mt = ColumnarMemtable::from_snapshot(snap, ColumnarMemtableConfig::default())?;

        let tid = crate::types::TenantId::new(tenant_id);
        let db_id = nodedb_types::DatabaseId::new(database_id);
        let map_key = (db_id, tid, collection.clone());
        self.columnar_memtables.insert(map_key, mt);

        // Persist the restored memtable to an on-disk segment immediately so
        // timeseries data is durable across restart. Uses a wall-clock timestamp
        // (same source as the idle-flush path in maintenance.rs) because there
        // is no Calvin epoch in a restore context.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        // Propagate the flush error directly — flush_ts_collection already
        // wraps the underlying I/O error in crate::Error::Storage with the
        // collection name included.
        self.flush_ts_collection(tid, db_id, &collection, now_ms)?;

        Ok(())
    }
}

/// Parse a timeseries snapshot key into `(database_id, tenant_id, collection)`.
///
/// Backward-compatible with pre-scoping snapshots:
///   * 3+ parts → `database:tenant:collection` (collection may itself contain
///     ':' — only the first two ':' are structural).
///   * 2 parts → legacy `tenant:collection`; database defaults to 0
///     (`DatabaseId::DEFAULT`).
///   * 1 part → bare collection; database and tenant default to 0.
pub(super) fn parse_timeseries_snapshot_key(key: &str) -> (u64, u64, String) {
    let mut it = key.splitn(3, ':');
    let first = it.next().unwrap_or("");
    let second = it.next();
    let third = it.next();
    match (second, third) {
        (Some(tenant), Some(collection)) => {
            let db = first.parse::<u64>().unwrap_or(0);
            let tid = tenant.parse::<u64>().unwrap_or(0);
            (db, tid, collection.to_string())
        }
        (Some(collection), None) => {
            // Legacy 2-part key: "{tenant}:{collection}".
            let tid = first.parse::<u64>().unwrap_or(0);
            (0, tid, collection.to_string())
        }
        _ => (0, 0, first.to_string()),
    }
}

/// Parse a vector snapshot key into `(database_id, collection_key)`.
///
/// Backward-compatible with pre-scoping snapshots:
///   * 3 parts where the first two are BOTH numeric → `db:tenant:coll_key`
///     (new format; `coll_key` may itself contain ':').
///   * otherwise → legacy `tenant:coll_key`; strip the leading numeric tenant
///     component and default the database to 0 (`DatabaseId::DEFAULT`).
///
/// `coll_key` is returned as a borrowed slice of `key` so the caller can pass
/// it straight into `restore_vector_collection` without an allocation.
pub(super) fn parse_vector_snapshot_key(key: &str, tenant_id: u64) -> (u64, &str) {
    let mut it = key.splitn(3, ':');
    let first = it.next().unwrap_or("");
    let second = it.next();
    let third = it.next();
    if let (Some(second), Some(_)) = (second, third)
        && let (Ok(db), Ok(_tid)) = (first.parse::<u64>(), second.parse::<u64>())
    {
        // New format: "{db}:{tid}:{coll_key}".
        let prefix_len = first.len() + 1 + second.len() + 1;
        return (db, &key[prefix_len..]);
    }
    // Legacy 2-part key "{tid}:{coll_key}" (or a bare key); strip the tenant
    // prefix if present and default the database to 0.
    let tid_prefix = format!("{tenant_id}:");
    let coll_key = key.strip_prefix(&tid_prefix).unwrap_or(key);
    (0, coll_key)
}

/// Recover the database id encoded in a db-qualified collection name.
///
/// Non-default databases qualify their collections as `"{database_id}/{name}"`;
/// the default database uses the bare name. A bare name (no leading numeric
/// segment before a `/`) maps to `DatabaseId::DEFAULT` (0).
pub(super) fn database_id_from_qualified(collection: &str) -> u64 {
    match collection.split_once('/') {
        Some((prefix, _)) => prefix.parse::<u64>().unwrap_or(0),
        None => 0,
    }
}
