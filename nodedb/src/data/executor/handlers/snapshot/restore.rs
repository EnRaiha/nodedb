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
    ) -> Response {
        info!(core = self.core_id, tenant_id, "restoring tenant snapshot");

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
        let mut ts_written = 0u64;

        {
            // Restore graph edges. Keys are the unscoped
            // `"src\0label\0dst"` form; tenant is supplied from context.
            let tid = crate::types::TenantId::new(tenant_id);
            for (key, props) in &snap.edges {
                if let Err(e) = self.edge_store.put_edge_raw(tid, key, props) {
                    warn!(key, error = %e, "failed to restore edge");
                    continue;
                }
                edges_written += 1;
            }
            // Rebuild CSR from restored edges.
            if edges_written > 0
                && let Ok(rebuilt) =
                    crate::engine::graph::csr::rebuild::rebuild_sharded_from_store(&self.edge_store)
            {
                self.csr = rebuilt;
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
                self.restore_vector_collection(database_id, tenant_id, coll_key, vectors);
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

            // Restore CRDT state.
            for (_key, bytes) in &snap.crdt_state {
                if let Err(e) = self.restore_crdt_state(tenant_id, bytes) {
                    warn!(tenant_id, error = %e, "failed to restore crdt state");
                } else {
                    crdt_written += 1;
                }
            }

            // Restore timeseries memtables.
            for (key, bytes) in &snap.timeseries {
                if let Err(e) = self.restore_timeseries(key, bytes) {
                    warn!(key, error = %e, "failed to restore timeseries");
                } else {
                    ts_written += 1;
                }
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
            ts_written,
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
            "timeseries_restored": ts_written,
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
            self.kv_engine.put(
                database_id,
                tenant_id,
                collection,
                &key,
                &value,
                ttl_ms,
                now_ms,
                nodedb_types::Surrogate::ZERO,
            );
        }
    }

    fn restore_crdt_state(&mut self, tenant_id: u64, bytes: &[u8]) -> crate::Result<()> {
        let tid = crate::types::TenantId::new(tenant_id);
        // If an engine already exists, import into it. Otherwise create a fresh one.
        if let Some(engine) = self.crdt_engines.get(&tid) {
            engine.import_snapshot_bytes(bytes)
        } else {
            let engine = crate::engine::crdt::TenantCrdtEngine::new(
                tid,
                0, // Default peer_id for restore.
                Default::default(),
            )?;
            engine.import_snapshot_bytes(bytes)?;
            self.crdt_engines.insert(tid, engine);
            Ok(())
        }
    }

    fn restore_timeseries(&mut self, key: &str, bytes: &[u8]) -> crate::Result<()> {
        let columns: Vec<(String, Vec<u8>)> =
            zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: e.to_string(),
            })?;
        // Snapshot keys are backward-compatible:
        //   * new format: "{database_id}:{tenant_id}:{collection}"
        //   * legacy format (pre-scoping): "{tenant_id}:{collection}" →
        //     database_id defaults to `DatabaseId::DEFAULT` (0).
        // Re-emit a canonical db-scoped key so restored data lands in the
        // right database namespace regardless of the snapshot's age.
        let (database_id, tenant_id, collection) = parse_timeseries_snapshot_key(key);
        let scoped_key = format!("{database_id}:{tenant_id}:{collection}");
        // Store column data in sparse engine keyed by scoped collection.
        // Timeseries engine will rebuild memtable from these on access.
        for (col_name, col_data) in columns {
            let restore_key = format!("{scoped_key}:{col_name}");
            if let Err(e) = self.sparse.put_raw(&restore_key, &col_data) {
                warn!(restore_key, error = %e, "failed to restore timeseries column");
            }
        }
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
fn parse_timeseries_snapshot_key(key: &str) -> (u64, u64, String) {
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
fn parse_vector_snapshot_key(key: &str, tenant_id: u64) -> (u64, &str) {
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
fn database_id_from_qualified(collection: &str) -> u64 {
    match collection.split_once('/') {
        Some((prefix, _)) => prefix.parse::<u64>().unwrap_or(0),
        None => 0,
    }
}
