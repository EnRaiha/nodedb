// SPDX-License-Identifier: BUSL-1.1

//! WAL replay for vector engine startup recovery.

use crate::bridge::envelope::{PhysicalPlan, Priority, Request};
use crate::data::executor::task::{ExecutionTask, TaskState};
use crate::types::{DatabaseId, ReadConsistency};

use super::core_loop::CoreLoop;

impl CoreLoop {
    /// Build a synthetic `ExecutionTask` for WAL replay.
    ///
    /// Mirrors the equivalent helper in `timeseries_wal.rs`. The task carries
    /// no meaningful request semantics — it is only needed so that the handler
    /// methods can return a typed `Response`.
    fn replay_vector_task(
        tenant_id: crate::types::TenantId,
        database_id: DatabaseId,
        vshard_id: crate::types::VShardId,
        plan: PhysicalPlan,
    ) -> ExecutionTask {
        ExecutionTask {
            request: Request {
                request_id: crate::types::RequestId::new(0),
                tenant_id,
                database_id,
                vshard_id,
                plan,
                deadline: std::time::Instant::now() + std::time::Duration::from_secs(60),
                priority: Priority::Normal,
                trace_id: crate::types::TraceId::ZERO,
                consistency: ReadConsistency::Strong,
                idempotency_key: None,
                event_source: crate::event::EventSource::User,
                user_roles: Vec::new(),
                user_id: None,
                statement_digest: None,
            },
            state: TaskState::Running,
        }
    }

    /// Replay WAL vector records to rebuild in-memory HNSW indexes after crash.
    ///
    /// Called once during startup, after `open()` but before the event loop.
    /// Processes `VectorPut` and `VectorDelete` records, ignoring records
    /// for other vShards (each core only replays records routed to it).
    ///
    /// Records are replayed in LSN order (WAL guarantees this). For batch
    /// inserts, the payload contains multiple vectors in a single record.
    pub fn replay_vector_wal(
        &mut self,
        records: &[nodedb_wal::WalRecord],
        num_cores: usize,
        tombstones: &nodedb_wal::TombstoneSet,
    ) {
        use crate::engine::vector::collection::VectorCollection;
        use crate::engine::vector::hnsw::HnswParams;
        use nodedb_wal::record::RecordType;

        let mut inserted = 0usize;
        let mut deleted = 0usize;
        let mut skipped = 0usize;

        for record in records {
            let logical_type = record.logical_record_type();

            let record_type = RecordType::from_raw(logical_type);
            let is_vector_put = record_type == Some(RecordType::VectorPut);
            let is_vector_delete = record_type == Some(RecordType::VectorDelete);
            let is_vector_params = record_type == Some(RecordType::VectorParams);
            if !is_vector_put && !is_vector_delete && !is_vector_params {
                continue;
            }

            let vshard_id = record.header.vshard_id as usize;
            let target_core = if num_cores > 0 {
                vshard_id % num_cores
            } else {
                0
            };
            if target_core != self.core_id {
                skipped += 1;
                continue;
            }

            let tenant_id = record.header.tenant_id;
            let database_id = record.header.database_id;
            let record_lsn = record.header.lsn;

            if is_vector_params {
                // Newer records append the vector field name as the 9th
                // element; older records have 8 (quantization params, no field
                // name) or 4 (no quantization params). Try the full shape, fall
                // back to the legacy 4-tuple with the default (unnamed) field.
                let decoded = zerompk::from_msgpack::<(
                    String,
                    usize,
                    usize,
                    String,
                    String,
                    usize,
                    usize,
                    usize,
                    String,
                )>(&record.payload)
                .ok()
                .map(|(c, m, ef, metric, _it, _pq, _ic, _ip, field)| (c, m, ef, metric, field))
                .or_else(|| {
                    zerompk::from_msgpack::<(String, usize, usize, String)>(&record.payload)
                        .ok()
                        .map(|(c, m, ef, metric)| (c, m, ef, metric, String::new()))
                });
                if let Some((collection, m, ef_construction, metric, field_name)) = decoded {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    use crate::engine::vector::distance::DistanceMetric;
                    let metric_enum = match metric.as_str() {
                        "l2" | "euclidean" => DistanceMetric::L2,
                        "cosine" => DistanceMetric::Cosine,
                        "inner_product" | "ip" | "dot" => DistanceMetric::InnerProduct,
                        "manhattan" | "l1" => DistanceMetric::Manhattan,
                        "chebyshev" | "linf" => DistanceMetric::Chebyshev,
                        "hamming" => DistanceMetric::Hamming,
                        "jaccard" => DistanceMetric::Jaccard,
                        "pearson" => DistanceMetric::Pearson,
                        _ => DistanceMetric::Cosine,
                    };
                    let params = HnswParams {
                        m,
                        m0: m * 2,
                        ef_construction,
                        metric: metric_enum,
                        dtype: nodedb_types::vector_dtype::VectorStorageDtype::F32,
                    };
                    self.vector_params.insert(index_key, params);
                    tracing::debug!(
                        core = self.core_id,
                        %collection,
                        field = %field_name,
                        m,
                        ef_construction,
                        %metric,
                        "WAL replay: restored vector params"
                    );
                }
                continue;
            }

            if is_vector_put {
                // Try the newest shape first (7 elements with trailing provenance),
                // then the 5-element shape (surrogate, no provenance),
                // then legacy 3-element shapes. The 7-element arm threads
                // provenance into `execute_vector_insert` so the idempotency
                // gate runs on replay exactly as it does on the live path.
                if let Ok((
                    collection,
                    vector,
                    dim,
                    field_name,
                    doc_id,
                    surrogate_u32,
                    provenance,
                )) = zerompk::from_msgpack::<(
                    String,
                    Vec<f32>,
                    usize,
                    String,
                    Option<String>,
                    u32,
                    Option<nodedb_types::sync::wire::SyncProvenance>,
                )>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
                    // Local replay rebinds by the carried surrogate; the
                    // compat doc-id slot (always `None` on this write path)
                    // maps straight through to `pk_bytes` for fidelity.
                    let pk_bytes = doc_id.as_ref().map(|d| d.as_bytes().to_vec());
                    let vshard = crate::types::VShardId::from_collection_in_database(
                        DatabaseId::new(database_id),
                        &collection,
                    );
                    let task = Self::replay_vector_task(
                        nodedb_types::TenantId::new(tenant_id),
                        DatabaseId::new(database_id),
                        vshard,
                        PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::Insert {
                            collection: collection.clone(),
                            vector: vector.clone(),
                            dim,
                            field_name: field_name.clone(),
                            surrogate,
                            pk_bytes,
                            provenance: provenance.clone(),
                        }),
                    );
                    let response = self.execute_vector_insert(
                        crate::data::executor::handlers::vector::VectorInsertParams {
                            task: &task,
                            tid: tenant_id,
                            collection: &collection,
                            vector: &vector,
                            dim,
                            field_name: &field_name,
                            surrogate,
                            provenance: provenance.as_ref(),
                        },
                    );
                    if response.status != crate::bridge::envelope::Status::Ok {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            lsn = record_lsn,
                            "WAL vector replay: insert handler returned error; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                    inserted += 1;
                } else if let Ok((collection, vector, dim, field_name, doc_id)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize, String, Option<String>)>(
                        &record.payload,
                    )
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let index_key = CoreLoop::vector_index_key(
                        database_id,
                        tenant_id,
                        &collection,
                        &field_name,
                    );
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    if index.dim() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            index_dim = index.dim(),
                            record_dim = dim,
                            "skipping WAL vector record: index dimension mismatch"
                        );
                        continue;
                    }
                    // WAL replay rebinds vectors on the local node;
                    // surrogate identity is restored via the dedicated
                    // `SurrogateBind` replay path. Engine inserts here are
                    // local-id-only and bind to `Surrogate::ZERO`.
                    let _ = doc_id;
                    index.insert_with_surrogate(vector, nodedb_types::Surrogate::ZERO);
                    inserted += 1;
                } else if let Ok((collection, vector, dim)) =
                    zerompk::from_msgpack::<(String, Vec<f32>, usize)>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    if vector.len() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            expected = dim,
                            actual = vector.len(),
                            "skipping WAL vector record: dimension mismatch"
                        );
                        continue;
                    }
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found during WAL replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    if index.dim() != dim {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            index_dim = index.dim(),
                            record_dim = dim,
                            "skipping WAL vector record: index dimension mismatch"
                        );
                        continue;
                    }
                    index.insert(vector);
                    inserted += 1;
                } else if let Ok((collection, vectors, dim)) =
                    zerompk::from_msgpack::<(String, Vec<Vec<f32>>, usize)>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    let index_key =
                        CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                    let params = self
                        .vector_params
                        .get(&index_key)
                        .cloned()
                        .unwrap_or_else(|| {
                            tracing::debug!(
                                core = self.core_id,
                                %collection,
                                "no VectorParams found for batch replay; using defaults"
                            );
                            HnswParams::default()
                        });
                    let index = self
                        .vector_collections
                        .entry(index_key)
                        .or_insert_with(|| VectorCollection::new(dim, params));
                    for vector in vectors {
                        index.insert(vector);
                    }
                    inserted += 1;
                }
            } else if is_vector_delete {
                // Decode order (longest shape first for backward compatibility):
                //
                //   4-element: (collection, surrogate_u32, field_name, Option<SyncProvenance>)
                //     → sync-path delete-by-surrogate; routes through the handler so the
                //       idempotency gate fires on replay.
                //
                //   3-element: (collection, vector_id, Option<SyncProvenance>)
                //     → local delete-by-node-id with provenance (discarded here).
                //
                //   2-element: (collection, vector_id)
                //     → legacy shape; direct node-id deletion.
                if let Ok((collection, surrogate_u32, field_name, provenance)) =
                    zerompk::from_msgpack::<(
                        String,
                        u32,
                        String,
                        Option<nodedb_types::sync::wire::SyncProvenance>,
                    )>(&record.payload)
                {
                    if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                        skipped += 1;
                        continue;
                    }
                    let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
                    let vshard = crate::types::VShardId::from_collection_in_database(
                        DatabaseId::new(database_id),
                        &collection,
                    );
                    let task = Self::replay_vector_task(
                        nodedb_types::TenantId::new(tenant_id),
                        DatabaseId::new(database_id),
                        vshard,
                        PhysicalPlan::Vector(
                            nodedb_physical::physical_plan::VectorOp::DeleteBySurrogate {
                                collection: collection.clone(),
                                surrogate,
                                field_name: field_name.clone(),
                                provenance: provenance.clone(),
                            },
                        ),
                    );
                    let response = self.execute_vector_delete_by_surrogate(
                        &task,
                        tenant_id,
                        &collection,
                        surrogate,
                        &field_name,
                        provenance.as_ref(),
                    );
                    if response.status != crate::bridge::envelope::Status::Ok {
                        tracing::warn!(
                            core = self.core_id,
                            %collection,
                            lsn = record_lsn,
                            "WAL vector replay: delete-by-surrogate handler returned error; skipping"
                        );
                        skipped += 1;
                        continue;
                    }
                    deleted += 1;
                } else {
                    // Legacy: 3-element (with discarded provenance) or 2-element.
                    let delete_decoded = zerompk::from_msgpack::<(
                        String,
                        u32,
                        Option<nodedb_types::sync::wire::SyncProvenance>,
                    )>(&record.payload)
                    .map(|(c, id, _prov)| (c, id))
                    .or_else(|_| zerompk::from_msgpack::<(String, u32)>(&record.payload));
                    if let Ok((collection, vector_id)) = delete_decoded {
                        if tombstones.is_tombstoned(tenant_id, &collection, record_lsn) {
                            skipped += 1;
                            continue;
                        }
                        let index_key =
                            CoreLoop::vector_index_key(database_id, tenant_id, &collection, "");
                        if let Some(index) = self.vector_collections.get_mut(&index_key) {
                            index.delete(vector_id);
                            deleted += 1;
                        }
                    }
                }
            }
        }

        if inserted > 0 || deleted > 0 {
            tracing::info!(
                core = self.core_id,
                inserted,
                deleted,
                skipped,
                collections = self.vector_collections.len(),
                "WAL vector replay complete"
            );
        }
    }
}
