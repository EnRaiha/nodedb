// SPDX-License-Identifier: BUSL-1.1

//! Snapshot, checkpoint, WAL append, cancel, range scan, and collection policy handlers.

use sonic_rs;
use tracing::{debug, info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::types::RequestId;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for a document range scan (`field` within `[lower, upper)`).
pub(in crate::data::executor) struct RangeScanArgs<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub lower: Option<&'a [u8]>,
    pub upper: Option<&'a [u8]>,
    pub limit: usize,
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_wal_append(
        &self,
        task: &ExecutionTask,
        payload: &[u8],
    ) -> Response {
        debug!(core = self.core_id, len = payload.len(), "wal append");
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_cancel(
        &mut self,
        task: &ExecutionTask,
        target_request_id: RequestId,
    ) -> Response {
        debug!(core = self.core_id, %target_request_id, "cancel");
        let pos = self
            .task_queue
            .iter()
            .position(|t| t.request_id() == target_request_id);
        if let Some(pos) = pos {
            self.task_queue.remove(pos);
        }
        self.response_ok(task)
    }

    /// Read the conflict resolution policy for a collection and return it as JSON.
    /// Returns the ephemeral default when no explicit policy has been registered.
    pub(in crate::data::executor) fn execute_get_collection_policy(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "get collection policy");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine for get policy");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let policy = engine.get_collection_policy(collection);
        match sonic_rs::to_string(&policy) {
            Ok(json) => self.response_with_payload(task, json.into_bytes()),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("failed to serialize policy: {e}"),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_set_collection_policy(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        policy_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "set collection policy");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.set_collection_policy(collection, policy_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "set collection policy failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_range_scan(
        &mut self,
        task: &ExecutionTask,
        args: RangeScanArgs<'_>,
    ) -> Response {
        // Bitemporal collections keep every write on the versioned redb table;
        // their plain INDEXES / DOCUMENTS tables are empty, so the index probe
        // + plain-table fallback below would return ZERO rows. Route them to
        // the versioned current-state scan instead (full-scan + range filter,
        // matching `execute_document_scan`).
        if self.is_bitemporal(args.tid, args.collection) {
            return self.execute_range_scan_bitemporal(task, args);
        }

        let RangeScanArgs {
            tid,
            collection,
            field,
            lower,
            upper,
            limit,
        } = args;
        debug!(core = self.core_id, %collection, %field, limit, "range scan");

        // Try index-backed range scan first.
        let results = match self.sparse.range_scan(
            task.request.database_id.as_u64(),
            tid,
            collection,
            field,
            lower,
            upper,
            limit,
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "sparse range scan failed");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // If the index returned nothing, fall back to full scan + sort.
        // This handles collections without a secondary index on `field`.
        if results.is_empty() {
            let scan_result = self.scan_collection(
                task.request.database_id.as_u64(),
                tid,
                collection,
                limit.max(1000),
            );
            match scan_result {
                Ok(mut docs) => {
                    if let Err(e) = super::super::document::sort::sort_rows(
                        &mut docs,
                        &[(field.to_string(), true)],
                    ) {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("in-memory sort failed: {e}"),
                            },
                        );
                    }
                    docs.truncate(limit);
                    // Raw msgpack passthrough — no decode/re-encode.
                    let rows: Vec<_> = docs
                        .iter()
                        .map(|(id, val)| {
                            let mp = super::super::super::doc_format::json_to_msgpack(val);
                            (id.clone(), mp)
                        })
                        .collect();
                    match super::super::super::response_codec::encode_raw_document_rows(&rows) {
                        Ok(payload) => return self.response_with_payload(task, payload),
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: e.to_string(),
                                },
                            );
                        }
                    }
                }
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        }

        match super::super::super::response_codec::encode(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "range scan serialization failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Execute a snapshot creation request: export all engine state as bytes.
    ///
    /// Returns the serialized `CoreSnapshot` as the response payload.
    /// The Control Plane collects these from all cores and writes to disk.
    pub(in crate::data::executor) fn execute_create_snapshot(
        &self,
        task: &ExecutionTask,
    ) -> Response {
        match self.export_snapshot() {
            Ok(snapshot) => match snapshot.to_bytes() {
                Ok(bytes) => {
                    info!(
                        core = self.core_id,
                        watermark = snapshot.watermark,
                        documents = snapshot.sparse_documents.len(),
                        vectors = snapshot.hnsw_indexes.len(),
                        size_bytes = bytes.len(),
                        "snapshot exported"
                    );
                    self.response_with_payload(task, bytes)
                }
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "snapshot serialization failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            },
            Err(e) => {
                warn!(core = self.core_id, error = %e, "snapshot export failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Execute a coordinated checkpoint: flush all engine state to disk
    /// and return this core's checkpoint LSN.
    ///
    /// 1. Checkpoint vector indexes (HNSW segments → disk files).
    /// 2. Export CRDT snapshots (Loro docs → disk files).
    /// 3. redb sparse engine is already ACID — no action needed.
    /// 4. CSR is rebuilt from redb edge store on startup — no action needed.
    /// 5. Return the core's watermark LSN as the checkpoint point.
    pub(in crate::data::executor) fn execute_checkpoint(
        &mut self,
        task: &ExecutionTask,
    ) -> Response {
        let checkpoint_lsn = self.watermark.as_u64();

        // 1. Flush vector indexes to disk.
        let vectors_checkpointed = self.checkpoint_vector_indexes();

        // 2. Flush CRDT snapshots to disk.
        let crdts_checkpointed = self.checkpoint_crdt_engines();

        // 3. Flush spatial R-tree indexes to disk.
        let spatial_checkpointed = self.checkpoint_spatial_indexes();

        // 4. Compact CSR write buffers into dense arrays for clean state.
        if let Err(e) = self.csr.compact_all() {
            tracing::warn!(error = %e, "CSR compaction rejected by memory governor during snapshot; skipping");
        }

        // 5. Record completed flushes in the checkpoint coordinator
        //    and advance the checkpoint LSN for WAL truncation safety.
        self.checkpoint_coordinator
            .record_flush("vector", vectors_checkpointed);
        self.checkpoint_coordinator
            .record_flush("crdt", crdts_checkpointed);
        self.checkpoint_coordinator
            .record_flush("spatial", spatial_checkpointed);
        self.checkpoint_coordinator
            .complete_checkpoint(checkpoint_lsn);

        info!(
            core = self.core_id,
            checkpoint_lsn,
            vectors_checkpointed,
            crdts_checkpointed,
            spatial_checkpointed,
            dirty_pages = self.checkpoint_coordinator.total_dirty_pages(),
            "core checkpoint complete"
        );

        // Return the checkpoint LSN as the response payload.
        let payload = checkpoint_lsn.to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }

    /// Checkpoint all CRDT tenant engines to disk.
    ///
    /// Each tenant's Loro state is exported per collection and written to
    /// `{data_dir}/crdt-ckpt/core-{core_id}/tenant-{tid}-coll-{hex(collection)}.ckpt`
    /// with atomic temp+rename. The per-core subdir is required because `data_dir` is
    /// shared across all cores and a tenant's CRDT state is fragmented across
    /// cores by collection — without the subdir, cores would race-overwrite
    /// the same file and persist only a partial fragment.
    ///
    /// Called from both `snapshot.rs` (explicit checkpoint command) and
    /// `compact.rs` (periodic maintenance via `maybe_run_maintenance`).
    pub(in crate::data::executor) fn checkpoint_crdt_engines(&self) -> usize {
        if self.crdt_engines.is_empty() {
            return 0;
        }

        let ckpt_dir =
            crate::data::executor::crdt_checkpoint::crdt_ckpt_dir(&self.data_dir, self.core_id);
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            warn!(core = self.core_id, "failed to create CRDT checkpoint dir");
            return 0;
        }

        let mut checkpointed = 0;
        for (tenant_id, engine) in &self.crdt_engines {
            let tid = tenant_id.as_u64();
            // One checkpoint file per (tenant, collection) — each collection
            // owns its own LoroDoc. Filenames are
            // `tenant-{id}-coll-{hex(collection)}.ckpt`, matching the loader's
            // parse and the cluster-restore writer.
            let snapshots = match engine.export_all_snapshots() {
                Ok(s) => s,
                Err(e) => {
                    warn!(
                        core = self.core_id,
                        tenant = tid,
                        error = %e,
                        "CRDT checkpoint export failed"
                    );
                    continue;
                }
            };
            for (collection, snapshot) in snapshots {
                if snapshot.is_empty() {
                    continue;
                }
                let fname =
                    crate::data::executor::crdt_checkpoint::crdt_ckpt_filename(tid, &collection);
                let ckpt_path = ckpt_dir.join(&fname);
                let tmp_path = ckpt_dir.join(format!("{fname}.tmp"));
                if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &snapshot).is_ok()
                {
                    checkpointed += 1;
                }
            }
        }

        if checkpointed > 0 {
            info!(
                core = self.core_id,
                checkpointed,
                total = self.crdt_engines.len(),
                "CRDT engines checkpointed"
            );
        }
        checkpointed
    }
}
