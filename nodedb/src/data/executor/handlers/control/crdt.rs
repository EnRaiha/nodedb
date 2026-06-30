// SPDX-License-Identifier: BUSL-1.1

//! CRDT operation handlers: read, versioned read, version vector, delta export,
//! restore, compact, list insert/delete/move, and apply.

use tracing::{debug, warn};

use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::sync_gate::SyncAdmit;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_crdt_read(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read");
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
        match engine.read_snapshot(collection, document_id) {
            Ok(Some(snapshot)) => self.response_with_payload(task, snapshot),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt read snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Read a CRDT document at a historical version.
    pub(in crate::data::executor) fn execute_crdt_read_at_version(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        version_vector_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.read_at_version_json(collection, document_id, version_vector_json) {
            Ok(Some(json_bytes)) => self.response_with_payload(task, json_bytes),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Get the current CRDT version vector.
    pub(in crate::data::executor) fn execute_crdt_get_version_vector(
        &mut self,
        task: &ExecutionTask,
        _collection: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.version_vector_json() {
            Ok(json) => self.response_with_payload(task, json.into_bytes()),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Export CRDT delta from a version to current.
    pub(in crate::data::executor) fn execute_crdt_export_delta(
        &mut self,
        task: &ExecutionTask,
        _collection: &str,
        from_version_json: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.export_delta(from_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Restore a CRDT document to a historical version.
    pub(in crate::data::executor) fn execute_crdt_restore(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt restore");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.restore_to_version(collection, document_id, target_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Compact CRDT history at a specific version.
    pub(in crate::data::executor) fn execute_crdt_compact(
        &mut self,
        task: &ExecutionTask,
        _collection: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, "crdt compact at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.compact_at_version(target_version_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Insert a block (LoroMap) into a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_insert(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
        fields_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list insert");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        let doc = engine.state().doc();

        // Parse fields and insert as LoroMap container.
        let map = match nodedb_crdt::list_ops::list_insert_container(
            doc,
            collection,
            document_id,
            list_path,
            index,
        ) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Populate fields from JSON.
        if let Ok(fields) =
            sonic_rs::from_str::<serde_json::Map<String, serde_json::Value>>(fields_json)
        {
            for (key, val) in &fields {
                let loro_val = super::convert::json_to_loro_value(val);
                if let Err(e) = map.insert(key, loro_val) {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            }
        }

        self.response_ok(task)
    }

    /// Delete a block from a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_delete(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, index, "crdt list delete");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match nodedb_crdt::list_ops::list_delete(
            engine.state().doc(),
            collection,
            document_id,
            list_path,
            index,
        ) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Move a block within a document's block list.
    pub(in crate::data::executor) fn execute_crdt_list_move(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, list_path, from_index, to_index, "crdt list move");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match nodedb_crdt::list_ops::list_move(
            engine.state().doc(),
            collection,
            document_id,
            list_path,
            from_index,
            to_index,
        ) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    pub(in crate::data::executor) fn execute_crdt_apply(
        &mut self,
        task: &ExecutionTask,
        _collection: &str,
        delta: &[u8],
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        let tenant_id = task.request.tenant_id;

        let Some(prov) = provenance else {
            // Non-sync path (SQL / native client): apply unconditionally, no gate.
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
            return match engine.apply_committed_delta(delta) {
                Ok(()) => {
                    self.checkpoint_coordinator.mark_dirty("crdt", 1);
                    self.response_ok(task)
                }
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "crdt apply failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            };
        };

        // Sync path: run the idempotency gate before touching the engine.
        // Call sync_admit first (exclusive &mut self borrow, no engine borrow).
        let admit = self.sync_admit(prov);

        // Snapshot the current HWM for Duplicate / Fenced / Gap responses
        // before any engine borrow.
        let current_hwm = self.sync_hwm_value(prov.producer_id, prov.stream_id);

        let (status, applied_seq) = match admit {
            SyncAdmit::Apply => {
                // Borrow the engine in a nested block so the &mut borrow is
                // dropped before sync_commit takes &mut self for sync_hwm.
                let apply_result = {
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
                    engine.apply_committed_delta(delta)
                };
                match apply_result {
                    Ok(()) => {
                        self.checkpoint_coordinator.mark_dirty("crdt", 1);
                    }
                    Err(e) => {
                        warn!(core = self.core_id, error = %e, "crdt apply failed");
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: e.to_string(),
                            },
                        );
                    }
                }
                // Advance the HWM only after the engine apply succeeds.
                // engine borrow is already dropped at this point.
                self.sync_commit(prov);
                (AckStatus::Applied, prov.seq)
            }
            SyncAdmit::Duplicate => (AckStatus::Duplicate, current_hwm),
            SyncAdmit::Fenced => (AckStatus::Fenced, current_hwm),
            SyncAdmit::Gap { expected } => (AckStatus::Gap { expected }, current_hwm),
        };

        self.sync_ack_response(task, status, applied_seq)
    }

    /// Import a full whole-tenant Loro snapshot into the tenant CRDT engine.
    ///
    /// The durable RESTORE re-issue path replicates this through Raft so every
    /// replica of the data group lands the same snapshot. `import_snapshot_bytes`
    /// is a monotonic, idempotent, commutative Loro merge, so applying the same
    /// bytes on every replica converges deterministically — there is no sync
    /// idempotency gate and no per-document surrogate to bind.
    pub(in crate::data::executor) fn execute_crdt_import_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        bytes: &[u8],
    ) -> Response {
        let tid = crate::types::TenantId::new(tenant_id);
        debug!(core = self.core_id, %tid, "crdt import snapshot");
        let engine = match self.get_crdt_engine(tid) {
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
        match engine.import_snapshot_bytes(bytes) {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.response_ok(task)
            }
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt import snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Read the current HWM for a `(producer_id, stream_id)` pair without
    /// advancing it. Returns `0` when no frame from this producer has been
    /// committed on this stream yet.
    pub(in crate::data::executor) fn sync_hwm_value(
        &self,
        producer_id: u64,
        stream_id: u64,
    ) -> u64 {
        *self.sync_hwm.get(&(producer_id, stream_id)).unwrap_or(&0)
    }
}
