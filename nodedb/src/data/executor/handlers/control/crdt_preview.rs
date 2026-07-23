// SPDX-License-Identifier: BUSL-1.1

//! Read-only bounded CRDT delta preview handler.

use nodedb_types::CrdtPreviewResult;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Preview a delta without creating tenant or collection state and without
    /// touching HWM, DLQ, checkpoints, sparse storage, or WAL state.
    pub(in crate::data::executor) fn execute_crdt_preview_apply(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        delta: &[u8],
    ) -> Response {
        let key = (task.request.database_id, task.request.tenant_id);
        let preview = if let Some(engine) = self.crdt_engines.get(&key) {
            engine
                .preview_delta(collection, document_id, delta)
                .map(|preview| {
                    (
                        preview,
                        engine.frontier_digest(task.request.database_id, collection),
                    )
                })
        } else {
            let engine = match crate::engine::crdt::tenant_state::TenantCrdtEngine::new(
                task.request.tenant_id,
                self.core_id as u64,
                nodedb_crdt::ConstraintSet::new(),
            ) {
                Ok(engine) => engine,
                Err(error) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: error.to_string(),
                        },
                    );
                }
            };
            engine
                .preview_delta(collection, document_id, delta)
                .map(|preview| {
                    (
                        preview,
                        engine.frontier_digest(task.request.database_id, collection),
                    )
                })
        };
        let (preview, frontier_digest) = match preview {
            Ok(preview) => preview,
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: error.to_string(),
                    },
                );
            }
        };
        let imported_ops = match u64::try_from(preview.imported_ops) {
            Ok(imported_ops) => imported_ops,
            Err(_) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "CRDT preview operation count exceeds wire range".into(),
                    },
                );
            }
        };
        let result = CrdtPreviewResult {
            post_image_msgpack: preview.post_image_msgpack,
            imported_ops,
            frontier_digest,
        };
        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("CRDT preview response encode: {error}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{DatabaseId, Surrogate, TenantId, Value};

    use super::*;
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::document::store::surrogate_to_doc_id;

    fn task() -> ExecutionTask {
        crate::data::executor::core_loop::tests::make_default_task()
    }

    fn snapshot_delta(collection: &str, row: &str, value: &str) -> Vec<u8> {
        let source = nodedb_crdt::CrdtState::new(42).expect("source state");
        source
            .upsert(
                collection,
                row,
                &[("value", LoroValue::String(value.into()))],
            )
            .expect("source write");
        source.export_snapshot().expect("source snapshot")
    }

    fn preview_response(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        collection: &str,
        row: &str,
        delta: &[u8],
    ) -> Response {
        core.execute_crdt_preview_apply(task, collection, row, delta)
    }

    #[test]
    fn preview_missing_collection_returns_typed_post_image_without_residue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let delta = snapshot_delta("docs", "one", "value");
        let dirty_before = core.checkpoint_coordinator.total_dirty_pages();

        let response = preview_response(&mut core, &task, "docs", "one", &delta);
        assert_eq!(response.status, Status::Ok);
        let result: CrdtPreviewResult =
            zerompk::from_msgpack(response.payload.as_bytes()).expect("typed preview response");
        let post_image: Option<Value> =
            zerompk::from_msgpack(&result.post_image_msgpack).expect("typed post image");
        let Some(Value::Object(fields)) = post_image else {
            panic!("preview must return the target object");
        };
        assert_eq!(fields.get("value"), Some(&Value::String("value".into())));
        assert!(result.imported_ops > 0);
        assert!(
            core.crdt_engines.is_empty(),
            "preview must not create an engine"
        );
        assert!(core.sync_hwm.is_empty(), "preview must not advance HWM");
        assert_eq!(
            core.checkpoint_coordinator.total_dirty_pages(),
            dirty_before
        );
        assert_eq!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", "00000001")
                .expect("sparse read"),
            None,
            "preview must not materialize sparse state"
        );
    }

    #[test]
    fn preview_rejections_leave_no_engine_or_checkpoint_residue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let valid = snapshot_delta("docs", "one", "value");
        let cases = [
            ("malformed", b"not-loro".as_slice(), "docs", "one"),
            ("wrong target", valid.as_slice(), "docs", "other"),
        ];

        for (name, delta, collection, row) in cases {
            let response = preview_response(&mut core, &task, collection, row, delta);
            assert_eq!(response.status, Status::Error, "{name}");
            assert!(matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedPrevalidation { .. })
            ));
            assert!(core.crdt_engines.is_empty(), "{name} created an engine");
            assert!(core.sync_hwm.is_empty(), "{name} changed HWM");
            assert_eq!(
                core.checkpoint_coordinator.total_dirty_pages(),
                0,
                "{name} dirtied checkpoint"
            );
        }

        let over_byte = vec![0; nodedb_crdt::CrdtDeltaPreviewLimits::default().max_delta_bytes + 1];
        let response = preview_response(&mut core, &task, "docs", "one", &over_byte);
        assert_eq!(response.status, Status::Error);
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);

        let source = nodedb_crdt::CrdtState::new(43).expect("source state");
        source
            .upsert(
                "docs",
                "one",
                &[("value", LoroValue::String("base".into()))],
            )
            .expect("base write");
        let _snapshot = source.export_snapshot().expect("commit base state");
        let base_version = source.oplog_version_vector();
        source
            .set_fields(
                "docs",
                "one",
                &[("value", LoroValue::String("next".into()))],
            )
            .expect("dependent update");
        let pending = source
            .export_updates_since(&base_version)
            .expect("incremental delta");
        let response = preview_response(&mut core, &task, "docs", "one", &pending);
        assert_eq!(response.status, Status::Error, "pending dependency");
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);

        let over_op_source = nodedb_crdt::CrdtState::new(44).expect("over-op source");
        for index in 0..=nodedb_crdt::CrdtDeltaPreviewLimits::default().max_imported_ops {
            let field = format!("f{index}");
            over_op_source
                .set_fields(
                    "docs",
                    "one",
                    &[(field.as_str(), LoroValue::I64(index as i64))],
                )
                .expect("source operation");
        }
        let over_op = over_op_source.export_snapshot().expect("over-op snapshot");
        let response = preview_response(&mut core, &task, "docs", "one", &over_op);
        assert_eq!(response.status, Status::Error, "over operation limit");
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);
    }

    #[test]
    fn stale_sync_frontier_does_not_consume_a_newer_producer_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let delta = snapshot_delta("docs", "one", "value");
        let provenance = SyncProvenance {
            producer_id: 900,
            epoch: 7,
            stream_id: 11,
            seq: 1,
        };
        let response = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate: Surrogate::new(77),
                peer_id: 42,
                provenance: Some(&provenance),
                constraint_version_required: 1,
                expected_frontier_digest: Some([0xFF; 32]),
            },
        );

        assert_eq!(response.status, Status::Error);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::CrdtFrontierMismatch { .. })
        ));
        assert!(core.crdt_engines.is_empty());
        assert!(core.producer_epoch_floor.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);
        assert_eq!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", "0000004d")
                .expect("sparse read"),
            None
        );
    }

    #[test]
    fn matching_frontier_applies_and_stale_frontier_is_side_effect_free() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let delta = snapshot_delta("docs", "one", "value");
        let preview = preview_response(&mut core, &task, "docs", "one", &delta);
        let preview: CrdtPreviewResult =
            zerompk::from_msgpack(preview.payload.as_bytes()).expect("preview result");
        let surrogate = Surrogate::new(77);

        let applied = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: Some(preview.frontier_digest),
            },
        );
        assert_eq!(applied.status, Status::Ok);
        assert!(
            core.crdt_engines
                .get(&(DatabaseId::DEFAULT, TenantId::new(1)))
                .and_then(|engine| engine.read_row("docs", "one"))
                .is_some()
        );
        let doc_id = surrogate_to_doc_id(surrogate);
        assert!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
                .expect("sparse read")
                .is_some()
        );

        let dirty_before = core.checkpoint_coordinator.total_dirty_pages();
        let hwm_before = core.sync_hwm.clone();
        let sparse_before = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
            .expect("sparse read");
        let stale = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: Some(preview.frontier_digest),
            },
        );
        assert_eq!(stale.status, Status::Error);
        assert!(matches!(
            stale.error_code.as_deref(),
            Some(ErrorCode::CrdtFrontierMismatch { .. })
        ));
        assert_eq!(
            core.checkpoint_coordinator.total_dirty_pages(),
            dirty_before
        );
        assert_eq!(core.sync_hwm, hwm_before);
        assert_eq!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
                .expect("sparse read"),
            sparse_before
        );

        let legacy = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            },
        );
        assert_eq!(
            legacy.status,
            Status::Ok,
            "legacy unfenced apply remains supported"
        );
    }
}
