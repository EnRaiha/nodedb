// SPDX-License-Identifier: BUSL-1.1

//! Records the per-core last-write-LSN version for every key a committed
//! transaction batch wrote.
//!
//! This is the transaction-batch funnel for the version index: it runs once,
//! after the batch has committed, over the buffered sub-plans — covering both
//! the single-shard fast-path commit and every Calvin apply (both delegate to
//! `execute_transaction_batch`). One WAL LSN (the batch's single
//! `append_transaction` LSN, threaded onto the task) applies to every key in
//! the batch.

use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::write_index::KeyRepr;
use crate::data::executor::task::ExecutionTask;
use crate::types::{Lsn, TenantId};
use nodedb_physical::physical_plan::{
    ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
use nodedb_types::Surrogate;

impl CoreLoop {
    /// Record the version of every key written by a committed transaction batch.
    ///
    /// No-op when the task carries no WAL LSN (the version is not advanced with
    /// a wrong value). Per-key engines record `KeyRepr`; engines whose per-key
    /// identity is internal (columnar / timeseries / spatial / FTS) record only
    /// the collection floor.
    pub(in crate::data::executor) fn record_batch_write_versions(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        plans: &[PhysicalPlan],
    ) {
        let Some(lsn) = task.wal_lsn() else {
            return;
        };
        let db = task.request.database_id;
        let tenant = TenantId::new(tid);
        for plan in plans {
            self.record_plan_write_version(db, tenant, plan, lsn);
        }
    }

    fn record_plan_write_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        plan: &PhysicalPlan,
        lsn: Lsn,
    ) {
        match plan {
            PhysicalPlan::Document(op) => self.record_document_version(db, tenant, op, lsn),
            PhysicalPlan::Vector(op) => self.record_vector_version(db, tenant, op, lsn),
            PhysicalPlan::Graph(op) => self.record_graph_version(db, tenant, op, lsn),
            PhysicalPlan::Kv(op) => self.record_kv_version(db, tenant, op, lsn),
            // Collection-floor engines: per-key identity is engine-internal.
            PhysicalPlan::Columnar(op) => {
                let coll = match op {
                    ColumnarOp::Insert { collection, .. }
                    | ColumnarOp::Update { collection, .. }
                    | ColumnarOp::Delete { collection, .. } => Some(collection.as_str()),
                    _ => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
                self.note_write_lsn(db, tenant, collection, None, lsn);
            }
            PhysicalPlan::Spatial(op) => {
                let coll = match op {
                    SpatialOp::Insert { collection, .. } | SpatialOp::Delete { collection, .. } => {
                        Some(collection.as_str())
                    }
                    _ => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Text(op) => {
                let coll = match op {
                    TextOp::FtsIndexDoc { collection, .. }
                    | TextOp::FtsDeleteDoc { collection, .. } => Some(collection.as_str()),
                    _ => None,
                };
                if let Some(c) = coll {
                    self.note_write_lsn(db, tenant, c, None, lsn);
                }
            }
            PhysicalPlan::Crdt(CrdtOp::Apply { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::DocUpsert { collection, .. })
            | PhysicalPlan::Crdt(CrdtOp::DocDelete { collection, .. }) => {
                self.note_write_lsn(db, tenant, collection, None, lsn);
            }
            // No per-key/collection version recorded for reads, control ops, or
            // engines not (yet) part of the funnel (array is keyed by tile).
            PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => {}
        }
    }

    fn record_document_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &DocumentOp,
        lsn: Lsn,
    ) {
        let (collection, surrogate) = match op {
            DocumentOp::PointPut {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointInsert {
                collection,
                surrogate,
                ..
            }
            | DocumentOp::PointDelete {
                collection,
                surrogate,
                ..
            } => (collection.as_str(), *surrogate),
            _ => return,
        };
        self.note_write_lsn(
            db,
            tenant,
            collection,
            Some(KeyRepr::Surrogate(surrogate.as_u32())),
            lsn,
        );
    }

    /// Records the version of a vector-engine write within a committed
    /// transaction batch.
    ///
    /// Mirrors the fork resolution the live write handlers apply (see
    /// `handlers/vector*.rs`): surrogate-carrying ops record `KeyRepr::Surrogate`
    /// per surrogate (a superset of the collection floor, since vector SEARCH
    /// reads always record `ReadKey::Predicate`, the collection floor); sparse
    /// ops (keyed by `doc_id: String`, no cross-engine surrogate) and
    /// `Delete` (whose `vector_id` is the internal HNSW node id, NOT the
    /// surrogate — recording it as `KeyRepr::Surrogate` would be a wrong
    /// identity) record the collection floor only. Read/config/query ops
    /// write nothing and are named explicitly below so the match stays
    /// exhaustive.
    fn record_vector_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &VectorOp,
        lsn: Lsn,
    ) {
        match op {
            VectorOp::Insert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DirectUpsert {
                collection,
                surrogate,
                ..
            }
            | VectorOp::DeleteBySurrogate {
                collection,
                surrogate,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection,
                    Some(KeyRepr::Surrogate(surrogate.as_u32())),
                    lsn,
                );
            }
            VectorOp::MultiVectorInsert {
                collection,
                document_surrogate,
                ..
            }
            | VectorOp::MultiVectorDelete {
                collection,
                document_surrogate,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection,
                    Some(KeyRepr::Surrogate(document_surrogate.as_u32())),
                    lsn,
                );
            }
            VectorOp::BatchInsert {
                collection,
                surrogates,
                ..
            } => {
                let mut any_surrogate_recorded = false;
                for s in surrogates {
                    if *s != Surrogate::ZERO {
                        self.note_write_lsn(
                            db,
                            tenant,
                            collection,
                            Some(KeyRepr::Surrogate(s.as_u32())),
                            lsn,
                        );
                        any_surrogate_recorded = true;
                    }
                }
                if !any_surrogate_recorded {
                    self.note_write_lsn(db, tenant, collection, None, lsn);
                }
            }
            // Sparse (doc_id-keyed, no surrogate) and `Delete` (vector_id is
            // the internal HNSW node id, not the surrogate): collection floor
            // only.
            VectorOp::SparseInsert { collection, .. }
            | VectorOp::SparseDelete { collection, .. }
            | VectorOp::Delete { collection, .. } => {
                self.note_write_lsn(db, tenant, collection, None, lsn);
            }
            // Read / config / query ops: nothing written, no version to record.
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::SetParams { .. }
            | VectorOp::DropIndex { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. } => {}
        }
    }

    fn record_graph_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &GraphOp,
        lsn: Lsn,
    ) {
        match op {
            GraphOp::EdgePut {
                collection,
                src_id,
                label,
                dst_id,
                ..
            }
            | GraphOp::EdgeDelete {
                collection,
                src_id,
                label,
                dst_id,
                ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection,
                    Some(KeyRepr::Edge {
                        src: Box::from(src_id.as_str()),
                        label: Box::from(label.as_str()),
                        dst: Box::from(dst_id.as_str()),
                    }),
                    lsn,
                );
            }
            GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
                for edge in edges {
                    self.note_write_lsn(
                        db,
                        tenant,
                        &edge.collection,
                        Some(KeyRepr::Edge {
                            src: Box::from(edge.src_id.as_str()),
                            label: Box::from(edge.label.as_str()),
                            dst: Box::from(edge.dst_id.as_str()),
                        }),
                        lsn,
                    );
                }
            }
            _ => {}
        }
    }

    fn record_kv_version(
        &mut self,
        db: crate::types::DatabaseId,
        tenant: TenantId,
        op: &KvOp,
        lsn: Lsn,
    ) {
        match op {
            KvOp::Put {
                collection, key, ..
            }
            | KvOp::Insert {
                collection, key, ..
            }
            | KvOp::InsertIfAbsent {
                collection, key, ..
            }
            | KvOp::InsertOnConflictUpdate {
                collection, key, ..
            }
            | KvOp::Expire {
                collection, key, ..
            }
            | KvOp::Persist {
                collection, key, ..
            }
            | KvOp::FieldSet {
                collection, key, ..
            }
            | KvOp::Incr {
                collection, key, ..
            }
            | KvOp::IncrFloat {
                collection, key, ..
            }
            | KvOp::Cas {
                collection, key, ..
            }
            | KvOp::GetSet {
                collection, key, ..
            } => {
                self.note_write_lsn(
                    db,
                    tenant,
                    collection,
                    Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                    lsn,
                );
            }
            KvOp::Delete {
                collection, keys, ..
            } => {
                for key in keys {
                    self.note_write_lsn(
                        db,
                        tenant,
                        collection,
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        lsn,
                    );
                }
            }
            KvOp::BatchPut {
                collection,
                entries,
                ..
            } => {
                for (key, _value) in entries {
                    self.note_write_lsn(
                        db,
                        tenant,
                        collection,
                        Some(KeyRepr::KvKey(Box::from(key.as_slice()))),
                        lsn,
                    );
                }
            }
            KvOp::Truncate { collection } => {
                // Whole-collection mutation: record the collection floor only.
                self.note_write_lsn(db, tenant, collection, None, lsn);
            }
            _ => {}
        }
    }
}
