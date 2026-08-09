// SPDX-License-Identifier: BUSL-1.1

//! Write-metadata extraction for dispatched writes: which rows a physical plan
//! changes, named the way a CDC subscriber addresses them.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::change_stream::ChangeOperation;
use crate::types::TenantId;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, CrdtOp, DocumentOp, KvOp, MetaOp, TimeseriesOp, VectorOp,
};

/// Extract write metadata from a physical plan for change event publishing.
///
/// Returns one `(collection, document_id, op)` tuple per logical row change
/// in the plan (empty for reads, DDL/config ops, and index/overlay
/// maintenance ops whose underlying data row already published its own
/// event). Almost all write plans produce exactly one tuple; only
/// `KvOp::TransferItem` spans two distinct collections and so produces two.
///
/// The match is exhaustive over the top-level [`PhysicalPlan`] enum (no
/// catch-all `_ =>`) so a new engine variant is a compile error here, not a
/// silent CDC gap.
///
/// `_tenant_id` is reserved for future tenant-scoped change stream filtering.
pub(super) fn extract_write_metadata(
    plan: &PhysicalPlan,
    _tenant_id: TenantId,
) -> Vec<(String, String, ChangeOperation)> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )],
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Update,
        )],
        // `PointInsert` is the plan `sql_plan_convert` emits for a plain SQL
        // `INSERT INTO <document collection>` (see
        // `dml/insert.rs::convert_insert`) — distinct from `PointPut`
        // (unconditional overwrite, used by non-SQL write paths).
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::Upsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection, ..
        }) => vec![(
            target_collection.clone(),
            "*".into(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Update)]
        }
        PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Delete)]
        }
        PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Delete)]
        }
        // `resolve_only = true` is the Control-Plane read-only classification
        // pass (see the field's own doc comment: "WITHOUT writing ... or
        // emitting events") — no row has actually changed yet, so no event.
        PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection,
            resolve_only,
            ..
        }) => {
            if *resolve_only {
                Vec::new()
            } else {
                vec![(
                    target_collection.clone(),
                    "*".into(),
                    ChangeOperation::Update,
                )]
            }
        }
        // MERGE mixes INSERT/UPDATE/DELETE per matched arm; not individually
        // addressable with today's single-tuple-per-plan shape, so — like
        // `BulkUpdate` — it's reported as one `Update` covering the
        // collection. Same `resolve_only` read-only-pass guard as
        // `UpdateFromJoin`.
        PhysicalPlan::Document(DocumentOp::Merge {
            target_collection,
            resolve_only,
            ..
        }) => {
            if *resolve_only {
                Vec::new()
            } else {
                vec![(
                    target_collection.clone(),
                    "*".into(),
                    ChangeOperation::Update,
                )]
            }
        }
        // Remaining DocumentOp variants (PointGet, Scan, RangeScan, Register,
        // IndexLookup, IndexedFetch, DropIndex, BackfillIndex, EstimateCount,
        // MaterializeScan) are reads or catalog/schema DDL — no row changed.
        PhysicalPlan::Document(_) => Vec::new(),

        // Timeseries ingest: batch write. CDC is opt-in for timeseries
        // collections (high-cardinality metrics would flood the bus).
        // The change event uses document_id="*" to indicate a batch.
        // Consumers can subscribe with collection_filter to get these events.
        PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Insert)]
        }
        // TimeseriesOp::Scan is a read — no row changed.
        PhysicalPlan::Timeseries(_) => Vec::new(),

        // KV engine write operations.
        PhysicalPlan::Kv(KvOp::Put {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Insert {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::InsertIfAbsent {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::InsertOnConflictUpdate {
            collection, key, ..
        }) => vec![(
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Kv(KvOp::Delete { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Delete)]
        }
        PhysicalPlan::Kv(KvOp::FieldSet {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Incr {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::IncrFloat {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Cas {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::GetSet {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Expire {
            collection, key, ..
        })
        | PhysicalPlan::Kv(KvOp::Persist {
            collection, key, ..
        }) => vec![(
            collection.clone(),
            String::from_utf8_lossy(key).into_owned(),
            ChangeOperation::Update,
        )],
        PhysicalPlan::Kv(KvOp::BatchPut { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Kv(KvOp::Truncate { collection }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Delete)]
        }
        // Atomic fungible transfer: debits + credits two keys in the SAME
        // collection in one Data Plane pass. Not individually addressable
        // with today's single-`document_id` tuple, so it is reported the
        // same way other multi-row batch ops in this match are (BatchPut,
        // BulkUpdate, ...): one event, document_id="*".
        PhysicalPlan::Kv(KvOp::Transfer { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Update)]
        }
        // Atomic item transfer spans TWO distinct collections (delete from
        // source, insert at dest) — the only write in this match that can't
        // be expressed as a single tuple, so it reports two.
        PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection,
            dest_collection,
            item_key,
            dest_key,
            ..
        }) => vec![
            (
                source_collection.clone(),
                String::from_utf8_lossy(item_key).into_owned(),
                ChangeOperation::Delete,
            ),
            (
                dest_collection.clone(),
                String::from_utf8_lossy(dest_key).into_owned(),
                ChangeOperation::Insert,
            ),
        ],
        // Remaining KvOp variants (Get, Scan, GetTtl, BatchGet, secondary /
        // sorted-index DDL and reads, MaterializeScan) are reads or
        // catalog-only — no row changed.
        PhysicalPlan::Kv(_) => Vec::new(),

        // Columnar storage core: base `columnar` collections AND `spatial`
        // collections (spatial rows are stored via the same `ColumnarOp`
        // path — see `nodedb-sql`'s spatial engine rules / dml/insert.rs).
        // Both are data-bearing peer engines, not index/overlay engines, so
        // their writes need their own CDC event.
        PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Update)]
        }
        PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Delete)]
        }
        // Scan / MaterializeScan are reads — no row changed.
        PhysicalPlan::Columnar(_) => Vec::new(),

        // Array engine: ND sparse cells are data-bearing rows in their own
        // right (not an index over another engine's rows), so cell writes
        // need CDC. `array_id.name` is the user-visible collection name.
        PhysicalPlan::Array(ArrayOp::Put { array_id, .. }) => {
            vec![(array_id.name.clone(), "*".into(), ChangeOperation::Insert)]
        }
        PhysicalPlan::Array(ArrayOp::Delete { array_id, .. }) => {
            vec![(array_id.name.clone(), "*".into(), ChangeOperation::Delete)]
        }
        // OpenArray/Slice/Project/Aggregate/Elementwise/Flush/Compact/
        // SurrogateBitmapScan/DropArray are reads or maintenance — no
        // user-data row changed by this event's semantics.
        PhysicalPlan::Array(_) => Vec::new(),

        // Graph edge writes (`EdgePut`/`EdgeDelete`/`EdgePutBatch`/
        // `EdgeDeleteBatch`) do NOT emit a standalone CDC event here.
        // Implicit edges (a document INSERT carrying `_from`/`_to`) are
        // mirrored into a SEPARATE `GraphOp::EdgePut` task on the same
        // collection (see `implicit_edges/insert.rs`), whose underlying
        // `DocumentOp` write already published the change above — emitting
        // again here would double-publish (the change stream does not
        // dedup). Explicit `GRAPH INSERT EDGE` writes (no backing document)
        // are the only case that legitimately has no other CDC source;
        // surfacing those requires threading an implicit-vs-explicit
        // distinction through the edge plan variants, which is a separate
        // follow-up. Hop/Neighbors/NeighborsMulti/Path/Subgraph/RagFusion/
        // Algo/Match*/Stats/superstep ops are reads. `SetNodeLabels`/
        // `RemoveNodeLabels` ARE node-content writes but carry no
        // `collection` field to key a `ChangeEvent` on (labels are addressed
        // by `node_id` alone) — left uncovered here rather than guessing a
        // collection; flagged as a known gap, not a read.
        PhysicalPlan::Graph(_) => Vec::new(),

        // Vector engine: normally a secondary index over a Document row —
        // that row's own write already published its CDC event above, so
        // publishing again here would duplicate it (Insert/BatchInsert/
        // Delete/DeleteBySurrogate/Sparse*/MultiVector*/Search* all fall
        // here). `DirectUpsert` is the one exception: it's the SOLE write
        // for a vector-primary collection (`WITH (primary='vector', ...)`)
        // — no parallel Document write exists for it to piggyback on — so it
        // needs its own event.
        PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection,
            surrogate,
            ..
        }) => vec![(
            collection.clone(),
            surrogate.as_u32().to_string(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Vector(_) => Vec::new(),

        // Spatial R-tree writes are index maintenance for a document row
        // that lives in the columnar store (see `SpatialOp::Insert`'s own
        // doc comment) — that row's write already went through
        // `PhysicalPlan::Columnar` above and published its event.
        PhysicalPlan::Spatial(_) => Vec::new(),

        // Full-text search writes (`FtsIndexDoc`/`FtsDeleteDoc`/analyzer
        // config) are BM25 index maintenance for a document row that
        // already published its own event.
        PhysicalPlan::Text(_) => Vec::new(),

        // CRDT engine: data-bearing (Loro-backed document content), so its
        // mutating ops need CDC like any other data engine.
        PhysicalPlan::Crdt(CrdtOp::Apply {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListInsert {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Insert,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )],
        PhysicalPlan::Crdt(CrdtOp::ListMove {
            collection,
            document_id,
            ..
        })
        | PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Update,
        )],
        // Collection-wide snapshot import: no single document identity.
        PhysicalPlan::Crdt(CrdtOp::ImportSnapshot { collection, .. }) => {
            vec![(collection.clone(), "*".into(), ChangeOperation::Update)]
        }
        // Document-row field-carrying ops: a full replace / partial-update is
        // an Insert / Update respectively; a delete is a Delete.
        PhysicalPlan::Crdt(CrdtOp::DocUpsert {
            collection,
            document_id,
            partial,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            if *partial {
                ChangeOperation::Update
            } else {
                ChangeOperation::Insert
            },
        )],
        PhysicalPlan::Crdt(CrdtOp::DocDelete {
            collection,
            document_id,
            ..
        }) => vec![(
            collection.clone(),
            document_id.clone(),
            ChangeOperation::Delete,
        )],
        // Read (Read/ReadConstraints/GetPolicy/ReadAtVersion/
        // GetVersionVector/ExportDelta), history maintenance
        // (CompactAtVersion), and config/DDL (SetConstraints/
        // DropConstraints/SetPolicy) ops — no content row changed.
        PhysicalPlan::Crdt(_) => Vec::new(),

        // Query: joins, aggregates, and coordinator Exchange nodes are
        // read-only.
        PhysicalPlan::Query(_) => Vec::new(),

        // A committed transaction batch publishes the same logical row events
        // its constituent writes would have emitted under autocommit. Overlay
        // and maintenance Meta operations remain event-free.
        PhysicalPlan::Meta(MetaOp::TransactionBatch { plans, .. }) => plans
            .iter()
            .flat_map(|plan| extract_write_metadata(plan, _tenant_id))
            .collect(),
        PhysicalPlan::Meta(_) => Vec::new(),

        // Cluster-mode array ops: like their single-node `ArrayOp` counterparts
        // (see the `PhysicalPlan::Array` arm above), `Put`/`Delete` are
        // data-bearing cell writes that need their own CDC event. This match
        // arm is never reached via the normal Data-Plane dispatch funnel (see
        // `PhysicalPlan::ClusterArray`'s own doc comment) — the coordinator
        // dispatch path in `routing/cluster_array.rs` calls this function
        // directly via `publish_cluster_array_change_events` after a
        // successful execute, so it IS load-bearing there.
        PhysicalPlan::ClusterArray(op) => cluster_array_change_meta(op),
        PhysicalPlan::ClusterEvent(_) => Vec::new(),
    }
}

/// Map a `ClusterArrayOp` to its CDC change metadata. Shared by the
/// `PhysicalPlan::ClusterArray` arm above and the coordinator dispatch path
/// (`publish_cluster_array_change_events`), which holds the op by reference and
/// must not clone the whole write batch just to read the array name.
pub(crate) fn cluster_array_change_meta(
    op: &ClusterArrayOp,
) -> Vec<(String, String, ChangeOperation)> {
    match op {
        ClusterArrayOp::Put { array_id, .. } => {
            vec![(array_id.name.clone(), "*".into(), ChangeOperation::Insert)]
        }
        ClusterArrayOp::Delete { array_id, .. } => {
            vec![(array_id.name.clone(), "*".into(), ChangeOperation::Delete)]
        }
        // Slice/Agg are reads — no row changed.
        ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. } => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, GraphOp};
    use nodedb_types::{Surrogate, VectorQuantization, VectorStorageDtype};

    // Regression coverage for the C-CALLGRAPH CDC gap: `Columnar`, `Array`,
    // and `Vector`(`DirectUpsert`) writes used to fall through the old
    // blanket `_ => None` and emit no change event at all. `Graph` edge
    // writes stay silent (see `graph_edge_put_emits_no_change_event`) to
    // avoid double-publishing implicit-edge writes alongside their
    // underlying `DocumentOp` event.

    #[test]
    fn transaction_batch_emits_each_subplan_change_event() {
        let plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![
                PhysicalPlan::Document(DocumentOp::PointPut {
                    collection: "users".into(),
                    document_id: "u1".into(),
                    value: Vec::new(),
                    surrogate: Surrogate::new(1),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
                PhysicalPlan::Document(DocumentOp::PointDelete {
                    collection: "users".into(),
                    document_id: "u2".into(),
                    surrogate: Surrogate::new(2),
                    pk_bytes: Vec::new(),
                    returning: None,
                    rls_filters: Vec::new(),
                    rls_write_check: Vec::new(),
                    resolved_sum_targets: Vec::new(),
                }),
            ],
            txn_id: None,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![
                ("users".into(), "u1".into(), ChangeOperation::Insert),
                ("users".into(), "u2".into(), ChangeOperation::Delete),
            ]
        );
    }

    #[test]
    fn columnar_insert_emits_change_event() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: "metrics".into(),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            rls_write_check: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "metrics".to_string(),
                "*".to_string(),
                ChangeOperation::Insert
            )]
        );
    }

    #[test]
    fn columnar_delete_emits_change_event() {
        let plan = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: "metrics".into(),
            filters: Vec::new(),
            rls_write_check: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "metrics".to_string(),
                "*".to_string(),
                ChangeOperation::Delete
            )]
        );
    }

    #[test]
    fn array_put_emits_change_event() {
        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "genome".to_string(),
                "*".to_string(),
                ChangeOperation::Insert
            )]
        );
    }

    #[test]
    fn array_delete_emits_change_event() {
        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "genome".to_string(),
                "*".to_string(),
                ChangeOperation::Delete
            )]
        );
    }

    #[test]
    fn cluster_array_put_emits_change_event() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            cells: Vec::new(),
            wal_lsn: 7,
            prefix_bits: 8,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "genome".to_string(),
                "*".to_string(),
                ChangeOperation::Insert
            )]
        );
    }

    #[test]
    fn cluster_array_delete_emits_change_event() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            coords: Vec::new(),
            wal_lsn: 7,
            prefix_bits: 8,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "genome".to_string(),
                "*".to_string(),
                ChangeOperation::Delete
            )]
        );
    }

    #[test]
    fn cluster_array_slice_and_agg_emit_no_change_event() {
        let slice = PhysicalPlan::ClusterArray(ClusterArrayOp::Slice {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            slice_msgpack: Vec::new(),
            attr_projection: Vec::new(),
            limit: 0,
            slice_hilbert_ranges: Vec::new(),
            prefix_bits: 8,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(extract_write_metadata(&slice, TenantId::new(1)).is_empty());

        let agg = PhysicalPlan::ClusterArray(ClusterArrayOp::Agg {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            attr_idx: 0,
            reducer_msgpack: Vec::new(),
            group_by_dim: -1,
            slice_hilbert_ranges: Vec::new(),
            prefix_bits: 8,
            system_as_of: None,
            valid_at_ms: None,
        });
        assert!(extract_write_metadata(&agg, TenantId::new(1)).is_empty());
    }

    // Graph edge writes must stay silent: implicit edges (a document INSERT
    // carrying `_from`/`_to`) mirror into a separate `GraphOp::EdgePut` task
    // on the same collection, and that write's underlying `DocumentOp`
    // already published this row's event — emitting again here would
    // double-publish (the change stream has no dedup).
    #[test]
    fn graph_edge_put_emits_no_change_event() {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: "follows".into(),
            src_id: "alice".into(),
            label: "FOLLOWS".into(),
            dst_id: "bob".into(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert!(meta.is_empty());
    }

    #[test]
    fn vector_direct_upsert_emits_change_event() {
        let plan = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: "embeddings".into(),
            field: "emb".into(),
            surrogate: Surrogate::new(42),
            vector: vec![0.0, 1.0],
            payload: Vec::new(),
            quantization: VectorQuantization::default(),
            storage_dtype: VectorStorageDtype::default(),
            payload_indexes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "embeddings".to_string(),
                "42".to_string(),
                ChangeOperation::Insert
            )]
        );
    }

    // Vector's secondary-index maintenance ops (everything except
    // `DirectUpsert`) must stay silent — the underlying Document write
    // already published this row's event; a second event here would
    // duplicate it.
    #[test]
    fn vector_secondary_index_insert_emits_no_change_event() {
        let plan = PhysicalPlan::Vector(VectorOp::Delete {
            collection: "docs".into(),
            vector_id: 7,
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert!(meta.is_empty());
    }

    #[test]
    fn document_point_insert_emits_change_event() {
        let plan = PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: "users".into(),
            document_id: "u1".into(),
            value: Vec::new(),
            if_absent: false,
            surrogate: Surrogate::new(1),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![(
                "users".to_string(),
                "u1".to_string(),
                ChangeOperation::Insert
            )]
        );
    }

    #[test]
    fn kv_transfer_item_emits_two_change_events_across_collections() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: "inventory_a".into(),
            dest_collection: "inventory_b".into(),
            item_key: b"sword".to_vec(),
            dest_key: b"sword".to_vec(),
            surrogate: Surrogate::new(9),
            source_rls_write_check: Vec::new(),
            dest_rls_write_check: Vec::new(),
        });
        let meta = extract_write_metadata(&plan, TenantId::new(1));
        assert_eq!(
            meta,
            vec![
                (
                    "inventory_a".to_string(),
                    "sword".to_string(),
                    ChangeOperation::Delete
                ),
                (
                    "inventory_b".to_string(),
                    "sword".to_string(),
                    ChangeOperation::Insert
                ),
            ]
        );
    }
}
