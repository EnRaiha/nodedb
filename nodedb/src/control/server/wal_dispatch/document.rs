// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Document(DocumentOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

/// Encode a document PUT redo record.
///
/// Shape: `(collection, document_id, value, Option<SyncProvenance>, surrogate)`.
/// The trailing `surrogate` carries the row's global identity so startup replay
/// can rebuild any secondary vector index bound to this document with its real
/// cross-engine identity. This is an arity-cascade extension of the legacy
/// `(collection, document_id, value, provenance)` shape that older decoders
/// still parse, and it is **byte-identical** to what the redo replay decoder
/// expects (`data::executor::wal_replay_redo_document`). Producer (`PointPut` /
/// `PointInsert` autocommit) and the post-apply write-set redo helper share this
/// one encoder so the shape lives in exactly one place.
pub(crate) fn encode_document_put_record(
    collection: &str,
    document_id: &str,
    value: &[u8],
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, document_id, value, prov, surrogate)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document put: {e}"),
        }
    })
}

/// Encode a document DELETE redo record.
///
/// Shape: `(collection, document_id, Option<SyncProvenance>, surrogate)` — the
/// four-element redo shape the replay decoder expects (the autocommit
/// `PointDelete` three-element shape omits the surrogate, which replay needs to
/// key the redb storage row). Used by the post-apply write-set redo helper.
pub(crate) fn encode_document_delete_record(
    collection: &str,
    document_id: &str,
    surrogate: u32,
) -> crate::Result<Vec<u8>> {
    let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
    zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate)).map_err(|e| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("wal document delete: {e}"),
        }
    })
}

/// Append the WAL record for a single `DocumentOp`, returning the allocated
/// LSN for the point-write variants (`Some`) or `None` for every read / bulk /
/// DDL variant that carries no durable per-write effect on THIS path.
///
/// The match over [`DocumentOp`] is **exhaustive** (`wildcard_enum_match_arm`
/// is denied), so a future write variant cannot silently become non-durable:
/// every variant's durability is decided here by name.
pub(super) fn wal_append_document_op(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    op: &DocumentOp,
) -> crate::Result<Option<Lsn>> {
    let appended = match op {
        DocumentOp::PointPut {
            collection,
            document_id,
            value,
            surrogate,
            pk_bytes: _,
            // The WAL record carries the row; the projection is answered
            // from the Data Plane's response, not from the journal.
            returning: _,
            rls_filters: _,
            // The journal carries the row; the plan-time materialized-sum
            // resolution is not part of the applied record.
            resolved_sum_targets: _,
        } => {
            let entry =
                encode_document_put_record(collection, document_id, value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent: _,
            surrogate,
            returning: _,
            rls_filters: _,
            // See `PointPut`.
            resolved_sum_targets: _,
            deferred_sum_targets: _,
        } => {
            let entry =
                encode_document_put_record(collection, document_id, value, surrogate.as_u32())?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        } => {
            // Surrogate-carrying 4-tuple so the redo-document replay can key the
            // secondary vector-index removal by surrogate on restart (a 3-tuple
            // omits it, leaving the deleted embedding to resurrect).
            let entry = encode_document_delete_record(collection, document_id, surrogate.as_u32())?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        // Durability comes from the post-apply write-set redo, which names the
        // TARGET collection and re-derives its vShard per entry — the same
        // route the co-resident derived write already takes.
        | DocumentOp::ApplyBalanceDelta { .. } => None,
        // DurableElsewhere — row is redb-synchronous-durable; secondary-vector-index
        // restart fidelity would need an apply-time per-row Put/Delete record —
        // tracked, not built here
        DocumentOp::PointUpdate { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::UpdateFromJoin { .. } => None,
        // DurableElsewhere — row deletion is redb-durable; a vector-indexed
        // collection's per-row HNSW cleanup is carried back in
        // `Response::write_set` and minted as a post-apply `Delete` redo by
        // `plan_post_apply_redo` / `append_write_set_redo` (mirrors
        // `BulkDelete`), so restart does not resurrect truncated vectors.
        DocumentOp::Truncate { .. } => None,
        // DurableElsewhere — index state is catalog + redb durable
        DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => None,
    };
    Ok(appended)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::PhysicalPlan;
    use nodedb_types::Surrogate;

    fn open_wal(dir: &std::path::Path) -> WalManager {
        WalManager::open_for_testing(&dir.join("test.wal")).expect("open wal")
    }

    fn last_record_of_type(
        wal: &WalManager,
        record_type: nodedb_wal::record::RecordType,
    ) -> nodedb_wal::WalRecord {
        wal.sync().expect("sync wal");
        wal.replay()
            .expect("read wal")
            .into_iter()
            .rfind(|r| {
                nodedb_wal::record::RecordType::from_raw(r.logical_record_type())
                    == Some(record_type)
            })
            .expect("expected record of this type")
    }

    #[test]
    fn point_put_appends_put_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: "users".to_string(),
            document_id: "u1".to_string(),
            value: vec![1, 2, 3],
            surrogate: Surrogate::new(5),
            pk_bytes: vec![],
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_some(), "PointPut must produce a durable LSN");

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Put);
        let (collection, document_id, _value, _prov, surrogate) = zerompk::from_msgpack::<(
            String,
            String,
            Vec<u8>,
            Option<nodedb_types::sync::wire::SyncProvenance>,
            u32,
        )>(&record.payload)
        .expect("decode point put payload");
        assert_eq!(collection, "users");
        assert_eq!(document_id, "u1");
        assert_eq!(surrogate, 5);
    }

    #[test]
    fn point_delete_appends_delete_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::PointDelete {
            collection: "users".to_string(),
            document_id: "u1".to_string(),
            surrogate: Surrogate::new(5),
            pk_bytes: vec![],
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(
            outcome.lsn.is_some(),
            "PointDelete must produce a durable LSN"
        );
        let _ = last_record_of_type(&wal, nodedb_wal::record::RecordType::Delete);
    }

    #[test]
    fn read_op_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: "users".to_string(),
            field: "id".to_string(),
        });

        let outcome = super::super::wal_append_if_write(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            &plan,
        )
        .expect("append");
        assert!(outcome.lsn.is_none(), "read op must produce no durable LSN");
    }
}
