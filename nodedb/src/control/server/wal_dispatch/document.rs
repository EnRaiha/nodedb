// SPDX-License-Identifier: BUSL-1.1

//! WAL append dispatch for `PhysicalPlan::Document(DocumentOp)`.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::DocumentOp;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;

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
        } => {
            // The row's global surrogate is appended as a trailing element so
            // startup replay can rebuild any secondary vector index bound to
            // this document with its real cross-engine identity (headless
            // local ids otherwise leak into vector-search projections after a
            // restart). Appending keeps the record an arity-cascade extension
            // of the legacy `(collection, document_id, value, provenance)`
            // shape, which older decoders still parse.
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                value,
                prov,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal point put: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointInsert {
            collection,
            document_id,
            value,
            if_absent: _,
            surrogate,
        } => {
            // Trailing surrogate element (see `PointPut` above) — carries the
            // row's global identity for restart-time vector-index rebuild.
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(
                collection,
                document_id,
                value,
                prov,
                surrogate.as_u32(),
            ))
            .map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("wal point insert: {e}"),
            })?;
            Some(wal.append_put(tenant_id, vshard_id, database_id, &entry)?)
        }
        DocumentOp::PointDelete {
            collection,
            document_id,
            ..
        } => {
            let prov: Option<nodedb_types::sync::wire::SyncProvenance> = None;
            let entry = zerompk::to_msgpack_vec(&(collection, document_id, prov)).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("wal point delete: {e}"),
                }
            })?;
            Some(wal.append_delete(tenant_id, vshard_id, database_id, &entry)?)
        }
        // NotAWrite — reads / query ops / DDL that produces no engine mutation here
        DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. } => None,
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
        // DurableElsewhere — row deletion is redb-durable; vector-rebuild tombstone
        // barrier tracked
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
