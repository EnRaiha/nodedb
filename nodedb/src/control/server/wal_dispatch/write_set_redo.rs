// SPDX-License-Identifier: BUSL-1.1

//! Post-apply document redo for Data-Plane write-sets.
//!
//! Some write handlers apply a row mutation whose autocommit WAL path mints no
//! redo record of its own, yet whose effect must still survive a WAL-only
//! restart. The canonical case is a `DocumentOp::PointUpdate` on a document
//! collection carrying a secondary vector (HNSW) index: the HNSW is an
//! in-memory side-effect rebuilt at startup from the document `Put` redo
//! records, so a PointUpdate that changes an embedding but journals nothing
//! would let restart rebuild the index from the pre-update body and resurrect
//! the old vector.
//!
//! The Data Plane carries the surrogate + post-image back to the Control Plane
//! in [`Response::write_set`](crate::bridge::envelope::Response::write_set)
//! after applying such a write; the Control Plane then mints a durable `Put`
//! (or `Delete`) redo record here, under the write-admission guard, so its
//! ordering matches apply order. The record reuses the shared document redo
//! shape (`encode_document_put_record` / `encode_document_delete_record`) so
//! `replay_document_redo` reconstructs it through the normal apply path.

use crate::bridge::envelope::{PhysicalPlan, Response, Status, WriteSetEntry};
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use crate::wal::manager::WalManager;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_types::Surrogate;

use super::document::{encode_document_delete_record, encode_document_put_record};

/// Return `Some(collection)` when `plan` is a write whose durable redo record is
/// minted *after* the Data Plane applies it — from the surrogate + post-image it
/// carries back in [`Response::write_set`](crate::bridge::envelope::Response) —
/// rather than on the pre-dispatch autocommit WAL path. Returns `None` for every
/// other plan (its durability is owned on the WAL-append path or elsewhere).
///
/// Today `DocumentOp::PointUpdate`, `DocumentOp::Upsert`, `DocumentOp::BulkUpdate`,
/// `DocumentOp::BulkDelete`, `DocumentOp::BatchInsert`, and
/// `DocumentOp::UpdateFromJoin` qualify. `BulkUpdate` and `UpdateFromJoin` each
/// carry one `Put` write-set entry per updated row (post-image), keyed to the
/// join's write TARGET collection — `UpdateFromJoin` ships source rows across
/// cores but writes only `target_collection`, so all entries in its write-set
/// share that single collection; `BulkDelete` carries one `Delete` entry per
/// removed row (surrogate only) — the shared `Delete` redo replays through
/// `apply_point_delete`, whose cascade soft-deletes the row's HNSW nodes, so a
/// deleted vector does not resurrect on a WAL-only restart. `BatchInsert`
/// carries one `Put` entry per inserted row on a vector-indexed collection —
/// `wal_append_document_op` returns `None` for `BatchInsert` (row durability is
/// redb-synchronous), so without this the HNSW rebuild on restart would
/// silently drop every batch-inserted row's vector. `Truncate` carries one
/// `Delete` entry per removed row on a vector-indexed collection, same
/// rationale as `BulkDelete` — `wal_append_document_op` mints no per-row redo
/// for `Truncate` either, so without this every truncated row's HNSW vector
/// would resurrect on a WAL-only restart (the original insert `Put` record
/// replays with no matching `Delete` to cancel it).
/// `PointPut`, `PointInsert` and `PointDelete` qualify for a different reason:
/// their OWN row is journalled on the pre-dispatch WAL path, but a collection
/// with a materialized-sum binding also writes a row in the TARGET collection,
/// and no record names that write. Those entries carry `Some(target)` and are
/// appended here. A point write on a collection with no such binding returns an
/// empty write-set, which appends nothing.
///
/// Additional post-apply redo variants (`Merge`) will extend this as their
/// post-apply redo is built — do not add them until that handler support exists,
/// or a write would be admitted with its guard held for a redo that is never
/// appended.
pub fn plan_post_apply_redo(plan: &PhysicalPlan) -> Option<String> {
    if let PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::PointPut { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::Upsert { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::Truncate { collection, .. }) = plan {
        Some(collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection, ..
    }) = plan
    {
        Some(target_collection.clone())
    } else if let PhysicalPlan::Document(DocumentOp::ApplyBalanceDelta { collection, .. }) = plan {
        // The cross-shard balance write journals nothing of its own on the
        // pre-dispatch WAL path: no record names the target row it moves. Its
        // one write-set entry carries the row's absolute post-image and names
        // the target collection, so the redo appended here homes with the row.
        // Without it a WAL-only restart replays every source row and leaves the
        // total as it stood BEFORE the statement.
        Some(collection.clone())
    } else {
        None
    }
}

/// Append a document redo record for each entry in a Data-Plane write-set,
/// returning the last allocated LSN (or `None` for an empty write-set).
///
/// Each entry is keyed by the row's global surrogate; the redb storage key is
/// `surrogate_to_doc_id(surrogate)`, which is also the `document_id` written
/// into the redo record so replay keys on the same identity. A put carries the
/// post-image body; a delete carries none. Called under the write-admission
/// guard so two concurrent same-surrogate writes append in apply order.
pub fn append_write_set_redo(
    wal: &WalManager,
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    collection: &str,
    write_set: &[WriteSetEntry],
) -> crate::Result<Option<Lsn>> {
    let mut last: Option<Lsn> = None;
    for entry in write_set {
        let entry_collection = entry.collection.as_deref().unwrap_or(collection);
        let doc_id = surrogate_to_doc_id(Surrogate::new(entry.surrogate));
        // A cross-collection entry homes to a different vShard than the plan's
        // own collection, so the vShard must be re-derived per entry rather
        // than reusing the caller-hoisted `vshard_id` (which is correct only
        // for `entry.collection == None`).
        let entry_vshard_id = match &entry.collection {
            Some(c) => VShardId::from_collection_in_database(database_id, c),
            None => vshard_id,
        };
        let lsn = if entry.is_delete {
            let record = encode_document_delete_record(entry_collection, &doc_id, entry.surrogate)?;
            wal.append_delete(tenant_id, entry_vshard_id, database_id, &record)?
        } else {
            let record = encode_document_put_record(
                entry_collection,
                &doc_id,
                &entry.value,
                entry.surrogate,
            )?;
            wal.append_put(tenant_id, entry_vshard_id, database_id, &record)?
        };
        last = Some(lsn);
    }
    Ok(last)
}

/// Mint the post-apply redo for a `dispatch_local` response, i.e. one built
/// outside the pgwire autocommit funnel's own `submit_to_data_plane` redo
/// minting (`insert_select` / `update_from_join_orchestrator`'s DP→CP→DP
/// round trips). No-op when the response is not `Ok` or carries no
/// write-set (the common case: a non-vector-indexed collection).
pub fn mint_dispatch_local_redo(
    wal: &WalManager,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    resp: &Response,
) -> crate::Result<()> {
    if resp.status != Status::Ok || resp.write_set.is_empty() {
        return Ok(());
    }
    let vshard_id = VShardId::from_collection_in_database(database_id, collection);
    append_write_set_redo(
        wal,
        tenant_id,
        vshard_id,
        database_id,
        collection,
        &resp.write_set,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::ReturningSpec;
    use nodedb_types::sync::wire::SyncProvenance;

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
    fn point_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection: "docs".to_string(),
            document_id: "d1".to_string(),
            surrogate: Surrogate::new(1),
            pk_bytes: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn bulk_update_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: "docs".to_string(),
            filters: Vec::new(),
            updates: Vec::new(),
            returning: None::<ReturningSpec>,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn update_from_join_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: "docs".to_string(),
            source_collection: "src".to_string(),
            source_alias: "s".to_string(),
            target_join_col: "sku".to_string(),
            source_join_col: "sku".to_string(),
            updates: Vec::new(),
            target_filters: Vec::new(),
            returning: None::<ReturningSpec>,
            resolve_only: false,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn bulk_delete_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: "docs".to_string(),
            filters: Vec::new(),
            returning: None::<ReturningSpec>,
            ollp_predicted_surrogates: None,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: Vec::new(),
            resolved_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn batch_insert_is_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection: "docs".to_string(),
            documents: Vec::new(),
            surrogates: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        });
        assert_eq!(plan_post_apply_redo(&plan).as_deref(), Some("docs"));
    }

    #[test]
    fn point_get_is_not_post_apply_redo() {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "docs".to_string(),
            document_id: "d1".to_string(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert!(plan_post_apply_redo(&plan).is_none());
    }

    #[test]
    fn write_set_put_appends_replayable_put_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let entries = vec![WriteSetEntry {
            surrogate: 9,
            is_delete: false,
            value: vec![1, 2, 3],
            collection: None,
        }];

        let lsn = append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &entries,
        )
        .expect("append");
        assert!(
            lsn.is_some(),
            "a put write-set entry must append a redo LSN"
        );

        // Byte-shape must match the redo replay decoder's PUT tuple.
        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Put);
        let (collection, document_id, value, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(
                &record.payload,
            )
            .expect("decode put payload");
        assert_eq!(collection, "docs");
        assert_eq!(document_id, surrogate_to_doc_id(Surrogate::new(9)));
        assert_eq!(value, vec![1, 2, 3]);
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn write_set_delete_appends_replayable_delete_record() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let entries = vec![WriteSetEntry {
            surrogate: 9,
            is_delete: true,
            value: Vec::new(),
            collection: None,
        }];

        append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &entries,
        )
        .expect("append");

        let record = last_record_of_type(&wal, nodedb_wal::record::RecordType::Delete);
        let (collection, document_id, _prov, surrogate) =
            zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(&record.payload)
                .expect("decode delete payload");
        assert_eq!(collection, "docs");
        assert_eq!(document_id, surrogate_to_doc_id(Surrogate::new(9)));
        assert_eq!(surrogate, 9);
    }

    #[test]
    fn empty_write_set_appends_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = open_wal(dir.path());
        let lsn = append_write_set_redo(
            &wal,
            TenantId::new(1),
            VShardId::new(0),
            DatabaseId::DEFAULT,
            "docs",
            &[],
        )
        .expect("append");
        assert!(lsn.is_none(), "empty write-set must append no record");
    }
}
