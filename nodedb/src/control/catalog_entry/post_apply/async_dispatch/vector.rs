// SPDX-License-Identifier: BUSL-1.1

//! Async post-apply for vector-index catalog entries.
//!
//! `PutVectorIndexParams` appends this node's `VectorParams` redo record and
//! dispatches `VectorOp::SetParams` to every core. `DeleteVectorIndexParams`
//! appends and fsyncs the drop record, then dispatches `VectorOp::DropIndex`.
//! Both run on every node, so a follower builds and tears down the index it
//! serves instead of learning about it at its next boot.
//!
//! Nothing here can propagate a failure: the catalog row is already
//! committed. Every failed stage files a `Capture` instead, because a node
//! silently missing an index is the defect this module exists to stop.

use std::sync::Arc;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::StoredVectorIndexParams;

use super::core_fanout::{CoreFanout, dispatch_to_every_core};

/// One vector index, named the way every stage below reports it.
struct IndexTarget<'a> {
    database_id: u64,
    tenant_id: u64,
    collection: &'a str,
    field_name: &'a str,
}

/// Build the `SetParams` plan the boot seed and the CREATE handler both
/// reproduce, so runtime and restart install identical parameters.
fn set_params_plan(entry: &StoredVectorIndexParams) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(entry.database_id),
            &entry.collection,
        ),
        field_name: entry.field_name.clone(),
        dim: entry.dim,
        m: entry.m,
        ef_construction: entry.ef_construction,
        metric: entry.metric.clone(),
        index_type: entry.index_type.clone(),
        pq_m: entry.pq_m,
        ivf_cells: entry.ivf_cells,
        ivf_nprobe: entry.ivf_nprobe,
    })
}

/// Build the `DropIndex` plan for one index.
fn drop_index_plan(database_id: u64, collection: &str, field_name: &str) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::DropIndex {
        collection: nodedb_types::QualifiedCollection::new(
            DatabaseId::new(database_id),
            collection,
        ),
        field_name: field_name.to_string(),
    })
}

/// Install one vector index's build parameters on this node: append the redo
/// record, then dispatch `SetParams` to every core.
pub async fn put_async(entry: StoredVectorIndexParams, shared: Arc<SharedState>) {
    let target = IndexTarget {
        database_id: entry.database_id,
        tenant_id: entry.tenant_id,
        collection: &entry.collection,
        field_name: &entry.field_name,
    };
    let plan = set_params_plan(&entry);

    // The record makes this node's log self-sufficient: replay rebuilds the
    // index from it in LSN order alongside the vector writes around it.
    if let Err(error) = append_redo(&shared, &target, &plan) {
        report(&error, "set_params_wal_append", &target);
    }

    if let Err(error) = dispatch_to_every_core(&shared, &fanout(&target), &plan).await {
        report(&error, "set_params_dispatch", &target);
    }
}

/// Remove one vector index from this node: append and fsync the drop record,
/// then dispatch `DropIndex` to every core.
pub async fn delete_async(
    database_id: u64,
    tenant_id: u64,
    collection: String,
    field_name: String,
    shared: Arc<SharedState>,
) {
    let target = IndexTarget {
        database_id,
        tenant_id,
        collection: &collection,
        field_name: &field_name,
    };
    let plan = drop_index_plan(database_id, &collection, &field_name);

    // The vector writes this drop cancels are already fsynced in this node's
    // log, so replay rebuilds the dropped index unless the drop record is
    // durable too. Append and fsync before touching the cores.
    match append_redo(&shared, &target, &plan) {
        Ok(Some(lsn)) => {
            if let Err(error) = shared.wal.wait_durable(lsn).await {
                report(&error, "drop_index_fsync", &target);
            }
        }
        Ok(None) => {
            let error = crate::Error::Internal {
                detail: "vector index drop minted no WAL record".to_string(),
            };
            report(&error, "drop_index_wal_append", &target);
        }
        Err(error) => report(&error, "drop_index_wal_append", &target),
    }

    if let Err(error) = dispatch_to_every_core(&shared, &fanout(&target), &plan).await {
        report(&error, "drop_index_dispatch", &target);
    }
}

/// Append `plan`'s redo record to this node's WAL, returning its LSN.
fn append_redo(
    shared: &SharedState,
    target: &IndexTarget<'_>,
    plan: &PhysicalPlan,
) -> crate::Result<Option<Lsn>> {
    let database_id = DatabaseId::new(target.database_id);
    let vshard = VShardId::from_collection_in_database(database_id, target.collection);
    let outcome = crate::control::server::wal_dispatch::wal_append_if_write(
        &shared.wal,
        TenantId::new(target.tenant_id),
        vshard,
        database_id,
        plan,
    )?;
    Ok(outcome.lsn)
}

/// Name this index for the core fan-out's ack line and error detail.
fn fanout<'a>(target: &'a IndexTarget<'a>) -> CoreFanout<'a> {
    CoreFanout {
        database_id: target.database_id,
        tenant_id: target.tenant_id,
        collection: target.collection,
        what: "vector index change",
        detail: target.field_name,
    }
}

/// File the one report for a stage this node lost, naming the index.
fn report(error: &crate::Error, stage: &'static str, target: &IndexTarget<'_>) {
    crate::diag::vector_index_not_applied(
        error,
        stage,
        target.database_id,
        target.tenant_id,
        target.collection,
        target.field_name,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stored() -> StoredVectorIndexParams {
        StoredVectorIndexParams {
            database_id: 7,
            tenant_id: 3,
            collection: "documents".to_string(),
            field_name: "embedding".to_string(),
            dim: 384,
            metric: "l2".to_string(),
            m: 32,
            ef_construction: 400,
            index_type: "hnsw_pq".to_string(),
            pq_m: 8,
            ivf_cells: 64,
            ivf_nprobe: 16,
        }
    }

    /// Every build parameter the catalog row carries reaches the plan. A field
    /// dropped here is a follower index built with a different shape than the
    /// node that ran the statement.
    #[test]
    fn set_params_plan_carries_every_stored_parameter() {
        let entry = stored();
        let PhysicalPlan::Vector(VectorOp::SetParams {
            collection,
            field_name,
            dim,
            m,
            ef_construction,
            metric,
            index_type,
            pq_m,
            ivf_cells,
            ivf_nprobe,
        }) = set_params_plan(&entry)
        else {
            panic!("set_params_plan must build a VectorOp::SetParams");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(field_name, "embedding");
        assert_eq!(dim, 384);
        assert_eq!(m, 32);
        assert_eq!(ef_construction, 400);
        assert_eq!(metric, "l2");
        assert_eq!(index_type, "hnsw_pq");
        assert_eq!(pq_m, 8);
        assert_eq!(ivf_cells, 64);
        assert_eq!(ivf_nprobe, 16);
    }

    /// The drop plan targets the same `(database, collection, field)` the
    /// create plan installed, so it removes what the create built.
    #[test]
    fn drop_index_plan_targets_the_created_index() {
        let entry = stored();
        let PhysicalPlan::Vector(VectorOp::DropIndex {
            collection,
            field_name,
        }) = drop_index_plan(entry.database_id, &entry.collection, &entry.field_name)
        else {
            panic!("drop_index_plan must build a VectorOp::DropIndex");
        };
        assert_eq!(collection.as_str(), "7/documents");
        assert_eq!(field_name, "embedding");
    }

    /// An unnamed vector field keys on the empty string, matching
    /// `CoreLoop::vector_index_key`'s default-field slot.
    #[test]
    fn an_unnamed_field_keeps_the_empty_field_slot() {
        let PhysicalPlan::Vector(VectorOp::DropIndex { field_name, .. }) =
            drop_index_plan(DatabaseId::DEFAULT.as_u64(), "documents", "")
        else {
            panic!("drop_index_plan must build a VectorOp::DropIndex");
        };
        assert!(field_name.is_empty());
    }
}
