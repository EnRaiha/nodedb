// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored vector-engine rows.
//!
//! The snapshot-install path lands vector engine state in in-memory-only Data
//! Plane maps with no WAL record and no Raft entry — lost on restart
//! (single-node) and never replicated (cluster). RESTORE instead re-issues
//! each restored vector as a durable `VectorOp::Insert`, branching on cluster
//! vs single-node exactly like a normal write:
//!
//! - Cluster (`async_raft_proposer` present): build a `ReplicatedEntry` and
//!   propose it through Raft (replicates to all replicas; recovery via Raft-log
//!   re-apply).
//! - Single-node: WAL-append the plan, then dispatch it into the Data Plane so
//!   it is installed live (WAL makes it durable for restart replay).

use std::time::Duration;

use nodedb_types::surrogate::Surrogate;

use crate::Error;
use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::ddl::sync_dispatch;
use crate::control::server::wal_dispatch::wal_append_if_write;
use crate::control::state::SharedState;
use crate::engine::vector::index_config::{IndexConfig, IndexType};
use crate::types::{DatabaseId, TenantId, VShardId};
use nodedb_physical::physical_plan::VectorOp;
use nodedb_types::vector_distance::DistanceMetric;

/// Split a vector snapshot's `coll_key` back into its `(collection,
/// field_name)` parts.
///
/// `CoreLoop::vector_index_key` builds `coll_key` as `collection` (default
/// field) or `collection:field_name` (named field); collection and field
/// names never contain `:`, so a single split reverses it exactly.
pub fn split_vector_coll_key(coll_key: &str) -> (&str, &str) {
    coll_key.split_once(':').unwrap_or((coll_key, ""))
}

/// Build the durable `VectorOp::Insert` plan for one restored vector row.
///
/// The snapshot format carries no PK and no sync provenance for vector rows
/// (see `TenantDataSnapshot::vectors`), so both are `None` — matching what
/// the raw snapshot-install path (`restore_vector_collection`) does today.
pub fn build_vector_insert_plan(
    collection: &str,
    field_name: &str,
    vector: Vec<f32>,
    surrogate: Surrogate,
) -> PhysicalPlan {
    let dim = vector.len();
    PhysicalPlan::Vector(VectorOp::Insert {
        collection: collection.to_string(),
        vector,
        dim,
        field_name: field_name.to_string(),
        surrogate,
        pk_bytes: None,
        provenance: None,
    })
}

/// Map a decoded `DistanceMetric` back to the string form `VectorOp::SetParams`
/// and `execute_set_vector_params` expect (mirrors the inverse mapping in
/// `execute_set_vector_params`, `handlers/vector_params.rs`).
fn metric_to_str(metric: DistanceMetric) -> &'static str {
    match metric {
        DistanceMetric::L2 => "l2",
        DistanceMetric::Cosine => "cosine",
        DistanceMetric::InnerProduct => "inner_product",
        DistanceMetric::Manhattan => "manhattan",
        DistanceMetric::Chebyshev => "chebyshev",
        DistanceMetric::Hamming => "hamming",
        DistanceMetric::Jaccard => "jaccard",
        DistanceMetric::Pearson => "pearson",
        _ => "cosine",
    }
}

/// Map a decoded `IndexType` back to the string form `VectorOp::SetParams`
/// expects (`IndexType::parse` is the inverse).
fn index_type_to_str(index_type: &IndexType) -> &'static str {
    match index_type {
        IndexType::Hnsw => "hnsw",
        IndexType::HnswPq => "hnsw_pq",
        IndexType::IvfPq => "ivf_pq",
        _ => "hnsw",
    }
}

/// Build the durable `VectorOp::SetParams` plan for one restored
/// (collection, field) HNSW index configuration.
///
/// The caller is responsible for resolving a restored `IndexConfig` snapshot
/// entry, or — when only the older `vector_params`-only section is present
/// for a (collection, field) — wrapping the decoded `HnswParams` in
/// `IndexConfig { hnsw, ..IndexConfig::default() }` before calling this.
pub fn build_vector_set_params_plan(
    collection: &str,
    field_name: &str,
    config: &IndexConfig,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::SetParams {
        collection: collection.to_string(),
        field_name: field_name.to_string(),
        dim: config.declared_dim,
        m: config.hnsw.m,
        ef_construction: config.hnsw.ef_construction,
        metric: metric_to_str(config.hnsw.metric).to_string(),
        index_type: index_type_to_str(&config.index_type).to_string(),
        pq_m: config.pq_m,
        ivf_cells: config.ivf_cells,
        ivf_nprobe: config.ivf_nprobe,
    })
}

/// Re-issue a restored vector insert durably.
///
/// Branches identically to a normal write:
/// - Cluster: `to_replicated_entry` + `propose_replicated_entry`.
/// - Single-node: `wal_append_if_write` then `sync_dispatch::dispatch_system`.
pub async fn reissue_vector_durably(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    collection: &str,
    plan: PhysicalPlan,
) -> crate::Result<()> {
    let vshard = VShardId::from_collection_in_database(database_id, collection);

    if let Some(proposer) = state.async_raft_proposer() {
        let entry = crate::control::wal_replication::to_replicated_entry(
            tenant_id,
            database_id,
            vshard,
            &plan,
        )
        .ok_or_else(|| Error::Internal {
            detail: format!(
                "restore reissue: vector plan for '{collection}' did not map to a \
                     replicated write"
            ),
        })?;
        crate::control::wal_replication::propose_replicated_entry(state, proposer, entry).await?;
        return Ok(());
    }

    // Single-node: WAL first (durable for restart replay), then install live.
    wal_append_if_write(&state.wal, tenant_id, vshard, database_id, &plan)?;
    sync_dispatch::dispatch_system(
        state,
        sync_dispatch::SystemTask::new(
            sync_dispatch::SystemReason::BackupRestore,
            tenant_id,
            database_id,
            collection,
            plan,
        ),
        REISSUE_TIMEOUT,
    )
    .await?;
    Ok(())
}

/// Per-vector re-issue dispatch timeout. Mirrors the columnar/timeseries
/// reissue timeout; a single-vector `Insert` completes far under this.
const REISSUE_TIMEOUT: Duration = Duration::from_secs(120);
