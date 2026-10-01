// SPDX-License-Identifier: BUSL-1.1

//! Durable re-issue of restored vector-engine rows.
//!
//! Snapshot-install lands vector state in in-memory-only Data Plane maps with
//! no WAL record or Raft entry — lost on restart, never replicated. RESTORE
//! re-issues each vector as a durable `VectorOp::Insert`, proposed through
//! Raft. Each plan names its collection by the stored, database-qualified
//! name.

use nodedb_types::surrogate::Surrogate;

use crate::bridge::envelope::PhysicalPlan;
use crate::engine::vector::index_config::{IndexConfig, IndexType};
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
/// The snapshot carries no sync provenance for vector rows. `surrogate` is the
/// one the source bound, never `Surrogate::ZERO`: an insert under the same
/// surrogate replaces the node it bound, so a repeated re-issue lands each
/// vector once.
///
/// `pk_bytes` is the key the backup binds `surrogate` to, when it binds one.
/// The insert binds by it, as the source's insert did. `None` names a
/// headless row, which self-keys. Self-keying a row the backup binds to a
/// key binds its surrogate to a second key, and the next restore of the
/// same backup refuses it as a surrogate conflict.
pub fn build_vector_insert_plan(
    collection: &str,
    field_name: &str,
    vector: Vec<f32>,
    surrogate: Surrogate,
    pk_bytes: Option<Vec<u8>>,
) -> PhysicalPlan {
    let dim = vector.len();
    PhysicalPlan::Vector(VectorOp::Insert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        vector,
        dim,
        field_name: field_name.to_string(),
        surrogate,
        pk_bytes,
        provenance: None,
    })
}

/// Delete every vector of one multi-vector document. Idempotent on the Data
/// Plane, so it clears a partial earlier re-issue and is a no-op otherwise.
pub fn build_multi_vector_delete_plan(
    collection: &str,
    field_name: &str,
    document_surrogate: Surrogate,
) -> PhysicalPlan {
    PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        field_name: field_name.to_string(),
        document_surrogate,
    })
}

/// Insert the full vector set of one multi-vector document as one op.
/// Every vector has the width of the first.
///
/// `pk_bytes` is the key the backup binds `document_surrogate` to, when it
/// binds one, so the insert binds by it as [`build_vector_insert_plan`] does.
pub fn build_multi_vector_insert_plan(
    collection: &str,
    field_name: &str,
    document_surrogate: Surrogate,
    pk_bytes: Option<Vec<u8>>,
    vectors: Vec<Vec<f32>>,
) -> PhysicalPlan {
    let dim = vectors.first().map_or(0, Vec::len);
    let count = vectors.len();
    PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
        field_name: field_name.to_string(),
        document_surrogate,
        pk_bytes,
        vectors: vectors.into_iter().flatten().collect(),
        count,
        dim,
    })
}

/// A restored vector index's rows, grouped for re-issue.
#[derive(Debug, Default, PartialEq)]
pub struct RestoredVectors {
    /// Single-vector rows with their surrogate.
    pub single: Vec<(Surrogate, Vec<f32>)>,
    /// Multi-vector documents by surrogate, in first-seen order, each with
    /// its full vector set. Membership comes from the snapshot's
    /// `vector_multi_documents`, never from a vector count: a one-vector
    /// document is still a multi-vector document.
    pub multi: Vec<(Surrogate, Vec<Vec<f32>>)>,
}

/// Group the `(node_id, vector, surrogate)` export rows of one index.
/// `multi_documents` are the index's multi-vector document surrogates.
///
/// Every stored vector is bound, so a row with no surrogate (or
/// `Surrogate::ZERO`) is a corrupt capture and fails the group.
pub fn group_restored_vectors(
    rows: Vec<(u32, Vec<f32>, Option<Surrogate>)>,
    multi_documents: &std::collections::HashSet<Surrogate>,
) -> crate::Result<RestoredVectors> {
    let mut grouped = RestoredVectors::default();
    let mut slot_of: std::collections::HashMap<Surrogate, usize> = std::collections::HashMap::new();
    for (node_id, vector, surrogate) in rows {
        match surrogate.filter(|s| *s != Surrogate::ZERO) {
            Some(surrogate) if multi_documents.contains(&surrogate) => {
                let slot = *slot_of.entry(surrogate).or_insert_with(|| {
                    grouped.multi.push((surrogate, Vec::new()));
                    grouped.multi.len() - 1
                });
                if let Some((_, group)) = grouped.multi.get_mut(slot) {
                    group.push(vector);
                }
            }
            Some(surrogate) => grouped.single.push((surrogate, vector)),
            None => {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "restored vector node {node_id} carries no surrogate; every stored \
                         vector is bound"
                    ),
                });
            }
        }
    }
    Ok(grouped)
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
        collection: nodedb_types::QualifiedCollection::from_stored(collection.to_string()),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// A multi-vector document re-issued with its backup key binds its
    /// surrogate to that key, so a repeated restore of the same backup
    /// verifies. Re-issued headless, the surrogate self-keys, and the repeated
    /// restore's bind check refuses it.
    #[tokio::test(flavor = "current_thread")]
    async fn a_repeated_restore_of_a_multi_vector_document_verifies() {
        use std::sync::Arc;

        use crate::bridge::dispatch::Dispatcher;
        use crate::control::backup::restore::bind_conflicts::{CarriedBind, check_carried_binds};
        use crate::control::state::SharedState;
        use crate::types::{DatabaseId, TenantId};
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("mv.wal")).expect("open wal"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");
        let db = DatabaseId::new(1024);
        let tenant = TenantId::new(1);

        // One restore: bind the backup's key, then re-issue the document
        // through the production binder. The plan carries the stored name,
        // as `reissue_vector_snapshots` builds it.
        let restore = |collection: &str, pk: &[u8], surrogate: u32, keyed: bool| {
            state
                .surrogate_assigner
                .bind(
                    nodedb_types::CollectionKey::from_bare(db, collection),
                    tenant,
                    pk,
                    Surrogate::new(surrogate),
                )
                .expect("rebind the backup key");
            let stored = nodedb_types::QualifiedCollection::new(db, collection);
            let mut plan = build_multi_vector_insert_plan(
                stored.as_str(),
                "emb",
                Surrogate::new(surrogate),
                keyed.then(|| pk.to_vec()),
                vec![vec![1.0, 0.0], vec![0.0, 1.0]],
            );
            crate::control::surrogate::bind_plan_identities(
                &state.surrogate_assigner,
                db,
                tenant,
                &mut plan,
            )
            .expect("bind the re-issued document");
        };
        let carried = |collection: &str, pk: &[u8], surrogate: u32| {
            vec![CarriedBind {
                collection: collection.to_string(),
                pk: pk.to_vec(),
                surrogate,
            }]
        };

        restore("mv", b"d1", 12, true);
        check_carried_binds(&state, 1, db, carried("mv", b"d1", 12))
            .await
            .expect("a repeated restore verifies");

        restore("mv_headless", b"d2", 13, false);
        assert!(
            check_carried_binds(&state, 1, db, carried("mv_headless", b"d2", 13))
                .await
                .is_err(),
            "a self-keyed re-issue binds the surrogate to a second key"
        );
    }

    fn members(surrogates: &[u32]) -> std::collections::HashSet<Surrogate> {
        surrogates.iter().map(|s| Surrogate::new(*s)).collect()
    }

    #[test]
    fn a_multi_vector_document_keeps_every_vector_in_one_group() {
        let rows = vec![
            (0, vec![1.0, 0.0], Some(Surrogate::new(7))),
            (1, vec![0.0, 1.0], Some(Surrogate::new(9))),
            (2, vec![0.5, 0.5], Some(Surrogate::new(7))),
        ];
        let grouped = group_restored_vectors(rows, &members(&[7])).unwrap();
        assert_eq!(
            grouped.multi,
            vec![(Surrogate::new(7), vec![vec![1.0, 0.0], vec![0.5, 0.5]])]
        );
        assert_eq!(grouped.single, vec![(Surrogate::new(9), vec![0.0, 1.0])]);
    }

    #[test]
    fn a_one_vector_member_is_still_a_multi_vector_document() {
        let rows = vec![
            (0, vec![1.0, 0.0], Some(Surrogate::new(4))),
            (1, vec![0.0, 1.0], Some(Surrogate::new(5))),
        ];
        let grouped = group_restored_vectors(rows, &members(&[4])).unwrap();
        assert_eq!(
            grouped.multi,
            vec![(Surrogate::new(4), vec![vec![1.0, 0.0]])]
        );
        assert_eq!(grouped.single, vec![(Surrogate::new(5), vec![0.0, 1.0])]);
    }

    #[test]
    fn a_row_without_a_bound_surrogate_fails_the_group() {
        for surrogate in [None, Some(Surrogate::ZERO)] {
            assert!(
                group_restored_vectors(vec![(0, vec![1.0], surrogate)], &members(&[])).is_err(),
                "{surrogate:?} names no bound vector"
            );
        }
    }

    #[test]
    fn the_multi_vector_insert_carries_the_full_set() {
        let plan = build_multi_vector_insert_plan(
            "docs",
            "emb",
            Surrogate::new(7),
            Some(b"doc-7".to_vec()),
            vec![vec![1.0, 0.0], vec![0.5, 0.5], vec![0.0, 1.0]],
        );
        let PhysicalPlan::Vector(VectorOp::MultiVectorInsert {
            vectors,
            count,
            dim,
            pk_bytes,
            ..
        }) = plan
        else {
            panic!("expected a MultiVectorInsert");
        };
        assert_eq!(pk_bytes.as_deref(), Some(b"doc-7".as_slice()));
        assert_eq!((count, dim), (3, 2));
        assert_eq!(vectors, vec![1.0, 0.0, 0.5, 0.5, 0.0, 1.0]);
    }
}
