// SPDX-License-Identifier: BUSL-1.1

//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs on
//! a collection-scoped CsrIndex built on-demand from the EdgeStore,
//! ensuring that `ON <collection>` and `EDGE_LABEL` filters are applied
//! before the algorithm executes rather than post-filtered on output.

use nodedb_graph::CsrIndex;
use nodedb_graph::csr::weights::extract_weight_from_properties;
use nodedb_types::{DatabaseId, TenantId};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::algo::result::AlgoResultBatch;
use crate::engine::graph::edge_store::EdgeStore;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        tid: u64,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = algorithm.name(),
            collection = %params.collection,
            edge_label = ?params.edge_label,
            "graph algorithm dispatch"
        );

        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        let database_id = task.request.database_id.as_u64();
        let scoped_csr = match build_csr_for_collection(
            &self.edge_store,
            database_id,
            tid,
            &params.collection,
            params.edge_label.as_deref(),
            None,
        ) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, ErrorCode::from(e)),
        };

        if scoped_csr.node_count() == 0 {
            return match AlgoResultBatch::new(*algorithm).to_msgpack() {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(task, ErrorCode::from(e)),
            };
        }

        run_algorithm(&scoped_csr, algorithm, params, &self.graph_tuning)
            .and_then(|batch| batch.to_msgpack())
            .map_or_else(
                |e| self.response_error(task, ErrorCode::from(e)),
                |payload| self.response_with_payload(task, payload),
            )
    }
}

/// Build a `CsrIndex` containing only the edges for a specific
/// `(database, tid, collection)`, optionally filtered to a single edge label.
/// Reads from the versioned EdgeStore so the result reflects current state
/// (pass `None` for `system_as_of`) or a historical snapshot (pass a bitemporal
/// ordinal cutoff).
///
/// Two-pass construction: first intern all endpoint nodes so isolated nodes get
/// stable ids, then insert edges. This matches the pattern in `CsrSnapshot::from_edge_store_as_of`.
pub(super) fn build_csr_for_collection(
    edge_store: &EdgeStore,
    database_id: u64,
    tid: u64,
    collection: &str,
    edge_label: Option<&str>,
    system_as_of: Option<i64>,
) -> crate::Result<CsrIndex> {
    let records = edge_store.scan_all_edges_decoded(system_as_of)?;
    let target_db = DatabaseId::new(database_id);
    let target_tid = TenantId::new(tid);

    let mut csr = CsrIndex::new();

    // Pass 1: intern endpoint nodes.
    for (rec_db, rec_tid, coll, src, label, dst, _props) in &records {
        if *rec_db != target_db || *rec_tid != target_tid || coll != collection {
            continue;
        }
        if edge_label.is_some_and(|el| label != el) {
            continue;
        }
        csr.add_node(src).map_err(|e| crate::Error::Internal {
            detail: format!("build_csr_for_collection add src: {e}"),
        })?;
        csr.add_node(dst).map_err(|e| crate::Error::Internal {
            detail: format!("build_csr_for_collection add dst: {e}"),
        })?;
    }

    // Pass 2: insert edges.
    for (rec_db, rec_tid, coll, src, label, dst, props) in &records {
        if *rec_db != target_db || *rec_tid != target_tid || coll != collection {
            continue;
        }
        if edge_label.is_some_and(|el| label != el) {
            continue;
        }
        let weight = extract_weight_from_properties(props);
        let res = if weight != 1.0 {
            csr.add_edge_weighted(src, label, dst, weight)
        } else {
            csr.add_edge(src, label, dst)
        };
        res.map_err(|e| crate::Error::Internal {
            detail: format!("build_csr_for_collection add edge: {e}"),
        })?;
    }

    csr.compact().map_err(|e| crate::Error::Internal {
        detail: format!("build_csr_for_collection compact: {e}"),
    })?;

    Ok(csr)
}

/// Shared implementation used by both current-state and temporal
/// `execute_graph_algo*` handlers. Runs the algorithm and encodes the
/// response (success payload or structured error).
pub(super) fn run_algo_response(
    core: &CoreLoop,
    task: &ExecutionTask,
    csr: &CsrIndex,
    algorithm: &GraphAlgorithm,
    params: &AlgoParams,
) -> Response {
    if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
        return core.response_error(
            task,
            ErrorCode::Internal {
                detail: "SSSP requires FROM '<source_node>'".into(),
            },
        );
    }
    run_algorithm(csr, algorithm, params, &core.graph_tuning)
        .and_then(|batch| batch.to_msgpack())
        .map_or_else(
            |e| core.response_error(task, ErrorCode::from(e)),
            |payload| core.response_with_payload(task, payload),
        )
}

pub(super) fn run_algorithm(
    csr: &CsrIndex,
    algorithm: &GraphAlgorithm,
    params: &AlgoParams,
    tuning: &nodedb_types::config::tuning::GraphTuning,
) -> Result<AlgoResultBatch, crate::Error> {
    use crate::engine::graph::algo;
    match algorithm {
        GraphAlgorithm::PageRank => Ok(algo::pagerank::run(csr, params)),
        GraphAlgorithm::Wcc => Ok(algo::wcc::run(csr)),
        GraphAlgorithm::LabelPropagation => Ok(algo::label_propagation::run(csr, params)),
        GraphAlgorithm::Lcc => Ok(algo::lcc::run(
            csr,
            tuning.lcc_high_degree_threshold,
            tuning.lcc_sample_pairs,
        )),
        GraphAlgorithm::Sssp => algo::sssp::run(csr, params),
        GraphAlgorithm::Betweenness => Ok(algo::betweenness::run(csr, params)),
        GraphAlgorithm::Closeness => Ok(algo::closeness::run(csr)),
        GraphAlgorithm::Harmonic => Ok(algo::harmonic::run(csr)),
        GraphAlgorithm::Degree => Ok(algo::degree::run(csr, params)),
        GraphAlgorithm::Louvain => Ok(algo::louvain::run(csr, params)),
        GraphAlgorithm::Triangles => Ok(algo::triangles::run(csr, params)),
        GraphAlgorithm::Diameter => Ok(algo::diameter::run(csr, params)),
        GraphAlgorithm::KCore => Ok(algo::kcore::run(csr)),
    }
}
