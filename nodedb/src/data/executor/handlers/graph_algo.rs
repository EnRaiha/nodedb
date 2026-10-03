// SPDX-License-Identifier: BUSL-1.1

//! Graph algorithm dispatch handler.
//!
//! Routes `PhysicalPlan::GraphAlgo` to the appropriate algorithm
//! implementation in `engine::graph::algo::*`. Each algorithm runs on
//! a collection-scoped CsrIndex built on-demand from the EdgeStore,
//! ensuring that `ON <collection>` and `EDGE_LABEL` filters are applied
//! before the algorithm executes rather than post-filtered on output.

use nodedb_graph::CsrIndex;
use nodedb_physical::physical_plan::AlgoStage;
use nodedb_types::TenantId;
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::graph::algo::params::{AlgoParams, GraphAlgorithm};
use crate::engine::graph::algo::result::AlgoResultBatch;
use crate::engine::graph::edge_store::EdgeStore;

use super::graph_algo_edges::{collection_edges, csr_from_edges};

impl CoreLoop {
    pub(in crate::data::executor) fn execute_graph_algo(
        &self,
        task: &ExecutionTask,
        tid: u64,
        algorithm: &GraphAlgorithm,
        params: &AlgoParams,
        stage: &AlgoStage,
    ) -> Response {
        debug!(
            core = self.core_id,
            tid,
            algorithm = algorithm.name(),
            collection = %params.collection,
            edge_label = ?params.edge_label,
            "graph algorithm dispatch"
        );

        let database_id = task.request.database_id.as_u64();

        // The export stage runs nothing: it answers this core's edges for the
        // coordinator to gather. The gathered stage reads no edges of its own.
        let gathered = match stage {
            AlgoStage::ExportEdges { system_as_of_ms } => {
                return self.export_algo_edges(task, tid, params, *system_as_of_ms);
            }
            AlgoStage::Gathered { edges } => Some(edges),
            AlgoStage::Local => None,
        };

        if *algorithm == GraphAlgorithm::Sssp && params.source_node.is_none() {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "SSSP requires FROM '<source_node>'".into(),
                },
            );
        }

        let memory = nodedb_mem::ScopedMemory::new(
            self.governor.clone(),
            task.request.database_id,
            TenantId::new(tid),
            nodedb_mem::EngineId::Graph,
        );
        let built = match gathered {
            Some(edges) => csr_from_edges(edges, memory),
            None => build_csr_for_collection(
                &self.edge_store,
                database_id,
                tid,
                &params.collection,
                params.edge_label.as_deref(),
                None,
                memory,
            ),
        };
        let scoped_csr = match built {
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

    /// Answer this core's edges of `params.collection`, after the label
    /// filter, as a msgpack array of `AlgoEdge`. `system_as_of_ms` bounds them
    /// to the edges live at that system time.
    fn export_algo_edges(
        &self,
        task: &ExecutionTask,
        tid: u64,
        params: &AlgoParams,
        system_as_of_ms: Option<i64>,
    ) -> Response {
        collection_edges(
            &self.edge_store,
            task.request.database_id.as_u64(),
            tid,
            &params.collection,
            params.edge_label.as_deref(),
            system_as_of_ms.map(nodedb_types::ms_to_ordinal_upper),
        )
        .and_then(|edges| {
            zerompk::to_msgpack_vec(&edges).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("algorithm edge export: {e}"),
            })
        })
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
    memory: nodedb_mem::ScopedMemory,
) -> crate::Result<CsrIndex> {
    let edges = collection_edges(
        edge_store,
        database_id,
        tid,
        collection,
        edge_label,
        system_as_of,
    )?;
    csr_from_edges(&edges, memory)
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
