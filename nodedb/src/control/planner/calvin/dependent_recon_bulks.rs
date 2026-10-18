// SPDX-License-Identifier: BUSL-1.1

//! The predicate writes of one dependent submission.
//!
//! A submission can hold several `BulkUpdate`/`BulkDelete` tasks, for example
//! the statements of one request. Each task has its own collection, filters
//! and edge lifecycle, so each gets its own reconnaissance scan and carries
//! only the rows and edges that scan found.

use std::collections::BTreeMap;

use nodedb_physical::physical_plan::OllpPredictedEdge;
use nodedb_physical::physical_task::PhysicalTask;

use crate::control::planner::calvin::is_dependent_predicate;
use crate::control::planner::calvin::preexec::PreexecScan;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

use super::dependent_recon_node_edges::reconnoitre;
use super::dependent_recon_plan::{inject_ollp_predicted_edges, inject_ollp_surrogates};
use super::dependent_recon_predicate::{
    EdgeLifecycle, classify_edge_lifecycle, extract_bulk_predicate_info,
};

/// What one predicate write carries into the submission: the rows it matched
/// and the implicit edges of those rows.
pub(super) type BulkPrediction = (Vec<u32>, Vec<OllpPredictedEdge>);

/// One predicate write of a submission and the inputs of its reconnaissance.
/// They are constant across retries.
pub(super) struct BulkRecon {
    /// Position of the task in the submission's task list.
    pub(super) index: usize,
    pub(super) collection: String,
    pub(super) filters: Vec<u8>,
    pub(super) edge_mode: EdgeLifecycle,
}

impl BulkRecon {
    /// Every predicate write of `tasks`, in task order.
    pub(super) fn of(tasks: &[PhysicalTask]) -> crate::Result<Vec<Self>> {
        tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| is_dependent_predicate(&task.plan))
            .map(|(index, task)| {
                let (collection, filters) = extract_bulk_predicate_info(&task.plan);
                Ok(Self {
                    index,
                    collection,
                    filters,
                    edge_mode: classify_edge_lifecycle(&task.plan)?,
                })
            })
            .collect()
    }
}

/// The reconnaissance scan of every predicate write, in the order of `bulks`.
pub(super) async fn reconnoitre_all(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    bulks: &[BulkRecon],
) -> crate::Result<Vec<PreexecScan>> {
    let mut scans = Vec::with_capacity(bulks.len());
    for bulk in bulks {
        scans.push(
            reconnoitre(
                state,
                tenant_id,
                database_id,
                &bulk.collection,
                bulk.filters.clone(),
                &bulk.edge_mode,
            )
            .await?,
        );
    }
    Ok(scans)
}

/// A copy of `tasks` in which the predicate write at each index of
/// `predictions` carries its own rows and edges.
pub(super) fn with_bulk_predictions(
    tasks: &[PhysicalTask],
    predictions: &BTreeMap<usize, BulkPrediction>,
) -> Vec<PhysicalTask> {
    let mut out = tasks.to_vec();
    for (index, (surrogates, edges)) in predictions {
        if let Some(task) = out.get_mut(*index) {
            inject_ollp_surrogates(&mut task.plan, surrogates.clone());
            inject_ollp_predicted_edges(&mut task.plan, edges.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};

    fn bulk_delete(collection: &str) -> PhysicalTask {
        PhysicalTask {
            tenant_id: TenantId::new(1),
            vshard_id: crate::types::VShardId::new(0),
            database_id: DatabaseId::DEFAULT,
            plan: PhysicalPlan::Document(DocumentOp::BulkDelete {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, collection),
                filters: Vec::new(),
                returning: None,
                ollp_predicted_surrogates: None,
                ollp_predicted_edges: None,
                rls_filters: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
                resolved_sum_targets: Vec::new(),
                declared_primary_key: None,
            }),
            post_set_op: nodedb_physical::physical_task::PostSetOp::None,
            txn_id: None,
        }
    }

    fn carried(task: &PhysicalTask) -> (Option<Vec<u32>>, Option<Vec<OllpPredictedEdge>>) {
        match &task.plan {
            PhysicalPlan::Document(DocumentOp::BulkDelete {
                ollp_predicted_surrogates,
                ollp_predicted_edges,
                ..
            }) => (
                ollp_predicted_surrogates.clone(),
                ollp_predicted_edges.clone(),
            ),
            _ => (None, None),
        }
    }

    #[test]
    fn each_predicate_write_receives_its_own_prediction() {
        let edge = OllpPredictedEdge {
            surrogate: 3,
            from: "a".into(),
            to: "z".into(),
            label: None,
        };
        let tasks = vec![bulk_delete("orders"), bulk_delete("users")];
        let predictions = BTreeMap::from([
            (0, (vec![3, 5], vec![edge.clone()])),
            (1, (vec![9], Vec::new())),
        ]);

        let out = with_bulk_predictions(&tasks, &predictions);

        assert_eq!(carried(&out[0]), (Some(vec![3, 5]), Some(vec![edge])));
        assert_eq!(carried(&out[1]), (Some(vec![9]), Some(Vec::new())));
    }

    #[test]
    fn a_predicate_write_is_found_at_its_task_position() {
        let tasks = vec![bulk_delete("orders"), bulk_delete("users")];

        let bulks = BulkRecon::of(&tasks).expect("classify the predicate writes");

        let found: Vec<_> = bulks
            .iter()
            .map(|bulk| (bulk.index, bulk.collection.as_str()))
            .collect();
        assert_eq!(found.len(), 2);
        assert_eq!(found[0].0, 0);
        assert_eq!(found[1].0, 1);
        assert!(found[0].1.ends_with("orders") && found[1].1.ends_with("users"));
    }
}
