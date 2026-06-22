// SPDX-License-Identifier: BUSL-1.1

//! Calvin multi-shard distributed dispatch.
//!
//! Handles the strict multi-shard path via the Calvin sequencer, including
//! the OLLP-dependent-predicate variant that runs an optimistic pre-execution
//! scan before submitting the transaction.

use pgwire::api::results::Response;
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};

use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::planner::calvin::preexec::{PreexecScan, run_preexec_scan};
use crate::control::planner::calvin::{
    build_dependent_tx_class, dispatch_tasks_to_calvin, is_dependent_predicate,
    predicate_class_for_filters, run_dependent_with_retry, submit_calvin_routed_assign,
};
use crate::control::planner::implicit_edges::append_implicit_edge_delete_tasks;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::session::TransactionState;
use crate::types::{TenantId, TraceId};
use nodedb_cluster::calvin::sequencer::error::SequencerError;
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::types::error_to_sqlstate;
use super::super::core::NodeDbPgHandler;
use super::ollp_helpers::{
    extract_bulk_predicate_info, inject_ollp_predicted_edges, inject_ollp_surrogates,
};
use super::planning::calvin_execution_response;
use nodedb_physical::physical_plan::OllpPredictedEdge;

impl NodeDbPgHandler {
    /// Drive Calvin strict multi-shard dispatch for the given task set.
    ///
    /// Returns the response vec on success (one tag per task). The caller
    /// should return this immediately — Calvin tasks do not go through the
    /// per-task dispatch loop.
    pub(super) async fn dispatch_calvin_multishard(
        &self,
        tasks: Vec<PhysicalTask>,
        tenant_id: TenantId,
        _identity: &AuthenticatedIdentity,
        addr: &std::net::SocketAddr,
    ) -> PgWireResult<Vec<Response>> {
        let cross_shard_mode = self.sessions.cross_shard_txn_mode(addr);
        let tx_state = self.sessions.transaction_state(addr);

        let orchestrator = self.state.ollp_orchestrator.get();
        let registry = self.state.calvin_completion_registry.get().ok_or_else(|| {
            let (severity, code, message) = error_to_sqlstate(&crate::Error::SequencerUnavailable);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        let dependent_task = tasks.iter().find(|t| is_dependent_predicate(&t.plan));

        // Static (non-OLLP) Calvin path: build the TxClass and route the
        // submit-and-await to the SEQUENCER-GROUP leader via
        // `submit_calvin_routed`. Submitting to the LOCAL inbox here is the
        // silent-loss bug this fix addresses: only the sequencer leader's service
        // assigns and only its registry receives the replicated completion ack,
        // so a submit on a non-leader coordinator never completes. Routing fixes
        // that for cross-shard document writes from any coordinator.
        //
        // The OLLP (dependent-predicate) path below is COORDINATOR-OWNED: this
        // handler runs `run_dependent_with_retry`, which owns the
        // submit → await-assignment → await-completion loop and, on a post-exec
        // predicate-drift mismatch, runs a FRESH pre-execution reconnaissance
        // before resubmitting (the scheduler releases the aborted attempt's
        // locks and only signals the mismatch back — it no longer re-submits a
        // stale prediction). The submit step ROUTES to the sequencer-group leader
        // via `submit_calvin_routed_assign` (returning the leader-assigned
        // assignment) while the completion is awaited on this coordinator's local
        // registry, which receives the replicated completion ack on every
        // sequencer-group member. This makes the dependent path complete from a
        // non-leader coordinator, unifying single-node and cross-node into one
        // path, while still passing through this node's circuit-breaker / budget
        // gate.
        if dependent_task.is_none() {
            // Static (non-OLLP) path: delegate to the protocol-neutral
            // `dispatch_tasks_to_calvin` helper, supplying the session-derived
            // inputs (cross-shard mode, in-block state) it needs as parameters.
            // The helper classifies, rejects cross-shard writes inside an
            // explicit transaction block, builds the static TxClass, and routes
            // the SINGLE submit-and-await to the sequencer leader. On success we
            // synthesise one command tag per task. This is a pure extraction —
            // behaviour is identical to the inlined static branch.
            let in_txn_block = tx_state == TransactionState::InBlock;
            dispatch_tasks_to_calvin(
                &self.state,
                &tasks,
                tenant_id,
                cross_shard_mode,
                in_txn_block,
            )
            .await
            .map_err(|e| {
                let (severity, code, message) = error_to_sqlstate(&e);
                PgWireError::UserError(Box::new(ErrorInfo::new(
                    severity.to_owned(),
                    code.to_owned(),
                    message,
                )))
            })?;

            let mut calvin_responses: Vec<Response> = Vec::with_capacity(tasks.len());
            for task in &tasks {
                calvin_responses.push(calvin_execution_response(task));
            }
            return Ok(calvin_responses);
        }

        // OLLP path: the coordinator owns the retry loop. `run_dependent_with_retry`
        // submits + awaits the assignment/completion via the local registry and, on
        // a post-exec predicate-drift mismatch, runs a FRESH pre-execution scan
        // (`rescan`) before resubmitting with the fresh prediction.
        let dep_task = dependent_task.ok_or_else(|| {
            // Unreachable: the static (non-dependent) path returns early above.
            // Surface a typed error rather than panicking if the invariant is ever
            // broken by a future refactor.
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                "internal: static Calvin path reached the OLLP dispatch branch".to_owned(),
            )))
        })?;

        let orc = orchestrator.ok_or_else(|| {
            let (severity, code, message) = error_to_sqlstate(&crate::Error::SequencerUnavailable);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;
        // Hoisted across the retry loop so both `submit` and `rescan` can borrow them.
        let (dep_collection, dep_filter_bytes) = extract_bulk_predicate_info(&dep_task.plan);
        let pred_class = predicate_class_for_filters(&dep_filter_bytes, &dep_collection);
        let database_id = dep_task.database_id;

        // Initial reconnaissance — the first prediction the loop submits.
        let initial_predicted = run_preexec_scan(
            &self.state,
            tenant_id,
            database_id,
            &dep_collection,
            dep_filter_bytes.clone(),
        )
        .await
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        let timeout =
            std::time::Duration::from_secs(self.state.tuning.network.default_deadline_secs);
        let ollp_max_retries = orc.ollp_max_retries() as u32;

        // `submit`: build the TxClass with the loop-supplied prediction (NOT a
        // frozen clone), pass through this coordinator's circuit-breaker / tenant
        // budget gate, then ROUTE the inbox submit to the sequencer-group leader
        // via `submit_calvin_routed_assign` (returning the leader-assigned
        // `RoutedAssignment`). This lets a non-leader coordinator drive the
        // dependent (OLLP) cross-shard write to completion.
        let submit = |predicted: &PreexecScan| {
            let surrogates = predicted.surrogates.clone();
            let edges = predicted.edges.clone();
            let tasks = &tasks;
            let dep_collection = &dep_collection;
            let state = &self.state;
            async move {
                // Implicit-edge cleanup: a matched edge document (`_from`/`_to`)
                // has an auto-created graph edge that must be deleted in the SAME
                // Calvin transaction, cross-shard-correctly. Build the symmetric
                // `EdgeDelete` tasks (async: each endpoint surrogate is resolved
                // via the routed surrogate exchange) BEFORE entering the sync
                // tx_builder, then splice them into the modified task set there.
                //
                // Content-drift TOCTOU (a concurrent UPDATE of a matched doc's
                // `_from`/`_to`/`_type`, or an edge appearing/disappearing among
                // the matched docs, between recon and execution) is closed below:
                // the recon edge set is carried into the plan as
                // `ollp_predicted_edges` and the data plane re-derives the ACTUAL
                // edge set from the matched docs, returning `OllpRetryRequired` on
                // any divergence BEFORE writing. The existing retry loop then
                // re-scans and re-derives fresh edges.
                //
                // `predicted_edges` mirrors the recon `edges` (which carry the
                // surrogate of each edge doc) into the plan-carried wire type.
                let predicted_edges: Vec<OllpPredictedEdge> = edges
                    .iter()
                    .map(|e| OllpPredictedEdge {
                        surrogate: e.surrogate,
                        from: e.from.clone(),
                        to: e.to.clone(),
                        label: e.label.clone(),
                    })
                    .collect();

                let mut edge_delete_tasks: Vec<PhysicalTask> = Vec::new();
                append_implicit_edge_delete_tasks(
                    state,
                    &mut edge_delete_tasks,
                    tenant_id,
                    database_id,
                    TraceId::ZERO,
                    dep_collection,
                    &edges,
                )
                .await
                .map_err(|_| OllpError::Sequencer(SequencerError::Unavailable))?;

                orc.submit_with_retry_via(
                    pred_class,
                    tenant_id,
                    || {
                        let mut modified_tasks: Vec<PhysicalTask> = tasks
                            .iter()
                            .map(|t| {
                                let mut t = t.clone();
                                // `inject_ollp_surrogates` / `_predicted_edges`
                                // only touch the original BulkUpdate/BulkDelete
                                // doc tasks (no-ops on any other plan); the
                                // edge-delete tasks are appended AFTER, so they
                                // are untouched. The tx_builder may run more than
                                // once, so clone the predicted sets per task.
                                inject_ollp_surrogates(&mut t.plan, surrogates.clone());
                                inject_ollp_predicted_edges(&mut t.plan, predicted_edges.clone());
                                t
                            })
                            .collect();
                        // Clone — `submit_with_retry_via`'s tx_builder may be
                        // invoked more than once, so the edge tasks must survive
                        // a rebuild.
                        modified_tasks.extend(edge_delete_tasks.iter().cloned());
                        build_dependent_tx_class(
                            &modified_tasks,
                            tenant_id,
                            dep_collection,
                            &surrogates,
                        )
                        .map_err(|_| {
                            nodedb_cluster::error::CalvinError::Sequencer(
                                SequencerError::Unavailable,
                            )
                        })
                    },
                    |tx_class| async move {
                        submit_calvin_routed_assign(state, tx_class)
                            .await
                            .map_err(|_| OllpError::Sequencer(SequencerError::Unavailable))
                    },
                )
                .await
            }
        };

        // `rescan`: FRESH reconnaissance on each post-exec mismatch.
        let rescan = || {
            run_preexec_scan(
                &self.state,
                tenant_id,
                database_id,
                &dep_collection,
                dep_filter_bytes.clone(),
            )
        };

        run_dependent_with_retry(
            registry,
            orc,
            pred_class,
            timeout,
            ollp_max_retries,
            initial_predicted,
            submit,
            rescan,
        )
        .await
        .map_err(|e| {
            let (severity, code, message) = error_to_sqlstate(&e);
            PgWireError::UserError(Box::new(ErrorInfo::new(
                severity.to_owned(),
                code.to_owned(),
                message,
            )))
        })?;

        // Emit one CommandComplete tag per accumulated task.
        let mut calvin_responses: Vec<Response> = Vec::with_capacity(tasks.len());
        for task in &tasks {
            calvin_responses.push(calvin_execution_response(task));
        }
        Ok(calvin_responses)
    }
}
