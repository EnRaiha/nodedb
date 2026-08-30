// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-owned OLLP dependent-read retry loop.
//!
//! The retry loop for dependent-read (OLLP) Calvin transactions is owned by the
//! coordinator (the pgwire handler), not the per-vshard scheduler. On a
//! post-exec predicate-drift mismatch the loop runs a FRESH pre-execution
//! reconnaissance before resubmitting — a stale prediction can never converge
//! under predicate drift. The scheduler's only job on mismatch is to release the
//! aborted attempt's locks and signal the completion registry so this loop wakes.

use nodedb_cluster::calvin::{AttemptOutcome, CalvinCompletionRegistry, TxnId};

use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::calvin::abort_error::calvin_abort_error;
use crate::control::planner::calvin::submit::RoutedAssignment;
use crate::{Error, OllpExhaustedCause};

// ── run_dependent_with_retry ──────────────────────────────────────────────────

/// Classify a pre-admission `OllpError` for the exhaustion error. A carried
/// `crate::Error` travels verbatim; the orchestrator's own admission verdicts
/// have no inner error and are reported as a refused gate.
fn pre_admission_cause(err: OllpError) -> OllpExhaustedCause {
    match err {
        OllpError::Terminal(cause) | OllpError::Retryable(cause) => {
            OllpExhaustedCause::PreAdmission(cause)
        }
        refused => OllpExhaustedCause::AdmissionRefused {
            detail: refused.to_string(),
        },
    }
}

/// Terminal outcome of the dependent-read retry loop.
#[derive(Debug)]
pub enum DependentOutcome {
    /// The dependent transaction committed; carries its `TxnId` so the caller
    /// can drain the applied response the scheduler deposited.
    Committed(TxnId),
    /// The batch decided an empty write set — the predicate matched no rows and
    /// no other task in it writes — so no Calvin entry was proposed. The
    /// statement's result is zero rows affected.
    NoOp,
}

/// Coordinator-owned OLLP dependent-read retry loop with FRESH reconnaissance
/// per attempt.
///
/// This is the single owner of the submit → await-assignment → await-completion
/// → (mismatch ? re-scan : done) loop for dependent-read Calvin transactions.
/// On a POST-EXEC predicate-drift mismatch (the executor released the aborted
/// attempt's locks and the scheduler signalled the registry), the loop runs the
/// injected `rescan` closure to produce a FRESH prediction and resubmits — a
/// stale prediction can never converge under predicate drift. On a PRE-ADMISSION
/// failure (`OllpError` from the circuit-breaker / sequencer / tenant budget),
/// nothing executed, so the loop resubmits the SAME prediction after backoff.
///
/// `submit` and `rescan` are injected so this loop is unit-testable WITHOUT a
/// live server/executor: a fake scheduler driving the real
/// [`CalvinCompletionRegistry`] suffices.
pub struct DependentRetryArgs<'a, P, SF, RF> {
    pub registry: &'a CalvinCompletionRegistry,
    pub orchestrator: &'a OllpOrchestrator,
    pub predicate_class_hash: u64,
    pub timeout: std::time::Duration,
    pub ollp_max_retries: u32,
    pub initial_predicted: P,
    pub submit: SF,
    pub rescan: RF,
}

pub async fn run_dependent_with_retry<P, SF, SFut, RF, RFut>(
    args: DependentRetryArgs<'_, P, SF, RF>,
) -> crate::Result<DependentOutcome>
where
    SF: FnMut(&P) -> SFut,
    SFut: std::future::Future<Output = Result<Option<RoutedAssignment>, OllpError>>,
    RF: FnMut() -> RFut,
    RFut: std::future::Future<Output = crate::Result<P>>,
{
    let DependentRetryArgs {
        registry,
        orchestrator,
        predicate_class_hash,
        timeout,
        ollp_max_retries,
        initial_predicted,
        mut submit,
        mut rescan,
    } = args;
    let mut predicted = initial_predicted;
    let mut retry: u32 = 0;
    loop {
        let assignment = match submit(&predicted).await {
            Ok(Some(assignment)) => assignment,
            // Nothing to write: the prediction decided an empty write set, so
            // no transaction was submitted and there is nothing to await.
            Ok(None) => return Ok(DependentOutcome::NoOp),
            // Deterministic pre-admission failure (TxClass construction,
            // authorization, edge-task synthesis). The same inputs reproduce
            // it, so surface the real error instead of retrying and reporting
            // predicate drift that never happened.
            Err(OllpError::Terminal(error)) => return Err(*error),
            Err(ollp_err) => {
                // PRE-ADMISSION failure (routed submit / circuit / budget).
                // Nothing executed, so there is no aborted attempt to re-scan
                // around — resubmit the SAME prediction after backoff.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
                        cause: pre_admission_cause(ollp_err),
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry)
                    .await;
                retry += 1;
                continue;
            }
        };

        // The assignment (`epoch`/`position`) is produced by
        // `submit_calvin_routed_assign` inside the injected `submit` closure —
        // which routes to the sequencer-group leader and awaits only the
        // assignment phase. The coordinator then awaits completion on its local
        // registry, which receives the replicated completion ack on every
        // sequencer-group member.
        let txn_id = TxnId::new(assignment.epoch, assignment.position);
        let completion_rx = registry.register_completion(txn_id, assignment.participants);
        let outcome = tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| Error::Internal {
                detail: "timed out waiting for Calvin completion".into(),
            })?
            .map_err(|_| Error::Internal {
                detail: "Calvin completion channel closed".into(),
            })?;

        match outcome {
            // Return the completed txn's id so the caller can drain the applied
            // Response (RETURNING rows) the scheduler deposited before the ack.
            AttemptOutcome::Completed => return Ok(DependentOutcome::Committed(txn_id)),
            // Terminal, NON-retryable: the global cross-shard verdict was ABORT.
            // A committed verdict is not OLLP predicate drift — a fresh
            // reconnaissance cannot change it — so surface it to the client
            // immediately instead of burning retries. The verdict's reason picks
            // the error: a stale read-set is SQLSTATE 40001, a participant error
            // is not.
            AttemptOutcome::Aborted { reason } => {
                return Err(calvin_abort_error(reason));
            }
            // Terminal, NON-retryable: the scheduler rejected the transaction's
            // local plan routing and broadcast `TxnRoutingFailed`. A fresh
            // reconnaissance can never fix a routing rejection, so surface it
            // to the caller immediately instead of burning retries.
            AttemptOutcome::Failed { detail } => {
                return Err(Error::Internal {
                    detail: format!("calvin transaction routing failed: {detail}"),
                });
            }
            AttemptOutcome::Mismatch => {
                // POST-EXEC predicate drift. The scheduler already released the
                // aborted attempt's locks before signalling the registry, so a
                // FRESH reconnaissance is safe — and necessary, since the stale
                // prediction can never converge under drift.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
                        cause: OllpExhaustedCause::PredicateDrift,
                    });
                }
                orchestrator
                    .on_retry_required(predicate_class_hash, retry)
                    .await;
                retry += 1;
                predicted = rescan().await?;
            }
        }
    }
}

#[cfg(test)]
#[path = "retry_loop_tests.rs"]
mod tests;
