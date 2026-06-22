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

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;
use crate::control::planner::calvin::submit::RoutedAssignment;

// ── run_dependent_with_retry ──────────────────────────────────────────────────

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
#[allow(clippy::too_many_arguments)]
pub async fn run_dependent_with_retry<SF, SFut, RF, RFut>(
    registry: &CalvinCompletionRegistry,
    orchestrator: &OllpOrchestrator,
    predicate_class_hash: u64,
    timeout: std::time::Duration,
    ollp_max_retries: u32,
    initial_predicted: Vec<u32>,
    mut submit: SF,
    mut rescan: RF,
) -> crate::Result<()>
where
    SF: FnMut(&[u32]) -> SFut,
    SFut: std::future::Future<Output = Result<RoutedAssignment, OllpError>>,
    RF: FnMut() -> RFut,
    RFut: std::future::Future<Output = crate::Result<Vec<u32>>>,
{
    let mut predicted = initial_predicted;
    let mut retry: u32 = 0;
    loop {
        let assignment = match submit(&predicted).await {
            Ok(assignment) => assignment,
            Err(_ollp_err) => {
                // PRE-ADMISSION failure (circuit/sequencer/budget). Nothing
                // executed, so there is no aborted attempt to re-scan around —
                // resubmit the SAME prediction after backoff bookkeeping.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
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
        let completion_rx = registry.register_completion(
            TxnId::new(assignment.epoch, assignment.position),
            assignment.participants,
        );
        let outcome = tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| Error::Internal {
                detail: "timed out waiting for Calvin completion".into(),
            })?
            .map_err(|_| Error::Internal {
                detail: "Calvin completion channel closed".into(),
            })?;

        match outcome {
            AttemptOutcome::Completed => return Ok(()),
            AttemptOutcome::Mismatch => {
                // POST-EXEC predicate drift. The scheduler already released the
                // aborted attempt's locks before signalling the registry, so a
                // FRESH reconnaissance is safe — and necessary, since the stale
                // prediction can never converge under drift.
                if retry >= ollp_max_retries {
                    return Err(Error::OllpExhausted {
                        retries: ollp_max_retries.min(u8::MAX as u32) as u8,
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
