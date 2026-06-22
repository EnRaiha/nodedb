// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-owned OLLP dependent-read retry loop.
//!
//! The retry loop for dependent-read (OLLP) Calvin transactions is owned by the
//! coordinator (the pgwire handler), not the per-vshard scheduler. On a
//! post-exec predicate-drift mismatch the loop runs a FRESH pre-execution
//! reconnaissance before resubmitting — a stale prediction can never converge
//! under predicate drift. The scheduler's only job on mismatch is to release the
//! aborted attempt's locks and signal the completion registry so this loop wakes.

use nodedb_cluster::calvin::sequencer::inbox::Inbox;
use nodedb_cluster::calvin::types::TxClass;
use nodedb_cluster::calvin::{AttemptOutcome, CalvinCompletionRegistry, TxnId};
use nodedb_types::TenantId;

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::cluster::calvin::executor::ollp::orchestrator::OllpOrchestrator;

// ── submit_once ───────────────────────────────────────────────────────────────

/// Submit a single OLLP dependent-read attempt — one admission gate, no retry.
///
/// Wraps `orchestrator.submit_with_retry` (same `tx_builder().map_err(...)`
/// shape) for use as the injected `submit` closure in [`run_dependent_with_retry`].
/// The coordinator owns the retry loop; this is the single-attempt submit the
/// loop calls on each iteration.
///
/// Returns the `inbox_seq` of the admitted txn, or the underlying [`OllpError`]
/// (circuit-open / sequencer / budget). A failure here means NOTHING executed —
/// the loop resubmits the same prediction without a fresh re-scan.
pub async fn submit_once(
    orchestrator: &OllpOrchestrator,
    inbox: &Inbox,
    predicate_class_hash: u64,
    tenant_id: TenantId,
    tx_builder: impl Fn() -> crate::Result<TxClass>,
) -> Result<u64, OllpError> {
    orchestrator
        .submit_with_retry(inbox, predicate_class_hash, tenant_id, || {
            tx_builder().map_err(|_e| {
                nodedb_cluster::error::CalvinError::Sequencer(
                    nodedb_cluster::calvin::sequencer::error::SequencerError::Unavailable,
                )
            })
        })
        .await
}

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
    SFut: std::future::Future<Output = Result<u64, OllpError>>,
    RF: FnMut() -> RFut,
    RFut: std::future::Future<Output = crate::Result<Vec<u32>>>,
{
    let mut predicted = initial_predicted;
    let mut retry: u32 = 0;
    loop {
        let inbox_seq = match submit(&predicted).await {
            Ok(seq) => seq,
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

        let assignment_rx = registry.register_submission(inbox_seq);
        let (epoch, position, _participants) = tokio::time::timeout(timeout, assignment_rx)
            .await
            .map_err(|_| Error::Internal {
                detail: "timed out waiting for Calvin assignment".into(),
            })?
            .map_err(|_| Error::Internal {
                detail: "Calvin assignment channel closed".into(),
            })?;

        let completion_rx = registry.register_completion(TxnId::new(epoch, position));
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
