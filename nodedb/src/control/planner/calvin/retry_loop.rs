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
mod tests {
    //! Unit tests for the coordinator-owned OLLP dependent-read retry loop.
    //!
    //! These drive the REAL `CalvinCompletionRegistry` via a fake "scheduler" task,
    //! without a live server/executor. The coordinator's injected `submit` closure
    //! returns a `RoutedAssignment` carrying a deterministic `(epoch, position)` —
    //! exactly the `(epoch, position)` the loop feeds to
    //! `register_completion(TxnId::new(epoch, position))`. The closure also forwards
    //! that same `TxnId` to the fake over an mpsc channel; the fake then either calls
    //! `note_ollp_mismatch(txn)` (first K submissions → `Mismatch`) or
    //! `note_completion_ack(txn, 1)` (submission K+1, 1 participant → fires
    //! `Completed`). The `rescan` closure increments a counter and returns a fresh
    //! prediction vec.
    //!
    //! Since the routed `submit` now returns the assignment itself (the leader
    //! assigns and replies with `(epoch, position)`), the loop no longer calls
    //! `register_submission` / the fake no longer needs `note_assigned`. The
    //! `(epoch, position)` source is deterministic on the test side: `epoch =
    //! inbox_seq`, `position = 0`. Both the `RoutedAssignment` returned by `submit`
    //! AND the fake's `note_completion_ack` / `note_ollp_mismatch` use that same
    //! `TxnId`, so they always match.
    //!
    //! Determinism: a current-thread runtime plus a bounded fake channel with enough
    //! capacity that the closure's `send().await` never yields control to the fake
    //! before `register_completion` runs.

    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

    use crate::control::cluster::calvin::executor::ollp::config::OllpConfig;
    use nodedb_cluster::calvin::sequencer::error::SequencerError;

    /// Build an orchestrator with zero backoff so the retry loop runs instantly.
    fn zero_backoff_orchestrator() -> OllpOrchestrator {
        OllpOrchestrator::new(OllpConfig {
            backoff_initial: std::time::Duration::ZERO,
            backoff_max: std::time::Duration::ZERO,
            ..OllpConfig::default()
        })
    }

    /// Spawn the fake scheduler. It reads `TxnId` events (the same `TxnId` the loop
    /// registers for completion) and, for the first `mismatch_count` events, signals
    /// an OLLP mismatch; on the next event it acks completion with a single
    /// participant (which fires `Completed`). The loop no longer calls
    /// `register_submission`, so `note_assigned` is not needed.
    fn spawn_fake_scheduler(
        registry: Arc<CalvinCompletionRegistry>,
        mut rx: tokio::sync::mpsc::Receiver<TxnId>,
        mismatch_count: u32,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut seen: u32 = 0;
            while let Some(txn) = rx.recv().await {
                if seen < mismatch_count {
                    registry.note_ollp_mismatch(txn);
                } else {
                    // 1 participant so a single ack completes the attempt.
                    registry.note_completion_ack(txn, 1);
                }
                seen += 1;
            }
        })
    }

    /// Build the deterministic `RoutedAssignment` for a given inbox seq: `epoch =
    /// inbox_seq`, `position = 0`, 1 participant. The matching `TxnId` is
    /// `TxnId::new(inbox_seq, 0)`.
    fn fake_assignment(inbox_seq: u64) -> RoutedAssignment {
        RoutedAssignment {
            inbox_seq,
            epoch: inbox_seq,
            position: 0,
            participants: 1,
        }
    }

    #[test]
    fn converges_after_two_mismatches() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();

            let (tx, rx) = tokio::sync::mpsc::channel::<TxnId>(16);
            let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, 2);

            let seq = Arc::new(AtomicU64::new(1));
            let submit_calls = Arc::new(AtomicU64::new(0));
            let rescan_calls = Arc::new(AtomicU32::new(0));

            let result = {
                let seq = Arc::clone(&seq);
                let submit_calls = Arc::clone(&submit_calls);
                let rescan_calls = Arc::clone(&rescan_calls);
                let tx = tx.clone();
                run_dependent_with_retry(DependentRetryArgs {
                    registry: &registry,
                    orchestrator: &orchestrator,
                    predicate_class_hash: 0xABCD,
                    timeout: std::time::Duration::from_secs(5),
                    ollp_max_retries: 5,
                    initial_predicted: vec![1, 2, 3],
                    submit: move |_predicted: &Vec<u32>| {
                        let seq = Arc::clone(&seq);
                        let submit_calls = Arc::clone(&submit_calls);
                        let tx = tx.clone();
                        async move {
                            submit_calls.fetch_add(1, Ordering::SeqCst);
                            let inbox_seq = seq.fetch_add(1, Ordering::SeqCst);
                            let assignment = fake_assignment(inbox_seq);
                            let txn = TxnId::new(assignment.epoch, assignment.position);
                            tx.send(txn).await.expect("fake recv alive");
                            Ok::<Option<RoutedAssignment>, OllpError>(Some(assignment))
                        }
                    },
                    rescan: move || {
                        let rescan_calls = Arc::clone(&rescan_calls);
                        async move {
                            let n = rescan_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(vec![100 + n])
                        }
                    },
                })
                .await
            };

            assert!(
                matches!(result, Ok(DependentOutcome::Committed(_))),
                "expected Committed, got {result:?}"
            );
            assert_eq!(
                submit_calls.load(Ordering::SeqCst),
                3,
                "two mismatches + one success → three submits"
            );
            assert_eq!(
                rescan_calls.load(Ordering::SeqCst),
                2,
                "fresh re-scan runs once per mismatch"
            );
            drop(tx);
            let _ = fake.await;
        });
    }

    #[test]
    fn exhausts_on_persistent_mismatch() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();

            // Mismatch on every attempt (large count → never acks).
            let (tx, rx) = tokio::sync::mpsc::channel::<TxnId>(16);
            let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, u32::MAX);

            let seq = Arc::new(AtomicU64::new(1));
            let submit_calls = Arc::new(AtomicU64::new(0));

            let result = {
                let seq = Arc::clone(&seq);
                let submit_calls = Arc::clone(&submit_calls);
                let tx = tx.clone();
                run_dependent_with_retry(DependentRetryArgs {
                    registry: &registry,
                    orchestrator: &orchestrator,
                    predicate_class_hash: 0xABCD,
                    timeout: std::time::Duration::from_secs(5),
                    ollp_max_retries: 3,
                    initial_predicted: vec![1],
                    submit: move |_predicted: &Vec<u32>| {
                        let seq = Arc::clone(&seq);
                        let submit_calls = Arc::clone(&submit_calls);
                        let tx = tx.clone();
                        async move {
                            submit_calls.fetch_add(1, Ordering::SeqCst);
                            let inbox_seq = seq.fetch_add(1, Ordering::SeqCst);
                            let assignment = fake_assignment(inbox_seq);
                            let txn = TxnId::new(assignment.epoch, assignment.position);
                            tx.send(txn).await.expect("fake recv alive");
                            Ok::<Option<RoutedAssignment>, OllpError>(Some(assignment))
                        }
                    },
                    rescan: move || async move { Ok(vec![1]) },
                })
                .await
            };

            assert!(
                matches!(
                    result,
                    Err(Error::OllpExhausted {
                        retries: 3,
                        cause: OllpExhaustedCause::PredicateDrift
                    })
                ),
                "expected drift exhaustion after 3 retries, got {result:?}"
            );
            assert_eq!(
                submit_calls.load(Ordering::SeqCst),
                4,
                "max_retries (3) + 1 → four submits before exhaustion"
            );
            drop(tx);
            let _ = fake.await;
        });
    }

    #[test]
    fn pre_admission_retry_does_not_rescan() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();

            // No mismatch path needed: the first two submits fail pre-admission, the
            // third admits and the fake immediately acks completion.
            let (tx, rx) = tokio::sync::mpsc::channel::<TxnId>(16);
            let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, 0);

            let seq = Arc::new(AtomicU64::new(1));
            let submit_calls = Arc::new(AtomicU64::new(0));
            let rescan_calls = Arc::new(AtomicU32::new(0));

            let result = {
                let seq = Arc::clone(&seq);
                let submit_calls = Arc::clone(&submit_calls);
                let rescan_calls = Arc::clone(&rescan_calls);
                let tx = tx.clone();
                run_dependent_with_retry(DependentRetryArgs {
                    registry: &registry,
                    orchestrator: &orchestrator,
                    predicate_class_hash: 0xABCD,
                    timeout: std::time::Duration::from_secs(5),
                    ollp_max_retries: 5,
                    initial_predicted: vec![1],
                    submit: move |_predicted: &Vec<u32>| {
                        let seq = Arc::clone(&seq);
                        let submit_calls = Arc::clone(&submit_calls);
                        let tx = tx.clone();
                        async move {
                            let n = submit_calls.fetch_add(1, Ordering::SeqCst);
                            if n < 2 {
                                // PRE-ADMISSION failure: nothing executes, no re-scan.
                                return Err(OllpError::Sequencer(SequencerError::Unavailable));
                            }
                            let inbox_seq = seq.fetch_add(1, Ordering::SeqCst);
                            let assignment = fake_assignment(inbox_seq);
                            let txn = TxnId::new(assignment.epoch, assignment.position);
                            tx.send(txn).await.expect("fake recv alive");
                            Ok::<Option<RoutedAssignment>, OllpError>(Some(assignment))
                        }
                    },
                    rescan: move || {
                        let rescan_calls = Arc::clone(&rescan_calls);
                        async move {
                            rescan_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(vec![1])
                        }
                    },
                })
                .await
            };

            assert!(
                matches!(result, Ok(DependentOutcome::Committed(_))),
                "expected Committed, got {result:?}"
            );
            assert_eq!(
                submit_calls.load(Ordering::SeqCst),
                3,
                "two pre-admission failures + one success → three submits"
            );
            assert_eq!(
                rescan_calls.load(Ordering::SeqCst),
                0,
                "pre-admission failure resubmits the SAME prediction — no re-scan"
            );
            drop(tx);
            let _ = fake.await;
        });
    }

    #[test]
    fn empty_write_set_short_circuits_without_submitting() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();
            let rescan_calls = Arc::new(AtomicU32::new(0));

            let result = {
                let rescan_calls = Arc::clone(&rescan_calls);
                run_dependent_with_retry(DependentRetryArgs {
                    registry: &registry,
                    orchestrator: &orchestrator,
                    predicate_class_hash: 0xABCD,
                    timeout: std::time::Duration::from_secs(5),
                    ollp_max_retries: 5,
                    initial_predicted: Vec::<u32>::new(),
                    // A zero-match prediction builds no TxClass, so nothing is
                    // submitted and nothing is awaited.
                    submit: move |_predicted: &Vec<u32>| async move {
                        Ok::<Option<RoutedAssignment>, OllpError>(None)
                    },
                    rescan: move || {
                        let rescan_calls = Arc::clone(&rescan_calls);
                        async move {
                            rescan_calls.fetch_add(1, Ordering::SeqCst);
                            Ok(vec![1])
                        }
                    },
                })
                .await
            };

            assert!(
                matches!(result, Ok(DependentOutcome::NoOp)),
                "expected NoOp, got {result:?}"
            );
            assert_eq!(
                rescan_calls.load(Ordering::SeqCst),
                0,
                "an empty write set is decided, not drifting — no re-scan"
            );
        });
    }

    #[test]
    fn terminal_pre_admission_error_is_surfaced_not_retried() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();
            let submit_calls = Arc::new(AtomicU64::new(0));

            let result = {
                let submit_calls = Arc::clone(&submit_calls);
                run_dependent_with_retry(DependentRetryArgs {
                    registry: &registry,
                    orchestrator: &orchestrator,
                    predicate_class_hash: 0xABCD,
                    timeout: std::time::Duration::from_secs(5),
                    ollp_max_retries: 5,
                    initial_predicted: vec![1],
                    submit: move |_predicted: &Vec<u32>| {
                        let submit_calls = Arc::clone(&submit_calls);
                        async move {
                            submit_calls.fetch_add(1, Ordering::SeqCst);
                            Err::<Option<RoutedAssignment>, OllpError>(OllpError::Terminal(
                                Box::new(Error::BadRequest {
                                    detail: "Calvin transaction spans multiple databases"
                                        .to_owned(),
                                }),
                            ))
                        }
                    },
                    rescan: move || async move { Ok(vec![1]) },
                })
                .await
            };

            assert!(
                matches!(result, Err(Error::BadRequest { .. })),
                "expected the underlying error, got {result:?}"
            );
            assert_eq!(
                submit_calls.load(Ordering::SeqCst),
                1,
                "a deterministic pre-admission failure is not retried"
            );
        });
    }

    #[test]
    fn exhausted_pre_admission_reports_its_cause_not_drift() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");
        rt.block_on(async {
            let registry = CalvinCompletionRegistry::new_detached();
            let orchestrator = zero_backoff_orchestrator();

            let result = run_dependent_with_retry(DependentRetryArgs {
                registry: &registry,
                orchestrator: &orchestrator,
                predicate_class_hash: 0xABCD,
                timeout: std::time::Duration::from_secs(5),
                ollp_max_retries: 2,
                initial_predicted: vec![1],
                // Retryable every time: the loop exhausts, and the carried cause
                // must reach the exhaustion error instead of a drift claim.
                submit: move |_predicted: &Vec<u32>| async move {
                    Err::<Option<RoutedAssignment>, OllpError>(OllpError::Retryable(Box::new(
                        Error::NoLeader {
                            vshard_id: crate::types::VShardId::new(0),
                        },
                    )))
                },
                rescan: move || async move { Ok(vec![1]) },
            })
            .await;

            let Err(Error::OllpExhausted { retries, cause }) = result else {
                panic!("expected OllpExhausted, got {result:?}");
            };
            assert_eq!(retries, 2);
            assert!(
                matches!(cause, OllpExhaustedCause::PreAdmission(_)),
                "pre-admission exhaustion must not be reported as drift"
            );
            assert!(
                !cause.to_string().contains("matching set"),
                "message must name the real cause: {cause}"
            );
        });
    }
}
