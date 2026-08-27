// SPDX-License-Identifier: BUSL-1.1

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
                        Err::<Option<RoutedAssignment>, OllpError>(OllpError::Terminal(Box::new(
                            Error::BadRequest {
                                detail: "Calvin transaction spans multiple databases".to_owned(),
                            },
                        )))
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
