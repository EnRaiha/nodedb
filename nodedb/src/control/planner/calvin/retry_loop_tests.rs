// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for the coordinator-owned OLLP dependent-read retry loop.
//!
//! These drive the REAL `CalvinCompletionRegistry` via a fake "scheduler" task,
//! without a live server/executor. The coordinator's injected `submit` closure
//! allocates a fresh `inbox_seq` + `TxnId` and forwards them to the fake over an
//! mpsc channel; the fake calls `note_assigned`, then either `note_ollp_mismatch`
//! (first K submissions) or `note_completion_ack` (submission K+1, 1 participant
//! → fires `Completed`). The `rescan` closure increments a counter and returns a
//! fresh prediction vec.
//!
//! Determinism: a current-thread runtime guarantees the coordinator loop runs
//! `register_submission(inbox_seq)` (synchronously after `submit().await` returns,
//! before the next `.await`) BEFORE the fake task gets scheduled and calls
//! `note_assigned`. The bounded fake channel has enough capacity that the
//! closure's `send().await` never yields control to the fake.

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

/// Spawn the fake scheduler. It reads `(inbox_seq, txn)` events and, for the
/// first `mismatch_count` events, signals an OLLP mismatch; on the next event it
/// acks completion with a single participant (which fires `Completed`).
fn spawn_fake_scheduler(
    registry: Arc<CalvinCompletionRegistry>,
    mut rx: tokio::sync::mpsc::Receiver<(u64, TxnId)>,
    mismatch_count: u32,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut seen: u32 = 0;
        while let Some((inbox_seq, txn)) = rx.recv().await {
            // 1 expected participant so a single ack completes the attempt.
            registry.note_assigned(inbox_seq, txn, 1);
            if seen < mismatch_count {
                registry.note_ollp_mismatch(txn);
            } else {
                registry.note_completion_ack(txn, 1);
            }
            seen += 1;
        }
    })
}

#[test]
fn converges_after_two_mismatches() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build current-thread runtime");
    rt.block_on(async {
        let registry = CalvinCompletionRegistry::new();
        let orchestrator = zero_backoff_orchestrator();

        let (tx, rx) = tokio::sync::mpsc::channel::<(u64, TxnId)>(16);
        let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, 2);

        let seq = Arc::new(AtomicU64::new(1));
        let submit_calls = Arc::new(AtomicU64::new(0));
        let rescan_calls = Arc::new(AtomicU32::new(0));

        let result = {
            let seq = Arc::clone(&seq);
            let submit_calls = Arc::clone(&submit_calls);
            let rescan_calls = Arc::clone(&rescan_calls);
            let tx = tx.clone();
            run_dependent_with_retry(
                &registry,
                &orchestrator,
                0xABCD,
                std::time::Duration::from_secs(5),
                5,
                vec![1, 2, 3],
                move |_predicted: &[u32]| {
                    let seq = Arc::clone(&seq);
                    let submit_calls = Arc::clone(&submit_calls);
                    let tx = tx.clone();
                    async move {
                        submit_calls.fetch_add(1, Ordering::SeqCst);
                        let inbox_seq = seq.fetch_add(1, Ordering::SeqCst);
                        let txn = TxnId::new(inbox_seq, 0);
                        tx.send((inbox_seq, txn)).await.expect("fake recv alive");
                        Ok::<u64, OllpError>(inbox_seq)
                    }
                },
                move || {
                    let rescan_calls = Arc::clone(&rescan_calls);
                    async move {
                        let n = rescan_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![100 + n])
                    }
                },
            )
            .await
        };

        assert!(result.is_ok(), "expected Ok(()), got {result:?}");
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
        let registry = CalvinCompletionRegistry::new();
        let orchestrator = zero_backoff_orchestrator();

        // Mismatch on every attempt (large count → never acks).
        let (tx, rx) = tokio::sync::mpsc::channel::<(u64, TxnId)>(16);
        let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, u32::MAX);

        let seq = Arc::new(AtomicU64::new(1));
        let submit_calls = Arc::new(AtomicU64::new(0));

        let result = {
            let seq = Arc::clone(&seq);
            let submit_calls = Arc::clone(&submit_calls);
            let tx = tx.clone();
            run_dependent_with_retry(
                &registry,
                &orchestrator,
                0xABCD,
                std::time::Duration::from_secs(5),
                3,
                vec![1],
                move |_predicted: &[u32]| {
                    let seq = Arc::clone(&seq);
                    let submit_calls = Arc::clone(&submit_calls);
                    let tx = tx.clone();
                    async move {
                        submit_calls.fetch_add(1, Ordering::SeqCst);
                        let inbox_seq = seq.fetch_add(1, Ordering::SeqCst);
                        let txn = TxnId::new(inbox_seq, 0);
                        tx.send((inbox_seq, txn)).await.expect("fake recv alive");
                        Ok::<u64, OllpError>(inbox_seq)
                    }
                },
                move || async move { Ok(vec![1]) },
            )
            .await
        };

        assert!(
            matches!(result, Err(Error::OllpExhausted { retries: 3 })),
            "expected OllpExhausted {{ retries: 3 }}, got {result:?}"
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
        let registry = CalvinCompletionRegistry::new();
        let orchestrator = zero_backoff_orchestrator();

        // No mismatch path needed: the first two submits fail pre-admission, the
        // third admits and the fake immediately acks completion.
        let (tx, rx) = tokio::sync::mpsc::channel::<(u64, TxnId)>(16);
        let fake = spawn_fake_scheduler(Arc::clone(&registry), rx, 0);

        let seq = Arc::new(AtomicU64::new(1));
        let submit_calls = Arc::new(AtomicU64::new(0));
        let rescan_calls = Arc::new(AtomicU32::new(0));

        let result = {
            let seq = Arc::clone(&seq);
            let submit_calls = Arc::clone(&submit_calls);
            let rescan_calls = Arc::clone(&rescan_calls);
            let tx = tx.clone();
            run_dependent_with_retry(
                &registry,
                &orchestrator,
                0xABCD,
                std::time::Duration::from_secs(5),
                5,
                vec![1],
                move |_predicted: &[u32]| {
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
                        let txn = TxnId::new(inbox_seq, 0);
                        tx.send((inbox_seq, txn)).await.expect("fake recv alive");
                        Ok::<u64, OllpError>(inbox_seq)
                    }
                },
                move || {
                    let rescan_calls = Arc::clone(&rescan_calls);
                    async move {
                        rescan_calls.fetch_add(1, Ordering::SeqCst);
                        Ok(vec![1])
                    }
                },
            )
            .await
        };

        assert!(result.is_ok(), "expected Ok(()), got {result:?}");
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
