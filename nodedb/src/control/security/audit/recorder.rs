// SPDX-License-Identifier: BUSL-1.1

//! Bounded hand-off of audit records to a Control Plane recorder task.
//!
//! Recording an entry appends the audit WAL and can wait for a flush.
//! The Event Plane must do neither, so it hands its records to this recorder.
//! The channel is bounded. A full channel makes the sender's `await` wait, and no record is dropped.
//! That wait slows the Event Plane consumer, and its ring backpressure then applies.
//! The sender gets an answer only once every record of its batch is recorded.
//! So the Event Plane watermark never passes an event whose audit record is not durable.
//!
//! The recorder records a whole batch in one call, so the batch costs one WAL sync.
//! Commit is per batch: either the batch is recorded, or the sender gets all of it back.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nodedb_types::DatabaseId;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use super::event::AuditEvent;
use crate::types::TenantId;

/// Batches the recorder channel holds before a sender waits.
pub const RECORDER_CHANNEL_CAPACITY: usize = 64;

/// One audit record waiting to be recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingAudit {
    pub event: AuditEvent,
    pub tenant_id: Option<TenantId>,
    pub database_id: Option<DatabaseId>,
    pub source: String,
    pub detail: String,
}

/// The commit mark of one batch.
///
/// The record callback calls [`Self::mark_committed`] once the batch is in the audit log.
/// It must do so before any later step that can panic, such as export.
/// An unmarked batch goes back to the sender whole.
#[derive(Clone, Default)]
pub struct BatchCommit(Arc<AtomicBool>);

impl BatchCommit {
    /// Record that the whole batch is in the audit log.
    pub fn mark_committed(&self) {
        self.0.store(true, Ordering::Release);
    }

    fn is_committed(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Records one whole batch and marks it committed.
/// Returns `false` when it cannot record the batch, which returns the batch to the sender.
pub type RecordBatch = Arc<dyn Fn(&[PendingAudit], &BatchCommit) -> bool + Send + Sync>;

/// Runs once after each committed batch, such as the backpressure wait. It records nothing.
pub type AfterBatch = Arc<dyn Fn() + Send + Sync>;

/// One batch on the channel, with the commit mark and answer the sender reads.
///
/// The sender keeps its own references to `records` and `commit`.
/// So after any failure the sender knows whether the batch is recorded.
struct AuditBatch {
    records: Arc<Vec<PendingAudit>>,
    commit: BatchCommit,
    finished: oneshot::Sender<()>,
}

/// Sender side of the recorder channel.
pub struct AuditRecorder {
    tx: mpsc::Sender<AuditBatch>,
}

impl AuditRecorder {
    /// Spawn the recorder task on the current Tokio runtime.
    ///
    /// The task records batches in arrival order, one whole batch per call, on the blocking pool.
    /// It ends when every sender is dropped.
    pub fn spawn(capacity: usize, record_batch: RecordBatch, after_batch: AfterBatch) -> Self {
        let (tx, mut rx) = mpsc::channel::<AuditBatch>(capacity.max(1));
        tokio::spawn(async move {
            while let Some(AuditBatch {
                records,
                commit,
                finished,
            }) = rx.recv().await
            {
                let record_batch = Arc::clone(&record_batch);
                let after_batch = Arc::clone(&after_batch);
                let outcome = tokio::task::spawn_blocking(move || {
                    if record_batch(&records, &commit) && commit.is_committed() {
                        after_batch();
                    }
                })
                .await;
                if let Err(join_error) = outcome {
                    error!(error = %join_error, "audit recorder batch panicked");
                }
                // A sender that stopped waiting no longer needs the answer.
                let _ = finished.send(());
            }
        });
        Self { tx }
    }

    /// Hand `records` to the recorder and wait until every one is recorded.
    ///
    /// A full channel makes this wait. The wait yields to the runtime and never blocks a thread.
    /// Returns all of `records` when the recorder did not commit the batch, so the caller records them another way.
    /// A committed batch is never returned, so no record is recorded twice.
    pub async fn record(&self, records: Vec<PendingAudit>) -> Result<(), Vec<PendingAudit>> {
        if records.is_empty() {
            return Ok(());
        }
        let records = Arc::new(records);
        let commit = BatchCommit::default();
        let (finished, answer) = oneshot::channel();
        let batch = AuditBatch {
            records: Arc::clone(&records),
            commit: commit.clone(),
            finished,
        };
        // A closed channel, a refused batch, or a task gone before it answered all end here.
        if self.tx.send(batch).await.is_ok() {
            let _ = answer.await;
        }
        if commit.is_committed() {
            return Ok(());
        }
        Err(Arc::try_unwrap(records).unwrap_or_else(|shared| shared.as_ref().clone()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::time::Duration;

    use super::*;

    fn no_after_batch() -> AfterBatch {
        Arc::new(|| {})
    }

    /// A batch callback that appends each record's detail to `recorded`, then marks the commit.
    fn collecting(recorded: &Arc<Mutex<Vec<String>>>) -> RecordBatch {
        let recorded = Arc::clone(recorded);
        Arc::new(move |batch: &[PendingAudit], commit: &BatchCommit| {
            recorded
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .extend(batch.iter().map(|record| record.detail.clone()));
            commit.mark_committed();
            true
        })
    }

    fn pending(i: usize) -> PendingAudit {
        PendingAudit {
            event: AuditEvent::DmlAudit,
            tenant_id: None,
            database_id: None,
            source: "test".into(),
            detail: format!("record-{i}"),
        }
    }

    #[tokio::test]
    async fn full_channel_waits_without_blocking_and_loses_nothing() {
        // The gate holds the recorder inside its first batch, so the channel fills.
        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let recorded = Arc::new(Mutex::new(Vec::<String>::new()));
        let record: RecordBatch = {
            let gate = Arc::clone(&gate);
            let inner = collecting(&recorded);
            Arc::new(move |batch: &[PendingAudit], commit: &BatchCommit| {
                let (open, opened) = &*gate;
                let mut open = open.lock().unwrap_or_else(|p| p.into_inner());
                while !*open {
                    open = opened.wait(open).unwrap_or_else(|p| p.into_inner());
                }
                drop(open);
                inner(batch, commit)
            })
        };
        let recorder = Arc::new(AuditRecorder::spawn(1, record, no_after_batch()));

        let senders: Vec<_> = (0..8)
            .map(|i| {
                let recorder = Arc::clone(&recorder);
                tokio::spawn(async move { recorder.record(vec![pending(i)]).await })
            })
            .collect();

        // With the recorder held and the channel full, the runtime still runs other work.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            senders.iter().all(|s| !s.is_finished()),
            "no sender gets an answer before its record is recorded"
        );

        let (open, opened) = &*gate;
        *open.lock().expect("gate") = true;
        opened.notify_all();
        for sender in senders {
            assert_eq!(sender.await.expect("sender task"), Ok(()));
        }
        let mut details = recorded.lock().expect("recorded").clone();
        details.sort();
        let mut expected: Vec<String> = (0..8).map(|i| format!("record-{i}")).collect();
        expected.sort();
        assert_eq!(details, expected, "every record is recorded exactly once");
    }

    #[tokio::test]
    async fn refused_batch_comes_back_to_the_sender() {
        let recorder = AuditRecorder::spawn(
            4,
            Arc::new(|_batch: &[PendingAudit], _commit: &BatchCommit| false),
            no_after_batch(),
        );
        let back = recorder.record(vec![pending(1), pending(2)]).await;
        assert_eq!(back, Err(vec![pending(1), pending(2)]));
    }

    #[tokio::test]
    async fn panic_before_commit_returns_the_whole_batch() {
        let recorded = Arc::new(Mutex::new(Vec::<String>::new()));
        let panicked = Arc::new(AtomicBool::new(false));
        let record: RecordBatch = {
            let inner = collecting(&recorded);
            let panicked = Arc::clone(&panicked);
            Arc::new(move |batch: &[PendingAudit], commit: &BatchCommit| {
                // The first call dies before the batch reaches the log, as a WAL panic does.
                if !panicked.swap(true, Ordering::AcqRel) {
                    panic!("injected recorder panic before the sync");
                }
                inner(batch, commit)
            })
        };
        let recorder = AuditRecorder::spawn(4, record, no_after_batch());

        let back = recorder
            .record((0..5).map(pending).collect())
            .await
            .expect_err("the panic must stop the batch");
        assert_eq!(back, (0..5).map(pending).collect::<Vec<_>>());

        // The caller records the returned batch, as `hold_audits` does.
        assert_eq!(recorder.record(back).await, Ok(()));
        let details = recorded.lock().expect("recorded").clone();
        let expected: Vec<String> = (0..5).map(|i| format!("record-{i}")).collect();
        assert_eq!(
            details, expected,
            "each record is recorded exactly once, in order"
        );
    }

    #[tokio::test]
    async fn panic_after_commit_returns_nothing() {
        let recorded = Arc::new(Mutex::new(Vec::<String>::new()));
        let record: RecordBatch = {
            let inner = collecting(&recorded);
            Arc::new(move |batch: &[PendingAudit], commit: &BatchCommit| {
                inner(batch, commit);
                // A later step, such as export, dies after the batch is in the log.
                panic!("injected recorder panic after the commit");
            })
        };
        let recorder = AuditRecorder::spawn(4, record, no_after_batch());
        assert_eq!(recorder.record((0..3).map(pending).collect()).await, Ok(()));
        assert_eq!(recorded.lock().expect("recorded").len(), 3);
    }

    #[tokio::test]
    async fn after_batch_runs_once_per_batch() {
        let runs = Arc::new(AtomicUsize::new(0));
        let after: AfterBatch = {
            let runs = Arc::clone(&runs);
            Arc::new(move || {
                runs.fetch_add(1, Ordering::AcqRel);
            })
        };
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let recorder = AuditRecorder::spawn(4, collecting(&recorded), after);
        assert_eq!(recorder.record((0..3).map(pending).collect()).await, Ok(()));
        assert_eq!(runs.load(Ordering::Acquire), 1);
    }
}
