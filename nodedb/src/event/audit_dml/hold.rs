// SPDX-License-Identifier: BUSL-1.1

//! Hand a delivered batch's audit records to the Control Plane recorder.
//!
//! The Event Plane does no storage I/O, so it never records an audit entry itself.
//! It sends the batch over the recorder's bounded channel and awaits the answer.
//! A full channel makes the await wait, which slows the consumer.
//! The ring then fills, and the Event Plane's ring backpressure and WAL catch-up apply.
//! No record is dropped, and no thread blocks.

use std::sync::Arc;

use tracing::error;

use crate::control::security::audit::PendingAudit;
use crate::control::state::SharedState;

/// Record `records` through the Control Plane recorder. Returns once every one is durable.
///
/// The caller awaits this before its watermark passes the batch.
/// So a crash before the answer leaves the events above the watermark, and WAL catch-up delivers them again.
pub async fn hold_audits(state: &Arc<SharedState>, records: Vec<PendingAudit>) {
    if records.is_empty() {
        return;
    }
    let Err(records) = state.audit_recorder().record(records).await else {
        return;
    };
    // The recorder is gone, or its batch stopped partway. `records` is only the unrecorded tail.
    // The tail goes to the blocking pool, so this Event Plane task still does no storage I/O.
    error!(
        count = records.len(),
        "audit recorder unavailable, recording the batch on the blocking pool"
    );
    let state = Arc::clone(state);
    if let Err(e) = tokio::task::spawn_blocking(move || state.record_pending_audits(&records)).await
    {
        error!(error = %e, "audit fallback recording panicked");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::audit::AuditEvent;
    use crate::wal::WalManager;

    fn pending(detail: &str) -> PendingAudit {
        PendingAudit {
            event: AuditEvent::DmlAudit,
            tenant_id: None,
            database_id: None,
            source: "event_plane_test".into(),
            detail: detail.into(),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn held_records_are_recorded_before_the_call_returns() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join("hold.wal")).expect("open WAL"));
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("shared state");

        let batch: Vec<PendingAudit> = (0..200).map(|i| pending(&format!("dml-{i}"))).collect();
        hold_audits(&state, batch).await;

        let recorded = state
            .recent_audit_entries(usize::MAX, |e| e.source == "event_plane_test")
            .expect("read audit");
        assert_eq!(recorded.len(), 200, "every handed-over record is recorded");
    }
}
