// SPDX-License-Identifier: BUSL-1.1

//! Records a task sent to cores and closes from their answers itself.
//!
//! [`super::spawn_owned_wait`] closes one request's records from its final
//! response. A task that fans one record out to several cores and decides the
//! close from all their answers owns the records instead. Once they are sent,
//! a drop of that task (an abort, or a runtime shutdown) leaves their outcome
//! unknown. [`SentRecords`] then holds the window: restart replay reaches the
//! records, and the floor files no leak.

use super::records::MintedRecords;

/// Records sent to at least one core, closed by the task that holds them.
#[must_use = "sent records hold the outcome floor until they settle or hold"]
pub(crate) struct SentRecords {
    /// `None` once a close took the records.
    records: Option<MintedRecords>,
}

impl SentRecords {
    /// Mark `records` sent. Call it in the same synchronous step as the
    /// enqueue of the requests that carry them.
    pub(crate) fn sent(records: MintedRecords) -> Self {
        records.mark_sent();
        Self {
            records: Some(records),
        }
    }

    /// Every core's outcome is final.
    pub(crate) fn settle(mut self) {
        if let Some(records) = self.records.take() {
            records.settle();
        }
    }

    /// Some core has no final outcome in this process.
    pub(crate) fn hold(mut self) {
        if let Some(records) = self.records.take() {
            records.hold();
        }
    }
}

impl Drop for SentRecords {
    fn drop(&mut self) {
        if let Some(records) = self.records.take() {
            records.hold();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::OutcomeFloor;
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use crate::wal::WalManager;
    use crate::wal::manager::NO_APPLY_KEY;

    fn sent_record(wal: &Arc<WalManager>, floor: &Arc<OutcomeFloor>) -> (SentRecords, Lsn) {
        let minted = MintedRecords::open(floor);
        let lsn = minted
            .appender(wal, NO_APPLY_KEY)
            .with_event_source(crate::event::EventSource::User)
            .append_put(
                TenantId::new(1),
                VShardId::new(0),
                DatabaseId::DEFAULT,
                b"x",
            )
            .expect("append");
        (SentRecords::sent(minted), lsn)
    }

    /// The task that owns sent records is aborted mid-wait. The window
    /// holds: nothing leaks, and the floor stays below the record.
    #[tokio::test]
    async fn an_aborted_owner_holds_its_sent_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (sent, lsn) = sent_record(&wal, &floor);

        let owner = tokio::spawn(async move {
            std::future::pending::<()>().await;
            sent.settle();
        });
        tokio::task::yield_now().await;
        owner.abort();
        assert!(owner.await.is_err(), "the owner was aborted");

        assert!(floor.floor() < lsn, "restart replay must reach the record");
        assert_eq!(floor.leaked_windows(), 0);
        assert_eq!(floor.held_windows(), 1);
    }

    #[test]
    fn settled_sent_records_release_the_floor_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = Arc::new(WalManager::open_for_testing(&dir.path().join("wal")).expect("wal"));
        let floor = OutcomeFloor::new();
        let (sent, lsn) = sent_record(&wal, &floor);

        sent.settle();

        assert!(floor.floor() >= lsn);
        assert_eq!(floor.leaked_windows(), 0);
        assert_eq!(floor.held_windows(), 0);
    }
}
