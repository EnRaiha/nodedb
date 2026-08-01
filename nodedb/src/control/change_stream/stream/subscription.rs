// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::{DatabaseId, TenantId};

use super::{ChangeEvent, SequencedChangeEvent};

/// A filtered, bounded change-stream subscription.
pub struct Subscription {
    pub id: u64,
    /// Legacy raw-event receiver retained for public API compatibility.
    pub receiver: tokio::sync::broadcast::Receiver<ChangeEvent>,
    pub collection_filter: Option<String>,
    pub tenant_filter: Option<TenantId>,
    pub field_filter: Vec<String>,
    pub(super) sequenced_receiver: tokio::sync::broadcast::Receiver<SequencedChangeEvent>,
    pub(super) database_filter: Option<DatabaseId>,
    pub(super) active_counter: Arc<AtomicU64>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.active_counter.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Subscription {
    pub async fn recv_sequenced(
        &mut self,
    ) -> Result<SequencedChangeEvent, tokio::sync::broadcast::error::RecvError> {
        loop {
            let event = self.sequenced_receiver.recv().await?;
            if self.matches_sequenced(&event) {
                return Ok(event);
            }
        }
    }

    /// Compatibility API for trigger and pgwire consumers which do not expose
    /// publication cursors. It consumes the database-aware channel and strips
    /// only the cursor wrapper, so non-default subscriptions remain isolated.
    pub async fn recv_filtered(
        &mut self,
    ) -> Result<ChangeEvent, tokio::sync::broadcast::error::RecvError> {
        self.recv_sequenced()
            .await
            .map(SequencedChangeEvent::into_event)
    }

    /// Non-blocking database-aware receive retaining the publication cursor.
    ///
    /// Consumers that need continuity validation (such as pgwire LIVE) must
    /// use this API rather than the compatibility API below.
    pub fn try_recv_sequenced(
        &mut self,
    ) -> Result<SequencedChangeEvent, tokio::sync::broadcast::error::TryRecvError> {
        loop {
            let event = self.sequenced_receiver.try_recv()?;
            if self.matches_sequenced(&event) {
                return Ok(event);
            }
        }
    }

    /// Non-blocking compatibility receive for consumers that do not expose
    /// publication cursors.
    pub fn try_recv_filtered(
        &mut self,
    ) -> Result<ChangeEvent, tokio::sync::broadcast::error::TryRecvError> {
        self.try_recv_sequenced()
            .map(SequencedChangeEvent::into_event)
    }

    fn matches_sequenced(&self, event: &SequencedChangeEvent) -> bool {
        self.matches_raw(event)
            && self
                .database_filter
                .is_none_or(|database| event.database_id() == database)
    }

    fn matches_raw(&self, event: &ChangeEvent) -> bool {
        self.collection_filter
            .as_ref()
            .is_none_or(|collection| event.collection == *collection)
            && self
                .tenant_filter
                .as_ref()
                .is_none_or(|tenant| event.tenant_id == *tenant)
    }
}
