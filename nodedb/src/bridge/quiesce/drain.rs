// SPDX-License-Identifier: BUSL-1.1

//! Drain coordination: `wait_until_drained`.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::refcount::CollectionQuiesce;

impl CollectionQuiesce {
    /// Returns a future that resolves once every open scan against
    /// `(tenant_id, collection)` has completed. Safe to await from the
    /// Control Plane (tokio) — internally uses [`tokio::sync::Notify`]
    /// for wake-up; no polling.
    ///
    /// `begin_drain` must be called before awaiting this future, or
    /// new scans could continue to bump the counter and the future
    /// would never resolve.
    pub fn wait_until_drained(
        self: &Arc<Self>,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
    ) -> WaitDrain {
        WaitDrain {
            registry: Arc::clone(self),
            database_id,
            tenant_id,
            collection: collection.to_string(),
            notified: None,
        }
    }
}

/// Future returned by [`CollectionQuiesce::wait_until_drained`].
///
/// Completes when the `(tenant, collection)` open-scan count reaches 0.
/// Implementation detail: each poll takes a fresh `Notify::notified()`
/// future so we don't race against a notification that fires between
/// check and await.
pub struct WaitDrain {
    registry: Arc<CollectionQuiesce>,
    database_id: u64,
    tenant_id: u64,
    collection: String,
    notified: Option<Pin<Box<tokio::sync::futures::Notified<'static>>>>,
}

// Safety: Notified borrows from the Notify inside `registry` (Arc).
// We transmute the lifetime to `'static` because we own the Arc for the
// future's lifetime, guaranteeing the Notify outlives the Notified.
unsafe impl Send for WaitDrain {}

impl Future for WaitDrain {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if self
                .registry
                .open_scans(self.database_id, self.tenant_id, &self.collection)
                == 0
            {
                return Poll::Ready(());
            }
            // Arm a notification, then re-check. If a release fires
            // between the check and the arm we handled it on the next
            // iteration (open_scans would be 0 then).
            let registry = Arc::clone(&self.registry);
            let fut = self.notified.get_or_insert_with(|| {
                let notify: &tokio::sync::Notify = &registry.notify;
                // SAFETY: `self` holds an Arc<CollectionQuiesce> for its
                // whole life; the Notify inside it outlives `self`.
                let notified: tokio::sync::futures::Notified<'_> = notify.notified();
                let notified: tokio::sync::futures::Notified<'static> =
                    unsafe { std::mem::transmute(notified) };
                Box::pin(notified)
            });
            match fut.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.notified = None;
                    // Loop: re-check open_scans.
                    continue;
                }
                Poll::Pending => {
                    // Re-check once before sleeping to close the race
                    // between arming the notified future and a release
                    // that just happened.
                    if self
                        .registry
                        .open_scans(self.database_id, self.tenant_id, &self.collection)
                        == 0
                    {
                        return Poll::Ready(());
                    }
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const DB: u64 = 0;

    #[tokio::test]
    async fn drain_resolves_immediately_when_no_open_scans() {
        let q = CollectionQuiesce::new();
        let _hold = q.begin_drain(DB, 1, "c");
        q.wait_until_drained(DB, 1, "c").await;
    }

    #[tokio::test]
    async fn drain_waits_for_last_scan_to_release() {
        let q = CollectionQuiesce::new();
        let g1 = q.try_start_scan(DB, 1, "c").unwrap();
        let g2 = q.try_start_scan(DB, 1, "c").unwrap();
        let _hold = q.begin_drain(DB, 1, "c");

        let q_clone = Arc::clone(&q);
        let drain_task = tokio::spawn(async move {
            q_clone.wait_until_drained(DB, 1, "c").await;
        });

        // Briefly yield so the drain task parks.
        tokio::task::yield_now().await;
        assert!(
            !drain_task.is_finished(),
            "drain must not resolve while scans open"
        );

        drop(g1);
        tokio::task::yield_now().await;
        assert!(
            !drain_task.is_finished(),
            "drain must not resolve with 1 scan still open"
        );

        drop(g2);
        drain_task.await.unwrap();
    }

    #[tokio::test]
    async fn a_released_hold_clears_the_drain() {
        let q = CollectionQuiesce::new();
        let hold = q.begin_drain(DB, 1, "c");
        assert!(q.is_draining(DB, 1, "c"));

        hold.release();
        assert!(!q.is_draining(DB, 1, "c"));
    }

    #[tokio::test]
    async fn is_draining_until_every_holder_releases() {
        let q = CollectionQuiesce::new();
        let first = q.begin_drain(DB, 1, "c");
        let second = q.begin_drain(DB, 1, "c");

        // One holder released — still draining while the other holds.
        drop(first);
        assert!(q.is_draining(DB, 1, "c"));

        // Last holder released — drain clears.
        drop(second);
        assert!(!q.is_draining(DB, 1, "c"));
        assert!(q.try_start_scan(DB, 1, "c").is_ok());
    }
}
