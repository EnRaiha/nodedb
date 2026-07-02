// SPDX-License-Identifier: BUSL-1.1

//! LIVE SELECT subscription methods on SessionStore.

use std::net::SocketAddr;

use super::store::SessionStore;

impl SessionStore {
    /// Store a LIVE SELECT subscription for a connection.
    ///
    /// `channel` is the notification channel name (e.g., "live_orders").
    pub fn add_live_subscription(
        &self,
        addr: &SocketAddr,
        channel: String,
        sub: crate::control::change_stream::Subscription,
    ) {
        self.write_session(addr, |session| {
            session.live_subscriptions.push((channel, sub));
        });
    }

    /// Drain pending change events from all LIVE SELECT subscriptions
    /// for a connection. Returns `(channel, payload)` pairs ready to be
    /// sent as pgwire `NotificationResponse` messages.
    ///
    /// Non-blocking: uses `try_recv` to avoid waiting. Called between
    /// queries to deliver notifications in the PostgreSQL standard way.
    pub fn drain_live_notifications(&self, addr: &SocketAddr) -> Vec<(String, String)> {
        self.write_session(addr, |session| {
            let mut notifications = Vec::new();
            for (channel, sub) in &mut session.live_subscriptions {
                // Non-blocking drain: collect all pending events.
                loop {
                    match sub.receiver.try_recv() {
                        Ok(event) => {
                            // Apply subscription filters.
                            if sub
                                .collection_filter
                                .as_ref()
                                .is_some_and(|c| event.collection != *c)
                            {
                                continue;
                            }
                            if sub.tenant_filter.is_some_and(|t| event.tenant_id != t) {
                                continue;
                            }
                            let payload =
                                format!("{}:{}", event.operation.as_str(), event.document_id);
                            notifications.push((channel.clone(), payload));
                        }
                        Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                            tracing::warn!(
                                channel = channel.as_str(),
                                lagged = n,
                                "LIVE SELECT subscription lagged — dropped events"
                            );
                            break;
                        }
                        Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                    }
                }
            }
            notifications
        })
        .unwrap_or_default()
    }

    /// Check if a connection has any active LIVE SELECT subscriptions.
    pub fn has_live_subscriptions(&self, addr: &SocketAddr) -> bool {
        self.read_session(addr, |s| !s.live_subscriptions.is_empty())
            .unwrap_or(false)
    }
}
