// SPDX-License-Identifier: BUSL-1.1

//! Session methods for LISTEN/NOTIFY/UNLISTEN state management.

use super::connection::SessionId;
use crate::control::notify_bus::{ListenHandle, Notification, NotifyBus, normalize_channel};
use crate::types::TenantId;

use super::store::SessionStore;

impl SessionStore {
    /// Register a LISTEN subscription for a session.
    ///
    /// If the session is already listening on this channel, this is a no-op.
    pub fn listen_channel<I: Into<SessionId> + Copy>(
        &self,
        addr: I,
        tenant_id: TenantId,
        channel: &str,
        bus: &NotifyBus,
    ) {
        let normalized = normalize_channel(channel);
        let already = self
            .read_session(addr, |s| {
                s.listen_handles.iter().any(|h| h.channel == normalized)
            })
            .unwrap_or(false);

        if already {
            return;
        }

        let (session_id, rx) = bus.listen(tenant_id, &normalized);
        let handle = ListenHandle {
            tenant_id,
            channel: normalized,
            session_id,
            rx,
        };
        self.write_session(addr, |s| s.listen_handles.push(handle));
    }

    /// Unregister a LISTEN subscription for a specific channel.
    pub fn unlisten_channel(
        &self,
        addr: impl Into<SessionId>,
        _tenant_id: TenantId,
        channel: &str,
        bus: &NotifyBus,
    ) {
        let normalized = normalize_channel(channel);
        let maybe_sid = self.write_session(addr, |s| {
            if let Some(pos) = s
                .listen_handles
                .iter()
                .position(|h| h.channel == normalized)
            {
                let handle = s.listen_handles.remove(pos);
                Some((handle.tenant_id, handle.session_id))
            } else {
                None
            }
        });
        if let Some(Some((stored_tenant_id, session_id))) = maybe_sid {
            bus.unlisten(stored_tenant_id, &normalized, session_id);
        }
    }

    /// Remove all LISTEN subscriptions for a session (UNLISTEN * or disconnect).
    pub fn unlisten_all_channels(&self, addr: impl Into<SessionId>, bus: &NotifyBus) {
        let handles = self.write_session(addr, |s| std::mem::take(&mut s.listen_handles));
        if let Some(handles) = handles {
            for handle in handles {
                bus.unlisten(handle.tenant_id, &handle.channel, handle.session_id);
            }
        }
    }

    /// Disconnect cleanup is exact-ID and uses each handle's immutable tenant.
    pub fn cleanup_listen_on_disconnect(&self, id: SessionId, bus: &NotifyBus) {
        self.unlisten_all_channels(id, bus);
    }

    /// Drain all pending notifications for a session.
    ///
    /// Returns `(channel, payload, pid)` triples ready to be sent as
    /// pgwire `NotificationResponse` messages. Non-blocking (`try_recv`).
    pub fn drain_listen_notifications(&self, addr: impl Into<SessionId>) -> Vec<Notification> {
        self.write_session(addr, |s| {
            let mut out = Vec::new();
            for handle in &mut s.listen_handles {
                loop {
                    match handle.rx.try_recv() {
                        Ok(n) => out.push(n),
                        Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
                        Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => break,
                    }
                }
            }
            out
        })
        .unwrap_or_default()
    }

    /// Return true if the session has any active LISTEN subscriptions.
    pub fn has_listen_subscriptions(&self, addr: impl Into<SessionId>) -> bool {
        self.read_session(addr, |s| !s.listen_handles.is_empty())
            .unwrap_or(false)
    }

    /// Buffer a NOTIFY for deferred delivery (inside a transaction).
    pub fn buffer_notify(&self, addr: impl Into<SessionId>, channel: String, payload: String) {
        self.write_session(addr, |s| {
            s.pending_notifies.push((channel, payload));
        });
    }

    /// Flush all buffered NOTIFYs to the bus (called on COMMIT).
    pub fn flush_pending_notifies(
        &self,
        addr: impl Into<SessionId>,
        tenant_id: TenantId,
        bus: &NotifyBus,
    ) {
        let notifies = self
            .write_session(addr, |s| std::mem::take(&mut s.pending_notifies))
            .unwrap_or_default();
        for (channel, payload) in notifies {
            bus.notify(tenant_id, &channel, &payload);
        }
    }

    /// Discard all buffered NOTIFYs without delivery (called on ROLLBACK).
    pub fn discard_pending_notifies(&self, addr: impl Into<SessionId>) {
        self.write_session(addr, |s| s.pending_notifies.clear());
    }

    /// Return the list of channels this session is currently listening on.
    pub fn listen_channels(&self, addr: impl Into<SessionId>) -> Vec<String> {
        self.read_session(addr, |s| {
            s.listen_handles.iter().map(|h| h.channel.clone()).collect()
        })
        .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::shared::session::{ConnectionId, ConnectionMetadata};

    fn id(value: u64) -> ConnectionId {
        ConnectionId::new(value).unwrap_or_else(|_| unreachable!())
    }

    #[test]
    fn disconnect_cleanup_is_exact_id_and_uses_stored_tenant() {
        let sessions = SessionStore::new();
        let bus = NotifyBus::new(8);
        let peer = "127.0.0.1:6000".parse().unwrap_or_else(|_| unreachable!());
        let local_one = "127.0.0.1:5432".parse().unwrap_or_else(|_| unreachable!());
        let local_two = "127.0.0.2:5432".parse().unwrap_or_else(|_| unreachable!());
        let first = id(1);
        let second = id(2);
        assert!(
            sessions
                .register_connection(
                    first,
                    ConnectionMetadata {
                        peer_addr: peer,
                        local_addr: local_one
                    }
                )
                .is_ok()
        );
        assert!(
            sessions
                .register_connection(
                    second,
                    ConnectionMetadata {
                        peer_addr: peer,
                        local_addr: local_two
                    }
                )
                .is_ok()
        );
        sessions.listen_channel(first, TenantId::new(10), "orders", &bus);
        sessions.listen_channel(second, TenantId::new(20), "orders", &bus);
        assert_eq!(bus.subscription_count(), 2);
        sessions.cleanup_listen_on_disconnect(first.into(), &bus);
        assert_eq!(bus.subscription_count(), 1);
        assert!(sessions.has_listen_subscriptions(second));
        sessions.cleanup_listen_on_disconnect(second.into(), &bus);
        assert_eq!(bus.subscription_count(), 0);
    }
}
