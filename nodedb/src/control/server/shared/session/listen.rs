// SPDX-License-Identifier: BUSL-1.1

//! Session methods for LISTEN/NOTIFY/UNLISTEN state management.

use std::net::SocketAddr;

use crate::control::notify_bus::{ListenHandle, Notification, NotifyBus, normalize_channel};
use crate::types::TenantId;

use super::store::SessionStore;

impl SessionStore {
    /// Register a LISTEN subscription for a session.
    ///
    /// If the session is already listening on this channel, this is a no-op.
    pub fn listen_channel(
        &self,
        addr: &SocketAddr,
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
            channel: normalized,
            session_id,
            rx,
        };
        self.write_session(addr, |s| s.listen_handles.push(handle));
    }

    /// Unregister a LISTEN subscription for a specific channel.
    pub fn unlisten_channel(
        &self,
        addr: &SocketAddr,
        tenant_id: TenantId,
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
                Some(handle.session_id)
            } else {
                None
            }
        });
        if let Some(Some(session_id)) = maybe_sid {
            bus.unlisten(tenant_id, &normalized, session_id);
        }
    }

    /// Remove all LISTEN subscriptions for a session (UNLISTEN * or disconnect).
    pub fn unlisten_all_channels(&self, addr: &SocketAddr, tenant_id: TenantId, bus: &NotifyBus) {
        let handles = self.write_session(addr, |s| std::mem::take(&mut s.listen_handles));
        if let Some(handles) = handles {
            let pairs: Vec<(String, u64)> = handles
                .into_iter()
                .map(|h| (h.channel, h.session_id))
                .collect();
            bus.unlisten_all(tenant_id, &pairs);
        }
    }

    /// Drain all pending notifications for a session.
    ///
    /// Returns `(channel, payload, pid)` triples ready to be sent as
    /// pgwire `NotificationResponse` messages. Non-blocking (`try_recv`).
    pub fn drain_listen_notifications(&self, addr: &SocketAddr) -> Vec<Notification> {
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
    pub fn has_listen_subscriptions(&self, addr: &SocketAddr) -> bool {
        self.read_session(addr, |s| !s.listen_handles.is_empty())
            .unwrap_or(false)
    }

    /// Buffer a NOTIFY for deferred delivery (inside a transaction).
    pub fn buffer_notify(&self, addr: &SocketAddr, channel: String, payload: String) {
        self.write_session(addr, |s| {
            s.pending_notifies.push((channel, payload));
        });
    }

    /// Flush all buffered NOTIFYs to the bus (called on COMMIT).
    pub fn flush_pending_notifies(&self, addr: &SocketAddr, tenant_id: TenantId, bus: &NotifyBus) {
        let notifies = self
            .write_session(addr, |s| std::mem::take(&mut s.pending_notifies))
            .unwrap_or_default();
        for (channel, payload) in notifies {
            bus.notify(tenant_id, &channel, &payload);
        }
    }

    /// Discard all buffered NOTIFYs without delivery (called on ROLLBACK).
    pub fn discard_pending_notifies(&self, addr: &SocketAddr) {
        self.write_session(addr, |s| s.pending_notifies.clear());
    }

    /// Return the list of channels this session is currently listening on.
    pub fn listen_channels(&self, addr: &SocketAddr) -> Vec<String> {
        self.read_session(addr, |s| {
            s.listen_handles.iter().map(|h| h.channel.clone()).collect()
        })
        .unwrap_or_default()
    }
}
