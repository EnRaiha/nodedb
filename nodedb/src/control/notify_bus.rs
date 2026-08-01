// SPDX-License-Identifier: BUSL-1.1

//! PostgreSQL-compatible LISTEN/NOTIFY notification bus.
//!
//! Lives entirely on the Control Plane (`Send + Sync`, Tokio).
//! The bus is keyed by `(database_id, tenant_id, channel_name)`. Each session
//! that executes `LISTEN <channel>` registers a bounded mpsc sender. When any
//! session executes `NOTIFY <channel> [, '<payload>']`, the bus delivers
//! the notification only to listeners in the same database and tenant.
//!
//! Backpressure policy: if a session's queue is full (configurable cap,
//! default 1024), the new notification is dropped and a per-bus counter is
//! incremented. The sender is never blocked.
//!
//! Transaction semantics: the bus exposes `notify` for immediate delivery
//! (outside a transaction) and `notify_deferred` whose payload the caller
//! must buffer and flush on COMMIT or discard on ROLLBACK.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use tracing::{debug, warn};

use crate::types::{DatabaseId, TenantId};

/// Default maximum pending notifications per session channel.
pub const DEFAULT_QUEUE_CAP: usize = 1024;

/// A single notification sent through the bus.
#[derive(Debug, Clone)]
pub struct Notification {
    /// Channel name (lowercased per PG identifier rules).
    pub channel: String,
    /// Optional payload (empty string when no payload was given).
    pub payload: String,
    /// PID of the backend that issued the NOTIFY (always 0 in NodeDB).
    pub pid: i32,
}

/// Key used to shard the subscriber map.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct BusKey {
    database_id: DatabaseId,
    tenant_id: u64,
    channel: String,
}

/// Per-session subscriber entry.
struct SessionSink {
    /// Bounded send half — we use a `VecDeque`-backed ring manually so we can
    /// implement drop-oldest without a separate task.  The Tokio bounded channel
    /// already blocks on `send` and uses `try_send` for non-blocking, but it
    /// does **not** expose drop-oldest.  We implement it on top with a small
    /// wrapper around `tokio::sync::mpsc::channel`.
    tx: tokio::sync::mpsc::Sender<Notification>,
    cap: usize,
}

/// The notification bus.  One instance per server, stored in `SharedState`.
pub struct NotifyBus {
    /// `(database, tenant, channel) → Vec<SessionSink>`.
    subscribers: RwLock<HashMap<BusKey, Vec<(u64 /* session_id */, SessionSink)>>>,
    /// Monotonic session ID counter.
    next_session_id: AtomicU64,
    /// Cumulative count of notifications dropped due to full queues.
    pub dropped: AtomicU64,
    /// Default per-session queue capacity.
    queue_cap: usize,
}

impl Default for NotifyBus {
    fn default() -> Self {
        Self::new(DEFAULT_QUEUE_CAP)
    }
}

impl NotifyBus {
    pub fn new(queue_cap: usize) -> Self {
        Self {
            subscribers: RwLock::new(HashMap::new()),
            next_session_id: AtomicU64::new(1),
            dropped: AtomicU64::new(0),
            queue_cap,
        }
    }

    /// Register a session as a listener for `(database_id, tenant_id, channel)`.
    ///
    /// Returns a `(session_id, Receiver)` pair.  The caller must poll the
    /// receiver between queries to drain notifications, and call
    /// `unlisten` / `unlisten_all` on session disconnect.
    pub fn listen(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        channel: &str,
    ) -> (u64, tokio::sync::mpsc::Receiver<Notification>) {
        let key = BusKey {
            database_id,
            tenant_id: tenant_id.as_u64(),
            channel: normalize_channel(channel),
        };
        let session_id = self.next_session_id.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = tokio::sync::mpsc::channel(self.queue_cap);
        let sink = SessionSink {
            tx,
            cap: self.queue_cap,
        };

        let mut map = self.subscribers.write().unwrap_or_else(|p| p.into_inner());
        map.entry(key.clone()).or_default().push((session_id, sink));
        debug!(
            session_id,
            database = database_id.as_u64(),
            tenant = tenant_id.as_u64(),
            channel = key.channel.as_str(),
            "LISTEN registered"
        );
        (session_id, rx)
    }

    /// Unregister a specific session from a channel.
    pub fn unlisten(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        channel: &str,
        session_id: u64,
    ) {
        let key = BusKey {
            database_id,
            tenant_id: tenant_id.as_u64(),
            channel: normalize_channel(channel),
        };
        let mut map = self.subscribers.write().unwrap_or_else(|p| p.into_inner());
        if let Some(sinks) = map.get_mut(&key) {
            sinks.retain(|(id, _)| *id != session_id);
            if sinks.is_empty() {
                map.remove(&key);
            }
        }
        debug!(
            session_id,
            database = database_id.as_u64(),
            tenant = tenant_id.as_u64(),
            channel = key.channel.as_str(),
            "UNLISTEN"
        );
    }

    /// Unregister a session from all channels it has subscribed to.
    ///
    /// `handles` is the slice of immutable `(database, tenant, channel,
    /// session_id)` entries held by the session. This is called on disconnect.
    pub fn unlisten_all(&self, handles: &[(DatabaseId, TenantId, String, u64)]) {
        if handles.is_empty() {
            return;
        }
        let mut map = self.subscribers.write().unwrap_or_else(|p| p.into_inner());
        for (database_id, tenant_id, channel, session_id) in handles {
            let key = BusKey {
                database_id: *database_id,
                tenant_id: tenant_id.as_u64(),
                channel: normalize_channel(channel),
            };
            if let Some(sinks) = map.get_mut(&key) {
                sinks.retain(|(id, _)| id != session_id);
                if sinks.is_empty() {
                    map.remove(&key);
                }
            }
        }
        debug!(count = handles.len(), "UNLISTEN * (session disconnect)");
    }

    /// Publish a notification to all listeners on `(database_id, tenant_id, channel)`.
    ///
    /// Non-blocking: uses `try_send`. When a session's queue is full, the new
    /// notification is dropped and the drop counter is incremented.
    pub fn notify(
        &self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        channel: &str,
        payload: &str,
    ) {
        let key = BusKey {
            database_id,
            tenant_id: tenant_id.as_u64(),
            channel: normalize_channel(channel),
        };
        let notification = Notification {
            channel: key.channel.clone(),
            payload: payload.to_string(),
            pid: 0,
        };

        let map = self.subscribers.read().unwrap_or_else(|p| p.into_inner());
        let sinks = match map.get(&key) {
            Some(s) => s,
            None => return, // no listeners — no-op
        };

        let mut dead = Vec::new();
        for (session_id, sink) in sinks {
            match sink.tx.try_send(notification.clone()) {
                Ok(()) => {}
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                    // Queue is full. The sender cannot remove an older item,
                    // so retain the bounded backlog and drop this new item.
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    warn!(
                        session_id,
                        channel = key.channel.as_str(),
                        cap = sink.cap,
                        "NOTIFY queue full — dropping new notification (metric incremented)"
                    );
                }
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    dead.push(*session_id);
                }
            }
        }
        drop(map);

        // Clean up closed sessions.
        if !dead.is_empty() {
            let mut map = self.subscribers.write().unwrap_or_else(|p| p.into_inner());
            if let Some(sinks) = map.get_mut(&key) {
                sinks.retain(|(id, _)| !dead.contains(id));
                if sinks.is_empty() {
                    map.remove(&key);
                }
            }
        }
    }

    /// Total dropped notifications since server start.
    pub fn total_dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of active `(database, tenant, channel)` subscriptions.
    pub fn subscription_count(&self) -> usize {
        let map = self.subscribers.read().unwrap_or_else(|p| p.into_inner());
        map.values().map(|v| v.len()).sum()
    }
}

/// Normalize a channel name to lowercase, matching PG unquoted-identifier rules.
///
/// Quoted identifiers preserve case; unquoted identifiers are folded to lowercase.
/// We receive them already stripped of quotes from the SQL layer, so we just
/// lowercase everything that wasn't explicitly quoted.
pub fn normalize_channel(channel: &str) -> String {
    channel.to_lowercase()
}

/// Handle for a session's LISTEN subscriptions.
///
/// Stores the immutable subscription database and tenant together with
/// `(channel, session_id, receiver)` so disconnect cleanup cannot be redirected
/// by a later session-binding or identity change.
pub struct ListenHandle {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub channel: String,
    pub session_id: u64,
    pub rx: tokio::sync::mpsc::Receiver<Notification>,
}

/// Shared Arc around `NotifyBus` for cheaply cloning into session tasks.
pub type NotifyBusHandle = Arc<NotifyBus>;

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant(n: u64) -> TenantId {
        TenantId::new(n)
    }

    fn database(n: u64) -> DatabaseId {
        DatabaseId::new(n)
    }

    #[tokio::test]
    async fn basic_listen_notify() {
        let bus = NotifyBus::new(64);
        let t = tenant(1);
        let db = database(1);
        let (_, mut rx) = bus.listen(db, t, "orders");
        bus.notify(db, t, "orders", "hello");
        let n = rx.try_recv().unwrap();
        assert_eq!(n.channel, "orders");
        assert_eq!(n.payload, "hello");
    }

    #[tokio::test]
    async fn unlisten_stops_delivery() {
        let bus = NotifyBus::new(64);
        let t = tenant(1);
        let db = database(1);
        let (sid, mut rx) = bus.listen(db, t, "orders");
        bus.notify(db, t, "orders", "first");
        assert!(rx.try_recv().is_ok());
        bus.unlisten(db, t, "orders", sid);
        bus.notify(db, t, "orders", "second");
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn tenant_isolation() {
        let bus = NotifyBus::new(64);
        let t1 = tenant(1);
        let t2 = tenant(2);
        let db = database(1);
        let (_, mut rx1) = bus.listen(db, t1, "ch");
        let (_, mut rx2) = bus.listen(db, t2, "ch");
        bus.notify(db, t1, "ch", "for-t1");
        // t1 receives; t2 does not.
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn queue_full_increments_dropped() {
        let bus = NotifyBus::new(2); // tiny cap
        let t = tenant(1);
        let db = database(1);
        let (_, _rx) = bus.listen(db, t, "ch"); // don't drain
        bus.notify(db, t, "ch", "a");
        bus.notify(db, t, "ch", "b"); // fills the queue
        bus.notify(db, t, "ch", "c"); // should drop
        assert_eq!(bus.total_dropped(), 1);
    }

    #[tokio::test]
    async fn unlisten_all() {
        let bus = NotifyBus::new(64);
        let t = tenant(1);
        let db = database(1);
        let (sid1, mut rx1) = bus.listen(db, t, "ch1");
        let (sid2, mut rx2) = bus.listen(db, t, "ch2");
        bus.unlisten_all(&[
            (db, t, "ch1".to_string(), sid1),
            (db, t, "ch2".to_string(), sid2),
        ]);
        bus.notify(db, t, "ch1", "msg");
        bus.notify(db, t, "ch2", "msg");
        assert!(rx1.try_recv().is_err());
        assert!(rx2.try_recv().is_err());
    }

    #[tokio::test]
    async fn database_isolation() {
        let bus = NotifyBus::new(64);
        let tenant = tenant(1);
        let database_a = database(1);
        let database_b = database(2);
        let (_, mut receiver_a) = bus.listen(database_a, tenant, "orders");
        let (_, mut receiver_b) = bus.listen(database_b, tenant, "orders");

        bus.notify(database_a, tenant, "orders", "only-a");

        assert!(receiver_a.try_recv().is_ok());
        assert!(receiver_b.try_recv().is_err());
    }

    #[test]
    fn channel_normalize() {
        assert_eq!(normalize_channel("Orders"), "orders");
        assert_eq!(normalize_channel("my_channel"), "my_channel");
    }
}
