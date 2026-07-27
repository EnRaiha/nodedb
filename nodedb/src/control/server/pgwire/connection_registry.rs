// SPDX-License-Identifier: BUSL-1.1

//! Exact-ID lifecycle control for accepted pgwire connections.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, watch};

use crate::control::server::shared::session::ConnectionId;

/// A duplicate ID is a listener invariant violation; never replace its control.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub(crate) enum ConnectionRegistryError {
    #[error("connection id {0} is already registered")]
    Duplicate(ConnectionId),
}

pub(crate) struct ConnectionControl {
    cancel_tx: watch::Sender<bool>,
    teardown_started: AtomicBool,
    teardown_complete: AtomicBool,
    completion: Notify,
}

impl ConnectionControl {
    fn new() -> (Arc<Self>, watch::Receiver<bool>) {
        let (cancel_tx, cancel_rx) = watch::channel(false);
        (
            Arc::new(Self {
                cancel_tx,
                teardown_started: AtomicBool::new(false),
                teardown_complete: AtomicBool::new(false),
                completion: Notify::new(),
            }),
            cancel_rx,
        )
    }

    fn request_cancel(&self) {
        self.cancel_tx.send_replace(true);
    }
}

/// A bounded registry. Every entry is owned by one accepted connection ID.
pub(crate) struct ConnectionRegistry {
    connections: Mutex<HashMap<ConnectionId, Arc<ConnectionControl>>>,
}

impl Default for ConnectionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionRegistry {
    pub(crate) fn new() -> Self {
        Self {
            connections: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn register(
        &self,
        id: ConnectionId,
    ) -> Result<watch::Receiver<bool>, ConnectionRegistryError> {
        let (control, receiver) = ConnectionControl::new();
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if connections.contains_key(&id) {
            return Err(ConnectionRegistryError::Duplicate(id));
        }
        connections.insert(id, control);
        Ok(receiver)
    }

    pub(crate) fn request_cancel(&self, id: ConnectionId) -> bool {
        let control = self
            .connections
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&id)
            .cloned();
        if let Some(control) = control {
            control.request_cancel();
            true
        } else {
            false
        }
    }

    /// Return the registered control and whether this caller owns cleanup.
    pub(crate) fn begin_teardown(
        &self,
        id: ConnectionId,
    ) -> Option<(Arc<ConnectionControl>, bool)> {
        let control = self
            .connections
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&id)
            .cloned()?;
        let first = !control.teardown_started.swap(true, Ordering::AcqRel);
        Some((control, first))
    }

    /// Await teardown completion without a lost wakeup.
    pub(crate) async fn wait_for_teardown(control: &ConnectionControl) {
        loop {
            let notified = control.completion.notified();
            tokio::pin!(notified);
            // Register before testing the completion flag. `Notify::notify_waiters`
            // retains no permit for a future that has not been enabled yet.
            notified.as_mut().enable();
            if control.teardown_complete.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    /// Publish completion and only remove the exact control that finished.
    pub(crate) fn complete_teardown(&self, id: ConnectionId, control: &Arc<ConnectionControl>) {
        control.teardown_complete.store(true, Ordering::Release);
        control.completion.notify_waiters();
        let mut connections = self
            .connections
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        if connections
            .get(&id)
            .is_some_and(|registered| Arc::ptr_eq(registered, control))
        {
            connections.remove(&id);
        }
    }

    #[cfg(test)]
    fn contains(&self, id: ConnectionId) -> bool {
        self.connections
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains_key(&id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: u64) -> ConnectionId {
        ConnectionId::new(value).unwrap_or_else(|_| unreachable!())
    }

    #[test]
    fn same_peer_connections_have_independent_controls() {
        let registry = ConnectionRegistry::new();
        let first = id(1);
        let second = id(2);
        let first_rx = registry.register(first).unwrap_or_else(|_| unreachable!());
        let second_rx = registry.register(second).unwrap_or_else(|_| unreachable!());
        assert!(!*first_rx.borrow());
        assert!(!*second_rx.borrow());
        assert!(registry.request_cancel(first));
        assert!(*first_rx.borrow());
        assert!(!*second_rx.borrow());
    }

    #[test]
    fn cancellation_is_sticky_for_existing_receivers() {
        let registry = ConnectionRegistry::new();
        let connection = id(1);
        let receiver = registry
            .register(connection)
            .unwrap_or_else(|_| unreachable!());
        assert!(registry.request_cancel(connection));
        assert!(*receiver.borrow());
        assert!(registry.request_cancel(connection));
        assert!(*receiver.borrow());
    }

    #[tokio::test]
    async fn teardown_is_once_only_and_waiters_complete() {
        let registry = Arc::new(ConnectionRegistry::new());
        let connection = id(1);
        let _ = registry
            .register(connection)
            .unwrap_or_else(|_| unreachable!());
        let (control, first) = registry
            .begin_teardown(connection)
            .unwrap_or_else(|| unreachable!());
        assert!(first);
        let (_, second) = registry
            .begin_teardown(connection)
            .unwrap_or_else(|| unreachable!());
        assert!(!second);
        let waiter_control = Arc::clone(&control);
        let waiter = tokio::spawn(async move {
            ConnectionRegistry::wait_for_teardown(&waiter_control).await;
        });
        tokio::task::yield_now().await;
        registry.complete_teardown(connection, &control);
        let joined = waiter.await;
        assert!(joined.is_ok());
        assert!(!registry.contains(connection));
    }
}
