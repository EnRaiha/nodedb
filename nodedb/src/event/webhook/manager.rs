// SPDX-License-Identifier: BUSL-1.1

//! Webhook manager: spawns and stops delivery tasks per stream.
//!
//! On startup, scans all change streams with webhook config and spawns
//! delivery tasks. On CREATE/DROP CHANGE STREAM with webhook config,
//! dynamically starts/stops tasks.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info};

use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::delivery::spawn_delivery_task;

type TaskKey = (DatabaseId, u64, String);

struct ManagerState {
    tasks: HashMap<TaskKey, tokio::task::JoinHandle<()>>,
    draining: bool,
}

/// Manages webhook delivery tasks for all webhook-enabled streams.
pub struct WebhookManager {
    /// Running delivery tasks and admission state, guarded together so a drain
    /// cannot race a newly accepted task.
    state: Mutex<ManagerState>,
    /// Shared shutdown receiver (cloned for each task).
    shutdown_rx: watch::Receiver<bool>,
    /// Shared state reference, set once after SharedState construction.
    shared_state: OnceLock<Arc<SharedState>>,
}

impl WebhookManager {
    pub fn new(shutdown_rx: watch::Receiver<bool>) -> Self {
        Self {
            state: Mutex::new(ManagerState {
                tasks: HashMap::new(),
                draining: false,
            }),
            shutdown_rx,
            shared_state: OnceLock::new(),
        }
    }

    /// Set the shared state reference (called once during startup).
    pub fn set_state(&self, state: Arc<SharedState>) {
        let _ = self.shared_state.set(state);
    }

    /// Start a delivery task for a specific stream.
    ///
    /// Returns `false` when shutdown/draining has started, state is not ready,
    /// or this stream already has a task.
    pub fn start_task(
        &self,
        database_id: DatabaseId,
        tenant_id: u64,
        stream_name: &str,
        config: super::types::WebhookConfig,
    ) -> bool {
        {
            let manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
            if manager.draining || *self.shutdown_rx.borrow() {
                debug!(
                    stream = stream_name,
                    "webhook manager is draining; task start rejected"
                );
                return false;
            }
        }
        let state = match self.shared_state.get() {
            Some(state) => Arc::clone(state),
            None => {
                tracing::warn!("webhook manager: state not set, cannot start task");
                return false;
            }
        };

        let key = (database_id, tenant_id, stream_name.to_string());
        let mut manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if manager.draining || *self.shutdown_rx.borrow() {
            debug!(
                stream = stream_name,
                "webhook manager is draining; task start rejected"
            );
            return false;
        }
        if manager.tasks.contains_key(&key) {
            debug!(
                stream = stream_name,
                "webhook delivery task already running, skipping"
            );
            return false;
        }

        let handle = spawn_delivery_task(
            state,
            database_id,
            tenant_id,
            stream_name.to_string(),
            config,
            self.shutdown_rx.clone(),
        );
        manager.tasks.insert(key, handle);
        info!(stream = stream_name, "webhook delivery task spawned");
        true
    }

    /// Stop a delivery task for a specific stream (on DROP CHANGE STREAM).
    pub fn stop_task(&self, database_id: DatabaseId, tenant_id: u64, stream_name: &str) {
        let key = (database_id, tenant_id, stream_name.to_string());
        let handle = self
            .state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .remove(&key);
        if let Some(handle) = handle {
            handle.abort();
            info!(stream = stream_name, "webhook delivery task stopped");
        }
    }

    /// Stop admitting delivery tasks, then join those already admitted.
    ///
    /// Tasks get until `deadline` to complete naturally. Any remaining tasks
    /// are aborted and joined after that deadline. Handles are removed from the
    /// map before awaiting, so stop/drop paths cannot await them a second time.
    pub async fn shutdown_and_join(&self, deadline: Duration) {
        let tasks = {
            let mut manager = self.state.lock().unwrap_or_else(|p| p.into_inner());
            manager.draining = true;
            std::mem::take(&mut manager.tasks)
        };
        let deadline_at = tokio::time::Instant::now() + deadline;
        for (_, mut handle) in tasks {
            if tokio::time::timeout_at(deadline_at, &mut handle)
                .await
                .is_err()
            {
                handle.abort();
                let _ = handle.await;
            }
        }
        debug!("webhook manager delivery tasks drained");
    }

    /// Number of active delivery tasks.
    pub fn active_count(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .len()
    }
}

impl Drop for WebhookManager {
    fn drop(&mut self) {
        let manager = self.state.get_mut().unwrap_or_else(|p| p.into_inner());
        manager.draining = true;
        for (_, handle) in manager.tasks.drain() {
            handle.abort();
        }
        debug!("webhook manager dropped, all delivery tasks aborted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Notify;

    fn held_task(release: Arc<Notify>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move { release.notified().await })
    }

    #[test]
    fn new_manager_has_no_tasks() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn start_without_state_is_rejected() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        assert!(!mgr.start_task(
            DatabaseId::new(7),
            1,
            "test_stream",
            super::super::types::WebhookConfig::default(),
        ));
    }

    #[tokio::test]
    async fn drain_blocks_later_phase_and_rejects_starts() {
        let (_tx, rx) = watch::channel(false);
        let mgr = Arc::new(WebhookManager::new(rx));
        let release = Arc::new(Notify::new());
        mgr.state
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .tasks
            .insert(
                (DatabaseId::new(7), 1, "held".to_string()),
                held_task(Arc::clone(&release)),
            );

        let mut draining = {
            let mgr = Arc::clone(&mgr);
            tokio::spawn(async move { mgr.shutdown_and_join(Duration::from_secs(2)).await })
        };
        tokio::task::yield_now().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(500), &mut draining)
                .await
                .is_err()
        );
        assert!(mgr.state.lock().unwrap_or_else(|p| p.into_inner()).draining);
        assert!(!mgr.start_task(
            DatabaseId::new(7),
            1,
            "later",
            super::super::types::WebhookConfig::default(),
        ));
        release.notify_one();
        draining.await.expect("manager drain task should complete");
    }

    #[test]
    fn stop_nonexistent_task_is_noop() {
        let (_tx, rx) = watch::channel(false);
        let mgr = WebhookManager::new(rx);
        mgr.stop_task(DatabaseId::new(7), 1, "nonexistent");
        assert_eq!(mgr.active_count(), 0);
    }
}
