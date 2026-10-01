// SPDX-License-Identifier: BUSL-1.1

//! Task set shared by the per-stream sink managers (webhook and Kafka).
//!
//! A manager keeps one task per change stream, keyed by database, tenant,
//! and stream name. The task map and the admission flag sit under one lock,
//! so a drain cannot race a task admitted by `start`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use tokio::task::JoinHandle;

use crate::types::DatabaseId;

/// Database, tenant, and stream name of one sink task.
pub type SinkTaskKey = (DatabaseId, u64, String);

/// Running sink tasks and the admission flag.
#[derive(Default)]
pub struct SinkTasks {
    pub tasks: HashMap<SinkTaskKey, JoinHandle<()>>,
    /// Set once a drain starts. A manager admits no task after that.
    pub draining: bool,
}

/// Stop admitting tasks, then join those already admitted.
///
/// Tasks finish on their own until `deadline`. After it, each remaining
/// handle is aborted and joined. The map is taken out before any await, so
/// no std mutex is held across an await and no handle has two owners.
pub async fn shutdown_and_join(state: &Mutex<SinkTasks>, deadline: Duration) {
    let tasks = {
        let mut guard = state.lock().unwrap_or_else(|p| p.into_inner());
        guard.draining = true;
        std::mem::take(&mut guard.tasks)
    };
    let deadline_at = tokio::time::Instant::now() + deadline;
    for (_, mut handle) in tasks {
        if tokio::time::timeout_at(deadline_at, &mut handle)
            .await
            .is_err()
        {
            handle.abort();
            // The task was aborted; its cancellation result carries nothing.
            let _ = handle.await;
        }
    }
}
