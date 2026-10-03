// SPDX-License-Identifier: BUSL-1.1

//! Waits on the metadata group's applied index.
//!
//! The applied-index watcher parks its caller on a condition variable. Each
//! park runs on the blocking pool, so no runtime flavor loses a worker to it.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::{AppliedIndexWatcher, WaitOutcome};

use crate::error::Error;

/// Wait until `watcher` reaches `index` or `timeout` passes, on the blocking
/// pool.
pub(crate) async fn wait_applied(
    watcher: Arc<AppliedIndexWatcher>,
    index: u64,
    timeout: Duration,
) -> Result<WaitOutcome, Error> {
    tokio::task::spawn_blocking(move || watcher.wait_for(index, timeout))
        .await
        .map_err(|error| Error::Config {
            detail: format!("metadata apply wait for log index {index} did not finish: {error}"),
        })
}

/// Wait until `watcher` reaches `log_index`, for as long as the apply moves.
/// Each window parks on the blocking pool.
///
/// Each window of `stall` that ends with neither the applied index nor
/// `progress()` changed ends the wait with `TimedOut`. Any change inside a
/// window opens another one, so a slow apply that keeps moving never times
/// out, and a stuck one times out after one quiet window.
pub(super) async fn wait_tracking_progress_async(
    watcher: Arc<AppliedIndexWatcher>,
    log_index: u64,
    stall: Duration,
    mut progress: impl FnMut() -> u64,
) -> Result<WaitOutcome, Error> {
    let mut seen = (watcher.current(), progress());
    loop {
        let outcome = wait_applied(Arc::clone(&watcher), log_index, stall).await?;
        if !matches!(outcome, WaitOutcome::TimedOut) {
            return Ok(outcome);
        }
        let now = (watcher.current(), progress());
        if now == seen {
            return Ok(outcome);
        }
        seen = now;
    }
}
