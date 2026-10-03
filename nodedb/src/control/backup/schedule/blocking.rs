// SPDX-License-Identifier: BUSL-1.1

//! Run a blocking step of a scheduled backup off the async runtime.
//!
//! Catalog reads and writes, job history writes, and durable audit appends
//! all block their thread. A scheduled backup runs as an async task, so each
//! of these runs on the blocking pool. A metadata proposal is awaited on the
//! task through the async proposer.

/// Run `work` on the blocking pool and return its result. `what` names the
/// step in the error when the pool task does not finish.
pub async fn off_runtime<T, F>(what: &'static str, work: F) -> crate::Result<T>
where
    T: Send + 'static,
    F: FnOnce() -> crate::Result<T> + Send + 'static,
{
    tokio::task::spawn_blocking(work)
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("{what} did not finish: {e}"),
        })?
}
