// SPDX-License-Identifier: BUSL-1.1

//! Cancellation-safe tenant request quota accounting.

use crate::types::TenantId;

use super::SharedState;

/// Records one in-flight tenant request until it is dropped.
///
/// Construct this immediately after quota admission and retain it for the
/// complete dispatch interval. Rust drop semantics balance accounting during
/// normal completion, early returns, panic unwinding, and future cancellation.
pub struct TenantRequestGuard<'a> {
    state: &'a SharedState,
    tenant_id: TenantId,
}

impl<'a> TenantRequestGuard<'a> {
    pub(super) fn start(state: &'a SharedState, tenant_id: TenantId) -> Self {
        state.tenant_request_start(tenant_id);
        Self { state, tenant_id }
    }
}

impl Drop for TenantRequestGuard<'_> {
    fn drop(&mut self) {
        self.state.tenant_request_end(self.tenant_id);
    }
}

impl SharedState {
    /// Start cancellation-safe in-flight request accounting for `tenant_id`.
    pub fn tenant_request_guard(&self, tenant_id: TenantId) -> TenantRequestGuard<'_> {
        TenantRequestGuard::start(self, tenant_id)
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;
    use std::panic::AssertUnwindSafe;
    use std::sync::Arc;

    use futures::FutureExt;
    use tokio::sync::oneshot;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::wal::WalManager;

    fn test_state() -> (Arc<SharedState>, tempfile::TempDir) {
        let directory = tempfile::tempdir().expect("create tenant request test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("tenant-request.wal"))
                .expect("open tenant request test WAL"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct tenant request state");
        (state, directory)
    }

    fn active_requests(state: &SharedState, tenant_id: TenantId) -> u32 {
        state
            .tenants
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
            .usage(tenant_id)
            .map_or(0, |usage| usage.active_requests)
    }

    #[tokio::test]
    async fn guard_balances_accounting_during_panic_unwind_and_cancellation() {
        let (state, _directory) = test_state();
        let tenant_id = TenantId::new(42);

        let panic_result = AssertUnwindSafe(async {
            let _request = state.tenant_request_guard(tenant_id);
            assert_eq!(active_requests(&state, tenant_id), 1);
            panic!("test tenant request panic");
        })
        .catch_unwind()
        .await;
        assert!(panic_result.is_err());
        assert_eq!(active_requests(&state, tenant_id), 0);

        let task_state = Arc::clone(&state);
        let (started_tx, started_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _request = task_state.tenant_request_guard(tenant_id);
            let _ = started_tx.send(());
            pending::<()>().await;
        });
        assert!(started_rx.await.is_ok());
        assert_eq!(active_requests(&state, tenant_id), 1);
        task.abort();
        let _ = task.await;
        assert_eq!(active_requests(&state, tenant_id), 0);
    }
}
