// SPDX-License-Identifier: BUSL-1.1

//! Shared propose-and-apply branch for replicated neutral DDL writes.
//!
//! A replicated catalog mutation proposes a `CatalogEntry` through the
//! metadata raft group. Only the node that owns the write runs the typed
//! catalog call and its post-apply hook. Each handler supplies both in
//! `local`, so this module never names a catalog method or a post-apply arm.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::super::result::DdlError;

/// Propose `entry`, then run `local` when this node owns the catalog write.
///
/// A propose failure maps to SQLSTATE `XX000`.
pub(crate) fn propose_and_apply(
    state: &SharedState,
    entry: &CatalogEntry,
    local: impl FnOnce() -> Result<(), DdlError>,
) -> Result<(), DdlError> {
    propose_and_apply_outcome(state, entry, local).map(|_| ())
}

/// [`propose_and_apply`], returning the outcome the proposer reported.
///
/// A handler whose per-node side effects live in the post-apply lane needs it:
/// only `LocalOnly` means no applier will run them for this node.
pub(crate) fn propose_and_apply_outcome(
    state: &SharedState,
    entry: &CatalogEntry,
    local: impl FnOnce() -> Result<(), DdlError>,
) -> Result<ProposeOutcome, DdlError> {
    let outcome = propose_catalog_entry(state, entry)
        .map_err(|e| DdlError::new("XX000", format!("catalog propose failed: {e}")))?;
    if outcome.needs_local_apply() {
        local()?;
    }
    Ok(outcome)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};
    use crate::wal::WalManager;

    fn test_state(name: &str) -> (tempfile::TempDir, Arc<SharedState>) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join(name)).expect("open test WAL"));
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");
        (dir, state)
    }

    fn sample_entry() -> CatalogEntry {
        CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "replicate-helper".to_string(),
        }
    }

    #[tokio::test]
    async fn local_runs_without_a_metadata_group() {
        let (_dir, state) = test_state("replicate-local.wal");
        let ran = AtomicBool::new(false);

        propose_and_apply(&state, &sample_entry(), || {
            ran.store(true, Ordering::SeqCst);
            Ok(())
        })
        .expect("single-node propose succeeds");

        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn local_is_skipped_while_ddl_is_buffered() {
        let (_dir, state) = test_state("replicate-buffered.wal");
        let ran = AtomicBool::new(false);

        conn_scope::scoped(async {
            ddl_buffer::activate();
            propose_and_apply(&state, &sample_entry(), || {
                ran.store(true, Ordering::SeqCst);
                Ok(())
            })
            .expect("buffering an entry succeeds");
            ddl_buffer::discard();
        })
        .await;

        assert!(!ran.load(Ordering::SeqCst));
    }
}
