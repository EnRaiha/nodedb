// SPDX-License-Identifier: BUSL-1.1

//! Shared propose helper for replicated neutral DDL writes.
//!
//! A replicated catalog mutation proposes a `CatalogEntry`. The proposer
//! applies it on this node through the metadata group, with its post-apply
//! hooks. A handler never writes the catalog itself.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::propose_outcome::ProposeOutcome;
use crate::control::state::SharedState;

use super::super::result::DdlError;

/// Propose `entry`, on any runtime flavor. It awaits every wait of the
/// propose, the entry's post-apply Data Plane work included.
///
/// A propose failure keeps the SQLSTATE class of its typed error.
pub(crate) async fn propose_and_apply_async(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<(), DdlError> {
    propose_and_apply_outcome_async(state, entry)
        .await
        .map(|_| ())
}

/// [`propose_and_apply_async`], returning the outcome the proposer reported.
pub(crate) async fn propose_and_apply_outcome_async(
    state: &SharedState,
    entry: &CatalogEntry,
) -> Result<ProposeOutcome, DdlError> {
    propose_catalog_entry_async(state, entry)
        .await
        .map_err(|e| DdlError::from_error_in_context("catalog propose failed", &e))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::bridge::dispatch::Dispatcher;
    use crate::control::security::credential::CredentialStore;
    use crate::control::server::shared::session::{conn_scope, ddl_buffer};
    use crate::wal::WalManager;

    /// A state no boot wired to a cluster. A buffered entry never reaches the
    /// metadata group, so buffering needs none.
    fn test_state(name: &str) -> (tempfile::TempDir, Arc<SharedState>) {
        let dir = tempfile::tempdir().expect("create test directory");
        let wal =
            Arc::new(WalManager::open_for_testing(&dir.path().join(name)).expect("open test WAL"));
        let credentials = Arc::new(
            CredentialStore::open(&dir.path().join("system.redb")).expect("open credential store"),
        );
        let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
        let state = SharedState::new_with_credentials(dispatcher, wal, credentials, false)
            .expect("construct shared state");
        crate::bootstrap::state_wiring::install_gateway(&state).expect("install gateway");
        (dir, state)
    }

    fn sample_entry() -> CatalogEntry {
        CatalogEntry::DeleteSequence {
            database_id: 0,
            tenant_id: 1,
            name: "replicate-helper".to_string(),
            target_descriptor_version: 0,
            target_hlc: nodedb_types::Hlc::ZERO,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn applies_through_the_one_node_metadata_group() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let outcome = propose_and_apply_outcome_async(&cluster.state, &sample_entry())
            .await
            .expect("one-node propose succeeds");
        assert!(outcome.is_replicated(), "{outcome:?}");
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_state_with_no_metadata_group_refuses_the_propose() {
        let (_dir, state) = test_state("replicate-unbooted.wal");
        let refused = propose_and_apply_outcome_async(&state, &sample_entry()).await;
        assert!(refused.is_err(), "no metadata group, no propose");
    }

    #[tokio::test]
    async fn nothing_applies_while_ddl_is_buffered() {
        let (_dir, state) = test_state("replicate-buffered.wal");
        let outcome = conn_scope::scoped(async {
            ddl_buffer::activate();
            let outcome = propose_and_apply_outcome_async(&state, &sample_entry())
                .await
                .expect("buffering an entry succeeds");
            ddl_buffer::discard();
            outcome
        })
        .await;
        assert_eq!(outcome, ProposeOutcome::Buffered);
    }
}
