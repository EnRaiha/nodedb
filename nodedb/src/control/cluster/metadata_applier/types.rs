// SPDX-License-Identifier: BUSL-1.1

//! `MetadataCommitApplier` struct definition, construction, and the
//! `CatalogChangeEvent` it broadcasts.

use std::sync::{Arc, OnceLock, RwLock, Weak};

use tokio::sync::broadcast;

use nodedb_cluster::MetadataCache;

use crate::control::security::credential::CredentialStore;
use crate::control::state::SharedState;

/// Broadcast channel capacity — small, because consumers are
/// internal subsystems that keep up or are lagged intentionally.
pub const CATALOG_CHANNEL_CAPACITY: usize = 64;

/// Event published on every committed metadata entry.
#[derive(Debug, Clone)]
pub struct CatalogChangeEvent {
    pub applied_index: u64,
}

/// Production `MetadataApplier` installed on the `RaftLoop`.
pub struct MetadataCommitApplier {
    pub(super) cache: Arc<RwLock<MetadataCache>>,
    pub(super) catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
    pub(super) credentials: Arc<CredentialStore>,
    pub(super) token_state: nodedb_cluster::SharedTokenStateMirror,
    pub(super) transport: OnceLock<Arc<nodedb_cluster::NexarTransport>>,
    /// Weak handle to `SharedState`. Installed by `start_raft` after
    /// construction so the applier can spawn async post-apply side
    /// effects (Data Plane register on `PutCollection`,
    /// `sequence_registry.create` on `PutSequence`, etc.). Weak to
    /// break the Arc cycle (SharedState → raft loop → applier →
    /// SharedState). `None` in unit tests.
    pub(super) shared: OnceLock<Weak<SharedState>>,
}

impl MetadataCommitApplier {
    pub fn new(
        cache: Arc<RwLock<MetadataCache>>,
        catalog_change_tx: broadcast::Sender<CatalogChangeEvent>,
        credentials: Arc<CredentialStore>,
        token_state: nodedb_cluster::SharedTokenStateMirror,
    ) -> Self {
        Self {
            cache,
            catalog_change_tx,
            credentials,
            token_state,
            transport: OnceLock::new(),
            shared: OnceLock::new(),
        }
    }

    pub fn install_transport(&self, transport: Arc<nodedb_cluster::NexarTransport>) {
        let _ = self.transport.set(transport);
    }

    /// Install a weak handle to `SharedState` so the applier can
    /// spawn post-apply side effects. Must be called **before** the
    /// raft loop starts ticking; `start_raft` does this as part of
    /// its construction sequence.
    pub fn install_shared(&self, shared: Weak<SharedState>) {
        let _ = self.shared.set(shared);
    }
}
