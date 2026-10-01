// SPDX-License-Identifier: BUSL-1.1

//! Host state a cluster node's `SharedState` needs before anything clones it:
//! the durable producer registry.

use std::sync::Arc;

use nodedb::control::state::SharedState;

/// Open the durable producer registry on the credential store's catalog
/// (mirrors production `SharedState::open`). Sync handshake fencing needs it
/// to replicate through the metadata Raft group on cluster nodes.
pub(super) fn open_producer_registry(
    state: &mut SharedState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let catalog = state.credentials.catalog().clone();
    match nodedb::control::sync_producer::registry::SyncProducerRegistry::open(Arc::new(catalog)) {
        Ok(reg) => {
            state.producer_registry = Some(Arc::new(reg));
            Ok(())
        }
        Err(e) => Err(format!("SyncProducerRegistry::open failed in test harness: {e}").into()),
    }
}
