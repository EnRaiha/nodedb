// SPDX-License-Identifier: BUSL-1.1

//! Durable side of the cluster epoch.
//!
//! The epoch a node has applied survives restarts in its local cluster
//! catalog. The stored value is a lower bound, never a claim of currency: a
//! node that crashed between applying a bump and persisting it comes back one
//! generation light and learns the rest from the metadata group, the same way
//! it would have if it had never applied the bump at all.

use crate::catalog::ClusterCatalog;
use crate::error::Result;

/// The applied epoch persisted by a previous run, or 0 at genesis.
pub fn load_persisted_epoch(catalog: &ClusterCatalog) -> Result<u64> {
    Ok(catalog.load_cluster_epoch()?.unwrap_or(0))
}

/// Persist `epoch` as this node's applied generation.
///
/// Called from the apply path, after the bump is committed — never from the
/// propose side, where the value is not yet agreed.
pub fn persist_applied_epoch(catalog: &ClusterCatalog, epoch: u64) -> Result<()> {
    catalog.save_cluster_epoch(epoch)
}
