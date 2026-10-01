// SPDX-License-Identifier: BUSL-1.1

//! One node life: the node id plus the incarnation the WAL archiver keys on.
//! Base snapshots and archived WAL of one life share the pair, so a wiped data
//! directory never mixes its bases with an earlier life's WAL.

use std::path::PathBuf;
use std::sync::Arc;

use object_store::ObjectStore;
use object_store::prefix::PrefixStore;

use crate::wal::archiver::{Incarnation, load_or_mint_incarnation};

/// Directory of one life's base snapshots in the snapshot store:
/// `{node_id}/{incarnation}`.
pub fn snapshot_dir(node_id: u64, incarnation: &str) -> String {
    format!("{node_id}/{incarnation}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeLife {
    pub node_id: u64,
    pub incarnation: Incarnation,
}

impl NodeLife {
    /// Read the incarnation the archiver uses, or mint it. Runs the file I/O
    /// on a blocking thread.
    pub async fn resolve(node_id: u64, data_dir: PathBuf) -> crate::Result<Self> {
        let incarnation = tokio::task::spawn_blocking(move || load_or_mint_incarnation(&data_dir))
            .await
            .map_err(|e| crate::Error::Internal {
                detail: format!("node incarnation read did not finish: {e}"),
            })??;
        Ok(Self {
            node_id,
            incarnation,
        })
    }

    /// Directory of this life's base snapshots.
    pub fn snapshot_dir(&self) -> String {
        snapshot_dir(self.node_id, self.incarnation.as_str())
    }

    /// `root` scoped to [`Self::snapshot_dir`].
    pub fn snapshot_store(&self, root: &Arc<dyn ObjectStore>) -> Arc<dyn ObjectStore> {
        Arc::new(PrefixStore::new(Arc::clone(root), self.snapshot_dir()))
    }

    /// The `created_by` name stamped on this life's bases.
    pub fn node_name(&self) -> String {
        format!("node-{}", self.node_id)
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::path::Path as ObjectPath;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    #[tokio::test]
    async fn the_snapshot_store_writes_under_the_node_life_directory() {
        let dir = tempfile::tempdir().unwrap();
        let life = NodeLife::resolve(7, dir.path().to_path_buf())
            .await
            .unwrap();
        let again = NodeLife::resolve(7, dir.path().to_path_buf())
            .await
            .unwrap();
        assert_eq!(life, again, "the incarnation is stable across calls");

        let root: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        life.snapshot_store(&root)
            .put(&ObjectPath::from("snap/x"), PutPayload::from_static(b"1"))
            .await
            .unwrap();
        let full = format!("7/{}/snap/x", life.incarnation.as_str());
        assert!(root.head(&ObjectPath::from(full)).await.is_ok());
    }
}
