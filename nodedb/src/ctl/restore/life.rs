// SPDX-License-Identifier: BUSL-1.1

//! The node life a restore reads: its incarnation and its base snapshots.
//!
//! Base snapshots of one life sit under `{node_id}/{incarnation}/` in the
//! snapshot store, and its archived WAL under the same pair in cold storage.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use object_store::prefix::PrefixStore;

use super::error::{LifeSummary, RestoreError};
use crate::control::pitr::node_life::snapshot_dir;
use crate::storage::snapshot::SnapshotMeta;
use crate::storage::snapshot_writer::discover_snapshots;

/// One base snapshot and the prefix its objects sit under.
#[derive(Debug, Clone)]
pub struct Base {
    pub prefix: String,
    pub meta: SnapshotMeta,
    /// The metadata group's applied index when the base began capturing its
    /// catalogs. `0` on a node outside a cluster.
    pub metadata_applied_index: u64,
    /// The metadata group's applied index when the capture ended. The
    /// catalogs hold no entry above it.
    pub metadata_captured_index: u64,
    /// The metadata timeline the catalogs belong to.
    pub metadata_timeline: u64,
}

/// The chosen node life.
pub struct Life {
    pub incarnation: String,
    /// The snapshot store scoped to this life.
    pub store: Arc<dyn ObjectStore>,
    /// Every base of this life, oldest `applied_high_lsn` first.
    pub bases: Vec<Base>,
}

/// Pick the node life to restore.
///
/// `requested` names it. Without it, the one life that holds bases is taken.
/// Several lives are ambiguous, and the error lists each.
pub async fn choose_life(
    root: &Arc<dyn ObjectStore>,
    key: &nodedb_wal::crypto::WalEncryptionKey,
    node_id: u64,
    requested: Option<&str>,
) -> Result<Life, RestoreError> {
    let mut lives = Vec::new();
    for incarnation in list_incarnations(root, node_id).await? {
        let store = life_store(root, node_id, &incarnation);
        let bases = list_bases(&store, key).await;
        if !bases.is_empty() {
            lives.push(Life {
                incarnation,
                store,
                bases,
            });
        }
    }
    if let Some(requested) = requested {
        let available: Vec<String> = lives.iter().map(|l| l.incarnation.clone()).collect();
        return lives
            .into_iter()
            .find(|life| life.incarnation == requested)
            .ok_or_else(|| RestoreError::UnknownIncarnation {
                node_id,
                requested: requested.to_string(),
                available,
            });
    }
    let summaries: Vec<LifeSummary> = lives.iter().map(summary).collect();
    let mut lives = lives.into_iter();
    match (lives.next(), lives.next()) {
        (None, _) => Err(RestoreError::NoBaseSnapshots { node_id }),
        (Some(only), None) => Ok(only),
        (Some(_), Some(_)) => Err(RestoreError::AmbiguousIncarnation {
            node_id,
            lives: summaries,
        }),
    }
}

/// The snapshot store scoped to one life, as `NodeLife::snapshot_store`
/// scopes it.
fn life_store(
    root: &Arc<dyn ObjectStore>,
    node_id: u64,
    incarnation: &str,
) -> Arc<dyn ObjectStore> {
    Arc::new(PrefixStore::new(
        Arc::clone(root),
        snapshot_dir(node_id, incarnation),
    ))
}

/// Every incarnation directory under `{node_id}/`.
async fn list_incarnations(
    root: &Arc<dyn ObjectStore>,
    node_id: u64,
) -> Result<Vec<String>, RestoreError> {
    let prefix = ObjectPath::from(node_id.to_string());
    let listed =
        root.list_with_delimiter(Some(&prefix))
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("list node lives under {prefix}: {e}"),
            })?;
    let mut incarnations: Vec<String> = listed
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename().map(str::to_string))
        .collect();
    incarnations.sort();
    Ok(incarnations)
}

/// Every base whose manifest loads, incremental ones included: each lists
/// every chunk it needs. The listing never deletes: a restore only reads the
/// store.
async fn list_bases(
    store: &Arc<dyn ObjectStore>,
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> Vec<Base> {
    discover_snapshots(store, key)
        .await
        .into_iter()
        .map(|(prefix, manifest)| Base {
            prefix,
            meta: manifest.meta,
            metadata_applied_index: manifest.metadata_applied_index,
            metadata_captured_index: manifest.metadata_captured_index,
            metadata_timeline: manifest.metadata_timeline,
        })
        .collect()
}

fn summary(life: &Life) -> LifeSummary {
    LifeSummary {
        incarnation: life.incarnation.clone(),
        bases: life.bases.len(),
        newest_created_at_us: life.bases.iter().map(|b| b.meta.created_at_us).max(),
        newest_applied_high_lsn: life
            .bases
            .iter()
            .map(|b| b.meta.applied_high_lsn.as_u64())
            .max(),
    }
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;
    use object_store::{ObjectStoreExt, PutPayload};

    use super::*;

    #[tokio::test]
    async fn incarnations_are_the_directories_under_the_node_id() {
        let root: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        for key in [
            "3/aaa/snap-1/manifest.msgpack",
            "3/bbb/snap-2/x",
            "31/ccc/snap-3/x",
        ] {
            root.put(&ObjectPath::from(key), PutPayload::from_static(b"1"))
                .await
                .unwrap();
        }
        assert_eq!(list_incarnations(&root, 3).await.unwrap(), ["aaa", "bbb"]);
        assert!(list_incarnations(&root, 4).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_node_without_bases_is_a_typed_error() {
        let root: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let key = nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[1; 32]).unwrap();
        assert!(matches!(
            choose_life(&root, &key, 7, None).await,
            Err(RestoreError::NoBaseSnapshots { node_id: 7 })
        ));
        assert!(matches!(
            choose_life(&root, &key, 7, Some("abc")).await,
            Err(RestoreError::UnknownIncarnation { .. })
        ));
    }
}
