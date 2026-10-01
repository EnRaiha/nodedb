// SPDX-License-Identifier: BUSL-1.1

//! Base snapshot retention: list the bases of one node life, keep the newest
//! N, and name the WAL floor the oldest kept base replays from.

use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::sync::Arc;

use object_store::{ObjectStore, ObjectStoreExt};
use tracing::warn;

use crate::storage::snapshot::SnapshotMeta;
use crate::storage::snapshot_writer::{
    CHUNK_DIR, SnapshotManifest, delete_snapshot, discover_snapshots, manifest_key,
};
use crate::types::Lsn;

/// One base snapshot in the store.
#[derive(Debug, Clone)]
pub struct ListedBase {
    pub prefix: String,
    pub meta: SnapshotMeta,
    /// Cold-store keys the base references.
    pub cold_segments: Vec<String>,
    /// Chunk ids the manifest lists, each once.
    pub chunk_ids: Vec<String>,
}

impl ListedBase {
    pub fn from_manifest(prefix: String, manifest: SnapshotManifest) -> Self {
        let mut chunk_ids: Vec<String> = manifest.chunk_ids().map(str::to_owned).collect();
        chunk_ids.sort_unstable();
        chunk_ids.dedup();
        Self {
            prefix,
            meta: manifest.meta,
            cold_segments: manifest.cold_segments,
            chunk_ids,
        }
    }
}

/// Every snapshot prefix of one node life, by what its manifest says.
#[derive(Debug, Default)]
pub struct Listed {
    /// Every snapshot, incremental ones included: each is a full base.
    pub bases: Vec<ListedBase>,
    /// Prefixes whose manifest exists but does not load.
    pub unreadable: Vec<String>,
    /// Prefixes without a manifest, deleted by this listing.
    pub abandoned: usize,
}

/// List every snapshot prefix in `store`.
///
/// A prefix without a manifest is a write that never finished, and it is
/// deleted. One task writes a node life's snapshots, one at a time, so no
/// write is in flight while the listing runs. The chunk directory is no
/// snapshot, and chunk garbage collection owns it.
pub async fn list_snapshots(
    store: &Arc<dyn ObjectStore>,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Listed> {
    let prefixes = store
        .list_with_delimiter(None)
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list snapshot prefixes: {e}"),
        })?
        .common_prefixes;
    let mut loaded: HashMap<String, SnapshotManifest> = discover_snapshots(store, encryption_key)
        .await
        .into_iter()
        .collect();

    let mut listed = Listed::default();
    for prefix in prefixes {
        let prefix = prefix.as_ref().trim_end_matches('/').to_string();
        if prefix == CHUNK_DIR {
            continue;
        }
        match loaded.remove(&prefix) {
            Some(manifest) => listed
                .bases
                .push(ListedBase::from_manifest(prefix, manifest)),
            None => {
                let manifest = manifest_key(&prefix);
                match store.head(&manifest).await {
                    Err(object_store::Error::NotFound { .. }) => {
                        delete_snapshot(store, &prefix).await?;
                        listed.abandoned += 1;
                    }
                    _ => {
                        warn!(prefix = %prefix, "snapshot manifest does not load");
                        listed.unreadable.push(prefix);
                    }
                }
            }
        }
    }
    Ok(listed)
}

/// Which bases retention keeps and which it deletes.
#[derive(Debug, Default)]
pub struct RetentionPlan {
    /// Kept bases, oldest first.
    pub keep: Vec<ListedBase>,
    /// Bases to delete, oldest first.
    pub delete: Vec<ListedBase>,
}

/// Keep the newest `keep` bases, `fresh` always among them.
///
/// Bases order by `applied_high_lsn`, then creation time, then id. `fresh` is
/// the base the current run wrote. It is kept even if an older base orders
/// after it, so a run never deletes the base it wrote.
pub fn plan_retention(bases: Vec<ListedBase>, fresh: &str, keep: NonZeroUsize) -> RetentionPlan {
    let (fresh_base, mut older): (Vec<_>, Vec<_>) =
        bases.into_iter().partition(|base| base.prefix == fresh);
    older.sort_by_key(|base| {
        let meta = &base.meta;
        (meta.applied_high_lsn, meta.created_at_us, meta.snapshot_id)
    });
    let kept_older = keep.get().saturating_sub(fresh_base.len());
    let split = older.len().saturating_sub(kept_older);
    let mut keep = older.split_off(split);
    keep.extend(fresh_base);
    RetentionPlan {
        keep,
        delete: older,
    }
}

/// The LSN the oldest remaining base replays WAL from, or `None` with no base.
/// Archived WAL wholly below it is needed by no remaining base.
pub fn wal_floor(bases: &[SnapshotMeta]) -> Option<Lsn> {
    bases.iter().map(|meta| meta.begin_lsn).min()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind};

    fn base(id: u64, begin: u64, applied_high: u64) -> ListedBase {
        ListedBase {
            prefix: format!("snap-{id}"),
            meta: SnapshotMeta {
                format_version: SNAPSHOT_FORMAT_VERSION,
                snapshot_id: id,
                begin_lsn: Lsn::new(begin),
                end_lsn: Lsn::new(begin),
                applied_high_lsn: Lsn::new(applied_high),
                created_at_us: id,
                created_by: "node-1".into(),
                kind: SnapshotKind::Base,
                parent_id: None,
                data_bytes: 0,
            },
            cold_segments: Vec::new(),
            chunk_ids: Vec::new(),
        }
    }

    fn ids(bases: &[ListedBase]) -> Vec<u64> {
        bases.iter().map(|base| base.meta.snapshot_id).collect()
    }

    fn n(keep: usize) -> NonZeroUsize {
        NonZeroUsize::new(keep).unwrap()
    }

    #[test]
    fn retention_keeps_the_newest_n_and_deletes_the_rest() {
        let bases = vec![
            base(3, 30, 35),
            base(1, 10, 15),
            base(4, 40, 45),
            base(2, 20, 25),
        ];
        let plan = plan_retention(bases, "snap-4", n(2));
        assert_eq!(ids(&plan.keep), [3, 4]);
        assert_eq!(ids(&plan.delete), [1, 2], "deleted oldest first");
    }

    #[test]
    fn the_only_base_is_never_deleted() {
        let plan = plan_retention(vec![base(1, 10, 15)], "snap-1", n(1));
        assert_eq!(ids(&plan.keep), [1]);
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn fewer_bases_than_the_retention_are_all_kept() {
        let plan = plan_retention(vec![base(1, 10, 15), base(2, 20, 25)], "snap-2", n(7));
        assert_eq!(ids(&plan.keep), [1, 2]);
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn the_fresh_base_is_kept_even_when_it_orders_first() {
        let bases = vec![base(1, 10, 15), base(2, 20, 25), base(9, 5, 8)];
        let plan = plan_retention(bases, "snap-9", n(2));
        assert_eq!(ids(&plan.keep), [2, 9]);
        assert_eq!(ids(&plan.delete), [1]);
    }

    #[test]
    fn the_wal_floor_is_the_lowest_begin_lsn_of_any_remaining_base() {
        let metas: Vec<SnapshotMeta> = [base(2, 20, 25), base(3, 18, 35)]
            .into_iter()
            .map(|base| base.meta)
            .collect();
        assert_eq!(wal_floor(&metas), Some(Lsn::new(18)));
        assert_eq!(wal_floor(&[]), None);
    }
}
