// SPDX-License-Identifier: BUSL-1.1

//! One base snapshot run after the capture: write the base, apply retention,
//! garbage-collect unlisted chunks and archived WAL, and build the new
//! catalog.

use std::num::NonZeroUsize;
use std::sync::Arc;

use object_store::ObjectStore;
use tracing::warn;

use super::chunk_gc::collect_unreferenced_chunks;
use super::node_life::NodeLife;
use super::pins::pending_pin_name;
use super::retention::{ListedBase, list_snapshots, plan_retention, wal_floor};
use super::state::PitrState;
use super::wal_gc::collect_archived_wal;
use crate::data::snapshot::NodeSnapshot;
use crate::storage::cold::ColdStorage;
use crate::storage::snapshot::{SnapshotCatalog, SnapshotMeta};
use crate::storage::snapshot_writer::{
    BasePlan, CdcParams, ChunkUploads, WrittenBase, delete_snapshot, list_cold_segments,
    write_base_snapshot,
};

/// Everything a run needs besides the captured images.
pub struct BaseCycle<'a> {
    /// The snapshot store root. Bases go under the node life directory.
    pub root: &'a Arc<dyn ObjectStore>,
    /// Cold storage: the WAL archive and the tiered segments a base relies
    /// on. `None` references no segment and skips WAL garbage collection.
    pub cold: Option<&'a ColdStorage>,
    pub life: &'a NodeLife,
    /// Holds the cold and chunk pins the run installs and releases, and the
    /// catalog that names the parent of the new base.
    pub state: &'a PitrState,
    pub encryption_key: &'a nodedb_wal::crypto::WalEncryptionKey,
    pub retention: NonZeroUsize,
    pub chunk_params: CdcParams,
}

/// What one run did.
#[derive(Debug)]
pub struct CycleOutcome {
    pub base: SnapshotMeta,
    pub uploads: ChunkUploads,
    /// The catalog after retention. `None` when the listing failed: the
    /// caller then adds `base` to the catalog it holds.
    pub catalog: Option<SnapshotCatalog>,
    pub deleted_bases: usize,
    pub collected_chunks: u64,
    pub collected_segments: u64,
    /// The first retention or garbage-collection error. The base itself is
    /// written.
    pub cleanup_error: Option<crate::Error>,
}

impl BaseCycle<'_> {
    /// Write a base from `cores` and `node`, then apply retention and
    /// garbage collection.
    ///
    /// Archived WAL is collected only below the floor of every base still in
    /// the store, a base whose delete failed included. Neither WAL nor chunks
    /// are collected while a snapshot prefix holds a manifest that does not
    /// load: that prefix can be a base still needing them.
    ///
    /// The cold keys and chunk ids of a base are pinned before the base
    /// relies on them, and released only when retention deletes it.
    pub async fn run(
        &self,
        cores: Vec<(usize, Vec<u8>)>,
        node: NodeSnapshot,
    ) -> crate::Result<CycleOutcome> {
        let store = self.life.snapshot_store(self.root);
        let pending = pending_pin_name();
        let written = self.write_pinned(&store, &pending, cores, node).await;
        let Pinned {
            written,
            cold_segments,
            chunk_ids,
        } = {
            let mut cold_pins = self.state.cold_pins().write().await;
            let mut chunk_pins = self.state.chunk_pins().write().await;
            match written {
                Ok(pinned) => {
                    cold_pins.rename(&pending, &pinned.written.prefix);
                    chunk_pins.rename(&pending, &pinned.written.prefix);
                    pinned
                }
                Err(e) => {
                    cold_pins.unpin(&pending);
                    chunk_pins.unpin(&pending);
                    return Err(e);
                }
            }
        };
        let WrittenBase {
            meta: base,
            prefix,
            uploads,
        } = written;
        let mut outcome = CycleOutcome {
            base: base.clone(),
            uploads,
            catalog: None,
            deleted_bases: 0,
            collected_chunks: 0,
            collected_segments: 0,
            cleanup_error: None,
        };

        let listed = match list_snapshots(&store, self.encryption_key).await {
            Ok(listed) => listed,
            Err(e) => {
                outcome.cleanup_error = Some(e);
                return Ok(outcome);
            }
        };
        let mut bases = listed.bases;
        if !bases.iter().any(|listed_base| listed_base.prefix == prefix) {
            bases.push(ListedBase {
                prefix: prefix.clone(),
                meta: base,
                cold_segments,
                chunk_ids,
            });
        }

        let plan = plan_retention(bases, &prefix, self.retention);
        let mut remaining = plan.keep;
        for old in plan.delete {
            match delete_snapshot(&store, &old.prefix).await {
                Ok(()) => {
                    self.state.cold_pins().write().await.unpin(&old.prefix);
                    self.state.chunk_pins().write().await.unpin(&old.prefix);
                    outcome.deleted_bases += 1;
                }
                Err(e) => {
                    warn!(prefix = %old.prefix, error = %e, "expired base snapshot not deleted");
                    remaining.push(old);
                    keep_first(&mut outcome.cleanup_error, e);
                }
            }
        }
        let complete = listed.unreadable.is_empty();
        self.state.cold_pins().write().await.rebuild(
            remaining
                .iter()
                .map(|kept| (kept.prefix.as_str(), kept.cold_segments.as_slice())),
            complete,
        );
        self.state.chunk_pins().write().await.rebuild(
            remaining
                .iter()
                .map(|kept| (kept.prefix.as_str(), kept.chunk_ids.as_slice())),
            complete,
        );
        let mut remaining: Vec<SnapshotMeta> =
            remaining.into_iter().map(|kept| kept.meta).collect();

        if !complete {
            let e = crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "archived WAL and snapshot chunks kept: snapshot prefixes {:?} hold a \
                     manifest that does not load",
                    listed.unreadable
                ),
            };
            keep_first(&mut outcome.cleanup_error, e);
        } else {
            match collect_unreferenced_chunks(&store, self.state.chunk_pins()).await {
                Ok(collected) => outcome.collected_chunks = collected,
                Err(e) => keep_first(&mut outcome.cleanup_error, e),
            }
            if let (Some(cold), Some(floor)) = (self.cold, wal_floor(&remaining)) {
                match collect_archived_wal(cold, self.life, floor).await {
                    Ok(collected) => outcome.collected_segments = collected,
                    Err(e) => keep_first(&mut outcome.cleanup_error, e),
                }
            }
        }

        remaining.sort_by_key(|meta| (meta.applied_high_lsn, meta.created_at_us));
        let mut catalog = SnapshotCatalog::new();
        for meta in remaining {
            catalog.add(meta);
        }
        outcome.catalog = Some(catalog);
        Ok(outcome)
    }

    /// Pin the cold keys, plan the base, pin its chunk ids, and write it.
    /// Every pin is held under `pending`, and the caller moves or releases
    /// it, on error too.
    async fn write_pinned(
        &self,
        store: &Arc<dyn ObjectStore>,
        pending: &str,
        cores: Vec<(usize, Vec<u8>)>,
        node: NodeSnapshot,
    ) -> crate::Result<Pinned> {
        // Listed under the write lock, so no cold delete lands between the
        // listing and the pin.
        let cold_segments = {
            let mut pins = self.state.cold_pins().write().await;
            let keys = match self.cold {
                Some(cold) => list_cold_segments(&cold.object_store(), cold.prefix()).await?,
                None => Vec::new(),
            };
            pins.pin(pending, keys.clone());
            keys
        };
        let key = self.encryption_key.clone();
        let params = self.chunk_params;
        let plan = tokio::task::spawn_blocking(move || {
            BasePlan::new(cores, &node, cold_segments, &key, params)
        })
        .await
        .map_err(|e| crate::Error::Internal {
            detail: format!("base snapshot planning did not finish: {e}"),
        })??;
        let chunk_ids = plan.chunk_ids();
        // Pinned before the store is checked, so no chunk this base finds
        // present is collected before its manifest lands.
        self.state
            .chunk_pins()
            .write()
            .await
            .pin(pending, chunk_ids.clone());
        let parent = newest_base(&self.state.catalog());
        let written = write_base_snapshot(
            store,
            &plan,
            parent,
            &self.life.node_name(),
            self.encryption_key,
        )
        .await?;
        Ok(Pinned {
            written,
            cold_segments: plan.cold_segments().to_vec(),
            chunk_ids,
        })
    }
}

/// A written base with the keys it pinned.
struct Pinned {
    written: WrittenBase,
    cold_segments: Vec<String>,
    chunk_ids: Vec<String>,
}

/// The id of the newest base in `catalog`.
fn newest_base(catalog: &SnapshotCatalog) -> Option<u64> {
    catalog
        .all()
        .iter()
        .max_by_key(|meta| (meta.applied_high_lsn, meta.created_at_us))
        .map(|meta| meta.snapshot_id)
}

fn keep_first(slot: &mut Option<crate::Error>, error: crate::Error) {
    if slot.is_none() {
        *slot = Some(error);
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use object_store::memory::InMemory;

    use super::*;
    use crate::control::security::catalog::SystemCatalog;
    use crate::data::snapshot::{CoreSnapshot, SnapshotComponent, SnapshotFile};
    use crate::storage::cold::ColdStorageConfig;
    use crate::storage::snapshot_node::capture_node_state;
    use crate::storage::snapshot_writer::{list_chunk_ids, rebuild_catalog};
    use crate::types::Lsn;
    use crate::types::replay_stamp::ReplayStamp;

    fn key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x5A; 32]).unwrap()
    }

    /// Small enough that a test image spans many chunks.
    const SMALL_CHUNKS: CdcParams = CdcParams {
        min: 64,
        max: 1024,
        mask_bits: 7,
    };

    /// One core image whose files hold every record through `floor`.
    fn cores(floor: u64) -> Vec<(usize, Vec<u8>)> {
        cores_with(floor, Vec::new())
    }

    /// One core image holding `bytes` as a file.
    fn cores_with(floor: u64, bytes: Vec<u8>) -> Vec<(usize, Vec<u8>)> {
        let snapshot = CoreSnapshot {
            stamp: ReplayStamp::through(floor),
            files: vec![SnapshotFile {
                component: SnapshotComponent::Kv,
                path: "kv-ckpt/core-0/data".into(),
                bytes,
            }],
            dirs: Vec::new(),
        };
        vec![(0, snapshot.to_bytes().unwrap())]
    }

    fn noise(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    struct Fixture {
        data_dir: tempfile::TempDir,
        cold_dir: tempfile::TempDir,
        root: Arc<dyn ObjectStore>,
        cold: ColdStorage,
        life: NodeLife,
        pitr: PitrState,
        key: nodedb_wal::crypto::WalEncryptionKey,
    }

    impl Fixture {
        async fn new() -> Self {
            let data_dir = tempfile::tempdir().unwrap();
            let cold_dir = tempfile::tempdir().unwrap();
            let cold = ColdStorage::new(ColdStorageConfig {
                local_dir: Some(cold_dir.path().to_path_buf()),
                ..Default::default()
            })
            .unwrap();
            let life = NodeLife::resolve(3, data_dir.path().to_path_buf())
                .await
                .unwrap();
            Self {
                data_dir,
                cold_dir,
                root: Arc::new(InMemory::new()),
                cold,
                life,
                pitr: PitrState::default(),
                key: key(),
            }
        }

        fn cycle(&self, retention: usize) -> BaseCycle<'_> {
            BaseCycle {
                root: &self.root,
                cold: Some(&self.cold),
                life: &self.life,
                state: &self.pitr,
                encryption_key: &self.key,
                retention: NonZeroUsize::new(retention).unwrap(),
                chunk_params: SMALL_CHUNKS,
            }
        }

        fn node_image(&self) -> NodeSnapshot {
            let system = SystemCatalog::open(&self.data_dir.path().join("system.redb")).unwrap();
            capture_node_state(self.data_dir.path(), &system, None, &[]).unwrap()
        }

        /// Archive a segment starting at each LSN, with its checksum marker.
        async fn archive_segments(&self, first_lsns: &[u64]) {
            let wal_dir = self.data_dir.path().join("wal");
            std::fs::create_dir_all(&wal_dir).unwrap();
            for &first_lsn in first_lsns {
                let path = nodedb_wal::segment::segment_path(&wal_dir, first_lsn);
                std::fs::write(&path, first_lsn.to_le_bytes()).unwrap();
                self.cold
                    .upload_wal_segment(
                        &path,
                        self.life.node_id,
                        self.life.incarnation.as_str(),
                        first_lsn,
                        &[],
                    )
                    .await
                    .unwrap();
            }
        }

        async fn archived(&self) -> Vec<u64> {
            let remote = self
                .cold
                .archived_wal_segments(self.life.node_id, self.life.incarnation.as_str(), 0)
                .await
                .unwrap();
            let mut lsns: Vec<u64> = remote
                .into_iter()
                .filter(|(_, seg)| seg.size.is_some() && !seg.crc32c.is_empty())
                .map(|(lsn, _)| lsn)
                .collect();
            lsns.sort_unstable();
            lsns
        }

        async fn rebuilt(&self) -> SnapshotCatalog {
            rebuild_catalog(&self.life.snapshot_store(&self.root), &self.key).await
        }

        fn cold_dir(&self) -> &Path {
            self.cold_dir.path()
        }
    }

    fn begin_lsns(catalog: &SnapshotCatalog) -> Vec<u64> {
        catalog.all().iter().map(|m| m.begin_lsn.as_u64()).collect()
    }

    #[tokio::test]
    async fn a_run_writes_a_base_the_rebuilt_catalog_lists() {
        let fx = Fixture::new().await;
        let outcome = fx.cycle(2).run(cores(40), fx.node_image()).await.unwrap();
        assert!(
            outcome.cleanup_error.is_none(),
            "{:?}",
            outcome.cleanup_error
        );
        assert_eq!(outcome.base.begin_lsn, Lsn::new(40));
        assert_eq!(begin_lsns(&outcome.catalog.unwrap()), [40]);

        let rebuilt = fx.rebuilt().await;
        assert_eq!(rebuilt.all(), std::slice::from_ref(&outcome.base));
        assert!(rebuilt.find_base(Lsn::new(40)).is_some());
        // The base lives under the node life directory, not at the root.
        assert!(rebuild_catalog(&fx.root, &fx.key).await.is_empty());
    }

    #[tokio::test]
    async fn retention_keeps_n_bases_and_deletes_older_ones() {
        let fx = Fixture::new().await;
        for floor in [10, 20, 30] {
            fx.cycle(2)
                .run(cores(floor), fx.node_image())
                .await
                .unwrap();
        }
        let outcome = fx.cycle(2).run(cores(40), fx.node_image()).await.unwrap();
        assert!(
            outcome.cleanup_error.is_none(),
            "{:?}",
            outcome.cleanup_error
        );
        assert_eq!(outcome.deleted_bases, 1);
        assert_eq!(begin_lsns(&outcome.catalog.unwrap()), [30, 40]);
        assert_eq!(begin_lsns(&fx.rebuilt().await), [30, 40]);
    }

    #[tokio::test]
    async fn the_last_base_is_never_deleted() {
        let fx = Fixture::new().await;
        for floor in [10, 20] {
            let outcome = fx
                .cycle(1)
                .run(cores(floor), fx.node_image())
                .await
                .unwrap();
            assert!(
                outcome.cleanup_error.is_none(),
                "{:?}",
                outcome.cleanup_error
            );
            assert_eq!(begin_lsns(&outcome.catalog.unwrap()), [floor]);
        }
        assert_eq!(begin_lsns(&fx.rebuilt().await), [20]);
    }

    #[tokio::test]
    async fn the_wal_chain_from_the_oldest_kept_base_stays_continuous() {
        let fx = Fixture::new().await;
        fx.archive_segments(&[1, 100, 200, 300, 400]).await;
        for floor in [150, 250] {
            fx.cycle(1)
                .run(cores(floor), fx.node_image())
                .await
                .unwrap();
        }
        // The oldest kept base replays from 250, inside the segment at 200.
        let left = fx.archived().await;
        assert_eq!(left, [200, 300, 400]);
        assert!(left[0] <= 250, "the segment holding the floor is kept");

        // Every object below the floor is gone, markers included.
        let node_dir = fx
            .cold_dir()
            .join(crate::wal::archiver::wal_archive_node_prefix(
                "data/",
                fx.life.node_id,
                fx.life.incarnation.as_str(),
            ));
        let mut names: Vec<String> = std::fs::read_dir(&node_dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names.len(),
            6,
            "three segments and their markers: {names:?}"
        );
        assert!(
            names[0].starts_with("wal-00000000000000000200.seg"),
            "{names:?}"
        );
    }

    #[tokio::test]
    async fn an_abandoned_write_is_removed_and_does_not_block_collection() {
        let fx = Fixture::new().await;
        fx.archive_segments(&[1, 100, 200]).await;
        use object_store::ObjectStoreExt;
        let store = fx.life.snapshot_store(&fx.root);
        store
            .put(
                &object_store::path::Path::from("snap-000999-lsn00000000000000000001/core-0.snap"),
                object_store::PutPayload::from_static(b"partial"),
            )
            .await
            .unwrap();
        let outcome = fx.cycle(1).run(cores(150), fx.node_image()).await.unwrap();
        assert!(
            outcome.cleanup_error.is_none(),
            "{:?}",
            outcome.cleanup_error
        );
        assert_eq!(fx.archived().await, [100, 200]);
    }

    /// Put a cold-tier segment object under the cold store's `segments/`.
    async fn put_cold_segment(fx: &Fixture, name: &str) -> String {
        use object_store::ObjectStoreExt;
        let key = format!("{}segments/{name}", fx.cold.prefix());
        fx.cold
            .object_store()
            .put(
                &object_store::path::Path::from(key.as_str()),
                object_store::PutPayload::from_static(b"seg"),
            )
            .await
            .unwrap();
        key
    }

    #[tokio::test]
    async fn a_base_pins_its_cold_segments_until_retention_retires_it() {
        use object_store::ObjectStoreExt;
        let fx = Fixture::new().await;
        let key = put_cold_segment(&fx, "a.seg").await;
        fx.cycle(1).run(cores(10), fx.node_image()).await.unwrap();
        assert!(fx.pitr.cold_pins().read().await.is_pinned(&key));

        // The segment leaves the store, so the next base does not reference it.
        fx.cold
            .object_store()
            .delete(&object_store::path::Path::from(key.as_str()))
            .await
            .unwrap();
        let outcome = fx.cycle(1).run(cores(20), fx.node_image()).await.unwrap();
        assert_eq!(outcome.deleted_bases, 1);
        assert!(!fx.pitr.cold_pins().read().await.is_pinned(&key));
    }

    impl Fixture {
        /// Run one cycle, then install its catalog as the base task does.
        async fn run(&self, retention: usize, cores: Vec<(usize, Vec<u8>)>) -> CycleOutcome {
            let outcome = self
                .cycle(retention)
                .run(cores, self.node_image())
                .await
                .unwrap();
            assert!(
                outcome.cleanup_error.is_none(),
                "{:?}",
                outcome.cleanup_error
            );
            if let Some(catalog) = &outcome.catalog {
                self.pitr.replace_catalog(catalog.clone(), None);
            }
            outcome
        }

        async fn stored_chunks(&self) -> std::collections::BTreeSet<String> {
            let store = self.life.snapshot_store(&self.root);
            list_chunk_ids(&store).await.unwrap().into_iter().collect()
        }

        async fn listed_chunks(&self) -> std::collections::BTreeSet<String> {
            let store = self.life.snapshot_store(&self.root);
            let listed = list_snapshots(&store, &self.key).await.unwrap();
            listed
                .bases
                .into_iter()
                .flat_map(|base| base.chunk_ids)
                .collect()
        }
    }

    #[tokio::test]
    async fn retention_deletes_a_chunk_only_when_no_kept_base_lists_it() {
        let fx = Fixture::new().await;
        let mut data = noise(30_000, 3);
        let first = fx.run(1, cores_with(10, data.clone())).await;
        let first_chunks = fx.listed_chunks().await;

        data[15_000] ^= 0xFF;
        let second = fx.run(1, cores_with(10, data)).await;
        assert_eq!(second.base.parent_id, Some(first.base.snapshot_id));
        assert!(second.uploads.reused > 0, "{:?}", second.uploads);
        assert_eq!(second.deleted_bases, 1);
        assert!(second.collected_chunks > 0);

        // The store holds exactly the chunks the kept base lists: the shared
        // ones stayed, the ones only the deleted base listed went.
        let kept = fx.listed_chunks().await;
        assert_eq!(fx.stored_chunks().await, kept);
        assert!(first_chunks.intersection(&kept).count() > 0);
        assert!(first_chunks.difference(&kept).count() > 0);
    }

    #[tokio::test]
    async fn a_chunk_listed_by_a_base_in_progress_survives_retention() {
        let fx = Fixture::new().await;
        fx.run(1, cores_with(10, noise(8_000, 5))).await;
        let in_progress = fx.listed_chunks().await;
        let pending = pending_pin_name();
        fx.pitr
            .chunk_pins()
            .write()
            .await
            .pin(&pending, in_progress.iter().cloned().collect());

        let second = fx.run(1, cores_with(20, noise(8_000, 6))).await;
        assert_eq!(second.deleted_bases, 1);
        assert!(fx.stored_chunks().await.is_superset(&in_progress));

        fx.pitr.chunk_pins().write().await.unpin(&pending);
        fx.run(1, cores_with(30, noise(8_000, 6))).await;
        let kept = fx.listed_chunks().await;
        assert_eq!(fx.stored_chunks().await, kept);
        assert!(
            in_progress.difference(&kept).count() > 0,
            "released chunks went"
        );
    }
}
