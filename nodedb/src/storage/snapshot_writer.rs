// SPDX-License-Identifier: BUSL-1.1

//! Base snapshot creation: writes a physical image of every Data Plane core
//! and of the node-level stores to an object store.
//!
//! A store holds:
//! - `chunks/{id}`: one content-addressed chunk, shared by every base that
//!   lists it. `{id}` is a keyed hash of the plaintext.
//! - `snap-{id:06}-lsn{lsn:020}/manifest.msgpack`: one snapshot. It lists
//!   the chunks of each core image and of the node image, in order, and the
//!   cold-tier segment keys the snapshot relies on.
//!
//! Each image is cut at content-defined boundaries, so an unchanged region
//! yields the chunks an earlier base already stored. A new base uploads only
//! the chunks the store lacks. Its manifest still lists every chunk it needs,
//! so each snapshot restores on its own. `{lsn}` is the lowest per-core
//! replay floor. The manifest is written last, so a snapshot without one is
//! incomplete and is never discovered.
//!
//! ## Creation flow
//!
//! 1. The Control Plane dispatches `MetaOp::CreateSnapshot` to every core.
//!    Each core checkpoints every engine, captures its files, and answers
//!    with an encoded [`CoreSnapshot`](crate::data::snapshot::CoreSnapshot).
//! 2. The Control Plane captures the node-level redb images
//!    (`storage::snapshot_node::capture_shared_state`) and lists the cold
//!    segments ([`list_cold_segments`]).
//! 3. [`BasePlan::new`] cuts every image into chunks. The caller pins the
//!    chunk ids, and [`write_base_snapshot`] writes the missing chunks, then
//!    the manifest.
//!
//! ## LSN bounds
//!
//! Each core states the records its files hold as a replay stamp. The
//! manifest records the lowest and highest per-core floor, and
//! `applied_high_lsn`: the highest LSN whose effect any core's files include.
//! A point-in-time restore picks a base whose `applied_high_lsn` is at or
//! below its target, so the base never holds a write after the target.
//!
//! ## Storage backend
//!
//! All I/O goes through `Arc<dyn ObjectStore>`. With the `LocalFileSystem`
//! backend (default when no endpoint is configured) this is equivalent to the
//! former `std::fs` path. With an `AmazonS3` backend, data is written to S3.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::info;

use crate::storage::snapshot::SnapshotMeta;

mod cdc;
mod chunk_id;
mod chunks;
mod create;
mod load;
mod object_envelope;
mod plan;
pub use cdc::CdcParams;
pub use chunks::{CHUNK_DIR, ChunkRef, ChunkUploads, chunk_path, delete_chunk, list_chunk_ids};
#[cfg(test)]
pub(crate) use create::create_base_snapshot_with;
pub use create::{WrittenBase, create_base_snapshot, list_cold_segments, write_base_snapshot};
pub use load::{
    discover_snapshots, load_core_snapshot, load_manifest, load_node_snapshot, rebuild_catalog,
};
pub use plan::BasePlan;

/// The manifest object name inside a snapshot prefix.
const MANIFEST_OBJECT: &str = "manifest.msgpack";

/// The lowest id this process can hand out next. Raised past every id the
/// store holds each time a snapshot starts.
static SNAPSHOT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// The object key of a snapshot's manifest.
pub fn manifest_key(prefix: &str) -> ObjectPath {
    ObjectPath::from(format!("{prefix}/{MANIFEST_OBJECT}"))
}

/// Configuration for the snapshot storage layer.
#[derive(Debug, Clone)]
pub struct SnapshotStorageConfig {
    /// S3-compatible endpoint URL. Empty = local filesystem.
    pub endpoint: String,
    /// Bucket name.
    pub bucket: String,
    /// Prefix path within the bucket.
    pub prefix: String,
    /// Access key (empty = IAM role / instance credentials).
    pub access_key: String,
    /// Secret key.
    pub secret_key: String,
    /// Region (required for AWS S3; ignored by most S3-compatible stores).
    pub region: String,
    /// Local directory for snapshot storage (used when endpoint is empty).
    pub local_dir: Option<PathBuf>,
}

/// Build an `ObjectStore` from a `SnapshotStorageConfig`.
///
/// When `endpoint` is empty, uses `LocalFileSystem` backed by `local_dir`
/// (or `data_dir/snapshots` if `local_dir` is unset). Every key lives under
/// `config.prefix` inside that store.
pub fn build_snapshot_store(
    config: &SnapshotStorageConfig,
    data_dir: &std::path::Path,
) -> crate::Result<Arc<dyn ObjectStore>> {
    let store = build_object_store(
        &config.endpoint,
        &config.bucket,
        &config.region,
        &config.access_key,
        &config.secret_key,
        config
            .local_dir
            .as_deref()
            .unwrap_or(&data_dir.join("snapshots")),
        "snapshot",
    )?;
    let prefix = config.prefix.trim_matches('/');
    if prefix.is_empty() {
        return Ok(store);
    }
    Ok(Arc::new(object_store::prefix::PrefixStore::new(
        store,
        ObjectPath::from(prefix),
    )))
}

/// Shared helper: construct an `ObjectStore` from endpoint / S3 credentials or
/// fall back to `LocalFileSystem` when the endpoint is empty.
fn build_object_store(
    endpoint: &str,
    bucket: &str,
    region: &str,
    access_key: &str,
    secret_key: &str,
    local_dir: &std::path::Path,
    label: &str,
) -> crate::Result<Arc<dyn ObjectStore>> {
    if endpoint.is_empty() {
        // no-objectstore: bootstrap for the LocalFileSystem ObjectStore backend; the store cannot create its own root.
        std::fs::create_dir_all(local_dir).map_err(crate::Error::Io)?;
        let store =
            LocalFileSystem::new_with_prefix(local_dir).map_err(|e| crate::Error::Storage {
                engine: label.into(),
                detail: format!("local {label} storage init: {e}"),
            })?;
        Ok(Arc::new(store))
    } else {
        let mut builder = AmazonS3Builder::new()
            .with_endpoint(endpoint)
            .with_bucket_name(bucket)
            .with_region(region)
            .with_allow_http(endpoint.starts_with("http://"));
        if !access_key.is_empty() {
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);
        }
        let s3 = builder.build().map_err(|e| crate::Error::Storage {
            engine: label.into(),
            detail: format!("S3 {label} client init: {e}"),
        })?;
        Ok(Arc::new(s3))
    }
}

/// Snapshot manifest: stored as `manifest.msgpack` inside the snapshot prefix.
#[derive(
    Debug,
    Clone,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct SnapshotManifest {
    /// Snapshot metadata.
    pub meta: SnapshotMeta,
    /// The chunks of each core's image, indexed by core ID.
    pub core_chunks: Vec<Vec<ChunkRef>>,
    /// The chunks of the node-level image.
    pub node_chunks: Vec<ChunkRef>,
    /// Cold-tier object keys the snapshot relies on. They are referenced,
    /// not copied: they already live in object storage.
    pub cold_segments: Vec<String>,
    /// Number of cores that contributed to this snapshot.
    pub num_cores: usize,
    /// The metadata group's applied index the node image's catalogs hold,
    /// `0` with no cluster.
    pub metadata_applied_index: u64,
    /// No catalog entry of the node image lies above this metadata index.
    pub metadata_captured_index: u64,
    /// The metadata timeline the node image's catalogs belong to.
    pub metadata_timeline: u64,
}

impl SnapshotManifest {
    /// Every chunk id the snapshot lists. An id listed twice appears twice.
    pub fn chunk_ids(&self) -> impl Iterator<Item = &str> {
        self.core_chunks
            .iter()
            .flatten()
            .chain(&self.node_chunks)
            .map(|chunk| chunk.id.as_str())
    }
}

/// Build the prefix for a specific snapshot (relative to the store root).
fn snapshot_prefix(snapshot_id: u64, lsn: u64) -> String {
    format!("snap-{snapshot_id:06}-lsn{lsn:020}")
}

/// The snapshot id a prefix built by [`snapshot_prefix`] names.
fn parse_snapshot_id(prefix: &str) -> Option<u64> {
    let (id, _) = prefix.strip_prefix("snap-")?.split_once("-lsn")?;
    id.parse().ok()
}

/// Delete a snapshot and all its objects from the object store.
///
/// The manifest goes first: a prefix without one is never discovered, so a
/// deletion cut short leaves unreachable chunks, never a manifest naming
/// missing ones. A later deletion of the same prefix removes the rest.
pub async fn delete_snapshot(store: &Arc<dyn ObjectStore>, prefix: &str) -> crate::Result<()> {
    use futures::TryStreamExt;

    match store.delete(&manifest_key(prefix)).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
        Err(e) => {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("delete {}: {e}", manifest_key(prefix)),
            });
        }
    }

    let list_prefix = ObjectPath::from(format!("{prefix}/"));
    let objects: Vec<_> = store
        .list(Some(&list_prefix))
        .try_collect()
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list objects for deletion: {e}"),
        })?;

    for obj in objects {
        store
            .delete(&obj.location)
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("delete {}: {e}", obj.location),
            })?;
    }

    info!(prefix = %prefix, "snapshot deleted");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::snapshot::{CoreSnapshot, NodeSnapshot, SnapshotComponent, SnapshotFile};
    use crate::storage::snapshot::SnapshotKind;
    use crate::types::Lsn;
    use crate::types::replay_stamp::{LsnRange, ReplayStamp};
    use futures::TryStreamExt;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    fn make_core_snapshot(floor: u64) -> Vec<u8> {
        let snap = CoreSnapshot {
            stamp: ReplayStamp::through(floor),
            ..CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    fn no_node() -> NodeSnapshot {
        NodeSnapshot::default()
    }

    fn in_memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn test_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0xA5; 32]).expect("test encryption key")
    }

    async fn create(
        store: &Arc<dyn ObjectStore>,
        cores: Vec<(usize, Vec<u8>)>,
        key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    ) -> crate::Result<(SnapshotMeta, String)> {
        create_base_snapshot(store, cores, &no_node(), &[], "n1", key).await
    }

    #[tokio::test]
    async fn create_and_load_snapshot() {
        let store = in_memory_store();
        let key = test_key();
        let core_snaps = vec![(0, make_core_snapshot(100)), (1, make_core_snapshot(105))];
        let (meta, prefix) = create(&store, core_snaps, Some(&key)).await.unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(100));
        assert_eq!(meta.end_lsn, Lsn::new(105));
        assert_eq!(meta.applied_high_lsn, Lsn::new(105));
        assert_eq!(meta.kind, SnapshotKind::Base);
        assert!(meta.data_bytes > 0);

        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        assert_eq!(manifest.num_cores, 2);
        assert_eq!(manifest.core_chunks.len(), 2);
        assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);

        let core0 = load_core_snapshot(&store, &prefix, &manifest, 0, &key)
            .await
            .unwrap();
        assert_eq!(core0.replay_floor(), 100);
        let core1 = load_core_snapshot(&store, &prefix, &manifest, 1, &key)
            .await
            .unwrap();
        assert_eq!(core1.replay_floor(), 105);
    }

    /// An image larger than one chunk is split into objects no larger than
    /// the chunk bound, each listed by id, and reassembles exactly.
    #[tokio::test]
    async fn a_large_image_is_split_into_bounded_chunks() {
        let params = CdcParams {
            min: 16,
            max: 100,
            mask_bits: 4,
        };
        let store = in_memory_store();
        let key = test_key();
        let big = CoreSnapshot {
            stamp: ReplayStamp::through(7),
            files: vec![SnapshotFile {
                component: SnapshotComponent::Kv,
                path: "kv-ckpt/core-0/MANIFEST".into(),
                bytes: (0..=255u8).cycle().take(1_000).collect(),
            }],
            dirs: Vec::new(),
        };
        let (_, prefix) = create_base_snapshot_with(
            &store,
            vec![(0, big.to_bytes().unwrap())],
            &no_node(),
            &[],
            "n1",
            Some(&key),
            params,
        )
        .await
        .unwrap();

        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        let chunks = &manifest.core_chunks[0];
        assert!(chunks.len() >= 10);
        assert!(chunks.iter().all(|c| c.len <= 100));
        for chunk in chunks {
            assert!(store.head(&chunk_path(&chunk.id)).await.is_ok());
        }
        assert_eq!(
            load_core_snapshot(&store, &prefix, &manifest, 0, &key)
                .await
                .unwrap(),
            big
        );
    }

    /// A chunk object moved to another id, or a manifest moved to another
    /// prefix, fails to open.
    #[tokio::test]
    async fn authenticated_context_rejects_chunk_and_manifest_substitution() {
        let store = in_memory_store();
        let key = test_key();
        let (_, first) = create(&store, vec![(0, make_core_snapshot(10))], Some(&key))
            .await
            .unwrap();
        let (_, second) = create(
            &store,
            vec![(0, make_core_snapshot(20)), (1, make_core_snapshot(21))],
            Some(&key),
        )
        .await
        .unwrap();
        let manifest = load_manifest(&store, &second, &key).await.unwrap();
        let first_manifest = load_manifest(&store, &first, &key).await.unwrap();

        let read = |path: ObjectPath| {
            let store = Arc::clone(&store);
            async move { store.get(&path).await.unwrap().bytes().await.unwrap() }
        };
        let core_zero = chunk_path(&manifest.core_chunks[0][0].id);
        let from_core_one = read(chunk_path(&manifest.core_chunks[1][0].id)).await;
        store
            .put(&core_zero, PutPayload::from(from_core_one))
            .await
            .unwrap();
        assert!(
            load_core_snapshot(&store, &second, &manifest, 0, &key)
                .await
                .is_err()
        );

        let from_first = read(chunk_path(&first_manifest.core_chunks[0][0].id)).await;
        store
            .put(&core_zero, PutPayload::from(from_first))
            .await
            .unwrap();
        assert!(
            load_core_snapshot(&store, &second, &manifest, 0, &key)
                .await
                .is_err()
        );

        let replay = read(manifest_key(&first)).await;
        store
            .put(&manifest_key(&second), PutPayload::from(replay))
            .await
            .unwrap();
        assert!(load_manifest(&store, &second, &key).await.is_err());
    }

    #[tokio::test]
    async fn discover_and_rebuild_catalog() {
        let store = in_memory_store();
        let key = test_key();
        create(&store, vec![(0, make_core_snapshot(50))], Some(&key))
            .await
            .unwrap();
        create(&store, vec![(0, make_core_snapshot(200))], Some(&key))
            .await
            .unwrap();

        let found = discover_snapshots(&store, &key).await;
        assert_eq!(found.len(), 2);
        assert!(found[0].1.meta.applied_high_lsn <= found[1].1.meta.applied_high_lsn);

        let catalog = rebuild_catalog(&store, &key).await;
        assert_eq!(catalog.len(), 2);
        assert!(catalog.find_base(Lsn::new(100)).is_some());
    }

    #[tokio::test]
    async fn delete_snapshot_removes_objects() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) = create(&store, vec![(0, make_core_snapshot(10))], Some(&key))
            .await
            .unwrap();
        let manifest_key = ObjectPath::from(format!("{prefix}/{MANIFEST_OBJECT}"));
        assert!(store.head(&manifest_key).await.is_ok());

        delete_snapshot(&store, &prefix).await.unwrap();
        let under_prefix = ObjectPath::from(format!("{prefix}/"));
        let objects: Vec<_> = store.list(Some(&under_prefix)).try_collect().await.unwrap();
        assert!(objects.is_empty());
        // Shared chunks outlive the snapshot. Chunk garbage collection
        // removes them once no kept manifest lists them.
        assert!(!list_chunk_ids(&store).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn object_store_snapshots_reject_missing_keys_and_plaintext_payloads() {
        let store = in_memory_store();
        assert!(
            create(&store, vec![(0, make_core_snapshot(1))], None)
                .await
                .is_err()
        );

        let prefix = "untrusted-plaintext";
        store
            .put(
                &ObjectPath::from(format!("{prefix}/{MANIFEST_OBJECT}")),
                PutPayload::from(make_core_snapshot(1)),
            )
            .await
            .expect("write plaintext fixture");
        assert!(load_manifest(&store, prefix, &test_key()).await.is_err());
    }

    #[tokio::test]
    async fn invalid_core_id_sets_are_rejected_before_object_writes() {
        for core_snapshots in [
            vec![(1, make_core_snapshot(1))],
            vec![(0, make_core_snapshot(1)), (0, make_core_snapshot(2))],
            vec![(0, make_core_snapshot(1)), (2, make_core_snapshot(2))],
        ] {
            let store = in_memory_store();
            let key = test_key();
            assert!(create(&store, core_snapshots, Some(&key)).await.is_err());
            let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
            assert!(objects.is_empty());
        }
    }

    #[tokio::test]
    async fn out_of_order_core_ids_are_canonicalized() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) = create(
            &store,
            vec![(1, make_core_snapshot(2)), (0, make_core_snapshot(1))],
            Some(&key),
        )
        .await
        .unwrap();
        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        let core = |id| load_core_snapshot(&store, &prefix, &manifest, id, &key);
        assert_eq!(core(0).await.unwrap().replay_floor(), 1);
        assert_eq!(core(1).await.unwrap().replay_floor(), 2);
    }

    #[tokio::test]
    async fn empty_cores_rejected() {
        let store = in_memory_store();
        assert!(create(&store, vec![], Some(&test_key())).await.is_err());
    }

    #[test]
    fn snapshot_prefix_naming() {
        assert_eq!(
            snapshot_prefix(1, 42),
            "snap-000001-lsn00000000000000000042"
        );
    }

    #[tokio::test]
    async fn local_filesystem_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());
        let key = test_key();
        let (meta, prefix) = create(&store, vec![(0, make_core_snapshot(77))], Some(&key))
            .await
            .unwrap();
        assert_eq!(meta.begin_lsn, Lsn::new(77));

        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        let loaded = load_core_snapshot(&store, &prefix, &manifest, 0, &key)
            .await
            .unwrap();
        assert_eq!(loaded.replay_floor(), 77);
    }

    #[tokio::test]
    async fn applied_high_lsn_is_the_highest_record_any_core_holds() {
        let store = in_memory_store();
        let key = test_key();
        let above_floor = CoreSnapshot {
            stamp: ReplayStamp {
                prefix: 20,
                applied_above: vec![LsnRange { start: 30, end: 31 }],
            },
            ..CoreSnapshot::empty()
        }
        .to_bytes()
        .unwrap();
        let (meta, _) = create(
            &store,
            vec![(0, make_core_snapshot(25)), (1, above_floor)],
            Some(&key),
        )
        .await
        .unwrap();
        assert_eq!(meta.begin_lsn, Lsn::new(20));
        assert_eq!(meta.end_lsn, Lsn::new(25));
        assert_eq!(meta.applied_high_lsn, Lsn::new(31));
    }

    #[tokio::test]
    async fn the_node_image_and_cold_keys_round_trip() {
        let store = in_memory_store();
        let key = test_key();
        let node = NodeSnapshot {
            files: vec![SnapshotFile {
                component: SnapshotComponent::SystemCatalog,
                path: "system.redb".into(),
                bytes: b"catalog".to_vec(),
            }],
            metadata_applied_index: 0,
            metadata_captured_index: 0,
            metadata_timeline: 0,
        };
        let cold = vec!["cold/segments/a".to_string()];
        let (_, prefix) = create_base_snapshot(
            &store,
            vec![(0, make_core_snapshot(1))],
            &node,
            &cold,
            "n1",
            Some(&key),
        )
        .await
        .unwrap();
        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        assert_eq!(manifest.cold_segments, cold);
        assert_eq!(
            load_node_snapshot(&store, &manifest, &key).await.unwrap(),
            node
        );
    }

    #[tokio::test]
    async fn a_core_file_in_the_node_image_is_refused() {
        let store = in_memory_store();
        let node = NodeSnapshot {
            files: vec![SnapshotFile {
                component: SnapshotComponent::Sparse,
                path: "sparse/core-0.redb".into(),
                bytes: vec![],
            }],
            metadata_applied_index: 0,
            metadata_captured_index: 0,
            metadata_timeline: 0,
        };
        let result = create_base_snapshot(
            &store,
            vec![(0, make_core_snapshot(1))],
            &node,
            &[],
            "n1",
            Some(&test_key()),
        )
        .await;
        assert!(result.is_err());
        let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn an_undecodable_core_snapshot_is_refused() {
        let store = in_memory_store();
        assert!(
            create(&store, vec![(0, vec![0xC1])], Some(&test_key()))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cold_segments_are_listed_under_the_tier_prefix() {
        let cold = in_memory_store();
        for name in ["pre/segments/b", "pre/segments/a", "pre/other/c"] {
            cold.put(&ObjectPath::from(name), PutPayload::from(vec![1u8]))
                .await
                .unwrap();
        }
        assert_eq!(
            list_cold_segments(&cold, "pre/").await.unwrap(),
            ["pre/segments/a", "pre/segments/b"]
        );
    }

    /// A restarted process starts its counter at 1 again. The store still
    /// holds every id it handed out, so the next snapshot takes a new one.
    #[tokio::test]
    async fn snapshot_ids_stay_unique_across_a_restart() {
        let store = in_memory_store();
        let key = test_key();
        let (first, first_prefix) = create(&store, vec![(0, make_core_snapshot(5))], Some(&key))
            .await
            .unwrap();
        SNAPSHOT_ID_COUNTER.store(1, std::sync::atomic::Ordering::SeqCst);
        let (second, _) = create(&store, vec![(0, make_core_snapshot(5))], Some(&key))
            .await
            .unwrap();
        assert!(second.snapshot_id > first.snapshot_id);
        let manifest = load_manifest(&store, &first_prefix, &key).await.unwrap();
        assert_eq!(manifest.meta.snapshot_id, first.snapshot_id);
    }

    #[tokio::test]
    async fn a_manifest_is_never_overwritten() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) = create(&store, vec![(0, make_core_snapshot(5))], Some(&key))
            .await
            .unwrap();
        let before = store
            .get(&manifest_key(&prefix))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(
            create::put_manifest_once(&store, &manifest_key(&prefix), vec![1, 2, 3])
                .await
                .is_err()
        );
        assert!(
            create::refuse_existing_prefix(&store, &prefix)
                .await
                .is_err()
        );
        let after = store
            .get(&manifest_key(&prefix))
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert_eq!(before, after);
    }

    #[tokio::test]
    async fn discovery_skips_a_prefix_with_no_manifest() {
        let store = in_memory_store();
        let key = test_key();
        create(&store, vec![(0, make_core_snapshot(5))], Some(&key))
            .await
            .unwrap();
        store
            .put(
                &ObjectPath::from("snap-999999-lsn00000000000000000001/core-0-000000.snap"),
                PutPayload::from(vec![1u8]),
            )
            .await
            .unwrap();
        assert_eq!(discover_snapshots(&store, &key).await.len(), 1);
    }

    #[tokio::test]
    async fn a_cut_short_deletion_leaves_no_manifest_behind() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) = create(&store, vec![(0, make_core_snapshot(5))], Some(&key))
            .await
            .unwrap();
        delete_snapshot(&store, &prefix).await.unwrap();
        assert!(store.head(&manifest_key(&prefix)).await.is_err());
        assert!(discover_snapshots(&store, &key).await.is_empty());
        // Deleting again, as retention does for leftovers, is not an error.
        delete_snapshot(&store, &prefix).await.unwrap();
    }

    #[test]
    fn snapshot_ids_parse_back_from_their_prefix() {
        assert_eq!(parse_snapshot_id(&snapshot_prefix(42, 7)), Some(42));
        assert_eq!(parse_snapshot_id("other"), None);
        assert_eq!(
            manifest_key("snap-000001-lsn00000000000000000002").as_ref(),
            "snap-000001-lsn00000000000000000002/manifest.msgpack"
        );
    }

    #[tokio::test]
    async fn the_configured_prefix_scopes_every_key() {
        let dir = tempfile::tempdir().unwrap();
        let config = SnapshotStorageConfig {
            endpoint: String::new(),
            bucket: String::new(),
            prefix: "cluster-a/snapshots/".into(),
            access_key: String::new(),
            secret_key: String::new(),
            region: String::new(),
            local_dir: Some(dir.path().to_path_buf()),
        };
        let store = build_snapshot_store(&config, dir.path()).unwrap();
        store
            .put(&ObjectPath::from("snap-1/x"), PutPayload::from(vec![1u8]))
            .await
            .unwrap();
        assert!(dir.path().join("cluster-a/snapshots/snap-1/x").is_file());
    }
}
