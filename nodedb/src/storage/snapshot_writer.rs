// SPDX-License-Identifier: BUSL-1.1

//! Snapshot creation: captures full engine state across all Data Plane cores.
//!
//! A snapshot consists of a set of object-store keys:
//! - `snapshots/snap-{id:06}-lsn{lsn:020}/manifest.msgpack`
//! - `snapshots/snap-{id:06}-lsn{lsn:020}/core-{core_id}.snap`
//!
//! ## Creation flow
//!
//! 1. Control Plane dispatches `PhysicalPlan::CreateSnapshot` to all cores.
//! 2. Each core calls `export_snapshot()` → serializes to bytes → responds.
//! 3. `SnapshotWriter` collects all core snapshots, writes them to the
//!    configured object store, and registers the snapshot in the catalog.
//!
//! ## Consistency
//!
//! The snapshot LSN is the minimum watermark across all cores at the time
//! of the snapshot. WAL records after this LSN may or may not be included
//! in individual core snapshots (cores are not paused during snapshot).
//! On restore, WAL replay from the snapshot LSN forward ensures consistency.
//!
//! ## Storage backend
//!
//! All I/O goes through `Arc<dyn ObjectStore>`. With the `LocalFileSystem`
//! backend (default when no endpoint is configured) this is equivalent to the
//! former `std::fs` path. With an `AmazonS3` backend, data is written to S3.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use tracing::{info, warn};

use crate::data::snapshot::CoreSnapshot;
use crate::storage::snapshot::{
    SNAPSHOT_FORMAT_VERSION, SnapshotCatalog, SnapshotKind, SnapshotMeta,
};
use crate::types::Lsn;

mod object_envelope;
use object_envelope::{
    SNAPSHOT_CORE_KIND, SNAPSHOT_MANIFEST_KIND, check_snapshot_object_size,
    decrypt_snapshot_object, encrypt_snapshot_object,
};

/// Monotonic snapshot ID counter.
static SNAPSHOT_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

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
/// (or `data_dir/snapshots` if `local_dir` is unset).
pub fn build_snapshot_store(
    config: &SnapshotStorageConfig,
    data_dir: &std::path::Path,
) -> crate::Result<Arc<dyn ObjectStore>> {
    build_object_store(
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
    )
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
    /// Per-core snapshot object key names (relative to snapshot prefix).
    pub core_files: Vec<String>,
    /// Number of cores that contributed to this snapshot.
    pub num_cores: usize,
}

/// Build the prefix for a specific snapshot (relative to the store root).
fn snapshot_prefix(snapshot_id: u64, lsn: u64) -> String {
    format!("snap-{snapshot_id:06}-lsn{lsn:020}")
}

/// Create a base snapshot from core snapshots using an `ObjectStore` backend.
///
/// `core_snapshots` contains `(core_id, snapshot_bytes)` pairs collected
/// from all Data Plane cores via `PhysicalPlan::CreateSnapshot`.
///
/// Every core and manifest object is an authenticated, context-bound segment
/// envelope. A key is mandatory at this untrusted storage boundary.
///
/// Returns the snapshot metadata and the object-store prefix where files
/// were written (e.g. `"snap-000001-lsn00000000000000000100"`).
pub async fn create_base_snapshot(
    store: &Arc<dyn ObjectStore>,
    mut core_snapshots: Vec<(usize, Vec<u8>)>,
    node_name: &str,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<(SnapshotMeta, String)> {
    if core_snapshots.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: "no core snapshots provided".into(),
        });
    }

    let encryption_key = encryption_key.ok_or_else(|| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: "object-store snapshots require an encryption key".into(),
    })?;

    core_snapshots.sort_unstable_by_key(|(core_id, _)| *core_id);
    if core_snapshots
        .iter()
        .enumerate()
        .any(|(expected, (core_id, _))| *core_id != expected)
    {
        return Err(crate::Error::BadRequest {
            detail: "snapshot core IDs must be unique and contiguous from zero".into(),
        });
    }

    let mut min_watermark = u64::MAX;
    let mut max_watermark = 0u64;
    let mut total_data_bytes = 0u64;

    for (_core_id, bytes) in &core_snapshots {
        if let Some(snap) = CoreSnapshot::from_bytes(bytes) {
            min_watermark = min_watermark.min(snap.watermark);
            max_watermark = max_watermark.max(snap.watermark);
        }
        total_data_bytes += bytes.len() as u64;
    }

    if min_watermark == u64::MAX {
        min_watermark = 0;
    }

    let snapshot_id = SNAPSHOT_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let prefix = snapshot_prefix(snapshot_id, min_watermark);

    let mut core_files = Vec::with_capacity(core_snapshots.len());
    for (core_id, bytes) in &core_snapshots {
        let filename = format!("core-{core_id}.snap");
        let object_key = ObjectPath::from(format!("{prefix}/{filename}"));

        let watermark = CoreSnapshot::from_bytes(bytes)
            .map(|snapshot| snapshot.watermark)
            .unwrap_or(min_watermark);
        let payload_bytes = encrypt_snapshot_object(
            bytes,
            &prefix,
            SNAPSHOT_CORE_KIND,
            Some(*core_id),
            node_name,
            watermark,
            encryption_key,
        )?;

        store
            .put(&object_key, PutPayload::from(payload_bytes))
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!("put {object_key}: {e}"),
            })?;

        core_files.push(filename);
    }

    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;

    let meta = SnapshotMeta {
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id,
        begin_lsn: Lsn::new(min_watermark),
        end_lsn: Lsn::new(max_watermark),
        created_at_us: now_us,
        created_by: node_name.to_string(),
        kind: SnapshotKind::Base,
        parent_id: None,
        data_bytes: total_data_bytes,
    };

    let manifest = SnapshotManifest {
        meta: meta.clone(),
        core_files,
        num_cores: core_snapshots.len(),
    };

    let manifest_bytes =
        zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;

    let manifest_key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
    let manifest_bytes = encrypt_snapshot_object(
        &manifest_bytes,
        &prefix,
        SNAPSHOT_MANIFEST_KIND,
        None,
        node_name,
        max_watermark,
        encryption_key,
    )?;
    store
        .put(&manifest_key, PutPayload::from(manifest_bytes))
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("put manifest {manifest_key}: {e}"),
        })?;

    info!(
        snapshot_id,
        begin_lsn = min_watermark,
        end_lsn = max_watermark,
        cores = manifest.num_cores,
        data_bytes = total_data_bytes,
        prefix = %prefix,
        "base snapshot created"
    );

    Ok((meta, prefix))
}

/// Load a snapshot manifest from the object store.
pub async fn load_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<SnapshotManifest> {
    let manifest_key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
    let result = store
        .get(&manifest_key)
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("get manifest {manifest_key}: {e}"),
        })?;
    check_snapshot_object_size(result.meta.size, "snapshot manifest")?;
    let raw = result.bytes().await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("read manifest bytes: {e}"),
    })?;
    let bytes =
        decrypt_snapshot_object(&raw, prefix, SNAPSHOT_MANIFEST_KIND, None, encryption_key)?;
    let manifest: SnapshotManifest =
        zerompk::from_msgpack(&bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;
    manifest.meta.validate_format_version()?;
    if snapshot_prefix(manifest.meta.snapshot_id, manifest.meta.begin_lsn.as_u64()) != prefix {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot manifest metadata does not match canonical prefix".into(),
        });
    }
    if manifest.num_cores != manifest.core_files.len()
        || manifest
            .core_files
            .iter()
            .enumerate()
            .any(|(core_id, name)| name != &format!("core-{core_id}.snap"))
    {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: "snapshot manifest core object list is non-canonical".into(),
        });
    }
    Ok(manifest)
}

/// Load a per-core snapshot from the object store.
///
/// The object is decrypted from its mandatory authenticated envelope before
/// deserialization. A missing key is rejected before any object payload use.
pub async fn load_core_snapshot(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    core_id: usize,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<CoreSnapshot> {
    let encryption_key = encryption_key.ok_or_else(|| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: "object-store snapshots require an encryption key".into(),
    })?;
    let key = ObjectPath::from(format!("{prefix}/core-{core_id}.snap"));
    let result = store.get(&key).await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("get core-{core_id} snapshot: {e}"),
    })?;
    check_snapshot_object_size(result.meta.size, &format!("core-{core_id} snapshot"))?;
    let raw = result.bytes().await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("read core-{core_id} bytes: {e}"),
    })?;
    let bytes = decrypt_snapshot_object(
        &raw,
        prefix,
        SNAPSHOT_CORE_KIND,
        Some(core_id),
        encryption_key,
    )?;

    CoreSnapshot::from_bytes(&bytes).ok_or_else(|| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("failed to deserialize core-{core_id} snapshot"),
    })
}

/// Discover all snapshot prefixes in the object store.
///
/// Returns manifests sorted by `end_lsn` (oldest first).
pub async fn discover_snapshots(
    store: &Arc<dyn ObjectStore>,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> Vec<(String, SnapshotManifest)> {
    use object_store::ListResult;

    let list_result: ListResult = match store.list_with_delimiter(None).await {
        Ok(r) => r,
        Err(e) => {
            warn!(error = %e, "failed to list snapshots from object store");
            return Vec::new();
        }
    };

    let mut results = Vec::new();
    for common_prefix in list_result.common_prefixes {
        // The prefix path ends with "/"; strip it to get the plain prefix name.
        let prefix_str = common_prefix.as_ref().trim_end_matches('/').to_string();
        match load_manifest(store, &prefix_str, encryption_key).await {
            Ok(manifest) => results.push((prefix_str, manifest)),
            Err(e) => {
                warn!(
                    prefix = %prefix_str,
                    error = %e,
                    "skipping snapshot with invalid manifest"
                );
            }
        }
    }

    results.sort_by_key(|(_, m)| m.meta.end_lsn);
    results
}

/// Rebuild the snapshot catalog from the object store on startup.
pub async fn rebuild_catalog(
    store: &Arc<dyn ObjectStore>,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> SnapshotCatalog {
    let mut catalog = SnapshotCatalog::new();
    for (_, manifest) in discover_snapshots(store, encryption_key).await {
        catalog.add(manifest.meta);
    }
    catalog
}

/// Delete a snapshot and all its objects from the object store.
pub async fn delete_snapshot(store: &Arc<dyn ObjectStore>, prefix: &str) -> crate::Result<()> {
    use futures::TryStreamExt;

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
    use crate::data::snapshot::CoreSnapshot;
    use futures::TryStreamExt;
    use object_store::memory::InMemory;

    fn make_core_snapshot(watermark: u64) -> Vec<u8> {
        let snap = CoreSnapshot {
            watermark,
            ..CoreSnapshot::empty()
        };
        snap.to_bytes().unwrap()
    }

    fn in_memory_store() -> Arc<dyn ObjectStore> {
        Arc::new(InMemory::new())
    }

    fn test_key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0xA5; 32]).expect("test encryption key")
    }

    #[tokio::test]
    async fn create_and_load_snapshot() {
        let store = in_memory_store();
        let core_snaps = vec![(0, make_core_snapshot(100)), (1, make_core_snapshot(105))];

        let key = test_key();
        let (meta, prefix) = create_base_snapshot(&store, core_snaps, "test-node", Some(&key))
            .await
            .unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(100));
        assert_eq!(meta.end_lsn, Lsn::new(105));
        assert_eq!(meta.kind, SnapshotKind::Base);
        assert!(meta.data_bytes > 0);

        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        assert_eq!(manifest.num_cores, 2);
        assert_eq!(manifest.core_files.len(), 2);
        assert_eq!(manifest.meta.snapshot_id, meta.snapshot_id);

        let core0 = load_core_snapshot(&store, &prefix, 0, Some(&key))
            .await
            .unwrap();
        assert_eq!(core0.watermark, 100);
        let core1 = load_core_snapshot(&store, &prefix, 1, Some(&key))
            .await
            .unwrap();
        assert_eq!(core1.watermark, 105);
    }

    #[tokio::test]
    async fn authenticated_context_rejects_snapshot_and_core_substitution() {
        let store = in_memory_store();
        let key = test_key();
        let (_, first) =
            create_base_snapshot(&store, vec![(0, make_core_snapshot(10))], "n1", Some(&key))
                .await
                .unwrap();
        let (_, second) = create_base_snapshot(
            &store,
            vec![(0, make_core_snapshot(20)), (1, make_core_snapshot(21))],
            "n1",
            Some(&key),
        )
        .await
        .unwrap();

        let first_core = ObjectPath::from(format!("{first}/core-0.snap"));
        let second_core = ObjectPath::from(format!("{second}/core-0.snap"));
        let second_core_one = ObjectPath::from(format!("{second}/core-1.snap"));
        let wrong_core = store
            .get(&second_core_one)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .put(&second_core, PutPayload::from(wrong_core))
            .await
            .unwrap();
        assert!(
            load_core_snapshot(&store, &second, 0, Some(&key))
                .await
                .is_err()
        );

        let replay = store.get(&first_core).await.unwrap().bytes().await.unwrap();
        store
            .put(&second_core, PutPayload::from(replay))
            .await
            .unwrap();
        assert!(
            load_core_snapshot(&store, &second, 0, Some(&key))
                .await
                .is_err()
        );

        let first_manifest = ObjectPath::from(format!("{first}/manifest.msgpack"));
        let second_manifest = ObjectPath::from(format!("{second}/manifest.msgpack"));
        let replay = store
            .get(&first_manifest)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        store
            .put(&second_manifest, PutPayload::from(replay))
            .await
            .unwrap();
        assert!(load_manifest(&store, &second, &key).await.is_err());
    }

    #[tokio::test]
    async fn discover_and_rebuild_catalog() {
        let store = in_memory_store();

        let key = test_key();
        create_base_snapshot(&store, vec![(0, make_core_snapshot(50))], "n1", Some(&key))
            .await
            .unwrap();
        create_base_snapshot(&store, vec![(0, make_core_snapshot(200))], "n1", Some(&key))
            .await
            .unwrap();

        let found = discover_snapshots(&store, &key).await;
        assert_eq!(found.len(), 2);
        assert!(found[0].1.meta.end_lsn <= found[1].1.meta.end_lsn);

        let catalog = rebuild_catalog(&store, &key).await;
        assert_eq!(catalog.len(), 2);
        assert!(catalog.find_base(Lsn::new(100)).is_some());
    }

    #[tokio::test]
    async fn delete_snapshot_removes_objects() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) =
            create_base_snapshot(&store, vec![(0, make_core_snapshot(10))], "n1", Some(&key))
                .await
                .unwrap();

        // Manifest should be present.
        let key = ObjectPath::from(format!("{prefix}/manifest.msgpack"));
        assert!(store.head(&key).await.is_ok());

        delete_snapshot(&store, &prefix).await.unwrap();

        // Manifest must be gone.
        assert!(store.head(&key).await.is_err());
    }

    #[tokio::test]
    async fn object_store_snapshots_reject_missing_keys_and_plaintext_payloads() {
        let store = in_memory_store();
        assert!(
            create_base_snapshot(&store, vec![(0, make_core_snapshot(1))], "n1", None)
                .await
                .is_err()
        );

        let prefix = "untrusted-plaintext";
        let object = ObjectPath::from(format!("{prefix}/core-0.snap"));
        store
            .put(&object, PutPayload::from(make_core_snapshot(1)))
            .await
            .expect("write plaintext fixture");
        let key = test_key();
        assert!(
            load_core_snapshot(&store, prefix, 0, Some(&key))
                .await
                .is_err()
        );
        assert!(load_core_snapshot(&store, prefix, 0, None).await.is_err());
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
            assert!(
                create_base_snapshot(&store, core_snapshots, "n1", Some(&key))
                    .await
                    .is_err()
            );
            let objects: Vec<_> = store.list(None).try_collect().await.unwrap();
            assert!(objects.is_empty());
        }
    }

    #[tokio::test]
    async fn out_of_order_core_ids_are_canonicalized() {
        let store = in_memory_store();
        let key = test_key();
        let (_, prefix) = create_base_snapshot(
            &store,
            vec![(1, make_core_snapshot(2)), (0, make_core_snapshot(1))],
            "n1",
            Some(&key),
        )
        .await
        .unwrap();
        let manifest = load_manifest(&store, &prefix, &key).await.unwrap();
        assert_eq!(manifest.core_files, ["core-0.snap", "core-1.snap"]);
    }

    #[tokio::test]
    async fn empty_cores_rejected() {
        let store = in_memory_store();
        let result = create_base_snapshot(&store, vec![], "n1", None).await;
        assert!(result.is_err());
    }

    #[test]
    fn snapshot_prefix_naming() {
        let name = snapshot_prefix(1, 42);
        assert_eq!(name, "snap-000001-lsn00000000000000000042");
    }

    #[tokio::test]
    async fn local_filesystem_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir.path()).unwrap());

        let core_snaps = vec![(0, make_core_snapshot(77))];
        let key = test_key();
        let (meta, prefix) = create_base_snapshot(&store, core_snaps, "local-node", Some(&key))
            .await
            .unwrap();

        assert_eq!(meta.begin_lsn, Lsn::new(77));

        let loaded = load_core_snapshot(&store, &prefix, 0, Some(&key))
            .await
            .unwrap();
        assert_eq!(loaded.watermark, 77);
    }
}
