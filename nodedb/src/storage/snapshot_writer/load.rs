// SPDX-License-Identifier: BUSL-1.1

//! Reading base snapshot objects back from the object store.

use std::sync::Arc;

use object_store::ObjectStore;
use object_store::ObjectStoreExt;
use tracing::{debug, warn};

use super::chunk_id::is_chunk_id;
use super::chunks::{CHUNK_DIR, ChunkRef, fetch_chunked};
use super::object_envelope::{
    ObjectContext, SNAPSHOT_MANIFEST_KIND, check_snapshot_object_size, decrypt_snapshot_object,
};
use super::{MANIFEST_OBJECT, SnapshotManifest, manifest_key, snapshot_prefix};
use crate::data::snapshot::{CoreSnapshot, NodeSnapshot};
use crate::storage::snapshot::SnapshotCatalog;

/// Load a snapshot manifest from the object store.
pub async fn load_manifest(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<SnapshotManifest> {
    let object = manifest_key(prefix);
    let result = store
        .get(&object)
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("get {object}: {e}"),
        })?;
    check_snapshot_object_size(result.meta.size, MANIFEST_OBJECT)?;
    let raw = result.bytes().await.map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("read {object}: {e}"),
    })?;
    let ctx = ObjectContext {
        name: prefix,
        kind: SNAPSHOT_MANIFEST_KIND,
    };
    let bytes = decrypt_snapshot_object(&raw, ctx, encryption_key)?;
    let manifest: SnapshotManifest =
        zerompk::from_msgpack(&bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;
    manifest.meta.validate_format_version()?;
    if snapshot_prefix(manifest.meta.snapshot_id, manifest.meta.begin_lsn.as_u64()) != prefix {
        return Err(non_canonical(
            "metadata does not match the canonical prefix",
        ));
    }
    if manifest.num_cores != manifest.core_chunks.len() {
        return Err(non_canonical(
            "core count does not match the core chunk lists",
        ));
    }
    for chunks in &manifest.core_chunks {
        check_chunk_ids(chunks)?;
    }
    check_chunk_ids(&manifest.node_chunks)?;
    Ok(manifest)
}

/// Every image lists at least one chunk, each named by a well-formed id.
fn check_chunk_ids(chunks: &[ChunkRef]) -> crate::Result<()> {
    if chunks.is_empty() {
        return Err(non_canonical("an image lists no chunks"));
    }
    if chunks.iter().any(|chunk| !is_chunk_id(&chunk.id)) {
        return Err(non_canonical("a chunk id is malformed"));
    }
    Ok(())
}

fn non_canonical(why: &str) -> crate::Error {
    crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("snapshot manifest is non-canonical: {why}"),
    }
}

/// Load one core's image: every chunk fetched, authenticated, and checked
/// against the manifest before the image decodes.
pub async fn load_core_snapshot(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
    manifest: &SnapshotManifest,
    core_id: usize,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<CoreSnapshot> {
    let chunks = manifest
        .core_chunks
        .get(core_id)
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("snapshot {prefix} has no core {core_id}"),
        })?;
    let bytes = fetch_chunked(store, chunks, encryption_key).await?;
    CoreSnapshot::from_bytes(&bytes)
}

/// Load the node-level image, checked the same way as a core image.
pub async fn load_node_snapshot(
    store: &Arc<dyn ObjectStore>,
    manifest: &SnapshotManifest,
    encryption_key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<NodeSnapshot> {
    let bytes = fetch_chunked(store, &manifest.node_chunks, encryption_key).await?;
    NodeSnapshot::from_bytes(&bytes)
}

/// Discover all snapshot prefixes in the object store.
///
/// Returns manifests sorted by `applied_high_lsn` (oldest first).
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
        if prefix_str == CHUNK_DIR {
            continue;
        }
        // A prefix with no manifest is an unfinished write or a deletion cut
        // short. It is not a snapshot, and retention removes it.
        if let Err(object_store::Error::NotFound { .. }) =
            store.head(&manifest_key(&prefix_str)).await
        {
            debug!(prefix = %prefix_str, "skipping snapshot prefix with no manifest");
            continue;
        }
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

    results.sort_by_key(|(_, m)| m.meta.applied_high_lsn);
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
