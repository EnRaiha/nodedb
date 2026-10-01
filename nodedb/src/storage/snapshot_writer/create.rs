// SPDX-License-Identifier: BUSL-1.1

//! Writing a base snapshot: the chunks the store lacks, then the manifest
//! that commits them.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use tracing::info;

use super::cdc::CdcParams;
use super::chunks::{ChunkUploads, ChunkWrite, put_missing_chunks};
use super::object_envelope::{ObjectContext, SNAPSHOT_MANIFEST_KIND, encrypt_snapshot_object};
use super::plan::BasePlan;
use super::{
    SNAPSHOT_ID_COUNTER, SnapshotManifest, manifest_key, parse_snapshot_id, snapshot_prefix,
};
use crate::data::snapshot::NodeSnapshot;
use crate::storage::snapshot::{SNAPSHOT_FORMAT_VERSION, SnapshotKind, SnapshotMeta};
use crate::types::Lsn;

/// A base the store now holds.
#[derive(Debug, Clone)]
pub struct WrittenBase {
    pub meta: SnapshotMeta,
    /// The snapshot prefix, e.g. `snap-000001-lsn00000000000000000100`.
    pub prefix: String,
    pub uploads: ChunkUploads,
}

/// The next snapshot id: above every id in the store and every id this
/// process has handed out.
///
/// The store is the record of which ids exist, so ids stay unique across
/// restarts without a separate counter file and without trusting the clock.
/// Two processes writing one store at the same moment can still pick the same
/// id; [`refuse_existing_prefix`] and the create-only manifest write refuse
/// the second.
async fn next_snapshot_id(store: &Arc<dyn ObjectStore>) -> crate::Result<u64> {
    let listed = store
        .list_with_delimiter(None)
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list snapshot prefixes to pick an id: {e}"),
        })?;
    let floor = listed
        .common_prefixes
        .iter()
        .filter_map(|p| parse_snapshot_id(p.as_ref().trim_end_matches('/')))
        .max()
        .map_or(1, |max| max.saturating_add(1));
    let mut current = SNAPSHOT_ID_COUNTER.load(Ordering::SeqCst);
    loop {
        let id = current.max(floor);
        match SNAPSHOT_ID_COUNTER.compare_exchange(
            current,
            id.saturating_add(1),
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => return Ok(id),
            Err(actual) => current = actual,
        }
    }
}

/// Refuse a prefix that already holds any object: a new snapshot never
/// writes into another one's objects.
pub(super) async fn refuse_existing_prefix(
    store: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> crate::Result<()> {
    let list_prefix = ObjectPath::from(format!("{prefix}/"));
    let existing = store
        .list_with_delimiter(Some(&list_prefix))
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list {list_prefix}: {e}"),
        })?;
    if !existing.objects.is_empty() || !existing.common_prefixes.is_empty() {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!(
                "snapshot prefix {prefix} already holds objects; refusing to overwrite it"
            ),
        });
    }
    Ok(())
}

/// Write the manifest only if none exists at `key`.
///
/// A store without conditional writes gets a presence check and a plain put.
/// The prefix was checked empty before the chunks went in, so only a
/// concurrent writer of the same id can slip between the two.
pub(super) async fn put_manifest_once(
    store: &Arc<dyn ObjectStore>,
    key: &ObjectPath,
    payload: Vec<u8>,
) -> crate::Result<()> {
    let refused = |detail: String| crate::Error::Storage {
        engine: "snapshot".into(),
        detail,
    };
    match store
        .put_opts(
            key,
            PutPayload::from(payload.clone()),
            PutMode::Create.into(),
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => Err(refused(format!(
            "snapshot manifest {key} already exists; refusing to overwrite it"
        ))),
        Err(
            object_store::Error::NotImplemented { .. } | object_store::Error::NotSupported { .. },
        ) => {
            if store.head(key).await.is_ok() {
                return Err(refused(format!(
                    "snapshot manifest {key} already exists; refusing to overwrite it"
                )));
            }
            store
                .put(key, PutPayload::from(payload))
                .await
                .map(|_| ())
                .map_err(|e| refused(format!("put {key}: {e}")))
        }
        Err(e) => Err(refused(format!("put {key}: {e}"))),
    }
}

/// List the cold-tier segment keys under `{prefix}segments/` in `cold`, so a
/// base snapshot can reference them.
pub async fn list_cold_segments(
    cold: &Arc<dyn ObjectStore>,
    prefix: &str,
) -> crate::Result<Vec<String>> {
    let list_prefix = ObjectPath::from(format!("{prefix}segments/"));
    let objects: Vec<_> = cold
        .list(Some(&list_prefix))
        .try_collect()
        .await
        .map_err(|e| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("list cold segments under {list_prefix}: {e}"),
        })?;
    let mut keys: Vec<String> = objects
        .into_iter()
        .map(|o| o.location.as_ref().to_string())
        .collect();
    keys.sort_unstable();
    Ok(keys)
}

/// Create a base snapshot from core images, the node image, and the cold
/// segment keys it relies on.
///
/// `core_snapshots` holds one `(core_id, encoded CoreSnapshot)` per core,
/// collected from `MetaOp::CreateSnapshot`. `node` holds only node-level
/// components. A key is mandatory at this untrusted storage boundary.
///
/// This takes no chunk pins. A store whose chunks are garbage-collected is
/// written through [`BasePlan`] and [`write_base_snapshot`] under a pin.
pub async fn create_base_snapshot(
    store: &Arc<dyn ObjectStore>,
    core_snapshots: Vec<(usize, Vec<u8>)>,
    node: &NodeSnapshot,
    cold_segments: &[String],
    node_name: &str,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
) -> crate::Result<(SnapshotMeta, String)> {
    create_base_snapshot_with(
        store,
        core_snapshots,
        node,
        cold_segments,
        node_name,
        encryption_key,
        CdcParams::DEFAULT,
    )
    .await
}

/// [`create_base_snapshot`] with the given chunk bounds.
pub(crate) async fn create_base_snapshot_with(
    store: &Arc<dyn ObjectStore>,
    core_snapshots: Vec<(usize, Vec<u8>)>,
    node: &NodeSnapshot,
    cold_segments: &[String],
    node_name: &str,
    encryption_key: Option<&nodedb_wal::crypto::WalEncryptionKey>,
    params: CdcParams,
) -> crate::Result<(SnapshotMeta, String)> {
    let key = encryption_key.ok_or_else(|| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: "object-store snapshots require an encryption key".into(),
    })?;
    let plan = BasePlan::new(core_snapshots, node, cold_segments.to_vec(), key, params)?;
    let written = write_base_snapshot(store, &plan, None, node_name, key).await?;
    Ok((written.meta, written.prefix))
}

/// Write the chunks of `plan` the store lacks, then the manifest.
///
/// `parent` is the snapshot id of the newest base already in the store. A
/// base that reuses any stored chunk records it and has kind
/// [`SnapshotKind::Delta`]. Its manifest still lists every chunk it needs,
/// so a restore never reads the parent.
///
/// Every chunk id of `plan` must be pinned against garbage collection for
/// the whole call: a chunk found present is not uploaded again.
pub async fn write_base_snapshot(
    store: &Arc<dyn ObjectStore>,
    plan: &BasePlan,
    parent: Option<u64>,
    node_name: &str,
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<WrittenBase> {
    let bounds = &plan.bounds;
    let snapshot_id = next_snapshot_id(store).await?;
    let prefix = snapshot_prefix(snapshot_id, bounds.lowest_floor);
    refuse_existing_prefix(store, &prefix).await?;

    let write = ChunkWrite {
        store,
        node_name,
        watermark: bounds.applied_high,
        key,
    };
    let mut present = HashSet::new();
    let mut uploads = ChunkUploads::default();
    for (image, chunks) in plan.cores.iter().zip(&plan.core_chunks) {
        put_missing_chunks(&write, image, chunks, &mut present, &mut uploads).await?;
    }
    put_missing_chunks(
        &write,
        &plan.node,
        &plan.node_chunks,
        &mut present,
        &mut uploads,
    )
    .await?;

    let parent_id = parent.filter(|_| uploads.reused > 0);
    let now_us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64;
    let meta = SnapshotMeta {
        format_version: SNAPSHOT_FORMAT_VERSION,
        snapshot_id,
        begin_lsn: Lsn::new(bounds.lowest_floor),
        end_lsn: Lsn::new(bounds.highest_floor),
        applied_high_lsn: Lsn::new(bounds.applied_high),
        created_at_us: now_us,
        created_by: node_name.to_string(),
        kind: if parent_id.is_some() {
            SnapshotKind::Delta
        } else {
            SnapshotKind::Base
        },
        parent_id,
        data_bytes: bounds.data_bytes + plan.node.len() as u64,
    };
    let manifest = SnapshotManifest {
        meta: meta.clone(),
        core_chunks: plan.core_chunks.iter().map(|c| c.refs.clone()).collect(),
        node_chunks: plan.node_chunks.refs.clone(),
        cold_segments: plan.cold_segments.clone(),
        num_cores: plan.cores.len(),
        metadata_applied_index: plan.metadata_applied_index,
        metadata_captured_index: plan.metadata_captured_index,
        metadata_timeline: plan.metadata_timeline,
    };
    let manifest_bytes =
        zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("snapshot manifest: {e}"),
        })?;
    let payload = encrypt_snapshot_object(
        &manifest_bytes,
        ObjectContext {
            name: &prefix,
            kind: SNAPSHOT_MANIFEST_KIND,
        },
        node_name,
        bounds.applied_high,
        key,
    )?;
    // The manifest is the commit point: it goes last.
    put_manifest_once(store, &manifest_key(&prefix), payload).await?;

    info!(
        snapshot_id,
        parent_id = ?meta.parent_id,
        begin_lsn = bounds.lowest_floor,
        applied_high_lsn = bounds.applied_high,
        cores = manifest.num_cores,
        data_bytes = meta.data_bytes,
        chunks_uploaded = uploads.uploaded,
        chunk_bytes_uploaded = uploads.uploaded_bytes,
        chunks_reused = uploads.reused,
        prefix = %prefix,
        "base snapshot created"
    );
    Ok(WrittenBase {
        meta,
        prefix,
        uploads,
    })
}

#[cfg(test)]
mod tests {
    use object_store::memory::InMemory;

    use super::*;
    use crate::data::snapshot::{CoreSnapshot, SnapshotComponent, SnapshotFile};
    use crate::storage::snapshot_writer::{list_chunk_ids, load_core_snapshot, load_manifest};
    use crate::types::replay_stamp::ReplayStamp;

    const SMALL: CdcParams = CdcParams {
        min: 64,
        max: 1024,
        mask_bits: 7,
    };

    fn key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0x3C; 32]).unwrap()
    }

    fn noise(len: usize) -> Vec<u8> {
        let mut state = 0x2545_F491_4F6C_DD1Du64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    fn core(bytes: Vec<u8>) -> CoreSnapshot {
        CoreSnapshot {
            stamp: ReplayStamp::through(9),
            files: vec![SnapshotFile {
                component: SnapshotComponent::Kv,
                path: "kv-ckpt/core-0/data".into(),
                bytes,
            }],
            dirs: Vec::new(),
        }
    }

    async fn write(
        store: &Arc<dyn ObjectStore>,
        image: &CoreSnapshot,
        parent: Option<u64>,
    ) -> (WrittenBase, BasePlan) {
        let key = key();
        let cores = vec![(0, image.to_bytes().unwrap())];
        let plan = BasePlan::new(cores, &NodeSnapshot::default(), Vec::new(), &key, SMALL).unwrap();
        let written = write_base_snapshot(store, &plan, parent, "n1", &key)
            .await
            .unwrap();
        (written, plan)
    }

    #[tokio::test]
    async fn an_unchanged_second_base_uploads_no_new_chunks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let image = core(noise(20_000));
        let (first, plan) = write(&store, &image, None).await;
        assert_eq!(first.meta.kind, SnapshotKind::Base);
        assert_eq!(first.meta.parent_id, None);
        assert!(first.uploads.uploaded > 10, "{:?}", first.uploads);
        let stored = list_chunk_ids(&store).await.unwrap().len();

        let (second, _) = write(&store, &image, Some(first.meta.snapshot_id)).await;
        assert_eq!(second.uploads.uploaded, 0, "{:?}", second.uploads);
        assert_eq!(second.uploads.uploaded_bytes, 0);
        assert_eq!(second.meta.kind, SnapshotKind::Delta);
        assert_eq!(second.meta.parent_id, Some(first.meta.snapshot_id));
        assert_eq!(list_chunk_ids(&store).await.unwrap().len(), stored);

        // The second manifest lists every chunk itself: it restores alone.
        let manifest = load_manifest(&store, &second.prefix, &key()).await.unwrap();
        assert_eq!(
            manifest.chunk_ids().count(),
            plan.core_chunks[0].refs.len() + 1
        );
        let loaded = load_core_snapshot(&store, &second.prefix, &manifest, 0, &key())
            .await
            .unwrap();
        assert_eq!(loaded, image);
    }

    #[tokio::test]
    async fn a_small_change_uploads_only_the_changed_chunks() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut bytes = noise(40_000);
        let (first, plan) = write(&store, &core(bytes.clone()), None).await;
        let total = plan.core_chunks[0].refs.len() as u64;
        assert!(total > 30, "{total} chunks");

        bytes[20_000] ^= 0xFF;
        let changed = core(bytes);
        let (second, _) = write(&store, &changed, Some(first.meta.snapshot_id)).await;
        let uploads = second.uploads;
        assert!(
            (1..=3).contains(&uploads.uploaded),
            "one flipped byte re-uploads its chunk and at most its neighbours: {uploads:?}"
        );
        assert!(uploads.uploaded_bytes <= 3 * SMALL.max as u64);
        assert!(uploads.reused + 3 >= total, "{uploads:?} of {total}");

        let manifest = load_manifest(&store, &second.prefix, &key()).await.unwrap();
        let loaded = load_core_snapshot(&store, &second.prefix, &manifest, 0, &key())
            .await
            .unwrap();
        assert_eq!(loaded, changed);
    }

    #[tokio::test]
    async fn a_base_reusing_no_stored_chunk_has_no_parent() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Repeated content inside one base is stored once, and is no reuse.
        // A run of one byte value holds no cut candidate, so it is cut at
        // `SMALL.max`. The first chunk also holds the image header. 5 000
        // bytes leave at least two whole chunks of the run, which are equal.
        let (alone, _) = write(&store, &core(vec![7; 5_000]), Some(99)).await;
        assert_eq!(alone.uploads.reused, 0, "{:?}", alone.uploads);
        assert_eq!(alone.meta.kind, SnapshotKind::Base);
        assert_eq!(alone.meta.parent_id, None);
        let manifest = load_manifest(&store, &alone.prefix, &key()).await.unwrap();
        assert!(manifest.chunk_ids().count() > list_chunk_ids(&store).await.unwrap().len());
    }
}
