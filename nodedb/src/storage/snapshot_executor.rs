// SPDX-License-Identifier: BUSL-1.1

//! Snapshot restore: write a physical base snapshot into an empty data
//! directory.
//!
//! Restore is offline: the server must not run over `data_dir`. It runs in
//! two passes:
//!
//! 1. Validate: fetch and check every chunk, decode every image, check every
//!    destination path, and check every cold-tier segment the manifest names
//!    exists. Nothing is written.
//! 2. Write: fetch every image again, write each file to its original path,
//!    then seed the WAL so the next LSN minted lies above every LSN the
//!    snapshot holds. On any error the directory is emptied again.
//!
//! The server then boots normally over the directory: each engine loads its
//! captured files, and redb repairs the allocator state of each captured
//! store on first open.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use tracing::info;

use crate::data::executor::snapshot::layout::check_restore_path;
use crate::data::snapshot::{CoreSnapshot, NodeSnapshot, SnapshotDir, SnapshotFile};
use crate::storage::snapshot_files::{RestoreWriter, clear_dir_contents, require_empty_dir};
use crate::storage::snapshot_writer::{
    SnapshotManifest, load_core_snapshot, load_manifest, load_node_snapshot,
};
use crate::types::Lsn;

/// The WAL directory under the data directory, as `ServerConfig::wal_dir`
/// places it.
const WAL_DIR: &str = "wal";

/// Result of a snapshot restore.
#[derive(Debug, Clone)]
pub struct RestoreResult {
    pub snapshot_id: u64,
    /// The lowest per-core replay floor: WAL above it brings every core
    /// forward.
    pub replay_floor_lsn: Lsn,
    /// The highest LSN whose effect the restored files include.
    pub applied_high_lsn: Lsn,
    pub cores_restored: usize,
    pub files_restored: u64,
    pub bytes_restored: u64,
}

/// Where a restore reads from.
pub struct RestoreSource<'a> {
    pub prefix: &'a str,
    pub snapshot_store: &'a Arc<dyn ObjectStore>,
    /// The cold-tier store. Required when the manifest names cold segments.
    pub cold_store: Option<&'a Arc<dyn ObjectStore>>,
    pub encryption_key: &'a nodedb_wal::crypto::WalEncryptionKey,
}

/// Restore the base snapshot `source` names into `data_dir`.
///
/// `data_dir` must be absent or empty. A non-empty directory is refused with
/// [`crate::Error::RestoreTargetNotEmpty`]. Every check runs before the first
/// write, and a failed write leaves `data_dir` empty.
pub async fn execute_restore(
    data_dir: &Path,
    source: &RestoreSource<'_>,
) -> crate::Result<RestoreResult> {
    require_empty_dir(data_dir)?;

    let manifest =
        load_manifest(source.snapshot_store, source.prefix, source.encryption_key).await?;
    info!(
        snapshot_id = manifest.meta.snapshot_id,
        applied_high_lsn = manifest.meta.applied_high_lsn.as_u64(),
        cores = manifest.num_cores,
        "starting snapshot restore"
    );
    let node = load_node_snapshot(source.snapshot_store, &manifest, source.encryption_key).await?;
    validate(source, &manifest, &node).await?;

    let (files_restored, bytes_restored) =
        write_or_discard(data_dir, source, &manifest, &node).await?;
    let result = RestoreResult {
        snapshot_id: manifest.meta.snapshot_id,
        replay_floor_lsn: manifest.meta.begin_lsn,
        applied_high_lsn: manifest.meta.applied_high_lsn,
        cores_restored: manifest.num_cores,
        files_restored,
        bytes_restored,
    };
    info!(
        snapshot_id = result.snapshot_id,
        applied_high_lsn = result.applied_high_lsn.as_u64(),
        files = result.files_restored,
        bytes = result.bytes_restored,
        "snapshot restore complete"
    );
    Ok(result)
}

/// Write one core's captured files into `data_dir`. The caller checks that
/// `data_dir` held nothing before the first write.
pub fn restore_core_snapshot(
    data_dir: &Path,
    core_id: usize,
    snapshot: &CoreSnapshot,
) -> crate::Result<(u64, u64)> {
    let mut writer = RestoreWriter::new(data_dir);
    write_core(&mut writer, core_id, snapshot)?;
    writer.finish()
}

/// Pass 1: everything a write can trip on, checked with nothing written.
async fn validate(
    source: &RestoreSource<'_>,
    manifest: &SnapshotManifest,
    node: &NodeSnapshot,
) -> crate::Result<()> {
    let mut paths = DestinationSet::default();
    paths.add_all(None, &node.files)?;
    let mut applied_high = 0u64;
    for core_id in 0..manifest.num_cores {
        let snapshot = load_core_snapshot(
            source.snapshot_store,
            source.prefix,
            manifest,
            core_id,
            source.encryption_key,
        )
        .await?;
        paths.add_all(Some(core_id), &snapshot.files)?;
        paths.add_all_dirs(core_id, &snapshot.dirs)?;
        applied_high = applied_high.max(snapshot.applied_high_lsn());
    }
    if applied_high != manifest.meta.applied_high_lsn.as_u64() {
        return Err(crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!(
                "snapshot {}: cores hold records through lsn {applied_high} but the \
                 manifest names lsn {}; the snapshot objects do not belong together",
                source.prefix,
                manifest.meta.applied_high_lsn.as_u64()
            ),
        });
    }
    check_cold_segments(source, manifest).await
}

/// Every cold-tier segment the snapshot relies on must still exist.
async fn check_cold_segments(
    source: &RestoreSource<'_>,
    manifest: &SnapshotManifest,
) -> crate::Result<()> {
    if manifest.cold_segments.is_empty() {
        return Ok(());
    }
    let cold = source.cold_store.ok_or_else(|| crate::Error::BadRequest {
        detail: format!(
            "snapshot {} relies on {} cold-tier segments; pass the cold store to restore it",
            source.prefix,
            manifest.cold_segments.len()
        ),
    })?;
    for key in &manifest.cold_segments {
        cold.head(&ObjectPath::from(key.as_str()))
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "snapshot {} relies on cold segment {key}, which is not readable: {e}",
                    source.prefix
                ),
            })?;
    }
    Ok(())
}

/// Pass 2, emptying `data_dir` again when any write fails.
async fn write_or_discard(
    data_dir: &Path,
    source: &RestoreSource<'_>,
    manifest: &SnapshotManifest,
    node: &NodeSnapshot,
) -> crate::Result<(u64, u64)> {
    match write_all(data_dir, source, manifest, node).await {
        Ok(counts) => Ok(counts),
        Err(error) => Err(match clear_dir_contents(data_dir) {
            Ok(()) => error,
            Err(clear) => crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "restore failed ({error}) and removing its partial files failed ({clear}); \
                     empty {} before retrying",
                    data_dir.display()
                ),
            },
        }),
    }
}

async fn write_all(
    data_dir: &Path,
    source: &RestoreSource<'_>,
    manifest: &SnapshotManifest,
    node: &NodeSnapshot,
) -> crate::Result<(u64, u64)> {
    let mut writer = RestoreWriter::new(data_dir);
    write_checked(&mut writer, None, &node.files)?;
    for core_id in 0..manifest.num_cores {
        // Fetched and checked again: pass 1 kept no core image, so memory
        // stays bounded by one image.
        let snapshot = load_core_snapshot(
            source.snapshot_store,
            source.prefix,
            manifest,
            core_id,
            source.encryption_key,
        )
        .await?;
        write_core(&mut writer, core_id, &snapshot)?;
        info!(
            core_id,
            files = snapshot.files.len(),
            applied_high_lsn = snapshot.applied_high_lsn(),
            "core state restored"
        );
    }
    seed_wal(&mut writer, manifest.meta.applied_high_lsn.as_u64())?;
    writer.finish()
}

/// Write one core's files, then create each directory it names. A directory
/// that already holds a restored file is created again as a no-op.
fn write_core(
    writer: &mut RestoreWriter<'_>,
    core_id: usize,
    snapshot: &CoreSnapshot,
) -> crate::Result<()> {
    write_checked(writer, Some(core_id), &snapshot.files)?;
    for dir in &snapshot.dirs {
        let rel = check_restore_path(dir.component, Some(core_id), &dir.path)?;
        writer.create_dir(&rel)?;
    }
    Ok(())
}

fn write_checked(
    writer: &mut RestoreWriter<'_>,
    core_id: Option<usize>,
    files: &[SnapshotFile],
) -> crate::Result<()> {
    for file in files {
        let rel = check_restore_path(file.component, core_id, &file.path)?;
        writer.write(&rel, &file.bytes)?;
    }
    Ok(())
}

/// Every destination path of a restore, checked so no two files land on the
/// same path and no file lands where another needs a directory.
#[derive(Default)]
struct DestinationSet {
    files: BTreeSet<PathBuf>,
    dirs: BTreeSet<PathBuf>,
}

impl DestinationSet {
    fn add_all(&mut self, core_id: Option<usize>, files: &[SnapshotFile]) -> crate::Result<()> {
        for file in files {
            let rel = check_restore_path(file.component, core_id, &file.path)?;
            self.add(rel)?;
        }
        Ok(())
    }

    fn add(&mut self, rel: PathBuf) -> crate::Result<()> {
        let clash = self.files.contains(&rel)
            || self.dirs.contains(&rel)
            || rel.ancestors().skip(1).any(|dir| self.files.contains(dir));
        if clash {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "snapshot file {} collides with another file of the same snapshot",
                    rel.display()
                ),
            });
        }
        for dir in rel.ancestors().skip(1) {
            if !self.dirs.insert(dir.to_path_buf()) {
                break;
            }
        }
        self.files.insert(rel);
        Ok(())
    }

    fn add_all_dirs(&mut self, core_id: usize, dirs: &[SnapshotDir]) -> crate::Result<()> {
        for dir in dirs {
            let rel = check_restore_path(dir.component, Some(core_id), &dir.path)?;
            self.add_dir(rel)?;
        }
        Ok(())
    }

    /// A directory clashes only with a file at its own path or at an
    /// ancestor's. Two entries naming one directory agree.
    fn add_dir(&mut self, rel: PathBuf) -> crate::Result<()> {
        if rel.ancestors().any(|dir| self.files.contains(dir)) {
            return Err(crate::Error::Storage {
                engine: "snapshot".into(),
                detail: format!(
                    "snapshot directory {} collides with a file of the same snapshot",
                    rel.display()
                ),
            });
        }
        for dir in rel.ancestors() {
            if !self.dirs.insert(dir.to_path_buf()) {
                break;
            }
        }
        Ok(())
    }
}

/// Create an empty WAL segment whose first LSN is `applied_high + 1`.
///
/// The WAL resumes numbering at the first LSN of its last segment, so every
/// LSN minted after the restore lies above every LSN the captured files hold.
/// Restart replay skips a lower LSN as already applied.
fn seed_wal(writer: &mut RestoreWriter<'_>, applied_high: u64) -> crate::Result<()> {
    writer.write(&wal_segment_rel(seed_first_lsn(applied_high)?), &[])
}

/// First LSN of the seed segment a restore of a base holding records through
/// `applied_high` writes.
pub fn seed_first_lsn(applied_high: u64) -> crate::Result<u64> {
    applied_high
        .checked_add(1)
        .ok_or_else(|| crate::Error::Storage {
            engine: "snapshot".into(),
            detail: format!("snapshot applied_high_lsn {applied_high} leaves no LSN to resume at"),
        })
}

/// Path of the WAL segment starting at `first_lsn`, relative to the data
/// directory.
pub fn wal_segment_rel(first_lsn: u64) -> PathBuf {
    Path::new(WAL_DIR).join(nodedb_wal::segment::segment_filename(first_lsn))
}

/// Remove the empty seed segment [`execute_restore`] wrote, once restored WAL
/// segments supersede it: their records run past `applied_high`, so the
/// seed restarts numbering at an LSN they already hold.
pub fn discard_wal_seed(data_dir: &Path, applied_high: u64) -> crate::Result<()> {
    let path = data_dir.join(wal_segment_rel(seed_first_lsn(applied_high)?));
    // no-objectstore: the seed segment lives in the local WAL directory.
    std::fs::remove_file(&path).map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("remove WAL seed segment {}: {e}", path.display()),
    })?;
    let wal_dir = data_dir.join(WAL_DIR);
    nodedb_wal::segment::fsync_directory(&wal_dir).map_err(|e| crate::Error::Storage {
        engine: "snapshot".into(),
        detail: format!("fsync {}: {e}", wal_dir.display()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::snapshot::SnapshotComponent;
    use crate::storage::snapshot_writer::{CdcParams, chunk_path, create_base_snapshot_with};
    use crate::types::replay_stamp::ReplayStamp;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    /// Small enough that every image here spans several chunks.
    const TINY_CHUNKS: CdcParams = CdcParams {
        min: 4,
        max: 16,
        mask_bits: 3,
    };

    fn key() -> nodedb_wal::crypto::WalEncryptionKey {
        nodedb_wal::crypto::WalEncryptionKey::from_bytes(&[0xA5; 32]).expect("test key")
    }

    fn file(component: SnapshotComponent, path: &str, bytes: &[u8]) -> SnapshotFile {
        SnapshotFile {
            component,
            path: path.into(),
            bytes: bytes.to_vec(),
        }
    }

    fn core_snapshot(core_id: usize, applied_high: u64) -> CoreSnapshot {
        CoreSnapshot {
            stamp: ReplayStamp::through(applied_high),
            files: vec![file(
                SnapshotComponent::Kv,
                &format!("kv-ckpt/core-{core_id}/MANIFEST"),
                &[core_id as u8; 40],
            )],
            dirs: Vec::new(),
        }
    }

    fn node() -> NodeSnapshot {
        NodeSnapshot {
            files: vec![
                file(SnapshotComponent::SystemCatalog, "system.redb", b"catalog"),
                file(
                    SnapshotComponent::EventPlane,
                    "event_plane/mv_state.redb",
                    b"mv",
                ),
            ],
            metadata_applied_index: 0,
            metadata_captured_index: 0,
            metadata_timeline: 0,
        }
    }

    async fn write_snapshot(
        store: &Arc<dyn ObjectStore>,
        cores: Vec<CoreSnapshot>,
        cold_segments: &[String],
    ) -> (crate::storage::snapshot::SnapshotMeta, String) {
        let core_bytes = cores
            .iter()
            .enumerate()
            .map(|(id, snap)| (id, snap.to_bytes().unwrap()))
            .collect();
        create_base_snapshot_with(
            store,
            core_bytes,
            &node(),
            cold_segments,
            "test",
            Some(&key()),
            TINY_CHUNKS,
        )
        .await
        .unwrap()
    }

    fn source<'a>(
        prefix: &'a str,
        store: &'a Arc<dyn ObjectStore>,
        cold: Option<&'a Arc<dyn ObjectStore>>,
        key: &'a nodedb_wal::crypto::WalEncryptionKey,
    ) -> RestoreSource<'a> {
        RestoreSource {
            prefix,
            snapshot_store: store,
            cold_store: cold,
            encryption_key: key,
        }
    }

    fn is_empty(dir: &Path) -> bool {
        std::fs::read_dir(dir).unwrap().next().is_none()
    }

    #[tokio::test]
    async fn restore_reassembles_chunks_and_seeds_the_wal() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (meta, prefix) = write_snapshot(
            &store,
            vec![core_snapshot(0, 40), core_snapshot(1, 55)],
            &[],
        )
        .await;
        let manifest = load_manifest(&store, &prefix, &key()).await.unwrap();
        assert!(
            manifest.core_chunks.iter().all(|chunks| chunks.len() > 1),
            "each core image spans several chunks"
        );
        let dir = tempfile::tempdir().unwrap();
        let data_dir = dir.path().join("data");

        let k = key();
        let result = execute_restore(&data_dir, &source(&prefix, &store, None, &k))
            .await
            .unwrap();
        assert_eq!(result.snapshot_id, meta.snapshot_id);
        assert_eq!(result.cores_restored, 2);
        assert_eq!(result.applied_high_lsn, Lsn::new(55));
        assert_eq!(result.replay_floor_lsn, Lsn::new(40));
        assert_eq!(
            std::fs::read(data_dir.join("system.redb")).unwrap(),
            b"catalog"
        );
        assert_eq!(
            std::fs::read(data_dir.join("event_plane/mv_state.redb")).unwrap(),
            b"mv"
        );
        assert_eq!(
            std::fs::read(data_dir.join("kv-ckpt/core-1/MANIFEST")).unwrap(),
            [1u8; 40]
        );

        let segments = nodedb_wal::segment::discover_segments(&data_dir.join("wal")).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(
            segments[0].first_lsn, 56,
            "the next LSN lies above every LSN the snapshot holds"
        );
    }

    #[tokio::test]
    async fn restore_refuses_a_non_empty_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, prefix) = write_snapshot(&store, vec![core_snapshot(0, 10)], &[]).await;
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("sparse.redb"), b"stale").unwrap();

        let k = key();
        match execute_restore(dir.path(), &source(&prefix, &store, None, &k)).await {
            Err(crate::Error::RestoreTargetNotEmpty { path }) => assert_eq!(path, dir.path()),
            other => panic!("expected RestoreTargetNotEmpty, got {other:?}"),
        }
        assert_eq!(
            std::fs::read(dir.path().join("sparse.redb")).unwrap(),
            b"stale",
            "a refused restore writes nothing"
        );
        assert!(!dir.path().join("wal").exists());
    }

    /// Core 0 and the node image are valid. Core 1 names another core's path.
    /// Nothing is written, not even the valid images.
    #[tokio::test]
    async fn a_bad_path_in_a_later_core_writes_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut foreign = core_snapshot(1, 10);
        foreign.files[0].path = "kv-ckpt/core-0/other".into();
        let (_, prefix) = write_snapshot(&store, vec![core_snapshot(0, 10), foreign], &[]).await;
        let dir = tempfile::tempdir().unwrap();

        let k = key();
        assert!(
            execute_restore(dir.path(), &source(&prefix, &store, None, &k))
                .await
                .is_err()
        );
        assert!(is_empty(dir.path()));
    }

    #[tokio::test]
    async fn a_corrupt_chunk_writes_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, prefix) = write_snapshot(
            &store,
            vec![core_snapshot(0, 10), core_snapshot(1, 12)],
            &[],
        )
        .await;
        let manifest = load_manifest(&store, &prefix, &key()).await.unwrap();
        let victim = &manifest.core_chunks[1][1].id;
        store
            .put(&chunk_path(victim), PutPayload::from(vec![0u8; 64]))
            .await
            .unwrap();
        let dir = tempfile::tempdir().unwrap();

        let k = key();
        assert!(
            execute_restore(dir.path(), &source(&prefix, &store, None, &k))
                .await
                .is_err()
        );
        assert!(is_empty(dir.path()));
    }

    /// Two chunks of one image swapped: each still authenticates as an
    /// object, but not under the id it sits at.
    #[tokio::test]
    async fn swapped_chunks_are_refused() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, prefix) = write_snapshot(&store, vec![core_snapshot(0, 10)], &[]).await;
        let manifest = load_manifest(&store, &prefix, &key()).await.unwrap();
        let chunks = &manifest.core_chunks[0];
        let other = chunks
            .iter()
            .find(|chunk| chunk.id != chunks[0].id)
            .unwrap();
        let first = chunk_path(&chunks[0].id);
        let second = chunk_path(&other.id);
        let a = store.get(&first).await.unwrap().bytes().await.unwrap();
        let b = store.get(&second).await.unwrap().bytes().await.unwrap();
        store.put(&first, PutPayload::from(b)).await.unwrap();
        store.put(&second, PutPayload::from(a)).await.unwrap();

        assert!(
            load_core_snapshot(&store, &prefix, &manifest, 0, &key())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn a_file_where_another_needs_a_directory_writes_nothing() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut clash = core_snapshot(0, 10);
        clash.files.push(file(
            SnapshotComponent::Kv,
            "kv-ckpt/core-0/MANIFEST/inner",
            b"x",
        ));
        let (_, prefix) = write_snapshot(&store, vec![clash], &[]).await;
        let dir = tempfile::tempdir().unwrap();

        let k = key();
        assert!(
            execute_restore(dir.path(), &source(&prefix, &store, None, &k))
                .await
                .is_err()
        );
        assert!(is_empty(dir.path()));
    }

    /// A write that fails part-way removes what the restore already wrote.
    /// The clash reaches the write pass directly here, past pass 1.
    #[tokio::test]
    async fn a_failed_write_empties_the_directory() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let mut clash = core_snapshot(0, 10);
        clash.files.push(file(
            SnapshotComponent::Kv,
            "kv-ckpt/core-0/MANIFEST/inner",
            b"x",
        ));
        let (_, prefix) = write_snapshot(&store, vec![clash], &[]).await;
        let k = key();
        let src = source(&prefix, &store, None, &k);
        let manifest = load_manifest(&store, &prefix, &k).await.unwrap();
        let node = load_node_snapshot(&store, &manifest, &k).await.unwrap();
        let dir = tempfile::tempdir().unwrap();

        assert!(
            write_or_discard(dir.path(), &src, &manifest, &node)
                .await
                .is_err()
        );
        assert!(
            is_empty(dir.path()),
            "the node files written first are removed"
        );
    }

    #[tokio::test]
    async fn cold_segments_must_exist_before_anything_is_written() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cold: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let segment = "cold/segments/seg-1".to_string();
        let (_, prefix) = write_snapshot(
            &store,
            vec![core_snapshot(0, 10)],
            std::slice::from_ref(&segment),
        )
        .await;
        let k = key();

        let dir = tempfile::tempdir().unwrap();
        assert!(
            execute_restore(dir.path(), &source(&prefix, &store, None, &k))
                .await
                .is_err()
        );
        assert!(
            execute_restore(dir.path(), &source(&prefix, &store, Some(&cold), &k))
                .await
                .is_err()
        );
        assert!(is_empty(dir.path()));

        cold.put(
            &ObjectPath::from(segment.as_str()),
            PutPayload::from(vec![1u8]),
        )
        .await
        .unwrap();
        execute_restore(dir.path(), &source(&prefix, &store, Some(&cold), &k))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_wal_seed_is_discarded_by_name() {
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let (_, prefix) = write_snapshot(&store, vec![core_snapshot(0, 30)], &[]).await;
        let dir = tempfile::tempdir().unwrap();
        let k = key();
        let result = execute_restore(dir.path(), &source(&prefix, &store, None, &k))
            .await
            .unwrap();
        let seed = dir.path().join(wal_segment_rel(31));
        assert!(seed.exists());

        discard_wal_seed(dir.path(), result.applied_high_lsn.as_u64()).unwrap();
        assert!(!seed.exists());
        assert!(
            discard_wal_seed(dir.path(), 30).is_err(),
            "a missing seed is an error"
        );
    }

    #[test]
    fn destinations_refuse_duplicates_and_file_directory_clashes() {
        let mut set = DestinationSet::default();
        set.add(PathBuf::from("a/b")).unwrap();
        assert!(set.add(PathBuf::from("a/b")).is_err());
        assert!(set.add(PathBuf::from("a/b/c")).is_err());
        assert!(set.add(PathBuf::from("a")).is_err());
        set.add(PathBuf::from("a/c")).unwrap();
    }
}
