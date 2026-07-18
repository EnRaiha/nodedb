// SPDX-License-Identifier: BUSL-1.1

//! Vector index checkpoint methods for [`CoreLoop`].
//!
//! Contains HNSW build completion polling and checkpoint load/save operations.

use nodedb_types::DatabaseId;

use super::checkpoint_outcome::CheckpointOutcome;
use super::core_loop::CoreLoop;

/// Canonical path for a core's vector checkpoint directory.
///
/// Used by the write path (`checkpoint_vector_indexes`), the load path
/// (`load_vector_checkpoints`), and the restore path
/// (`restore_vector_checkpoints`) so all three stay in sync if the scheme
/// changes. The previous bug was that `data_dir` is shared across all TPC
/// cores, so a flat directory caused every core to load every collection's
/// index. A per-core subdir means the loader needs no ownership filter.
pub(crate) fn vector_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("vector-ckpt").join(format!("core-{core_id}"))
}

/// Parse a `"{db}:{tid}:{coll_key}"` string (the current `BuildComplete.key`
/// and on-disk checkpoint filename form, produced by
/// `vector_checkpoint_filename`) back into the `(DatabaseId, TenantId, String)`
/// tuple map key.
///
/// Returns `None` when the string is not in the new format — i.e. it does not
/// have at least three `:`-separated components whose first two parse as `u64`
/// (db, tid). `coll_key` is the verbatim remainder and may itself contain `:`
/// (e.g. `collection:field`). Callers handle `None` by skipping.
fn parse_build_key(s: &str) -> Option<(DatabaseId, crate::types::TenantId, String)> {
    let mut it = s.splitn(3, ':');
    let db_str = it.next()?;
    let tid_str = it.next()?;
    let coll_key = it.next()?;
    let db = db_str.parse::<u64>().ok()?;
    let tid = tid_str.parse::<u64>().ok()?;
    Some((
        DatabaseId::new(db),
        crate::types::TenantId::new(tid),
        coll_key.to_string(),
    ))
}

impl CoreLoop {
    /// Drain completed HNSW builds from the background builder thread and
    /// promote the corresponding building segments to sealed segments.
    ///
    /// Called at the top of `tick()` before draining new requests.
    ///
    /// `BuildComplete.key` is the `"{db}:{tid}:{coll}"` string produced by
    /// `VectorCollection::seal` (fed the `vector_checkpoint_filename` of the
    /// index key). Parse it back to the tuple key to look up the map.
    pub fn poll_build_completions(&mut self) {
        let Some(rx) = &self.build_rx else { return };
        while let Ok(complete) = rx.try_recv() {
            // Parse the string key `"{db}:{tid}:{coll_key}"` back into the tuple.
            let Some(tuple_key) = parse_build_key(&complete.key) else {
                tracing::warn!(
                    core = self.core_id,
                    key = %complete.key,
                    "HNSW build completion has unparseable key; dropping"
                );
                continue;
            };
            if let Some(coll) = self.vector_collections.get_mut(&tuple_key) {
                coll.complete_build(complete.segment_id, complete.index);
                tracing::info!(
                    core = self.core_id,
                    key = %complete.key,
                    segment_id = complete.segment_id,
                    "HNSW build completed, segment promoted to sealed"
                );
            }
        }
    }

    /// Flush every vector index to disk and report the LSN they are now durable
    /// through, plus the number of checkpoint files published.
    ///
    /// Each index is serialized to `{data_dir}/vector-ckpt/core-{id}/{key}.ckpt`.
    /// After checkpointing, WAL replay only needs to process entries since the
    /// checkpoint — not the entire history.
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// The HNSW is not fully reconstructible without the WAL.
    /// `rebuild_vector_indexes_from_store` re-indexes the redb `sparse`
    /// documents of every collection carrying a `CREATE VECTOR INDEX`, but a
    /// vector does not have to arrive as a document: `VectorOp::Insert` writes a
    /// bare `(vector, surrogate, pk_bytes)` straight into `vector_collections`,
    /// and nothing on that path puts a row in `sparse` for the rebuild to find.
    /// Those vectors exist in exactly two places — this checkpoint and the
    /// `VectorOp::Insert` WAL records — so a flush that fails while still
    /// letting the core report its watermark deletes the only surviving copy.
    ///
    /// The failure is therefore all-or-nothing by construction: any index that
    /// cannot be published returns `Err`, and the caller clamps the reported
    /// checkpoint LSN to the last LSN vectors were known durable through. A
    /// partial success cannot be expressed, because the LSN it would justify
    /// does not exist.
    ///
    /// Stamping with the core watermark mirrors `checkpoint_kv_engines`: this
    /// runs on the core's own thread between tasks, and a vector write raises
    /// the watermark only after the collection has already been mutated, so
    /// every write with `lsn <= watermark` is in the bytes written below.
    pub(crate) fn checkpoint_vector_indexes(&self) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.watermark;
        if self.vector_collections.is_empty() {
            return Ok(CheckpointOutcome {
                durable_lsn,
                files_written: 0,
            });
        }

        let ckpt_dir = vector_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        let mut files_written = 0;
        for (key, collection) in &self.vector_collections {
            // An empty collection has no state to make durable, so it writes no
            // file and cannot be the reason an LSN is overstated.
            if collection.is_empty() {
                continue;
            }
            // Checkpoint filename is `"{db}:{tid}:{coll}"`.
            let filename = CoreLoop::vector_checkpoint_filename(key);
            let bytes = collection
                .checkpoint_to_bytes(self.segment_keks.vector_checkpoint_kek.as_ref())
                .map_err(|e| crate::Error::Serialization {
                    format: "msgpack".to_string(),
                    detail: format!(
                        "vector checkpoint encode failed for {filename} ({} vectors): {e}",
                        collection.len()
                    ),
                })?;
            let ckpt_path = ckpt_dir.join(format!("{filename}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{filename}.ckpt.tmp"));
            nodedb_wal::segment::write_checkpoint_framed(&tmp_path, &ckpt_path, &bytes)
                .map_err(|e| storage_err(&ckpt_path, "publish checkpoint", &e))?;
            files_written += 1;
        }

        if files_written > 0 {
            tracing::info!(
                core = self.core_id,
                files_written,
                total = self.vector_collections.len(),
                durable_through_lsn = durable_lsn.as_u64(),
                "vector collections checkpointed"
            );
        }
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }

    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/vector-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own indexes.
    ///
    /// For each checkpoint file, loads the index. WAL replay then only
    /// needs to process entries after the checkpoint LSN.
    ///
    /// # Fail-stop on corruption
    ///
    /// A vector checkpoint is the only non-WAL home of `VectorOp::Insert`
    /// vectors once the WAL below its LSN has been truncated, so a checkpoint
    /// that exists but cannot be read or decoded is unrecoverable data loss.
    /// This returns `Err` in that case instead of skipping the file, and the
    /// boot sequence (`load_boot_checkpoints`) refuses to bring the core up. An
    /// absent checkpoint directory is not an error — it just means nothing has
    /// been checkpointed yet, and WAL replay reconstructs everything.
    pub fn load_vector_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = vector_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }

        // The directory exists but cannot be enumerated: an I/O fault that could
        // hide checkpoint files. Fail-stop rather than silently loading none.
        let entries = std::fs::read_dir(&ckpt_dir)
            .map_err(|e| storage_err(&ckpt_dir, "read checkpoint dir", &e))?;

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames are `"{db}:{tid}:{coll}.ckpt"`.
            let filename = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if filename.is_empty() {
                continue;
            }
            let Some(tuple_key) = parse_build_key(&filename) else {
                tracing::warn!(
                    core = self.core_id,
                    key = %filename,
                    "unparseable vector checkpoint filename; skipping (WAL replay rebuilds)"
                );
                continue;
            };

            // A framing/CRC fault (`WalError::CheckpointCorrupt`) or a decode
            // fault (`VectorError`) below is fail-stop, not a skip: the file is
            // present, so its bytes are the only surviving copy of these vectors
            // once the WAL below the checkpoint LSN has been truncated.
            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path)?;
            let kek = self.segment_keks.vector_checkpoint_kek.as_ref();
            let collection =
                crate::engine::vector::collection::VectorCollection::from_checkpoint(&bytes, kek)?;
            tracing::info!(
                core = self.core_id,
                key = %filename,
                vectors = collection.len(),
                "loaded vector checkpoint"
            );
            self.vector_collections.insert(tuple_key, collection);
            loaded += 1;
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "vector checkpoints loaded");
        }
        Ok(())
    }
}

/// Wrap a filesystem failure as the vector engine's typed storage error.
fn storage_err(path: &std::path::Path, action: &str, e: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "vector".to_string(),
        detail: format!(
            "vector checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `vector_ckpt_dir` must return distinct paths for different `core_id`s
    /// sharing the same `data_dir`, and the paths must embed the core id.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = vector_ckpt_dir(base, 0);
        let d1 = vector_ckpt_dir(base, 1);
        assert_ne!(d0, d1, "different cores must get different checkpoint dirs");
        assert!(
            d0.to_str().unwrap().contains("core-0"),
            "core-0 dir must contain 'core-0'"
        );
        assert!(
            d1.to_str().unwrap().contains("core-1"),
            "core-1 dir must contain 'core-1'"
        );
    }

    /// A checkpoint file written under core-0's subdir must NOT be visible when
    /// scanning core-1's subdir, proving per-core isolation. This is the critical
    /// property: `load_vector_checkpoints` on core-1 would find an empty dir and
    /// load nothing, even though core-0 has checkpointed a collection.
    #[test]
    fn checkpoint_written_for_core0_is_invisible_to_core1_scan() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let data_dir = tmp.path();

        // Write a dummy .ckpt file into core-0's subdir.
        let core0_dir = vector_ckpt_dir(data_dir, 0);
        std::fs::create_dir_all(&core0_dir).unwrap();
        std::fs::write(core0_dir.join("1:2:mycoll.ckpt"), b"dummy").unwrap();

        // Scanning core-1's subdir should yield no .ckpt entries.
        let core1_dir = vector_ckpt_dir(data_dir, 1);
        // The directory does not even exist — loader returns early.
        assert!(
            !core1_dir.exists(),
            "core-1 dir must not exist when only core-0 has checkpointed"
        );

        // Round-trip within core-0's own dir: the file we wrote is visible.
        let entries: Vec<_> = std::fs::read_dir(&core0_dir)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().and_then(|x| x.to_str()) == Some("ckpt"))
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "core-0's own scan must find exactly the one .ckpt it wrote"
        );
    }
}
