// SPDX-License-Identifier: BUSL-1.1

//! Vector index checkpoint methods for [`CoreLoop`].
//!
//! Contains HNSW build completion polling and checkpoint load/save operations.

use nodedb_types::DatabaseId;

use super::core_loop::CoreLoop;

/// Parse a `"{db}:{tid}:{coll_key}"` string (the current `BuildComplete.key`
/// and on-disk checkpoint filename form, produced by
/// `vector_checkpoint_filename`) back into the `(DatabaseId, TenantId, String)`
/// tuple map key.
///
/// Returns `None` when the string is not in the new format — i.e. it does not
/// have at least three `:`-separated components whose first two parse as `u64`
/// (db, tid). `coll_key` is the verbatim remainder and may itself contain `:`
/// (e.g. `collection:field`). Callers handle `None` by attempting a legacy
/// parse and/or skipping.
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

/// Parse a legacy `"{tid}:{coll_key}"` checkpoint filename (pre-database
/// scoping). Returns `None` unless the first component parses as a `u64`
/// tenant id and a collection key remainder is present.
fn parse_legacy_build_key(s: &str) -> Option<(crate::types::TenantId, String)> {
    let (tid_str, coll_key) = s.split_once(':')?;
    let tid = tid_str.parse::<u64>().ok()?;
    Some((crate::types::TenantId::new(tid), coll_key.to_string()))
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

    /// Write HNSW checkpoints for all vector indexes to disk.
    ///
    /// Called periodically from the TPC event loop (e.g., every 5 minutes
    /// or when idle). Each index is serialized to a file at
    /// `{data_dir}/vector-ckpt/{index_key}.ckpt`.
    ///
    /// After checkpointing, WAL replay only needs to process entries
    /// since the checkpoint — not the entire history.
    pub fn checkpoint_vector_indexes(&self) -> usize {
        if self.vector_collections.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for (key, collection) in &self.vector_collections {
            if collection.is_empty() {
                continue;
            }
            let bytes = collection.checkpoint_to_bytes(self.vector_checkpoint_kek.as_ref());
            if bytes.is_empty() {
                continue;
            }
            // Checkpoint filename is `"{db}:{tid}:{coll}"`; legacy
            // `"{tid}:{coll}"` files are migrated on load.
            let filename = CoreLoop::vector_checkpoint_filename(key);
            let ckpt_path = ckpt_dir.join(format!("{filename}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{filename}.ckpt.tmp"));
            if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &bytes).is_ok() {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.vector_collections.len(),
                "vector collections checkpointed"
            );
        }
        checkpointed
    }

    /// Load HNSW checkpoints from disk on startup, before WAL replay.
    ///
    /// For each checkpoint file, loads the index. WAL replay then only
    /// needs to process entries after the checkpoint LSN.
    pub fn load_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("vector-ckpt");
        if !ckpt_dir.exists() {
            return;
        }

        let entries = match std::fs::read_dir(&ckpt_dir) {
            Ok(e) => e,
            Err(_) => return,
        };

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            // Checkpoint filenames are `"{db}:{tid}:{coll}.ckpt"` in the current
            // format. Legacy files are `"{tid}:{coll}.ckpt"` — load those under
            // `DatabaseId::DEFAULT` and migrate the file to the new stem.
            let filename = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if filename.is_empty() {
                continue;
            }
            let tuple_key = match parse_build_key(&filename) {
                Some(k) => k,
                None => match parse_legacy_build_key(&filename) {
                    Some((tid, coll_key)) => {
                        // Legacy file: adopt under the default database and
                        // atomically rename to the new `{DEFAULT}:{tid}:{coll}`
                        // stem so subsequent loads take the fast path. A rename
                        // failure is non-fatal — the in-memory load below still
                        // succeeds; we only warn and continue.
                        let new_stem = CoreLoop::vector_checkpoint_filename(&(
                            DatabaseId::DEFAULT,
                            tid,
                            coll_key.clone(),
                        ));
                        let new_path = ckpt_dir.join(format!("{new_stem}.ckpt"));
                        if new_path != path
                            && let Err(e) = std::fs::rename(&path, &new_path)
                        {
                            tracing::warn!(
                                core = self.core_id,
                                old = %path.display(),
                                new = %new_path.display(),
                                error = %e,
                                "legacy vector checkpoint rename failed; loaded in-memory anyway"
                            );
                        }
                        (DatabaseId::DEFAULT, tid, coll_key)
                    }
                    None => {
                        tracing::warn!(
                            core = self.core_id,
                            key = %filename,
                            "unparseable vector checkpoint filename; skipping (WAL replay rebuilds)"
                        );
                        continue;
                    }
                },
            };

            // Re-derive the path: a legacy file may have just been renamed.
            let read_path = ckpt_dir.join(format!(
                "{}.ckpt",
                CoreLoop::vector_checkpoint_filename(&tuple_key)
            ));
            let read_path = if read_path.exists() { read_path } else { path };
            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&read_path) else {
                continue;
            };
            let kek = self.vector_checkpoint_kek.as_ref();
            let load_result =
                crate::engine::vector::collection::VectorCollection::from_checkpoint(&bytes, kek);
            let collection = match load_result {
                Ok(Some(c)) => c,
                Ok(None) => continue,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        key = %filename,
                        error = %e,
                        "vector checkpoint rejected"
                    );
                    continue;
                }
            };
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
    }
}
