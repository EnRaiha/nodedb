// SPDX-License-Identifier: BUSL-1.1

//! Spatial R-tree checkpoint methods for [`CoreLoop`].
//!
//! Saves and restores R-tree indexes and the doc_map to disk.
//! Follows the same pattern as `vector_checkpoint.rs`.
//!
//! ## On-disk filename encoding
//!
//! Each index is checkpointed to a file whose stem encodes its logical key:
//! `{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`. `db`/`tid` are numeric and
//! pass through unchanged; `coll`/`field` are percent-encoded by
//! [`enc_component`] so the structural `_` separator can never collide with a
//! literal underscore in a collection or field name. This makes the encoding
//! round-trippable for arbitrary names. [`spatial_checkpoint_prefix`] is the
//! single shared builder used by both the write path and reclaim, so the two
//! can never drift.

use nodedb_types::DatabaseId;

use super::checkpoint_encoding::{dec_component, enc_component};
use super::checkpoint_outcome::CheckpointOutcome;
use super::core_loop::CoreLoop;
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::types::TenantId;

/// Canonical path for a core's spatial checkpoint directory.
///
/// Used by the write path (`checkpoint_spatial_indexes`) and the load path
/// (`load_spatial_checkpoints`) so both stay in sync. A per-core subdir means
/// the loader needs no core-ownership filter; docmap companion files reside in
/// the same dir and are therefore covered automatically.
pub(crate) fn spatial_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("spatial-ckpt")
        .join(format!("core-{core_id}"))
}

impl CoreLoop {
    /// Flush every in-memory R-tree to disk and report the LSN they are now
    /// durable through, plus the number of checkpoint files published.
    ///
    /// Each index is serialized via `nodedb_spatial::persist` to a file at
    /// `{data_dir}/spatial-ckpt/core-{core_id}/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`.
    /// The doc_map is saved alongside as `.docmap`.
    ///
    /// When `spatial_checkpoint_kek` is set, checkpoint files are written
    /// encrypted (AES-256-GCM SEGV framing) and plaintext loads are refused.
    ///
    /// ## Why this returns a `Result` and an LSN
    ///
    /// `spatial_indexes` holds entries from two different write paths, and once
    /// the WAL is truncated only one of them still has a rebuild independent of
    /// this file:
    ///
    /// - Geometry on a COLUMNAR-family collection (`engine='spatial'`) is
    ///   re-derived at boot by `restore_columnar_geometry_indexes` from the rows
    ///   the columnar checkpoint restored, so it survives the loss of this file.
    /// - Geometry on a DOCUMENT collection is indexed by `apply_point_put_spatial`,
    ///   the same side-effect on both the live write and the WAL redo path, so it
    ///   is rebuilt at boot from every document `Put` still in the WAL. But
    ///   nothing re-derives it from the redb `sparse` store where the document
    ///   itself lives on. So once the WAL is truncated below a row's `Put`, this
    ///   checkpoint is the R-tree's only surviving copy of that row's geometry
    ///   entry — for the document half this checkpoint and the un-truncated `Put`
    ///   records are the only two copies.
    ///
    /// Rather than rank those two halves against each other at truncation time,
    /// the flush reports honestly for both: any index or doc_map that cannot be
    /// published returns `Err`, and the caller clamps the reported checkpoint LSN
    /// to the last LSN the R-trees were known durable through. Over-reporting
    /// would drop geometry entries while the rows they point at survive — a
    /// spatial predicate silently stops matching rows a full scan still returns.
    ///
    /// Stamping with the core watermark mirrors `checkpoint_kv_engines`: this
    /// runs on the core's own thread between tasks, and a geometry write raises
    /// the watermark only after the R-tree has already been mutated.
    pub(crate) fn checkpoint_spatial_indexes(&self) -> crate::Result<CheckpointOutcome> {
        let durable_lsn = self.watermark;
        if self.spatial_indexes.is_empty() {
            return Ok(CheckpointOutcome {
                durable_lsn,
                files_written: 0,
            });
        }

        let ckpt_dir = spatial_ckpt_dir(&self.data_dir, self.core_id);
        std::fs::create_dir_all(&ckpt_dir).map_err(|e| storage_err(&ckpt_dir, "create dir", &e))?;

        let kek = self.segment_keks.spatial_checkpoint_kek.as_ref();

        let mut files_written = 0;
        for ((db, tid, coll, field), rtree) in &self.spatial_indexes {
            let stem = checkpoint_stem(*db, *tid, coll, field);
            let bytes = rtree
                .checkpoint_to_bytes(kek)
                .map_err(|e| storage_err(&ckpt_dir.join(&stem), "encode R-tree", &e))?;
            // An empty R-tree holds nothing to make durable, so it writes
            // neither checkpoint nor doc_map and cannot overstate the LSN.
            if bytes.is_empty() {
                continue;
            }

            // Write R-tree checkpoint.
            let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
            nodedb_wal::segment::write_checkpoint_framed(&tmp_path, &ckpt_path, &bytes)
                .map_err(|e| storage_err(&ckpt_path, "publish checkpoint", &e))?;
            files_written += 1;

            // Write doc_map entries for this index. It is not optional company
            // for the R-tree: `load_spatial_checkpoints` needs it to map an
            // entry id back to a document id, so an R-tree published without
            // its doc_map restores as entries that resolve to nothing.
            let doc_entries: Vec<(u64, String)> = self
                .spatial_doc_map
                .iter()
                .filter(|((d, t, c, f, _), _)| d == db && t == tid && c == coll && f == field)
                .map(|((_, _, _, _, entry_id), doc_id)| (*entry_id, doc_id.clone()))
                .collect();
            if !doc_entries.is_empty() {
                let map_bytes = zerompk::to_msgpack_vec(&doc_entries).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".to_string(),
                        detail: format!("spatial doc_map encode failed for {stem}: {e}"),
                    }
                })?;
                let map_path = ckpt_dir.join(format!("{stem}.docmap"));
                let map_tmp = ckpt_dir.join(format!("{stem}.docmap.tmp"));
                nodedb_wal::segment::write_checkpoint_framed(&map_tmp, &map_path, &map_bytes)
                    .map_err(|e| storage_err(&map_path, "publish doc_map", &e))?;
                files_written += 1;
            }
        }

        if files_written > 0 {
            tracing::info!(
                core = self.core_id,
                files_written,
                total = self.spatial_indexes.len(),
                durable_through_lsn = durable_lsn.as_u64(),
                "spatial indexes checkpointed"
            );
        }
        Ok(CheckpointOutcome {
            durable_lsn,
            files_written,
        })
    }

    /// Load R-tree checkpoints from disk on startup.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/spatial-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed. Docmap companion files (`.docmap`) reside in
    /// the same per-core dir and are covered automatically.
    ///
    /// When `spatial_checkpoint_kek` is set, plaintext checkpoint files are
    /// rejected and encrypted files are decrypted before loading.
    ///
    /// A corrupt or unreadable checkpoint (bad framing, bad CRC, an
    /// unparseable filename, a rejected R-tree decode, or a missing/corrupt
    /// docmap companion) is fail-stop: its `Err` propagates out of boot so
    /// the core refuses to come up, rather than silently loading a partial
    /// index once the WAL below the checkpoint's LSN is already truncated.
    /// An absent checkpoint directory is not corruption and stays `Ok(())`.
    pub fn load_spatial_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = spatial_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }

        // The directory exists but cannot be enumerated: an I/O fault that
        // could hide checkpoint files. Fail-stop rather than silently
        // loading none.
        let entries = std::fs::read_dir(&ckpt_dir)
            .map_err(|e| storage_err(&ckpt_dir, "read checkpoint dir", &e))?;

        let mut loaded = 0;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }

            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if stem.is_empty() {
                continue;
            }

            // Parse the filename stem into a logical key. This directory is
            // engine-private (only the write path in this file creates
            // `.ckpt` files here), so a `.ckpt` whose stem does not parse is
            // a corrupted real checkpoint, not a foreign file to skip.
            let map_key = match parse_spatial_key(&stem) {
                Some(k) => k,
                None => {
                    return Err(crate::Error::SegmentCorrupted {
                        detail: format!("unparseable spatial checkpoint key: {stem}"),
                    });
                }
            };

            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path)?;

            let kek = self.segment_keks.spatial_checkpoint_kek.as_ref();
            let rtree = crate::engine::spatial::RTree::from_checkpoint(&bytes, kek)?;

            tracing::info!(
                core = self.core_id,
                %stem,
                entries = rtree.len(),
                "loaded spatial checkpoint"
            );
            let (db, tid, coll, field) = map_key.clone();
            self.spatial_indexes.insert(map_key, rtree);
            loaded += 1;

            // Load doc_map (keyed off the same logical key). The doc_map is
            // not optional company for the R-tree: `checkpoint_spatial_indexes`
            // always writes it alongside a non-empty R-tree checkpoint, so a
            // missing or undecodable docmap here means the R-tree's entries
            // would resolve to nothing — an inconsistent, corrupt checkpoint
            // generation, not a legitimate absence.
            let map_path = ckpt_dir.join(format!("{stem}.docmap"));
            let map_bytes = nodedb_wal::segment::read_checkpoint_framed(&map_path)?;
            let doc_entries: Vec<(u64, String)> =
                zerompk::from_msgpack(&map_bytes).map_err(|source| {
                    CheckpointDecodeError::MsgpackDecode {
                        path: map_path,
                        source,
                    }
                })?;
            for (entry_id, doc_id) in doc_entries {
                self.spatial_doc_map
                    .insert((db, tid, coll.clone(), field.clone(), entry_id), doc_id);
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "spatial checkpoints loaded");
        }
        Ok(())
    }
}

/// Wrap a filesystem or encode failure as the spatial engine's typed storage
/// error.
fn storage_err(path: &std::path::Path, action: &str, e: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "spatial".to_string(),
        detail: format!(
            "spatial checkpoint: failed to {action} at {}: {e}",
            path.display()
        ),
    }
}

/// Build the full filename stem for a spatial checkpoint:
/// `{db}_{tid}_{enc(coll)}_{enc(field)}`.
fn checkpoint_stem(db: DatabaseId, tid: TenantId, coll: &str, field: &str) -> String {
    format!(
        "{}_{}_{}_{}",
        db.as_u64(),
        tid.as_u64(),
        enc_component(coll),
        enc_component(field)
    )
}

/// Shared prefix builder for reclaim: every checkpoint file for
/// `(db, tid, coll)` begins with `{db}_{tid}_{enc(coll)}_`. This is the single
/// authority on the filename encoding so reclaim can never drift from the
/// write path.
pub(crate) fn spatial_checkpoint_prefix(db: u64, tid: u64, coll: &str) -> String {
    format!("{}_{}_{}_", db, tid, enc_component(coll))
}

/// Parse a new-format stem `{db}_{tid}_{enc(coll)}_{enc(field)}` into a key.
/// Requires EXACTLY 4 underscore-separated parts with numeric db + tid.
/// Returns `None` on any structural or numeric parse failure.
fn parse_spatial_key(stem: &str) -> Option<(DatabaseId, TenantId, String, String)> {
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() != 4 {
        return None;
    }
    let db: u64 = parts[0].parse().ok()?;
    let tid: u64 = parts[1].parse().ok()?;
    let coll = dec_component(parts[2]);
    let field = dec_component(parts[3]);
    Some((DatabaseId::new(db), TenantId::new(tid), coll, field))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enc_dec_roundtrips_special_chars() {
        for raw in ["geom", "my_coll", "a%b", "x/y", "weird_:_name", "a_b_c"] {
            assert_eq!(dec_component(&enc_component(raw)), raw);
        }
    }

    #[test]
    fn stem_roundtrips_through_parse() {
        let db = DatabaseId::new(7);
        let tid = TenantId::new(42);
        let stem = checkpoint_stem(db, tid, "my_places", "geo_field");
        // No structural ambiguity: components are encoded.
        let parsed = parse_spatial_key(&stem).expect("new-format parse");
        assert_eq!(parsed.0, db);
        assert_eq!(parsed.1, tid);
        assert_eq!(parsed.2, "my_places");
        assert_eq!(parsed.3, "geo_field");
    }

    #[test]
    fn prefix_matches_stem_for_same_collection() {
        let stem = checkpoint_stem(DatabaseId::new(3), TenantId::new(9), "p_l", "f");
        let prefix = spatial_checkpoint_prefix(3, 9, "p_l");
        assert!(
            stem.starts_with(&prefix),
            "stem {stem} must start with prefix {prefix}"
        );
    }

    #[test]
    fn non_numeric_stem_is_none() {
        assert!(parse_spatial_key("a_b_c_d").is_none());
    }

    /// A core rooted at `dir`, so a corrupt checkpoint can be planted on disk
    /// and then read back through the real boot-time load path.
    fn open_core_at(dir: &std::path::Path) -> CoreLoop {
        use std::sync::Arc;

        use nodedb_bridge::buffer::RingBuffer;
        use nodedb_types::OrdinalClock;

        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let hlc = Arc::new(OrdinalClock::new());
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, _resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        drop(req_tx); // no requests are dispatched in this test
        CoreLoop::open(0, req_rx, resp_tx, dir, hlc).expect("CoreLoop::open")
    }

    /// An absent checkpoint directory is not corruption — a fresh data
    /// directory (or one that has never checkpointed spatial indexes) must
    /// load cleanly with nothing restored.
    #[test]
    fn absent_dir_is_ok() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut core = open_core_at(dir.path());
        core.load_spatial_checkpoints()
            .expect("an absent checkpoint dir must not be treated as corruption");
        assert!(core.spatial_indexes.is_empty());
    }

    /// A `.ckpt` file that exists but is not valid checkpoint framing (bad
    /// magic / truncated / bad CRC) must fail the load, not be treated as
    /// absent: for a document-collection index this file is the only
    /// surviving copy of that row's geometry entry once the WAL below its LSN
    /// is truncated. Silently skipping it would be permanent, unannounced
    /// loss of spatial predicate coverage over rows a full scan still
    /// returns.
    #[test]
    fn corrupt_ckpt_frame_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        std::fs::write(
            ckpt_dir.join(format!("{stem}.ckpt")),
            b"not a valid checkpoint frame",
        )
        .expect("write garbage checkpoint");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a corrupt R-tree checkpoint frame must fail the load, not skip it");
    }

    /// A `.ckpt` filename whose stem does not parse into `(db, tid, coll,
    /// field)` is a corrupted real checkpoint, not a foreign file to ignore —
    /// this directory is engine-private and only ever holds files this
    /// module wrote.
    #[test]
    fn unparseable_stem_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");
        std::fs::write(ckpt_dir.join("not_a_valid_stem.ckpt"), b"irrelevant")
            .expect("write file with bad stem");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("an unparseable checkpoint filename must fail the load, not skip it");
    }

    /// A valid, non-empty R-tree checkpoint whose companion `.docmap` file is
    /// entirely missing is an inconsistent generation: `checkpoint_spatial_indexes`
    /// always writes the docmap alongside a non-empty R-tree checkpoint, so a
    /// missing docmap here means the R-tree's entries would resolve to no
    /// document at all. That must fail the load rather than restore a
    /// half-usable index.
    #[test]
    fn missing_docmap_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");

        let rtree =
            crate::engine::spatial::RTree::bulk_load(vec![crate::engine::spatial::RTreeEntry {
                id: 1,
                bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
            }]);
        let bytes = rtree.checkpoint_to_bytes(None).expect("encode R-tree");
        assert!(
            !bytes.is_empty(),
            "a non-empty R-tree encodes non-empty bytes"
        );

        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
        let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
        nodedb_wal::segment::write_checkpoint_framed(&tmp_path, &ckpt_path, &bytes)
            .expect("publish checkpoint");
        // Deliberately do NOT write the `.docmap` companion.
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("a missing docmap companion must fail the load, not skip it");
    }

    /// A `.docmap` file that exists but does not decode as MessagePack must
    /// fail the load for the same reason a missing one does: the R-tree
    /// checkpoint's entries cannot be resolved to document ids.
    #[test]
    fn corrupt_docmap_fails_the_load() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");

        let rtree =
            crate::engine::spatial::RTree::bulk_load(vec![crate::engine::spatial::RTreeEntry {
                id: 1,
                bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
            }]);
        let bytes = rtree.checkpoint_to_bytes(None).expect("encode R-tree");

        let stem = checkpoint_stem(DatabaseId::new(0), TenantId::new(7), "pts", "geom");
        let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
        let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
        nodedb_wal::segment::write_checkpoint_framed(&tmp_path, &ckpt_path, &bytes)
            .expect("publish checkpoint");

        // Frame is valid, but the payload is not MessagePack.
        let map_path = ckpt_dir.join(format!("{stem}.docmap"));
        let map_tmp = ckpt_dir.join(format!("{stem}.docmap.tmp"));
        nodedb_wal::segment::write_checkpoint_framed(&map_tmp, &map_path, b"not msgpack")
            .expect("publish garbage docmap");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect_err("an undecodable docmap must fail the load, not skip it");
    }

    /// The happy path must still succeed end to end: a valid R-tree
    /// checkpoint paired with a valid docmap restores both the index and the
    /// entry-id-to-document-id mapping.
    #[test]
    fn valid_checkpoint_and_docmap_round_trip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let core = open_core_at(dir.path());
        let ckpt_dir = spatial_ckpt_dir(&core.data_dir, core.core_id);
        std::fs::create_dir_all(&ckpt_dir).expect("create ckpt dir");

        let rtree =
            crate::engine::spatial::RTree::bulk_load(vec![crate::engine::spatial::RTreeEntry {
                id: 1,
                bbox: nodedb_types::BoundingBox::new(0.0, 0.0, 1.0, 1.0),
            }]);
        let bytes = rtree.checkpoint_to_bytes(None).expect("encode R-tree");

        let db = DatabaseId::new(0);
        let tid = TenantId::new(7);
        let stem = checkpoint_stem(db, tid, "pts", "geom");
        let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
        let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
        nodedb_wal::segment::write_checkpoint_framed(&tmp_path, &ckpt_path, &bytes)
            .expect("publish checkpoint");

        let doc_entries: Vec<(u64, String)> = vec![(1, "doc-1".to_string())];
        let map_bytes = zerompk::to_msgpack_vec(&doc_entries).expect("encode docmap");
        let map_path = ckpt_dir.join(format!("{stem}.docmap"));
        let map_tmp = ckpt_dir.join(format!("{stem}.docmap.tmp"));
        nodedb_wal::segment::write_checkpoint_framed(&map_tmp, &map_path, &map_bytes)
            .expect("publish docmap");
        drop(core);

        let mut restored = open_core_at(dir.path());
        restored
            .load_spatial_checkpoints()
            .expect("a valid checkpoint and docmap must load cleanly");

        let key = (db, tid, "pts".to_string(), "geom".to_string());
        let loaded_rtree = restored
            .spatial_indexes
            .get(&key)
            .expect("R-tree must be restored under its logical key");
        assert_eq!(loaded_rtree.len(), 1);

        let doc_id = restored
            .spatial_doc_map
            .get(&(db, tid, "pts".to_string(), "geom".to_string(), 1))
            .expect("doc_map entry must be restored");
        assert_eq!(doc_id, "doc-1");
    }
}
