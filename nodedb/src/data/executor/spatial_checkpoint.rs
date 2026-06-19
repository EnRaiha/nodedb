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
use super::core_loop::CoreLoop;
use crate::types::TenantId;

impl CoreLoop {
    /// Write R-tree checkpoints for all spatial indexes to disk.
    ///
    /// Each index is serialized via `nodedb_spatial::persist` to a file at
    /// `{data_dir}/spatial-ckpt/{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`. The
    /// doc_map is saved alongside as `.docmap`.
    ///
    /// When `spatial_checkpoint_kek` is set, checkpoint files are written
    /// encrypted (AES-256-GCM SEGV framing) and plaintext loads are refused.
    pub fn checkpoint_spatial_indexes(&self) -> usize {
        if self.spatial_indexes.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("spatial-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create spatial checkpoint dir"
            );
            return 0;
        }

        let kek = self.spatial_checkpoint_kek.as_ref();

        let mut checkpointed = 0;
        for ((db, tid, coll, field), rtree) in &self.spatial_indexes {
            let stem = checkpoint_stem(*db, *tid, coll, field);
            let bytes = match rtree.checkpoint_to_bytes(kek) {
                Ok(b) if !b.is_empty() => b,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(%stem, error = %e, "R-tree checkpoint failed");
                    continue;
                }
            };

            // Write R-tree checkpoint.
            let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
            if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &bytes).is_ok() {
                checkpointed += 1;
            }

            // Write doc_map entries for this index.
            let doc_entries: Vec<(u64, String)> = self
                .spatial_doc_map
                .iter()
                .filter(|((d, t, c, f, _), _)| d == db && t == tid && c == coll && f == field)
                .map(|((_, _, _, _, entry_id), doc_id)| (*entry_id, doc_id.clone()))
                .collect();
            if !doc_entries.is_empty() {
                let map_bytes = match zerompk::to_msgpack_vec(&doc_entries) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::warn!(%stem, error = %e, "spatial doc_map serialization failed");
                        continue;
                    }
                };
                let map_path = ckpt_dir.join(format!("{stem}.docmap"));
                let map_tmp = ckpt_dir.join(format!("{stem}.docmap.tmp"));
                let _ = nodedb_wal::segment::atomic_write_fsync(&map_tmp, &map_path, &map_bytes);
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.spatial_indexes.len(),
                "spatial indexes checkpointed"
            );
        }
        checkpointed
    }

    /// Load R-tree checkpoints from disk on startup.
    ///
    /// When `spatial_checkpoint_kek` is set, plaintext checkpoint files are
    /// rejected and encrypted files are decrypted before loading.
    ///
    /// Legacy single-underscore filenames (`{tid}_{coll}_{field}`, pre-db
    /// scoping) are loaded under [`DatabaseId::DEFAULT`] and rewritten in the
    /// new `{db}_{tid}_...` format so the migration runs exactly once.
    pub fn load_spatial_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("spatial-ckpt");
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

            let stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_string();
            if stem.is_empty() {
                continue;
            }

            // Parse the filename stem into a logical key. Try the new 4-part
            // scheme first; fall back to the legacy 3-part scheme.
            let (map_key, needs_migration) = match parse_spatial_key(&stem) {
                Some(k) => (k, false),
                None => match parse_legacy_spatial_key(&stem) {
                    Some(k) => (k, true),
                    None => {
                        tracing::warn!(
                            core = self.core_id,
                            %stem,
                            "failed to parse spatial checkpoint key (ambiguous legacy file); \
                             skipping (WAL replay rebuilds it)"
                        );
                        continue;
                    }
                },
            };

            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&path) else {
                continue;
            };

            let kek = self.spatial_checkpoint_kek.as_ref();
            let rtree = match crate::engine::spatial::RTree::from_checkpoint(&bytes, kek) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        %stem,
                        error = %e,
                        "spatial checkpoint rejected"
                    );
                    continue;
                }
            };

            tracing::info!(
                core = self.core_id,
                %stem,
                entries = rtree.len(),
                "loaded spatial checkpoint"
            );
            let (db, tid, coll, field) = map_key.clone();
            self.spatial_indexes.insert(map_key, rtree);
            loaded += 1;

            // Load doc_map (keyed off the same logical key).
            let map_path = ckpt_dir.join(format!("{stem}.docmap"));
            if let Ok(map_bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&map_path)
                && let Ok(doc_entries) = zerompk::from_msgpack::<Vec<(u64, String)>>(&map_bytes)
            {
                for (entry_id, doc_id) in doc_entries {
                    self.spatial_doc_map
                        .insert((db, tid, coll.clone(), field.clone(), entry_id), doc_id);
                }
            }

            // One-time migration: rewrite legacy filenames in the new format
            // so the next startup parses them via the 4-part path. The
            // in-memory load already succeeded, so a rename failure is logged
            // and tolerated (it retries next startup).
            if needs_migration {
                migrate_legacy_files(self.core_id, &ckpt_dir, &stem, db, tid, &coll, &field);
            }
        }

        if loaded > 0 {
            tracing::info!(core = self.core_id, loaded, "spatial checkpoints loaded");
        }
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

/// Parse a LEGACY stem `{tid}_{coll}_{field}` (pre-db scoping, the old
/// `:`→`_` sanitized scheme) into a key under [`DatabaseId::DEFAULT`].
/// Requires EXACTLY 3 parts with a numeric tid. Components are returned
/// verbatim (legacy files were not percent-encoded). Matches the common
/// no-underscore-in-name case; genuinely ambiguous names fall through to a
/// warn at the call site.
fn parse_legacy_spatial_key(stem: &str) -> Option<(DatabaseId, TenantId, String, String)> {
    let parts: Vec<&str> = stem.split('_').collect();
    if parts.len() != 3 {
        return None;
    }
    let tid: u64 = parts[0].parse().ok()?;
    Some((
        DatabaseId::DEFAULT,
        TenantId::new(tid),
        parts[1].to_string(),
        parts[2].to_string(),
    ))
}

/// Atomically rename a legacy `.ckpt`/`.docmap` pair to the new stem.
/// Failures are logged and tolerated (the in-memory load already succeeded).
fn migrate_legacy_files(
    core_id: usize,
    ckpt_dir: &std::path::Path,
    old_stem: &str,
    db: DatabaseId,
    tid: TenantId,
    coll: &str,
    field: &str,
) {
    let new_stem = checkpoint_stem(db, tid, coll, field);
    if new_stem == old_stem {
        return;
    }
    for ext in ["ckpt", "docmap"] {
        let old_path = ckpt_dir.join(format!("{old_stem}.{ext}"));
        if !old_path.exists() {
            continue;
        }
        let new_path = ckpt_dir.join(format!("{new_stem}.{ext}"));
        if let Err(e) = std::fs::rename(&old_path, &new_path) {
            tracing::warn!(
                core = core_id,
                %old_stem,
                %new_stem,
                ext,
                error = %e,
                "spatial checkpoint legacy migration rename failed; will retry next startup"
            );
        }
    }
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
    fn legacy_3part_parses_under_default_db() {
        let parsed = parse_legacy_spatial_key("5_places_geom").expect("legacy parse");
        assert_eq!(parsed.0, DatabaseId::DEFAULT);
        assert_eq!(parsed.1, TenantId::new(5));
        assert_eq!(parsed.2, "places");
        assert_eq!(parsed.3, "geom");
    }

    #[test]
    fn new_format_takes_precedence_over_legacy() {
        // A 4-part numeric-db/tid stem is parsed as new, not legacy.
        let stem = checkpoint_stem(DatabaseId::new(1), TenantId::new(2), "c", "f");
        assert!(parse_spatial_key(&stem).is_some());
    }

    #[test]
    fn non_numeric_stem_is_none() {
        assert!(parse_spatial_key("a_b_c_d").is_none());
        assert!(parse_legacy_spatial_key("a_b_c").is_none());
    }
}
