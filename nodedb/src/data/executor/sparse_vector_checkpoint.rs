// SPDX-License-Identifier: BUSL-1.1

//! Sparse vector index checkpoint methods for [`CoreLoop`].
//!
//! Follows the same pattern as `vector_checkpoint.rs`: serialize each index
//! to `{data_dir}/sparse-vector-ckpt/{stem}.ckpt` via atomic temp+rename.
//!
//! ## On-disk filename encoding
//!
//! Each index is checkpointed to a file whose stem encodes its logical key:
//! `{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`. `db`/`tid` are numeric and pass
//! through unchanged; `coll`/`field` are percent-encoded by [`enc_component`]
//! so the structural `_` separator can never collide with a literal underscore
//! in a collection or field name. This makes the encoding round-trippable for
//! arbitrary names. [`sparse_vector_checkpoint_prefix`] is the single shared
//! builder used by both the write path and reclaim, so the two can never drift.

use nodedb_types::DatabaseId;

use super::checkpoint_encoding::{dec_component, enc_component};
use super::core_loop::CoreLoop;
use crate::types::TenantId;

impl CoreLoop {
    /// Write sparse vector index checkpoints to disk.
    ///
    /// Called periodically alongside HNSW checkpoints from the TPC event loop.
    pub fn checkpoint_sparse_vector_indexes(&self) -> usize {
        if self.sparse_vector_indexes.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("sparse-vector-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            tracing::warn!(
                core = self.core_id,
                "failed to create sparse vector checkpoint dir"
            );
            return 0;
        }

        let mut checkpointed = 0;
        for ((db, tid, coll, field), index) in &self.sparse_vector_indexes {
            if index.is_empty() {
                continue;
            }
            let bytes = index.checkpoint_to_bytes();
            if bytes.is_empty() {
                continue;
            }
            // Atomic write via temp file + rename. The stem encodes the full
            // logical key with percent-encoded components so the `_` separator
            // is unambiguous.
            let stem = sparse_vector_checkpoint_stem(db.as_u64(), tid.as_u64(), coll, field);
            let ckpt_path = ckpt_dir.join(format!("{stem}.ckpt"));
            let tmp_path = ckpt_dir.join(format!("{stem}.ckpt.tmp"));
            if nodedb_wal::segment::atomic_write_fsync(&tmp_path, &ckpt_path, &bytes).is_ok() {
                checkpointed += 1;
            }
        }

        if checkpointed > 0 {
            tracing::info!(
                core = self.core_id,
                checkpointed,
                total = self.sparse_vector_indexes.len(),
                "sparse vector indexes checkpointed"
            );
        }
        checkpointed
    }

    /// Load sparse vector index checkpoints from disk on startup.
    ///
    /// Legacy single-underscore filenames (`{tid}_{coll}_{field}`, pre-db
    /// scoping — produced by the old `:`→`_` sanitizer) are loaded under
    /// [`DatabaseId::DEFAULT`] and rewritten in the new `{db}_{tid}_...` format
    /// so the migration runs exactly once.
    pub fn load_sparse_vector_checkpoints(&mut self) {
        let ckpt_dir = self.data_dir.join("sparse-vector-ckpt");
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
            let (map_key, needs_migration) = match parse_sparse_vector_key(&stem) {
                Some(k) => (k, false),
                None => match parse_legacy_sparse_vector_key(&stem) {
                    Some(k) => (k, true),
                    None => {
                        tracing::warn!(
                            core = self.core_id,
                            %stem,
                            "failed to parse sparse vector checkpoint key (ambiguous legacy file); \
                             skipping"
                        );
                        continue;
                    }
                },
            };

            let Ok(bytes) = nodedb_wal::segment::read_checkpoint_dontneed(&path) else {
                continue;
            };
            let Some(index) =
                crate::engine::vector::sparse::SparseInvertedIndex::from_checkpoint(&bytes)
            else {
                continue;
            };

            tracing::info!(
                core = self.core_id,
                %stem,
                docs = index.doc_count(),
                dims = index.dim_count(),
                "loaded sparse vector checkpoint"
            );
            let (db, tid, coll, field) = map_key;
            // One-time migration: rewrite legacy filenames in the new format so
            // the next startup parses them via the 4-part path. The in-memory
            // load already succeeded, so a rename failure is logged and
            // tolerated (it retries next startup).
            if needs_migration {
                migrate_legacy_file(self.core_id, &ckpt_dir, &stem, db, tid, &coll, &field);
            }
            self.sparse_vector_indexes
                .insert((db, tid, coll, field), index);
            loaded += 1;
        }

        if loaded > 0 {
            tracing::info!(
                core = self.core_id,
                loaded,
                "sparse vector checkpoints loaded"
            );
        }
    }
}

/// Build the full filename stem for a sparse-vector checkpoint:
/// `{db}_{tid}_{enc(coll)}_{enc(field)}`.
fn sparse_vector_checkpoint_stem(db: u64, tid: u64, coll: &str, field: &str) -> String {
    format!(
        "{}_{}_{}_{}",
        db,
        tid,
        enc_component(coll),
        enc_component(field)
    )
}

/// Shared prefix builder for reclaim: every checkpoint file for
/// `(db, tid, coll)` begins with `{db}_{tid}_{enc(coll)}_`. This is the single
/// authority on the filename encoding so reclaim can never drift from the write
/// path (the field is always present, so the prefix ends with `_`).
pub(crate) fn sparse_vector_checkpoint_prefix(db: u64, tid: u64, coll: &str) -> String {
    format!("{}_{}_{}_", db, tid, enc_component(coll))
}

/// Parse a new-format stem `{db}_{tid}_{enc(coll)}_{enc(field)}` into a key.
/// Requires EXACTLY 4 underscore-separated parts with numeric db + tid.
/// Returns `None` on any structural or numeric parse failure.
fn parse_sparse_vector_key(stem: &str) -> Option<(DatabaseId, TenantId, String, String)> {
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

/// Parse a LEGACY stem `{tid}_{coll}_{field}` (pre-db scoping, the old `:`→`_`
/// sanitized scheme) into a key under [`DatabaseId::DEFAULT`]. Requires EXACTLY
/// 3 parts with a numeric tid. Components are returned verbatim (legacy files
/// were not percent-encoded). Matches the common no-underscore-in-name case;
/// genuinely ambiguous names fall through to a warn at the call site.
fn parse_legacy_sparse_vector_key(stem: &str) -> Option<(DatabaseId, TenantId, String, String)> {
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

/// Atomically rename a legacy `.ckpt` file to the new stem. Failures are logged
/// and tolerated (the in-memory load already succeeded).
fn migrate_legacy_file(
    core_id: usize,
    ckpt_dir: &std::path::Path,
    old_stem: &str,
    db: DatabaseId,
    tid: TenantId,
    coll: &str,
    field: &str,
) {
    let new_stem = sparse_vector_checkpoint_stem(db.as_u64(), tid.as_u64(), coll, field);
    if new_stem == old_stem {
        return;
    }
    let old_path = ckpt_dir.join(format!("{old_stem}.ckpt"));
    if !old_path.exists() {
        return;
    }
    let new_path = ckpt_dir.join(format!("{new_stem}.ckpt"));
    if let Err(e) = std::fs::rename(&old_path, &new_path) {
        tracing::warn!(
            core = core_id,
            %old_stem,
            %new_stem,
            error = %e,
            "sparse vector checkpoint legacy migration rename failed; will retry next startup"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stem_roundtrips_through_parse() {
        let stem = sparse_vector_checkpoint_stem(7, 42, "my_docs", "title_field");
        let parsed = parse_sparse_vector_key(&stem).expect("new-format parse");
        assert_eq!(parsed.0, DatabaseId::new(7));
        assert_eq!(parsed.1, TenantId::new(42));
        assert_eq!(parsed.2, "my_docs");
        assert_eq!(parsed.3, "title_field");
    }

    #[test]
    fn prefix_matches_stem_for_same_collection() {
        let stem = sparse_vector_checkpoint_stem(3, 9, "d_b", "f");
        let prefix = sparse_vector_checkpoint_prefix(3, 9, "d_b");
        assert!(
            stem.starts_with(&prefix),
            "stem {stem} must start with prefix {prefix}"
        );
    }

    #[test]
    fn legacy_3part_parses_under_default_db() {
        let parsed = parse_legacy_sparse_vector_key("5_docs_title").expect("legacy parse");
        assert_eq!(parsed.0, DatabaseId::DEFAULT);
        assert_eq!(parsed.1, TenantId::new(5));
        assert_eq!(parsed.2, "docs");
        assert_eq!(parsed.3, "title");
    }

    #[test]
    fn new_format_takes_precedence_over_legacy() {
        let stem = sparse_vector_checkpoint_stem(1, 2, "c", "f");
        assert!(parse_sparse_vector_key(&stem).is_some());
    }

    #[test]
    fn non_numeric_stem_is_none() {
        assert!(parse_sparse_vector_key("a_b_c_d").is_none());
        assert!(parse_legacy_sparse_vector_key("a_b_c").is_none());
    }
}
