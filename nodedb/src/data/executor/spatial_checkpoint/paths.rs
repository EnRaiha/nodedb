// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for spatial checkpoints: directories, generation
//! directories, per-index filenames, and the inverse parse.
//!
//! ## On-disk filename encoding
//!
//! Each index is checkpointed to a file whose stem encodes its logical key:
//! `{db}_{tid}_{enc(coll)}_{enc(field)}`, with the R-tree under `.ckpt` and its
//! doc_map companion under `.docmap`. `db`/`tid` are numeric and pass through
//! unchanged; `coll`/`field` are percent-encoded by [`enc_component`] so the
//! structural `_` separator can never collide with a literal underscore in a
//! collection or field name. This makes the encoding round-trippable for
//! arbitrary names. [`spatial_checkpoint_prefix`] is the single shared builder
//! used by both the write path and reclaim, so the two can never drift.

use nodedb_types::DatabaseId;

use crate::data::executor::checkpoint_encoding::{dec_component, enc_component};
use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(super) const SPATIAL_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's spatial checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; it also means the loader needs no core-ownership filter on the
/// filename.
pub(crate) fn spatial_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("spatial-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files (R-trees and doc_maps).
pub(crate) fn spatial_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// Build the full filename stem for a spatial checkpoint:
/// `{db}_{tid}_{enc(coll)}_{enc(field)}`.
pub(super) fn checkpoint_stem(db: DatabaseId, tid: TenantId, coll: &str, field: &str) -> String {
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

/// Parse a stem `{db}_{tid}_{enc(coll)}_{enc(field)}` into a key.
/// Requires EXACTLY 4 underscore-separated parts with numeric db + tid.
/// Returns `None` on any structural or numeric parse failure.
pub(super) fn parse_spatial_key(stem: &str) -> Option<(DatabaseId, TenantId, String, String)> {
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
        let parsed = parse_spatial_key(&stem).expect("stem must parse");
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

    /// A collection whose encoded name merely starts with the target's bytes
    /// must not match — the encoder escapes literal underscores precisely so
    /// the structural separator cannot collide.
    #[test]
    fn prefix_does_not_match_a_longer_collection_name() {
        let other = checkpoint_stem(DatabaseId::new(3), TenantId::new(9), "places_archive", "f");
        let prefix = spatial_checkpoint_prefix(3, 9, "places");
        assert!(
            !other.starts_with(&prefix),
            "prefix {prefix} must not swallow {other}"
        );
    }

    #[test]
    fn non_numeric_stem_is_none() {
        assert!(parse_spatial_key("a_b_c_d").is_none());
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/spatial-ckpt/core-0");
        assert_ne!(spatial_ckpt_gen_dir(base, 0), spatial_ckpt_gen_dir(base, 1));
    }
}
