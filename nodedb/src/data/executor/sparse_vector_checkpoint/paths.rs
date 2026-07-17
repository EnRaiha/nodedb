// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for sparse-vector checkpoints: directories, per-index
//! filenames, and the inverse parse.
//!
//! The write path, the load path, and reclaim all build every path through these
//! helpers so the three can never drift. A path divergence between writer and
//! reader is silent, and its symptom is indistinguishable from data loss.
//!
//! ## Filename encoding
//!
//! Each index is checkpointed to a file whose stem encodes its logical key:
//! `{db}_{tid}_{enc(coll)}_{enc(field)}.ckpt`. `db`/`tid` are numeric and pass
//! through unchanged; `coll`/`field` are percent-encoded by [`enc_component`] so
//! the structural `_` separator can never collide with a literal underscore in a
//! collection or field name. This makes the encoding round-trippable for
//! arbitrary names.

use nodedb_types::DatabaseId;

use crate::data::executor::checkpoint_encoding::{dec_component, enc_component};
use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(super) const SPARSE_VECTOR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's sparse-vector checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it, cores would race-overwrite each other's files and persist
/// only a partial fragment. It also means the loader needs no core-ownership
/// filter — a core only ever sees its own indexes.
pub(crate) fn sparse_vector_ckpt_dir(
    data_dir: &std::path::Path,
    core_id: usize,
) -> std::path::PathBuf {
    data_dir
        .join("sparse-vector-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files.
pub(crate) fn sparse_vector_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// Build the full filename stem for a sparse-vector checkpoint:
/// `{db}_{tid}_{enc(coll)}_{enc(field)}`.
pub(super) fn sparse_vector_checkpoint_stem(db: u64, tid: u64, coll: &str, field: &str) -> String {
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

/// Parse a stem `{db}_{tid}_{enc(coll)}_{enc(field)}` into a logical key.
/// Requires EXACTLY 4 underscore-separated parts with numeric db + tid.
/// Returns `None` on any structural or numeric parse failure.
pub(super) fn parse_sparse_vector_key(
    stem: &str,
) -> Option<(DatabaseId, TenantId, String, String)> {
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

    /// `sparse_vector_ckpt_dir` must isolate cores sharing one `data_dir` —
    /// without the per-core subdir they race-overwrite each other's checkpoints.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = sparse_vector_ckpt_dir(base, 0);
        let d1 = sparse_vector_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/sparse-vector-ckpt/core-0");
        assert_ne!(
            sparse_vector_ckpt_gen_dir(base, 0),
            sparse_vector_ckpt_gen_dir(base, 1)
        );
    }

    #[test]
    fn stem_roundtrips_through_parse() {
        let stem = sparse_vector_checkpoint_stem(7, 42, "my_docs", "title_field");
        let parsed = parse_sparse_vector_key(&stem).expect("stem must parse");
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

    /// A prefix must not match a DIFFERENT collection whose encoded name merely
    /// starts with the same bytes — that would unlink a live index's checkpoint.
    #[test]
    fn prefix_does_not_match_a_longer_collection_name() {
        let other = sparse_vector_checkpoint_stem(3, 9, "docs_archive", "f");
        let prefix = sparse_vector_checkpoint_prefix(3, 9, "docs");
        assert!(
            !other.starts_with(&prefix),
            "prefix {prefix} must not swallow {other}"
        );
    }

    #[test]
    fn non_numeric_stem_is_none() {
        assert!(parse_sparse_vector_key("a_b_c_d").is_none());
    }

    #[test]
    fn wrong_part_count_is_none() {
        assert!(parse_sparse_vector_key("1_2_3").is_none());
        assert!(parse_sparse_vector_key("1_2_3_4_5").is_none());
    }
}
