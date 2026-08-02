// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for vector checkpoints: directories, generation
//! directories, per-index filenames, and the inverse parse.
//!
//! The write path, the load path, and reclaim all build every path through
//! these helpers so the three can never drift. A path divergence between writer
//! and reader is silent, and its symptom is indistinguishable from data loss.

use nodedb_types::DatabaseId;

use crate::types::TenantId;

/// Filename of the manifest that names the live generation.
pub(super) const VECTOR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's vector checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it a flat directory made every core load every collection's
/// index. It also means the loader needs no core-ownership filter — a core only
/// ever sees its own indexes.
pub(crate) fn vector_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("vector-ckpt").join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-index files.
pub(crate) fn vector_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// The checkpoint filename stem of a collection's DEFAULT (unnamed) vector
/// index: `{db}:{tid}:{coll}`. A named-field index appends `:{field}`.
///
/// The single authority on the encoding, shared with reclaim so a DROP can
/// never miss a file the write path produced.
pub(crate) fn vector_ckpt_collection_stem(db: u64, tid: u64, collection: &str) -> String {
    format!("{db}:{tid}:{collection}")
}

/// Parse a `"{db}:{tid}:{coll_key}"` string (the `BuildComplete.key` and
/// on-disk checkpoint filename form, produced by `vector_checkpoint_filename`)
/// back into the `(DatabaseId, TenantId, String)` tuple map key.
///
/// Returns `None` when the string is not in that format — i.e. it does not have
/// at least three `:`-separated components whose first two parse as `u64`
/// (db, tid). `coll_key` is the verbatim remainder and may itself contain `:`
/// (e.g. `collection:field`).
pub(super) fn parse_build_key(s: &str) -> Option<(DatabaseId, TenantId, String)> {
    let mut it = s.splitn(3, ':');
    let db_str = it.next()?;
    let tid_str = it.next()?;
    let coll_key = it.next()?;
    let db = db_str.parse::<u64>().ok()?;
    let tid = tid_str.parse::<u64>().ok()?;
    Some((
        DatabaseId::new(db),
        TenantId::new(tid),
        coll_key.to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `vector_ckpt_dir` must isolate cores sharing one `data_dir` — without the
    /// per-core subdir every core loads every other core's indexes.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = vector_ckpt_dir(base, 0);
        let d1 = vector_ckpt_dir(base, 1);
        assert_ne!(d0, d1, "different cores must get different checkpoint dirs");
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/vector-ckpt/core-0");
        assert_ne!(vector_ckpt_gen_dir(base, 0), vector_ckpt_gen_dir(base, 1));
    }

    #[test]
    fn stem_roundtrips_through_parse() {
        let stem = vector_ckpt_collection_stem(3, 9, "docs");
        let (db, tid, coll) = parse_build_key(&stem).expect("stem must parse");
        assert_eq!(db, DatabaseId::new(3));
        assert_eq!(tid, TenantId::new(9));
        assert_eq!(coll, "docs");
    }

    /// A named-field key keeps its `:` in the collection remainder — the parse
    /// splits only the first two components.
    #[test]
    fn field_qualified_key_keeps_its_remainder() {
        let (_, _, coll) = parse_build_key("0:1:docs:emb").expect("must parse");
        assert_eq!(coll, "docs:emb");
    }

    #[test]
    fn non_numeric_key_is_none() {
        assert!(parse_build_key("a:b:c").is_none());
        assert!(parse_build_key("0:1").is_none());
    }
}
