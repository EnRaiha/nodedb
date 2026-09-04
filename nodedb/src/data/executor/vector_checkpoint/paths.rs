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

/// The checkpoint filename stem for one index key: `db-{db}-tenant-{tid}-key-`
/// followed by the hex-encoded `coll_key` (`{collection}` for the default
/// index, `{collection}:{field}` for a named-field one).
///
/// The single authority on the encoding, shared with reclaim so a DROP can
/// never miss a file the write path produced. The collection name is hex-encoded
/// rather than embedded verbatim: a name is user-supplied, and the stem becomes
/// a path component, so a verbatim `/` or `..` in it would address a file
/// outside the generation directory. Hex is byte-wise, so a prefix relation
/// between two names survives the encoding and reclaim can still match every
/// field index of one collection.
pub(crate) fn vector_ckpt_stem(db: u64, tid: u64, coll_key: &str) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(coll_key.len() * 2);
    for b in coll_key.as_bytes() {
        // Infallible: writing to a String never returns Err.
        let _ = write!(hex, "{b:02x}");
    }
    format!("db-{db}-tenant-{tid}-key-{hex}")
}

/// Parse a stem produced by [`vector_ckpt_stem`] back into the
/// `(DatabaseId, TenantId, coll_key)` tuple map key.
///
/// Returns `None` for any stem this module did not write.
pub(super) fn parse_vector_ckpt_stem(stem: &str) -> Option<(DatabaseId, TenantId, String)> {
    let rest = stem.strip_prefix("db-")?;
    let (db_str, rest) = rest.split_once("-tenant-")?;
    let db = db_str.parse::<u64>().ok()?;
    let (tid_str, hex) = rest.split_once("-key-")?;
    let tid = tid_str.parse::<u64>().ok()?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let raw = hex.as_bytes();
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    let coll_key = String::from_utf8(bytes).ok()?;
    Some((DatabaseId::new(db), TenantId::new(tid), coll_key))
}

/// Parse a `"{db}:{tid}:{coll_key}"` string (the in-memory `BuildComplete.key`
/// form, produced by `vector_build_key`) back into the
/// `(DatabaseId, TenantId, String)` tuple map key.
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
        for coll_key in ["docs", "docs:emb", "2/docs", "a-tenant-b", "-key-"] {
            let stem = vector_ckpt_stem(3, 9, coll_key);
            let (db, tid, parsed) = parse_vector_ckpt_stem(&stem).expect("stem must parse");
            assert_eq!(db, DatabaseId::new(3));
            assert_eq!(tid, TenantId::new(9));
            assert_eq!(parsed, coll_key);
        }
    }

    /// The stem is one plain path component whatever the collection is named,
    /// so the checkpoint write cannot address a file outside its generation
    /// directory.
    #[test]
    fn stem_is_a_plain_path_component() {
        for coll_key in ["docs", "../../etc/passwd", "a/b", "C:evil", ".."] {
            let stem = vector_ckpt_stem(0, 1, coll_key);
            assert!(
                nodedb_types::is_plain_path_component(&format!("{stem}.ckpt")),
                "{coll_key:?} produced a non-plain stem: {stem}"
            );
        }
    }

    /// Reclaim matches a collection's field indexes by prefix. Hex is
    /// byte-wise, so the prefix relation survives the encoding.
    #[test]
    fn field_stems_extend_the_collection_stem() {
        let bare = vector_ckpt_stem(0, 1, "docs");
        let field = vector_ckpt_stem(0, 1, "docs:emb");
        assert!(field.starts_with(&bare));
        assert!(!vector_ckpt_stem(0, 1, "docs_archive").starts_with(&format!("{bare}3a")));
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
