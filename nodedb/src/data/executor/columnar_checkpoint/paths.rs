// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for columnar checkpoints: directories, per-collection
//! filenames, and the inverse parse.
//!
//! Both the write path (`write.rs`) and the load path (`load.rs`) build every
//! path through these helpers so the two can never drift. A path divergence
//! between writer and reader is silent, and its symptom is indistinguishable
//! from data loss.

/// Filename of the manifest that names the live generation.
pub(crate) const COLUMNAR_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's columnar checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across all TPC
/// cores; without it, cores would race-overwrite each other's files and persist
/// only a partial fragment. It also means the loader needs no core-ownership
/// filter — a core only ever sees its own collections.
pub(crate) fn columnar_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir
        .join("columnar-ckpt")
        .join(format!("core-{core_id}"))
}

/// Directory holding one generation's collection files.
pub(crate) fn columnar_ckpt_gen_dir(
    ckpt_dir: &std::path::Path,
    generation: u64,
) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// Per-collection checkpoint filename:
/// `db-{did}-tenant-{tid}-coll-{hex(collection)}.ckpt`.
///
/// The database id is part of the name because `columnar_engines` is keyed by
/// `(DatabaseId, TenantId, String)` and the id cannot be recovered from the
/// collection name — restoring under the wrong database id would hide every
/// restored row from the queries that look for it.
///
/// The collection is hex-encoded so the filename is filesystem-safe (collection
/// names may contain `/`, `:` or `-`) and unambiguously parseable: hex contains
/// only `[0-9a-f]`, so the `-coll-` separator never collides with the encoded
/// name and the numeric ids never collide with the encoding.
pub(crate) fn columnar_ckpt_filename(database_id: u64, tenant_id: u64, collection: &str) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(collection.len() * 2);
    for b in collection.as_bytes() {
        // infallible: writing to a String never returns Err
        let _ = write!(hex, "{b:02x}");
    }
    format!("db-{database_id}-tenant-{tenant_id}-coll-{hex}.ckpt")
}

/// Parse a checkpoint file stem (no extension) back into `(database_id,
/// tenant_id, collection)`. Returns `None` for any unparseable stem.
pub(crate) fn parse_columnar_ckpt_stem(stem: &str) -> Option<(u64, u64, String)> {
    let rest = stem.strip_prefix("db-")?;
    let (did_str, rest) = rest.split_once("-tenant-")?;
    let database_id = did_str.parse::<u64>().ok()?;
    let (tid_str, hex) = rest.split_once("-coll-")?;
    let tenant_id = tid_str.parse::<u64>().ok()?;
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    let raw = hex.as_bytes();
    let mut i = 0;
    while i < raw.len() {
        let hi = (raw[i] as char).to_digit(16)?;
        let lo = (raw[i + 1] as char).to_digit(16)?;
        bytes.push((hi * 16 + lo) as u8);
        i += 2;
    }
    let collection = String::from_utf8(bytes).ok()?;
    Some((database_id, tenant_id, collection))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `columnar_ckpt_dir` must isolate cores sharing one `data_dir` — without
    /// the per-core subdir they race-overwrite each other's checkpoints.
    #[test]
    fn per_core_dirs_are_distinct() {
        let base = std::path::Path::new("/data");
        let d0 = columnar_ckpt_dir(base, 0);
        let d1 = columnar_ckpt_dir(base, 1);
        assert_ne!(d0, d1);
        assert!(d0.to_str().expect("utf8 path").contains("core-0"));
        assert!(d1.to_str().expect("utf8 path").contains("core-1"));
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/columnar-ckpt/core-0");
        assert_ne!(
            columnar_ckpt_gen_dir(base, 0),
            columnar_ckpt_gen_dir(base, 1)
        );
    }

    /// Filenames must round-trip collection names containing exactly the
    /// separators the hex encoding exists to neutralise.
    #[test]
    fn filename_roundtrips_through_parse() {
        for coll in [
            "events",
            "2/orders",
            "a-coll-b",
            "weird:name",
            "-coll-",
            "db-1-tenant-2",
        ] {
            let fname = columnar_ckpt_filename(7, 42, coll);
            let stem = fname.strip_suffix(".ckpt").expect("ckpt extension");
            let (did, tid, parsed) = parse_columnar_ckpt_stem(stem).expect("stem must parse");
            assert_eq!(did, 7);
            assert_eq!(tid, 42);
            assert_eq!(parsed, coll);
        }
    }

    /// Two collections differing only by database id must not share a file —
    /// otherwise one silently overwrites the other's rows within a generation.
    #[test]
    fn database_id_separates_filenames() {
        assert_ne!(
            columnar_ckpt_filename(1, 42, "events"),
            columnar_ckpt_filename(2, 42, "events")
        );
    }

    #[test]
    fn unparseable_stems_are_rejected() {
        assert!(
            parse_columnar_ckpt_stem("db-x-tenant-1-coll-6162").is_none(),
            "non-numeric database id"
        );
        assert!(
            parse_columnar_ckpt_stem("db-1-tenant-abc-coll-6162").is_none(),
            "non-numeric tenant"
        );
        assert!(
            parse_columnar_ckpt_stem("db-1-tenant-1-coll-616").is_none(),
            "odd hex length"
        );
        assert!(
            parse_columnar_ckpt_stem("db-1-tenant-1-coll-zz").is_none(),
            "non-hex digits"
        );
        assert!(parse_columnar_ckpt_stem("garbage").is_none(), "no prefix");
    }
}
