// SPDX-License-Identifier: BUSL-1.1

//! Filesystem naming for CRDT checkpoints: directories, generation
//! directories, per-collection filenames, and the inverse parse.
//!
//! The write path, the load path, and reclaim all build every path through
//! these helpers so the three can never drift.

/// Filename of the manifest that names the live generation.
pub(super) const CRDT_CKPT_MANIFEST: &str = "MANIFEST";

/// Canonical path for a core's CRDT checkpoint directory.
///
/// The per-core subdir is required because `data_dir` is shared across cores
/// and a tenant's CRDT state is fragmented across cores by collection —
/// without it, cores would race-overwrite the same file and persist only a
/// partial fragment.
pub(crate) fn crdt_ckpt_dir(data_dir: &std::path::Path, core_id: usize) -> std::path::PathBuf {
    data_dir.join("crdt-ckpt").join(format!("core-{core_id}"))
}

/// Directory holding one generation's per-collection files.
pub(crate) fn crdt_ckpt_gen_dir(ckpt_dir: &std::path::Path, generation: u64) -> std::path::PathBuf {
    ckpt_dir.join(format!("gen-{generation}"))
}

/// Per-collection checkpoint filename:
/// `db-{dbid}-tenant-{tid}-coll-{hex(collection)}.ckpt`.
///
/// The collection is hex-encoded so the filename is filesystem-safe (collection
/// names may contain `/`, `:` or `-`) and unambiguously parseable: hex contains
/// only `[0-9a-f]`, so the `-coll-` separator never collides with the encoded
/// name and the numeric tenant id never collides with the encoding.
pub(crate) fn crdt_ckpt_filename(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!(
        "db-{database_id}-tenant-{tenant_id}-coll-{}.ckpt",
        hex_encode(collection)
    )
}

/// Shared prefix builder for reclaim: every checkpoint file for
/// `(database, tenant, collection)` is EXACTLY this stem. The hex encoding is
/// injective, so an equality match cannot swallow a longer collection name the
/// way a `starts_with` could.
pub(crate) fn crdt_ckpt_stem(database_id: u64, tenant_id: u64, collection: &str) -> String {
    format!(
        "db-{database_id}-tenant-{tenant_id}-coll-{}",
        hex_encode(collection)
    )
}

/// Lowercase hex of a collection name's UTF-8 bytes.
fn hex_encode(collection: &str) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(collection.len() * 2);
    for b in collection.as_bytes() {
        // Infallible: writing to a String never returns Err.
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

/// Parse a per-collection checkpoint file stem (no extension) back into
/// `(database_id, tenant_id, collection)`. Returns `None` for any unparseable
/// stem.
pub(super) fn parse_crdt_ckpt_stem(stem: &str) -> Option<(u64, u64, String)> {
    let rest = stem.strip_prefix("db-")?;
    let (database_str, rest) = rest.split_once("-tenant-")?;
    let database_id = database_str.parse::<u64>().ok()?;
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

    #[test]
    fn stem_roundtrips_through_parse() {
        for coll in ["orders", "2/orders", "a-coll-b", "weird:name", "-coll-"] {
            let fname = crdt_ckpt_filename(3, 7, coll);
            let stem = fname.strip_suffix(".ckpt").expect("has .ckpt suffix");
            let (database_id, tid, parsed) =
                parse_crdt_ckpt_stem(stem).expect("must parse own filename");
            assert_eq!(database_id, 3);
            assert_eq!(tid, 7);
            assert_eq!(parsed, coll);
        }
    }

    /// The reclaim stem and the write path's filename must name the same file,
    /// or a DROP leaves the collection's checkpoint reachable forever.
    #[test]
    fn reclaim_stem_matches_the_written_filename() {
        assert_eq!(
            format!("{}.ckpt", crdt_ckpt_stem(3, 7, "orders")),
            crdt_ckpt_filename(3, 7, "orders")
        );
    }

    /// A collection whose name merely starts with the target's bytes must get a
    /// different stem — hex is injective, so equality can never collide.
    #[test]
    fn longer_collection_name_gets_a_distinct_stem() {
        assert_ne!(
            crdt_ckpt_stem(0, 1, "orders"),
            crdt_ckpt_stem(0, 1, "orders_archive")
        );
    }

    #[test]
    fn unparseable_stems_are_rejected() {
        assert!(parse_crdt_ckpt_stem("tenant-5").is_none(), "no db- prefix");
        assert!(
            parse_crdt_ckpt_stem("db-x-tenant-1-coll-6162").is_none(),
            "non-numeric database"
        );
        assert!(
            parse_crdt_ckpt_stem("db-0-tenant-1-coll-616").is_none(),
            "odd hex length"
        );
    }

    #[test]
    fn generation_dirs_are_distinct() {
        let base = std::path::Path::new("/data/crdt-ckpt/core-0");
        assert_ne!(crdt_ckpt_gen_dir(base, 0), crdt_ckpt_gen_dir(base, 1));
    }
}
