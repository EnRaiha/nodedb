// SPDX-License-Identifier: BUSL-1.1

//! Object-store keys for archived WAL segments.
//!
//! Every node life owns the directory `{prefix}wal/{node_id}/{incarnation}/`.
//! Nodes that share one bucket and prefix never write the same key, and a node
//! whose data directory was wiped never matches its earlier objects.
//!
//! Each segment object `wal-{first:020}.seg` has a zero-byte checksum marker
//! `wal-{first:020}.seg.{crc32c:08x}.crc` beside it. A listing alone then
//! yields both the size and the checksum of every archived segment.

use nodedb_wal::segment::{parse_segment_filename, segment_filename};

/// Suffix of a checksum marker.
const CHECKSUM_SUFFIX: &str = ".crc";

/// The directory holding every archived segment of one node life, ending in `/`.
pub fn wal_archive_node_prefix(prefix: &str, node_id: u64, incarnation: &str) -> String {
    format!("{prefix}wal/{node_id}/{incarnation}/")
}

/// The object key of the segment that starts at `first_lsn`.
pub fn wal_archive_segment_key(
    prefix: &str,
    node_id: u64,
    incarnation: &str,
    first_lsn: u64,
) -> String {
    format!(
        "{}{}",
        wal_archive_node_prefix(prefix, node_id, incarnation),
        segment_filename(first_lsn)
    )
}

/// The key of the checksum marker for the segment at `first_lsn`.
pub fn wal_archive_checksum_key(
    prefix: &str,
    node_id: u64,
    incarnation: &str,
    first_lsn: u64,
    crc32c: u32,
) -> String {
    format!(
        "{}.{crc32c:08x}{CHECKSUM_SUFFIX}",
        wal_archive_segment_key(prefix, node_id, incarnation, first_lsn)
    )
}

/// One object in a node's archive directory, by its last key component.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArchivedObject {
    /// A segment object.
    Segment { first_lsn: u64 },
    /// The checksum marker of a segment object.
    Checksum { first_lsn: u64, crc32c: u32 },
}

/// Classify an archive object by its filename. Anything else is `None`.
pub fn parse_wal_archive_filename(filename: &str) -> Option<ArchivedObject> {
    if let Some(first_lsn) = parse_segment_filename(filename) {
        return Some(ArchivedObject::Segment { first_lsn });
    }
    let marker = filename.strip_suffix(CHECKSUM_SUFFIX)?;
    let (segment, crc_hex) = marker.rsplit_once('.')?;
    if crc_hex.len() != 8 {
        return None;
    }
    let crc32c = u32::from_str_radix(crc_hex, 16).ok()?;
    let first_lsn = parse_segment_filename(segment)?;
    Some(ArchivedObject::Checksum { first_lsn, crc32c })
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn last_component(key: &str) -> &str {
        key.rsplit('/').next().unwrap_or_default()
    }

    #[test]
    fn two_node_ids_never_produce_the_same_key() {
        // Node ids that share a decimal prefix (1, 11, 111) are the case a
        // missing delimiter collides on.
        let node_ids = [0u64, 1, 2, 10, 11, 111, u64::MAX];
        let lsns = [0u64, 1, 11, 111, 1 << 32, u64::MAX];
        let mut seen = HashSet::new();
        for node_id in node_ids {
            for lsn in lsns {
                let key = wal_archive_segment_key("data/", node_id, "inc", lsn);
                assert!(seen.insert(key.clone()), "duplicate key {key}");
            }
        }
    }

    #[test]
    fn two_incarnations_never_produce_the_same_key() {
        assert_ne!(
            wal_archive_segment_key("data/", 1, "aa", 5),
            wal_archive_segment_key("data/", 1, "bb", 5)
        );
    }

    #[test]
    fn key_is_the_node_life_directory_plus_the_real_segment_name() {
        assert_eq!(
            wal_archive_segment_key("data/", 7, "abc", 42),
            "data/wal/7/abc/wal-00000000000000000042.seg"
        );
    }

    #[test]
    fn key_filename_round_trips_to_first_lsn() {
        for lsn in [1u64, 64, u64::MAX] {
            let key = wal_archive_segment_key("p/", 3, "i", lsn);
            assert_eq!(
                parse_wal_archive_filename(last_component(&key)),
                Some(ArchivedObject::Segment { first_lsn: lsn })
            );
        }
    }

    #[test]
    fn checksum_marker_round_trips() {
        for (lsn, crc) in [(1u64, 0u32), (64, 0xdead_beef), (u64::MAX, u32::MAX)] {
            let key = wal_archive_checksum_key("p/", 3, "i", lsn, crc);
            assert_eq!(
                parse_wal_archive_filename(last_component(&key)),
                Some(ArchivedObject::Checksum {
                    first_lsn: lsn,
                    crc32c: crc
                })
            );
        }
    }

    #[test]
    fn foreign_names_are_ignored() {
        assert_eq!(parse_wal_archive_filename("notes.txt"), None);
        assert_eq!(
            parse_wal_archive_filename("wal-00000000000000000001.seg.xyz.crc"),
            None
        );
    }
}
