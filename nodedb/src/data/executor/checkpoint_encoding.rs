// SPDX-License-Identifier: BUSL-1.1
//! Shared filename-component encoding for on-disk checkpoint files
//! (spatial R-tree, sparse-vector inverted index). Each path component is
//! percent-encoded so the structural `_` separator is unambiguous and the
//! encoding round-trips for collection/field names containing `_`/`%`/`/`.

/// Percent-encode one filename component. `%` FIRST.
pub(crate) fn enc_component(s: &str) -> String {
    s.replace('%', "%25")
        .replace('_', "%5F")
        .replace('/', "%2F")
        .replace('\0', "%00")
}
/// Inverse of `enc_component`. `%25` LAST.
pub(crate) fn dec_component(s: &str) -> String {
    s.replace("%5F", "_")
        .replace("%2F", "/")
        .replace("%00", "\0")
        .replace("%25", "%")
}
