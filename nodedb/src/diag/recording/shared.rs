// SPDX-License-Identifier: BUSL-1.1

//! Values every capture site shares: the error class a grouping key
//! carries, and the variant name of a metadata entry.

use nodedb_cluster::MetadataEntry;

/// The decoded entry's variant name, read off its `Debug` text rather than an
/// exhaustive match — a forensic label tolerates the approximation, and a
/// new variant keeps reporting a real name with no arm to maintain.
pub fn entry_kind(entry: &MetadataEntry) -> String {
    let debug = format!("{entry:?}");
    match debug.find(|c: char| !(c.is_alphanumeric() || c == '_')) {
        Some(end) => debug[..end].to_owned(),
        None => debug,
    }
}

/// The stable class of an error's `Display` text: the text before the first
/// colon, which names what failed rather than the per-occurrence detail
/// after it.
pub(super) fn error_class(err: &dyn std::error::Error) -> String {
    let text = err.to_string();
    text.split(':').next().unwrap_or(&text).trim().to_owned()
}
