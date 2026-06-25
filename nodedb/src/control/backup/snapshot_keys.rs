// SPDX-License-Identifier: BUSL-1.1

//! Pure collection-name extractors for `TenantDataSnapshot` section keys.
//!
//! `TenantDataSnapshot` sections key their entries with two distinct scoped
//! formats (see [`crate::types::TenantDataSnapshot`] field docs):
//!
//! - **tenant-scoped** — `"{tid}:{collection}:..."` (documents, indexes,
//!   timeseries memtable, vectors). Use [`extract_collection`].
//! - **db-scoped** — `"{db}:{tid}:{collection}"` where the collection may
//!   itself contain `':'` (flushed-ts segments, columnar engines). Use
//!   [`extract_db_scoped_collection`].
//!
//! Both the RESTORE topology splitter and the Raft snapshot SEND builder filter
//! sections by which vshard each entry's collection routes to, so the parsing
//! lives here once and is shared by both — never duplicated ad-hoc.

/// Extract the collection from a tenant-scoped `"{tid}:{collection}:..."` key.
///
/// Returns `None` when the key does not carry the expected `tenant_id` prefix
/// or the collection segment is empty.
pub fn extract_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let prefix_owned = format!("{tenant_id}:");
    let after = key.strip_prefix(prefix_owned.as_str())?;
    let coll = after.split(['\0', ':']).next()?;
    if coll.is_empty() { None } else { Some(coll) }
}

/// Extract the collection from a db-scoped `"{db}:{tid}:{collection}"` key,
/// verifying the embedded tenant matches `tenant_id`.
///
/// The first two ':' are structural (db, tid); the collection may itself
/// contain ':'. Returns `None` when the key has fewer than three parts or the
/// tenant does not match.
pub fn extract_db_scoped_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let mut it = key.splitn(3, ':');
    let _db = it.next()?;
    let tid = it.next()?;
    let coll = it.next()?;
    if tid.parse::<u64>().ok()? != tenant_id || coll.is_empty() {
        return None;
    }
    Some(coll)
}

#[cfg(test)]
mod tests {
    use super::{extract_collection, extract_db_scoped_collection};

    #[test]
    fn extract_collection_strips_prefix() {
        assert_eq!(extract_collection("7:users:doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users\u{0}doc1", 7), Some("users"));
        assert_eq!(extract_collection("7:users", 7), Some("users"));
        assert_eq!(extract_collection("8:users:doc1", 7), None);
        assert_eq!(extract_collection("7:", 7), None);
    }

    #[test]
    fn extract_db_scoped_collection_parses_db_prefixed_key() {
        // "{db}:{tid}:{collection}" — first two ':' are structural.
        assert_eq!(
            extract_db_scoped_collection("0:7:metrics", 7),
            Some("metrics")
        );
        // Collection may itself contain ':'.
        assert_eq!(
            extract_db_scoped_collection("0:7:a:b", 7),
            Some("a:b"),
            "collection retains embedded ':'"
        );
        // Tenant mismatch → None.
        assert_eq!(extract_db_scoped_collection("0:8:metrics", 7), None);
        // Missing collection part → None.
        assert_eq!(extract_db_scoped_collection("0:7", 7), None);
        // Empty collection → None.
        assert_eq!(extract_db_scoped_collection("0:7:", 7), None);
    }
}
