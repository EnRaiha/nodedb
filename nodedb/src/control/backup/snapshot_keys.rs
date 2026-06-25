// SPDX-License-Identifier: BUSL-1.1

//! Pure collection-name extractors for `TenantDataSnapshot` section keys.
//!
//! `TenantDataSnapshot` sections key their entries with three distinct scoped
//! formats (see [`crate::types::TenantDataSnapshot`] field docs):
//!
//! - **db-tenant-scoped** — `"{db}:{tid}:{collection}[:suffix...]"` (documents,
//!   indexes, timeseries memtable, vectors). Documents and indexes carry a
//!   trailing per-row suffix after the collection; vectors and timeseries have
//!   no suffix. The collection never contains `':'` or `'\0'`. Use
//!   [`extract_db_tenant_scoped_collection`].
//! - **db-scoped (collection-last)** — `"{db}:{tid}:{collection}"` where the
//!   collection is the remainder and may itself contain `':'` (flushed-ts
//!   segments, columnar engines). Use [`extract_db_scoped_collection`].
//! - **collection-name-only** — the key IS the bare collection name (kv tables).
//!   Routed directly; no extractor needed.
//!
//! Both the RESTORE topology splitter and the Raft snapshot SEND builder filter
//! sections by which vshard each entry's collection routes to, so the parsing
//! lives here once and is shared by both — never duplicated ad-hoc.

/// Extract the collection from a `"{db}:{tid}:{collection}[:suffix...]"` key.
///
/// Used by documents, indexes, vectors, and timeseries-memtable sections,
/// whose keys carry the leading `{db}:{tid}:` component and (for documents /
/// indexes) a trailing per-row suffix after the collection. Verifies the
/// embedded tenant matches `tenant_id`; the collection is the first
/// ':'-or-'\0'-delimited token after the prefix. Returns `None` on prefix
/// mismatch, too-few parts, or empty collection.
pub fn extract_db_tenant_scoped_collection(key: &str, tenant_id: u64) -> Option<&str> {
    let mut it = key.splitn(3, ':');
    let _db = it.next()?;
    let tid = it.next()?;
    if tid.parse::<u64>().ok()? != tenant_id {
        return None;
    }
    let rest = it.next()?;
    let coll = rest.split([':', '\u{0}']).next()?;
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
    use super::{extract_db_scoped_collection, extract_db_tenant_scoped_collection};

    #[test]
    fn extract_db_tenant_scoped_collection_parses_key() {
        // Documents / indexes: collection is the 3rd token, suffix follows.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:snap_rt_docs:abcd1234", 1),
            Some("snap_rt_docs")
        );
        // '\0'-delimited per-row suffix.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:users\u{0}doc1", 1),
            Some("users")
        );
        // Vectors / timeseries: no suffix — collection is the whole remainder.
        assert_eq!(
            extract_db_tenant_scoped_collection("0:1:metrics", 1),
            Some("metrics")
        );
        // Tenant mismatch → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:2:x:y", 1), None);
        // Empty collection → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:1:", 1), None);
        // Too few parts → None.
        assert_eq!(extract_db_tenant_scoped_collection("0:1", 1), None);
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
