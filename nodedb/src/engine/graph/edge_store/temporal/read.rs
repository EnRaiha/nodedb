// SPDX-License-Identifier: BUSL-1.1

//! Ceiling resolver — reverse range scan with optional valid-time filter.

use super::keys::{
    EdgeRef, edge_version_prefix, is_gdpr_erasure, is_sentinel, is_tombstone,
    parse_versioned_edge_key, versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use crate::engine::graph::edge_store::store::{EDGES, Edge, EdgeStore, redb_err};
use redb::{ReadableDatabase, ReadableTable};

impl EdgeStore {
    /// Resolve the Ceiling: the latest version of
    /// `(collection, src, label, dst)` whose `system_from ≤ system_as_of`.
    ///
    /// Returns `Ok(None)` if no version exists at or before the cutoff, or if
    /// the latest qualifying version is a tombstone/GDPR erasure.
    ///
    /// When `valid_at_ms` is supplied, the resolved version must also satisfy
    /// `valid_from_ms ≤ valid_at_ms < valid_until_ms`; otherwise the method
    /// continues scanning to earlier system-time versions.
    pub fn ceiling_resolve_edge(
        &self,
        edge: EdgeRef<'_>,
        system_as_of: i64,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        if system_as_of < 0 {
            return Err(crate::Error::BadRequest {
                detail: format!("ceiling_resolve_edge: negative system_as_of={system_as_of}"),
            });
        }
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper = versioned_edge_key(
            edge.collection,
            edge.src,
            edge.label,
            edge.dst,
            system_as_of,
        )?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| redb_err("begin_read", e))?;
        let table = read_txn
            .open_table(EDGES)
            .map_err(|e| redb_err("open edges", e))?;

        // Inclusive upper — the exact key at system_as_of is a valid ceiling.
        let range = table
            .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
            .map_err(|e| redb_err("ceiling range", e))?;

        // Walk newest-first by reversing the iterator.
        for entry in range.rev() {
            let (k, v) = entry.map_err(|e| redb_err("ceiling iter", e))?;
            let (kd, kt, composite) = k.value();
            if kd != d || kt != t || !composite.starts_with(&prefix) {
                break;
            }
            let bytes = v.value();
            if is_tombstone(bytes) || is_gdpr_erasure(bytes) {
                return Ok(None);
            }
            let payload = EdgeValuePayload::decode(bytes)?;
            match valid_at_ms {
                Some(vt) if !(payload.valid_from_ms <= vt && vt < payload.valid_until_ms) => {
                    // This system-time version didn't assert the fact at `vt` —
                    // scan older versions.
                    continue;
                }
                _ => return Ok(Some(payload.properties)),
            }
        }
        Ok(None)
    }
}

/// Decode a raw edge value to an [`Edge`] projection, treating sentinels as
/// absent. Used by future current-state scanners.
#[allow(dead_code)]
pub(crate) fn edge_from_versioned_entry(
    composite: &str,
    value: &[u8],
) -> Option<(Edge, EdgeValuePayload)> {
    if is_sentinel(value) {
        return None;
    }
    let (collection, src, label, dst, _sys) = parse_versioned_edge_key(composite)?;
    let payload = EdgeValuePayload::decode(value).ok()?;
    Some((
        Edge {
            collection: collection.to_string(),
            src_id: src.to_string(),
            label: label.to_string(),
            dst_id: dst.to_string(),
            properties: payload.properties.clone(),
        },
        payload,
    ))
}

#[cfg(test)]
mod tests {
    use nodedb_types::{DatabaseId, TenantId};

    use super::*;

    const T: TenantId = TenantId::new(1);
    const DB: DatabaseId = DatabaseId::DEFAULT;
    const COLL: &str = "people";

    fn make_store() -> (EdgeStore, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let store = EdgeStore::open(&dir.path().join("graph.redb")).unwrap();
        (store, dir)
    }

    fn e<'a>(src: &'a str, label: &'a str, dst: &'a str) -> EdgeRef<'a> {
        EdgeRef::new(DB, T, COLL, src, label, dst)
    }

    #[test]
    fn put_and_ceiling_resolves_latest_at_cutoff() {
        let (store, _dir) = make_store();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 100, 100, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 200, 200, i64::MAX)
            .unwrap();
        store
            .put_edge_versioned(e("a", "L", "b"), b"v3", 300, 300, i64::MAX)
            .unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 99, None)
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 100, None)
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 250, None)
                .unwrap(),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, None)
                .unwrap(),
            Some(b"v3".to_vec())
        );
    }

    #[test]
    fn valid_time_filter_skips_nonmatching_versions() {
        let (store, _dir) = make_store();
        // v1: valid_time [0, 100)
        store
            .put_edge_versioned(e("a", "L", "b"), b"v1", 10, 0, 100)
            .unwrap();
        // v2: valid_time [200, 300)  — disjoint hole between 100 and 200
        store
            .put_edge_versioned(e("a", "L", "b"), b"v2", 20, 200, 300)
            .unwrap();

        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(150))
                .unwrap(),
            None
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(50))
                .unwrap(),
            Some(b"v1".to_vec())
        );
        assert_eq!(
            store
                .ceiling_resolve_edge(e("a", "L", "b"), 1_000, Some(250))
                .unwrap(),
            Some(b"v2".to_vec())
        );
    }
}
