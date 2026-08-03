// SPDX-License-Identifier: BUSL-1.1

//! Ceiling resolver — reverse range scan with optional valid-time filter.

use super::keys::{
    EdgeRef, edge_version_prefix, is_gdpr_erasure, is_sentinel, is_tombstone,
    parse_versioned_edge_key, versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use crate::engine::graph::edge_store::store::{EDGES, Edge, EdgeStore, redb_err};
use redb::ReadableDatabase;

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
