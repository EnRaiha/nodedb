// SPDX-License-Identifier: BUSL-1.1

//! Bitemporal write paths on `EdgeStore`:
//! `put_edge_versioned`, `soft_delete_edge`, `gdpr_erase_edge`.

use super::keys::{
    EdgeRef, GDPR_ERASURE_SENTINEL, TOMBSTONE_SENTINEL, edge_version_prefix, is_sentinel,
    versioned_edge_key,
};
use super::payload::EdgeValuePayload;
use crate::engine::graph::edge_store::stats::update::{decrement_for_delete, increment_for_insert};
use crate::engine::graph::edge_store::store::{EDGES, EdgeStore, REVERSE_EDGES, redb_err};

impl EdgeStore {
    /// Write a new version of an edge at `system_from`. Maintains
    /// the reverse index with the same suffix so inbound traversal can
    /// version-scan symmetrically.
    ///
    /// Does NOT close prior versions' `system_until` — Ceiling infers the
    /// closed-open interval at read time from the next-newer version's
    /// `system_from`.
    pub fn put_edge_versioned(
        &self,
        edge: EdgeRef<'_>,
        properties: &[u8],
        system_from: i64,
        valid_from_ms: i64,
        valid_until_ms: i64,
    ) -> crate::Result<()> {
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
        let payload =
            EdgeValuePayload::new(valid_from_ms, valid_until_ms, properties.to_vec()).encode()?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((d, t, fwd.as_str()), payload.as_slice())
                .map_err(|e| redb_err("insert versioned edge", e))?;
            drop(edges);

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((d, t, rev.as_str()), &[] as &[u8])
                .map_err(|e| redb_err("insert reverse", e))?;
            drop(rev_t);

            increment_for_insert(
                &write_txn,
                d,
                t,
                edge.collection,
                edge.label,
                edge.src,
                edge.dst,
                system_from,
            )?;
        }
        write_txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// BiTemporalFK enforcement: close a referrer edge by appending a new
    /// version that copies the latest live version's properties and
    /// `valid_from_ms`, but bounds `valid_until_ms` to `now`. Preserves
    /// historical truth — the edge existed in valid time `[valid_from, now)`.
    ///
    /// Returns `Ok(false)` when no live version exists (already closed,
    /// tombstoned, GDPR-erased, or never written) — the caller may treat
    /// this as a no-op.
    pub fn close_referrer_edge(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        now_valid_ms: i64,
    ) -> crate::Result<bool> {
        let prefix = edge_version_prefix(edge.collection, edge.src, edge.label, edge.dst);
        let upper =
            versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let prior = {
            let read_txn = self
                .db
                .begin_read()
                .map_err(|e| redb_err("begin_read", e))?;
            let table = read_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            let range = table
                .range((d, t, prefix.as_str())..=(d, t, upper.as_str()))
                .map_err(|e| redb_err("close referrer range", e))?;
            let mut found: Option<EdgeValuePayload> = None;
            for entry in range.rev() {
                let (k, v) = entry.map_err(|e| redb_err("close referrer iter", e))?;
                let (kd, kt, composite) = k.value();
                if kd != d || kt != t || !composite.starts_with(&prefix) {
                    break;
                }
                let bytes = v.value();
                if is_sentinel(bytes) {
                    return Ok(false);
                }
                let payload = EdgeValuePayload::decode(bytes)?;
                if payload.valid_from_ms <= now_valid_ms && now_valid_ms < payload.valid_until_ms {
                    found = Some(payload);
                    break;
                }
            }
            match found {
                Some(p) => p,
                None => return Ok(false),
            }
        };

        self.put_edge_versioned(
            edge,
            &prior.properties,
            system_from,
            prior.valid_from_ms,
            now_valid_ms,
        )?;
        Ok(true)
    }

    /// Append a tombstone version at `system_from`.
    pub fn soft_delete_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, TOMBSTONE_SENTINEL)
    }

    /// Append a GDPR-erasure version — distinct from a soft-delete so audits
    /// can distinguish user-visible removal from regulatory erasure.
    pub fn gdpr_erase_edge(&self, edge: EdgeRef<'_>, system_from: i64) -> crate::Result<()> {
        self.write_sentinel(edge, system_from, GDPR_ERASURE_SENTINEL)
    }

    fn write_sentinel(
        &self,
        edge: EdgeRef<'_>,
        system_from: i64,
        sentinel: &[u8],
    ) -> crate::Result<()> {
        debug_assert!(
            is_sentinel(sentinel),
            "write_sentinel called with non-sentinel bytes"
        );
        let fwd = versioned_edge_key(edge.collection, edge.src, edge.label, edge.dst, system_from)?;
        let rev = versioned_edge_key(edge.collection, edge.dst, edge.label, edge.src, system_from)?;
        let d = edge.db.as_u64();
        let t = edge.tid.as_u64();

        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("begin_write", e))?;
        {
            let mut edges = write_txn
                .open_table(EDGES)
                .map_err(|e| redb_err("open edges", e))?;
            edges
                .insert((d, t, fwd.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel edge", e))?;
            drop(edges);

            let mut rev_t = write_txn
                .open_table(REVERSE_EDGES)
                .map_err(|e| redb_err("open reverse", e))?;
            rev_t
                .insert((d, t, rev.as_str()), sentinel)
                .map_err(|e| redb_err("insert sentinel reverse", e))?;
            drop(rev_t);

            decrement_for_delete(
                &write_txn,
                d,
                t,
                edge.collection,
                edge.label,
                edge.src,
                edge.dst,
                system_from,
            )?;
        }
        write_txn
            .commit()
            .map_err(|e| redb_err("commit sentinel", e))?;
        Ok(())
    }
}
