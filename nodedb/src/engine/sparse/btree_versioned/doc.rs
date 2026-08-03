// SPDX-License-Identifier: BUSL-1.1

//! Document-level operations on the versioned document table.

use redb::{ReadableDatabase, ReadableTable, TableDefinition};

use super::key::{
    coll_prefix, coll_prefix_end, doc_prefix, doc_prefix_end, format_sys_from, versioned_doc_key,
};
use super::value::{
    TAG_GDPR_ERASED, TAG_LIVE, TAG_TOMBSTONE, VersionedPut, VersionedScanParams, decode_value,
    encode_value,
};
use crate::engine::sparse::btree::{SparseEngine, redb_err};

/// One row of an audit-log (`AS OF SYSTEM TIME NULL`) scan: a single
/// system-time version of a document plus the temporal coordinates read from
/// the row's real stored envelope. `valid_from_ms` / `valid_until_ms` carry the
/// raw envelope sentinels (`i64::MIN` / `i64::MAX` when unbounded) so the
/// handler can surface them uniformly across engines.
pub struct VersionedRow {
    pub doc_id: String,
    pub system_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
    pub body: Vec<u8>,
}

/// Versioned document table. Distinct from `super::super::btree::DOCUMENTS`
/// so bitemporal and current-only collections coexist. Keys carry the leading
/// `{database_id}:` component.
pub(crate) const DOCUMENTS_VERSIONED: TableDefinition<&str, &[u8]> =
    TableDefinition::new("documents_versioned");

impl SparseEngine {
    /// Bootstrap: ensure the versioned document table exists. Called by
    /// `open()` alongside [`super::index::ensure_indexes_versioned_table`].
    pub(in crate::engine::sparse) fn ensure_documents_versioned_table(&self) -> crate::Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let _ = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open documents_versioned", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// Append one version. Always creates a new key; never overwrites an
    /// earlier version at the same `sys_from_ms`.
    pub fn versioned_put(&self, p: VersionedPut<'_>) -> crate::Result<()> {
        let key = versioned_doc_key(p.database_id, p.tenant, p.coll, p.doc_id, p.sys_from_ms)?;
        let val = encode_value(TAG_LIVE, p.valid_from_ms, p.valid_until_ms, p.body);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            t.insert(key.as_str(), val.as_slice())
                .map_err(|e| redb_err("insert", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_put` inside a caller-owned write transaction. Used by
    /// composite write paths (PointPut / UPSERT / UPDATE on bitemporal
    /// collections) so the document + its versioned index entries + the
    /// current-state side-effects (stats, text/vector/spatial indexes,
    /// document cache) commit atomically.
    pub fn versioned_put_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        p: VersionedPut<'_>,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(p.database_id, p.tenant, p.coll, p.doc_id, p.sys_from_ms)?;
        let val = encode_value(TAG_LIVE, p.valid_from_ms, p.valid_until_ms, p.body);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), val.as_slice())
            .map_err(|e| redb_err("insert", e))?;
        Ok(())
    }

    /// Append a tombstone version.
    pub fn versioned_tombstone(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms)?;
        let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            t.insert(key.as_str(), val.as_slice())
                .map_err(|e| redb_err("insert tombstone", e))?;
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(())
    }

    /// `versioned_tombstone` inside a caller-owned write transaction.
    pub fn versioned_tombstone_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms)?;
        let val = encode_value(TAG_TOMBSTONE, 0, 0, &[]);
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.insert(key.as_str(), val.as_slice())
            .map_err(|e| redb_err("insert tombstone", e))?;
        Ok(())
    }

    /// Physically remove one version's redb entry inside a caller-owned
    /// write transaction. Unlike [`Self::versioned_tombstone_in_txn`] (which
    /// appends a tombstone marker version), this deletes the entry at
    /// `sys_from_ms` outright. Used by the transaction-rollback path to
    /// undo a `versioned_put_in_txn` that must not survive an aborted
    /// transaction. Removing a non-existent key is a no-op.
    pub fn versioned_remove_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
        sys_from_ms: i64,
    ) -> crate::Result<()> {
        let key = versioned_doc_key(database_id, tenant, coll, doc_id, sys_from_ms)?;
        let mut t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        t.remove(key.as_str()).map_err(|e| redb_err("remove", e))?;
        Ok(())
    }

    /// Check whether the current-state version of `doc_id` is live
    /// (not tombstoned, not GDPR-erased, and at least one version
    /// exists) inside a caller-owned write transaction. Used by
    /// PointInsert on bitemporal collections to enforce primary-key
    /// uniqueness linearizably with the subsequent `versioned_put`.
    pub fn versioned_exists_current_in_txn(
        &self,
        txn: &redb::WriteTransaction,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
    ) -> crate::Result<bool> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;
        let mut last: Option<Vec<u8>> = None;
        for r in range {
            let (_k, v) = r.map_err(|e| redb_err("entry", e))?;
            last = Some(v.value().to_vec());
        }
        match last {
            Some(bytes) => Ok(decode_value(&bytes)?.is_live()),
            None => Ok(false),
        }
    }

    /// GDPR erasure: replace the body of every existing version for a
    /// doc_id with an empty body tagged `0xFE`. Preserves history
    /// structure (so AS-OF queries still see the doc existed) but removes
    /// personal data.
    pub fn versioned_gdpr_erase(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
    ) -> crate::Result<usize> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = doc_prefix_end(database_id, tenant, coll, doc_id);
        let txn = self
            .db
            .begin_write()
            .map_err(|e| redb_err("write txn", e))?;
        let mut replaced = 0;
        {
            let mut t = txn
                .open_table(DOCUMENTS_VERSIONED)
                .map_err(|e| redb_err("open table", e))?;
            let keys: Vec<String> = t
                .range(lo.as_str()..hi.as_str())
                .map_err(|e| redb_err("range", e))?
                .filter_map(|r| r.ok().map(|(k, _)| k.value().to_string()))
                .collect();
            for k in keys {
                let erased = encode_value(TAG_GDPR_ERASED, 0, 0, &[]);
                t.insert(k.as_str(), erased.as_slice())
                    .map_err(|e| redb_err("erase insert", e))?;
                replaced += 1;
            }
        }
        txn.commit().map_err(|e| redb_err("commit", e))?;
        Ok(replaced)
    }

    /// Ceiling read: most recent version at or before `sys_cutoff_ms`.
    /// Returns `Ok(None)` when nothing is present, or the newest entry is
    /// a tombstone / GDPR-erased (both hide the row from normal reads).
    pub fn versioned_get_as_of(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
        sys_cutoff_ms: Option<i64>,
        valid_at_ms: Option<i64>,
    ) -> crate::Result<Option<Vec<u8>>> {
        let lo = doc_prefix(database_id, tenant, coll, doc_id);
        let hi = match sys_cutoff_ms {
            Some(ms) => versioned_doc_key(database_id, tenant, coll, doc_id, ms)?,
            None => doc_prefix_end(database_id, tenant, coll, doc_id),
        };
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = match sys_cutoff_ms {
            Some(_) => t
                .range(lo.as_str()..=hi.as_str())
                .map_err(|e| redb_err("range", e))?,
            None => t
                .range(lo.as_str()..hi.as_str())
                .map_err(|e| redb_err("range", e))?,
        };
        let mut entries: Vec<Vec<u8>> = Vec::new();
        for r in range {
            let (_k, v) = r.map_err(|e| redb_err("entry", e))?;
            entries.push(v.value().to_vec());
        }
        // Iterate in reverse to pick the newest version ≤ cutoff whose
        // valid-time predicate holds.
        for v in entries.into_iter().rev() {
            let decoded = decode_value(&v)?;
            if !decoded.is_live() {
                // Ceiling hit a tombstone / erasure as the newest entry —
                // the row is absent at this cutoff, even if live versions
                // exist further back.
                return Ok(None);
            }
            if let Some(vt) = valid_at_ms
                && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
            {
                continue;
            }
            return Ok(Some(decoded.body.to_vec()));
        }
        Ok(None)
    }

    /// Current-state read = `versioned_get_as_of(None, None)`.
    pub fn versioned_get_current(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        doc_id: &str,
    ) -> crate::Result<Option<Vec<u8>>> {
        self.versioned_get_as_of(database_id, tenant, coll, doc_id, None, None)
    }

    /// Scan every doc_id in a collection at the requested cutoff.
    /// Returns `(doc_id, body)` pairs for live versions only. O(N)
    /// collection-wide; callers add filter/limit on top.
    /// `predicate` is evaluated against each surviving version's document body
    /// before it counts toward `limit`, so a selective filter never causes the
    /// scan to early-stop with fewer matching rows than exist. Pass `&|_| true`
    /// for an unfiltered scan.
    pub fn versioned_scan_as_of(
        &self,
        params: VersionedScanParams<'_>,
        predicate: &dyn Fn(&[u8]) -> bool,
    ) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let VersionedScanParams {
            database_id,
            tenant,
            coll,
            sys_cutoff_ms,
            valid_at_ms,
            limit,
        } = params;
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let cutoff_key = sys_cutoff_ms.map(format_sys_from);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;

        // Group entries by doc_id; keep the newest-in-window per group.
        // The table is sorted by (doc_id, sys_from) ascending so we can
        // stream and flush whenever doc_id changes.
        let mut out = Vec::new();
        let mut current_id: Option<String> = None;
        let mut best_for_current: Option<(i64, Vec<u8>)> = None;

        for r in range {
            let (k, v) = r.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let Some((id_part, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Some(doc_id) = id_part.strip_prefix(lo.as_str()) else {
                continue;
            };
            if let Some(ref c) = cutoff_key
                && suffix > c.as_str()
            {
                continue;
            }
            let Ok(sf) = suffix.parse::<i64>() else {
                continue;
            };
            if current_id.as_deref() != Some(doc_id) {
                if let Some(prev_id) = current_id.as_ref() {
                    flush_scan(prev_id, &best_for_current, valid_at_ms, predicate, &mut out)?;
                    if out.len() >= limit {
                        return Ok(out);
                    }
                }
                current_id = Some(doc_id.to_string());
                best_for_current = None;
            }
            let val = v.value().to_vec();
            // Keep the newest entry for this doc_id (largest sys_from).
            best_for_current = Some(match best_for_current.take() {
                Some((prev_sf, prev_v)) if prev_sf >= sf => (prev_sf, prev_v),
                _ => (sf, val),
            });
        }
        if let Some(prev_id) = current_id.as_ref() {
            flush_scan(prev_id, &best_for_current, valid_at_ms, predicate, &mut out)?;
        }
        Ok(out)
    }

    /// Audit-log scan: every live system-time version of every doc in the
    /// collection, ordered ascending by `sys_from_ms` (ties broken by
    /// `doc_id`). Tombstone / GDPR-erased versions are skipped. Unlike
    /// [`Self::versioned_scan_as_of`] this does **not** collapse to the
    /// newest-per-id — it yields the full history (`AS OF SYSTEM TIME NULL`).
    ///
    /// Returns [`VersionedRow`] values carrying `doc_id`, `system_from_ms`, the
    /// row's stored valid-time interval, and `body`. The handler projects these
    /// into the output as the synthetic temporal columns.
    ///
    /// `predicate` is evaluated against each version's document body **before**
    /// the `limit` truncation, so a selective filter never causes the scan to
    /// return fewer rows than exist (the caller must push its scan filters in
    /// here rather than filtering the truncated result). Pass `&|_| true` for
    /// an unfiltered scan.
    pub fn versioned_scan_all(
        &self,
        database_id: u64,
        tenant: u64,
        coll: &str,
        valid_at_ms: Option<i64>,
        limit: usize,
        predicate: &dyn Fn(&[u8]) -> bool,
    ) -> crate::Result<Vec<VersionedRow>> {
        let lo = coll_prefix(database_id, tenant, coll);
        let hi = coll_prefix_end(database_id, tenant, coll);
        let txn = self.db.begin_read().map_err(|e| redb_err("read txn", e))?;
        let t = txn
            .open_table(DOCUMENTS_VERSIONED)
            .map_err(|e| redb_err("open table", e))?;
        let range = t
            .range(lo.as_str()..hi.as_str())
            .map_err(|e| redb_err("range", e))?;

        let mut all: Vec<VersionedRow> = Vec::new();
        for r in range {
            let (k, v) = r.map_err(|e| redb_err("entry", e))?;
            let key_str = k.value();
            let Some((id_part, suffix)) = key_str.rsplit_once('\x00') else {
                continue;
            };
            let Some(doc_id) = id_part.strip_prefix(lo.as_str()) else {
                continue;
            };
            let Ok(sf) = suffix.parse::<i64>() else {
                continue;
            };
            let bytes = v.value().to_vec();
            let decoded = decode_value(&bytes)?;
            if !decoded.is_live() {
                continue;
            }
            if let Some(vt) = valid_at_ms
                && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
            {
                continue;
            }
            // Push the caller's scan filters down here so the `limit` truncation
            // below counts only matching versions, never raw scanned rows.
            if !predicate(decoded.body) {
                continue;
            }
            all.push(VersionedRow {
                doc_id: doc_id.to_string(),
                system_from_ms: sf,
                valid_from_ms: decoded.valid_from_ms,
                valid_until_ms: decoded.valid_until_ms,
                body: decoded.body.to_vec(),
            });
        }
        // Global ascending order by system time; deterministic on ties.
        all.sort_by(|a, b| {
            a.system_from_ms
                .cmp(&b.system_from_ms)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        all.truncate(limit);
        Ok(all)
    }
}

/// Emit the newest-per-doc-id entry into `out` if it's live and passes
/// the valid-time predicate.
fn flush_scan(
    id: &str,
    pick: &Option<(i64, Vec<u8>)>,
    valid_at_ms: Option<i64>,
    predicate: &dyn Fn(&[u8]) -> bool,
    out: &mut Vec<(String, Vec<u8>)>,
) -> crate::Result<()> {
    let Some((_sf, v)) = pick else { return Ok(()) };
    let decoded = decode_value(v)?;
    if !decoded.is_live() {
        return Ok(());
    }
    if let Some(vt) = valid_at_ms
        && (vt < decoded.valid_from_ms || vt >= decoded.valid_until_ms)
    {
        return Ok(());
    }
    // Caller's scan filters are pushed down here so they are applied before the
    // row counts toward the scan's `limit`.
    if !predicate(decoded.body) {
        return Ok(());
    }
    out.push((id.to_string(), decoded.body.to_vec()));
    Ok(())
}
