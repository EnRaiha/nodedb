// SPDX-License-Identifier: BUSL-1.1

//! Structural collection and tenant drops.
//!
//! Each table is scanned by tuple range and every matching entry is
//! removed; no lexical-prefix scans appear here. `purge_tenant` performs
//! the drop in a single write transaction so the teardown is atomic
//! across tables.
//!
//! A tenant purge is scoped to a single `(database_id, tenant_id)` pair —
//! the same tenant in a different database is unaffected.

use redb::ReadableTable;

use super::core::RedbFtsBackend;
use super::shared::{MAX_COLLECTION, MAX_SUBKEY, redb_err};
use crate::engine::sparse::fts_redb::tables::{
    DOC_LENGTHS, DOC_TERMS, INDEX_META, POSTINGS, SEGMENTS, STATS,
};

pub(super) fn collection(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    coll: &str,
) -> crate::Result<usize> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("purge write txn", e))?;
    let mut removed = 0;

    {
        let mut table = write_txn
            .open_table(POSTINGS)
            .map_err(|e| redb_err("open postings", e))?;
        let keys: Vec<String> = table
            .range((database_id, tid, coll, "")..=(database_id, tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("postings range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().3.to_string()))
            .collect();
        removed += keys.len();
        for term in &keys {
            let _ = table.remove((database_id, tid, coll, term.as_str()));
        }
    }

    // DOC_LENGTHS and DOC_TERMS are keyed by (u64, u64, &str, u32) — use
    // numeric range bounds. Both are per-document rows for this collection and
    // must go together: a surviving term set would name posting lists that no
    // longer exist.
    removed += drop_surrogate_quad_collection(&write_txn, DOC_LENGTHS, database_id, tid, coll)?;
    removed += drop_surrogate_quad_collection(&write_txn, DOC_TERMS, database_id, tid, coll)?;

    {
        let mut table = write_txn
            .open_table(INDEX_META)
            .map_err(|e| redb_err("open index_meta", e))?;
        let keys: Vec<String> = table
            .range((database_id, tid, coll, "")..=(database_id, tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("meta range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().3.to_string()))
            .collect();
        for sub in &keys {
            let _ = table.remove((database_id, tid, coll, sub.as_str()));
        }
    }

    {
        let mut table = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let _ = table.remove((database_id, tid, coll));
    }

    {
        let mut table = write_txn
            .open_table(SEGMENTS)
            .map_err(|e| redb_err("open segments", e))?;
        let ids: Vec<String> = table
            .range((database_id, tid, coll, "")..=(database_id, tid, coll, MAX_SUBKEY))
            .map_err(|e| redb_err("segments range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().3.to_string()))
            .collect();
        removed += ids.len();
        for id in &ids {
            let _ = table.remove((database_id, tid, coll, id.as_str()));
        }
    }

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge", e))?;
    Ok(removed)
}

pub(super) fn tenant(backend: &RedbFtsBackend, database_id: u64, tid: u64) -> crate::Result<usize> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("purge_tenant write txn", e))?;
    let mut removed = 0;

    removed += drop_str_quad_range(&write_txn, POSTINGS, database_id, tid)?;
    removed += drop_surrogate_quad_tenant(&write_txn, DOC_LENGTHS, database_id, tid)?;
    removed += drop_surrogate_quad_tenant(&write_txn, DOC_TERMS, database_id, tid)?;
    let _ = drop_str_quad_range(&write_txn, INDEX_META, database_id, tid)?;

    {
        let mut stats = write_txn
            .open_table(STATS)
            .map_err(|e| redb_err("open stats", e))?;
        let colls: Vec<String> = stats
            .range((database_id, tid, "")..=(database_id, tid, MAX_COLLECTION))
            .map_err(|e| redb_err("stats range", e))?
            .filter_map(|r| r.ok().map(|(k, _)| k.value().2.to_string()))
            .collect();
        for c in &colls {
            let _ = stats.remove((database_id, tid, c.as_str()));
        }
    }

    removed += drop_str_quad_range(&write_txn, SEGMENTS, database_id, tid)?;

    write_txn
        .commit()
        .map_err(|e| redb_err("commit purge_tenant", e))?;
    Ok(removed)
}

/// Delete every `(database_id, tid, *, *)` row from a
/// `TableDefinition<(u64, u64, &str, &str), &[u8]>`.
fn drop_str_quad_range(
    txn: &redb::WriteTransaction,
    def: redb::TableDefinition<(u64, u64, &str, &str), &[u8]>,
    database_id: u64,
    tid: u64,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err("open quad table", e))?;
    let keys: Vec<(String, String)> = table
        .range((database_id, tid, "", "")..=(database_id, tid, MAX_COLLECTION, MAX_SUBKEY))
        .map_err(|e| redb_err("quad range", e))?
        .filter_map(|r| {
            r.ok().map(|(k, _)| {
                let (_, _, c, s) = k.value();
                (c.to_string(), s.to_string())
            })
        })
        .collect();
    let n = keys.len();
    for (c, s) in &keys {
        let _ = table.remove((database_id, tid, c.as_str(), s.as_str()));
    }
    Ok(n)
}

/// Delete every `(database_id, tid, coll, *)` row from a per-document table
/// keyed by `(u64, u64, &str, u32)` (DOC_LENGTHS, DOC_TERMS).
fn drop_surrogate_quad_collection(
    txn: &redb::WriteTransaction,
    def: redb::TableDefinition<(u64, u64, &str, u32), &[u8]>,
    database_id: u64,
    tid: u64,
    coll: &str,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err("open surrogate-keyed table", e))?;
    let surrogates: Vec<u32> = table
        .range((database_id, tid, coll, 0u32)..=(database_id, tid, coll, u32::MAX))
        .map_err(|e| redb_err("surrogate-keyed collection range", e))?
        .filter_map(|r| r.ok().map(|(k, _)| k.value().3))
        .collect();
    let n = surrogates.len();
    for s in surrogates {
        let _ = table.remove((database_id, tid, coll, s));
    }
    Ok(n)
}

/// Delete every `(database_id, tid, *, *)` row from a per-document table
/// keyed by `(u64, u64, &str, u32)` (DOC_LENGTHS, DOC_TERMS).
fn drop_surrogate_quad_tenant(
    txn: &redb::WriteTransaction,
    def: redb::TableDefinition<(u64, u64, &str, u32), &[u8]>,
    database_id: u64,
    tid: u64,
) -> crate::Result<usize> {
    let mut table = txn
        .open_table(def)
        .map_err(|e| redb_err("open surrogate-keyed table", e))?;
    let keys: Vec<(String, u32)> = table
        .range((database_id, tid, "", 0u32)..=(database_id, tid, MAX_COLLECTION, u32::MAX))
        .map_err(|e| redb_err("surrogate-keyed tenant range", e))?
        .filter_map(|r| {
            r.ok().map(|(k, _)| {
                let (_, _, c, s) = k.value();
                (c.to_string(), s)
            })
        })
        .collect();
    let n = keys.len();
    for (c, s) in &keys {
        let _ = table.remove((database_id, tid, c.as_str(), *s));
    }
    Ok(n)
}
