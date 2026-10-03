// SPDX-License-Identifier: BUSL-1.1

//! Committed offsets as raw rows, for the metadata group snapshot image.
//!
//! Offsets commit through metadata Raft group 0, so a group 0 snapshot
//! carries them. The cache is rebuilt from the table after a replace.

use std::collections::HashMap;

use redb::{Database, ReadTransaction, ReadableDatabase, ReadableTable};
use tracing::debug;

use super::super::codec::{decode_offset, parse_offset_key};
use super::{GroupKey, OFFSETS, OffsetStore};
use crate::event::cdc::offset::CdcOffset;

fn storage_err(what: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "event_plane".into(),
        detail: format!("{what}: {e}"),
    }
}

/// A read transaction on the offset store, held to dump it at one commit
/// point.
pub struct OffsetImageRead {
    txn: ReadTransaction,
}

impl OffsetImageRead {
    /// Every committed offset row: `(key, encoded offset)` in key order.
    pub fn dump(&self) -> crate::Result<Vec<(String, Vec<u8>)>> {
        let table = self
            .txn
            .open_table(OFFSETS)
            .map_err(|e| storage_err("offset image: open table", e))?;
        let mut rows = Vec::new();
        for item in table
            .iter()
            .map_err(|e| storage_err("offset image: iterate", e))?
        {
            let (key, value) = item.map_err(|e| storage_err("offset image: read row", e))?;
            rows.push((key.value().to_string(), value.value().to_vec()));
        }
        Ok(rows)
    }
}

impl OffsetStore {
    /// Open a read transaction for [`OffsetImageRead::dump`].
    pub fn begin_image_read(&self) -> crate::Result<OffsetImageRead> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| storage_err("offset image: begin read", e))?;
        Ok(OffsetImageRead { txn })
    }

    /// Replace every committed offset with `rows` in one write transaction,
    /// then rebuild the cache from the table.
    pub fn replace_all_offsets(&self, rows: &[(String, Vec<u8>)]) -> crate::Result<()> {
        let _guard = self.mutation_lock.lock().unwrap_or_else(|p| p.into_inner());
        let txn = self
            .db
            .begin_write()
            .map_err(|e| storage_err("offset image: begin write", e))?;
        {
            let mut table = txn
                .open_table(OFFSETS)
                .map_err(|e| storage_err("offset image: open table", e))?;
            table
                .retain(|_, _| false)
                .map_err(|e| storage_err("offset image: clear", e))?;
            for (key, value) in rows {
                table
                    .insert(key.as_str(), value.as_slice())
                    .map_err(|e| storage_err("offset image: insert", e))?;
            }
        }
        txn.commit()
            .map_err(|e| storage_err("offset image: commit", e))?;
        let cache = load_cache(&self.db)?;
        *self.cache.write().unwrap_or_else(|p| p.into_inner()) = cache;
        Ok(())
    }
}

/// Every committed offset in `db`, keyed by group.
pub(super) fn load_cache(
    db: &Database,
) -> crate::Result<HashMap<GroupKey, HashMap<u32, CdcOffset>>> {
    let txn = db.begin_read().map_err(|e| storage_err("begin_read", e))?;
    let table = txn
        .open_table(OFFSETS)
        .map_err(|e| storage_err("open_table", e))?;
    let mut cache: HashMap<GroupKey, HashMap<u32, CdcOffset>> = HashMap::new();
    for item in table
        .iter()
        .map_err(|e| storage_err("iterate offsets", e))?
    {
        let (key_guard, value_guard) = item.map_err(|e| storage_err("read offset row", e))?;
        let key_str: &str = key_guard.value();
        let Some(offset) = decode_offset(value_guard.value()) else {
            continue;
        };
        if let Some((database_id, tenant, stream, group, partition)) = parse_offset_key(key_str) {
            cache
                .entry((database_id, tenant, stream, group))
                .or_default()
                .insert(partition, offset);
        }
    }
    let total: usize = cache.values().map(|m| m.len()).sum();
    if total > 0 {
        debug!(offsets = total, "loaded consumer offsets from redb");
    }
    Ok(cache)
}
