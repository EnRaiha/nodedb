// SPDX-License-Identifier: BUSL-1.1

//! Opaque metadata blobs (docmap, fieldnorms, analyzer, language)
//! against `INDEX_META` keyed by `(database_id, tenant_id, collection, subkey)`.

use super::core::RedbFtsBackend;
use super::shared::redb_err;
use crate::engine::sparse::fts_redb::tables::INDEX_META;
use redb::ReadableDatabase;

pub(super) fn read(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    collection: &str,
    subkey: &str,
) -> crate::Result<Option<Vec<u8>>> {
    let read_txn = backend
        .db
        .begin_read()
        .map_err(|e| redb_err("read txn", e))?;
    let table = read_txn
        .open_table(INDEX_META)
        .map_err(|e| redb_err("open index_meta", e))?;
    match table.get((database_id, tid, collection, subkey)) {
        Ok(Some(val)) => Ok(Some(val.value().to_vec())),
        Ok(None) => Ok(None),
        Err(e) => Err(redb_err("get meta", e)),
    }
}

pub(super) fn write(
    backend: &RedbFtsBackend,
    database_id: u64,
    tid: u64,
    collection: &str,
    subkey: &str,
    value: &[u8],
) -> crate::Result<()> {
    let write_txn = backend
        .db
        .begin_write()
        .map_err(|e| redb_err("write txn", e))?;
    {
        let mut table = write_txn
            .open_table(INDEX_META)
            .map_err(|e| redb_err("open index_meta", e))?;
        table
            .insert((database_id, tid, collection, subkey), value)
            .map_err(|e| redb_err("insert meta", e))?;
    }
    write_txn.commit().map_err(|e| redb_err("commit", e))?;
    Ok(())
}
