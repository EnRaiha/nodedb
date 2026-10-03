// SPDX-License-Identifier: BUSL-1.1

//! One redb table of MessagePack records keyed by a `u64` id.
//!
//! Each Event Plane queue that keeps records in its own redb file uses this
//! store for the file, the table, and the codec. The queue keeps its
//! in-memory index and mutates it only after a store call returns `Ok`, so
//! memory never holds a change redb refused.

use std::collections::VecDeque;
use std::marker::PhantomData;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tracing::warn;

/// Directory under the data directory that holds Event Plane redb files.
const EVENT_PLANE_DIR: &str = "event_plane";

/// Records read back from a store, in ascending key order.
pub struct LoadedRecords<T> {
    /// Every record that decoded. A record that fails to decode is skipped.
    pub records: VecDeque<T>,
    /// One past the largest key present, counting skipped records.
    pub next_key: u64,
}

/// A redb table of `u64` keys and MessagePack-encoded `T` values.
pub struct RedbStore<T> {
    db: Database,
    table: TableDefinition<'static, u64, &'static [u8]>,
    /// Store name used in error details and logs.
    name: &'static str,
    _record: PhantomData<fn() -> T>,
}

impl<T> RedbStore<T>
where
    T: zerompk::ToMessagePack + for<'a> zerompk::FromMessagePack<'a>,
{
    /// Open or create `{data_dir}/event_plane/{file_name}` and its table.
    pub fn open(
        data_dir: &Path,
        file_name: &str,
        table: TableDefinition<'static, u64, &'static [u8]>,
        name: &'static str,
    ) -> crate::Result<Self> {
        let dir = data_dir.join(EVENT_PLANE_DIR);
        std::fs::create_dir_all(&dir).map_err(|e| crate::Error::Storage {
            engine: EVENT_PLANE_DIR.into(),
            detail: format!("{name}: create dir {}: {e}", dir.display()),
        })?;
        let path = dir.join(file_name);
        let db = Database::create(&path).map_err(|e| crate::Error::Storage {
            engine: EVENT_PLANE_DIR.into(),
            detail: format!("{name}: open db {}: {e}", path.display()),
        })?;

        let store = Self {
            db,
            table,
            name,
            _record: PhantomData,
        };
        let txn = store
            .db
            .begin_write()
            .map_err(|e| store.err("begin_write", e))?;
        txn.open_table(store.table)
            .map_err(|e| store.err("open_table", e))?;
        txn.commit().map_err(|e| store.err("commit", e))?;
        Ok(store)
    }

    /// Read every record. A record that fails to decode is logged and
    /// skipped. Its key still counts toward `next_key`, so a new record never
    /// reuses it.
    pub fn load(&self) -> crate::Result<LoadedRecords<T>> {
        let txn = self
            .db
            .begin_read()
            .map_err(|e| self.err("begin_read", e))?;
        let table = txn
            .open_table(self.table)
            .map_err(|e| self.err("open_table", e))?;
        let iter = table.iter().map_err(|e| self.err("iter", e))?;

        let mut records = VecDeque::new();
        let mut next_key = 1u64;
        for item in iter {
            let (key, value) = item.map_err(|e| self.err("read entry", e))?;
            let key = key.value();
            next_key = next_key.max(key.saturating_add(1));
            match zerompk::from_msgpack::<T>(value.value()) {
                Ok(record) => records.push_back(record),
                Err(e) => warn!(store = self.name, key, error = %e, "skipping corrupt record"),
            }
        }
        Ok(LoadedRecords { records, next_key })
    }

    /// Write one record, replacing any record under the same key.
    pub fn put(&self, key: u64, record: &T) -> crate::Result<()> {
        self.put_evicting(key, record, &[])
    }

    /// Write one record and remove the `evicted` keys in one transaction.
    /// Either every change commits or none does.
    pub fn put_evicting(&self, key: u64, record: &T, evicted: &[u64]) -> crate::Result<()> {
        let bytes = zerompk::to_msgpack_vec(record).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("{}: key {key}: {e}", self.name),
        })?;
        let txn = self
            .db
            .begin_write()
            .map_err(|e| self.err("begin_write", e))?;
        {
            let mut table = txn
                .open_table(self.table)
                .map_err(|e| self.err("open_table", e))?;
            for old in evicted {
                table.remove(*old).map_err(|e| self.err("remove", e))?;
            }
            table
                .insert(key, bytes.as_slice())
                .map_err(|e| self.err("insert", e))?;
        }
        txn.commit().map_err(|e| self.err("commit", e))
    }

    fn err(&self, step: &str, e: impl std::fmt::Display) -> crate::Error {
        crate::Error::Storage {
            engine: EVENT_PLANE_DIR.into(),
            detail: format!("{}: {step}: {e}", self.name),
        }
    }
}

impl<T> crate::storage::RedbBacked for RedbStore<T> {
    fn redb_database(&self) -> &Database {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("test_records");

    #[derive(Debug, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
    struct Record {
        id: u64,
        text: String,
    }

    fn record(id: u64) -> Record {
        Record {
            id,
            text: format!("r-{id}"),
        }
    }

    fn open(dir: &Path) -> RedbStore<Record> {
        RedbStore::open(dir, "test.redb", TABLE, "test store").expect("open")
    }

    #[test]
    fn put_records_survive_reopen_in_key_order() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let store = open(dir.path());
            store.put(2, &record(2)).expect("put");
            store.put(1, &record(1)).expect("put");
        }
        let loaded = open(dir.path()).load().expect("load");
        assert_eq!(loaded.records, VecDeque::from([record(1), record(2)]));
        assert_eq!(loaded.next_key, 3);
    }

    #[test]
    fn put_evicting_removes_old_keys_with_the_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = open(dir.path());
        store.put(1, &record(1)).expect("put");
        store.put(2, &record(2)).expect("put");
        store.put_evicting(3, &record(3), &[1]).expect("put");
        let loaded = store.load().expect("load");
        assert_eq!(loaded.records, VecDeque::from([record(2), record(3)]));
    }

    #[test]
    fn an_empty_store_starts_at_key_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let loaded = open(dir.path()).load().expect("load");
        assert!(loaded.records.is_empty());
        assert_eq!(loaded.next_key, 1);
    }
}
