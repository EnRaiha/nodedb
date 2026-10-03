// SPDX-License-Identifier: BUSL-1.1

//! A redb database whose write transactions can defer their durability.
//!
//! A write whose Control Plane journals its write set after apply must not
//! reach stable storage before its write set does. While the gate defers,
//! every write transaction begun on the database commits with
//! [`Durability::None`]: redb keeps the commit visible to later reads, and a
//! crash rolls it back. The Data Plane core then stores the write set in one
//! transaction that commits with [`Durability::Immediate`], which persists
//! every deferred commit before it together with the write set.
//!
//! Every write transaction on a gated database begins through
//! [`GatedDatabase::begin_write`], which shadows `redb::Database::begin_write`
//! for every holder of the handle. Reads reach the database through `Deref`.

use std::ops::Deref;
use std::sync::atomic::{AtomicBool, Ordering};

use redb::{Database, Durability, WriteTransaction};

/// A redb database with a durability gate.
#[derive(Debug)]
pub struct GatedDatabase {
    db: Database,
    deferred: AtomicBool,
    /// Whether a transaction began deferred since the last [`Self::persist`].
    pending: AtomicBool,
}

impl GatedDatabase {
    pub fn new(db: Database) -> Self {
        Self {
            db,
            deferred: AtomicBool::new(false),
            pending: AtomicBool::new(false),
        }
    }

    /// Begin a write transaction. While the gate defers, it commits without
    /// persisting.
    pub fn begin_write(&self) -> Result<WriteTransaction, redb::Error> {
        let mut txn = self.db.begin_write()?;
        if self.deferred.load(Ordering::Acquire) {
            txn.set_durability(Durability::None)?;
            self.pending.store(true, Ordering::Release);
        }
        Ok(txn)
    }

    /// Begin a write transaction that persists its commit and every deferred
    /// commit before it, whatever the gate says.
    pub fn begin_durable_write(&self) -> Result<WriteTransaction, redb::Error> {
        let mut txn = self.db.begin_write()?;
        txn.set_durability(Durability::Immediate)?;
        Ok(txn)
    }

    /// Defer the durability of every write transaction begun from now on.
    pub fn defer(&self) {
        self.deferred.store(true, Ordering::Release);
    }

    /// Stop deferring. Transactions begun from now on persist at commit.
    pub fn resume(&self) {
        self.deferred.store(false, Ordering::Release);
    }

    /// Whether the gate defers.
    pub fn is_deferred(&self) -> bool {
        self.deferred.load(Ordering::Acquire)
    }

    /// Persist every deferred commit. A database with no transaction begun
    /// deferred since the last persist has nothing to persist.
    pub fn persist(&self) -> Result<(), redb::Error> {
        if !self.pending.swap(false, Ordering::AcqRel) {
            return Ok(());
        }
        let persisted = self
            .begin_durable_write()
            .and_then(|txn| txn.commit().map_err(redb::Error::from));
        if persisted.is_err() {
            self.pending.store(true, Ordering::Release);
        }
        persisted
    }
}

impl Deref for GatedDatabase {
    type Target = Database;

    fn deref(&self) -> &Database {
        &self.db
    }
}

#[cfg(test)]
mod tests {
    use redb::{ReadableDatabase, ReadableTable, TableDefinition};

    use super::*;

    const T: TableDefinition<u64, u64> = TableDefinition::new("t");

    fn put(db: &GatedDatabase, key: u64) {
        let txn = db.begin_write().unwrap();
        txn.open_table(T).unwrap().insert(key, key).unwrap();
        txn.commit().unwrap();
    }

    /// The keys a crash at this instant leaves: the file as it stands, opened
    /// from a copy while the live database keeps its own open.
    fn keys_after_crash(path: &std::path::Path, copy: &std::path::Path) -> Vec<u64> {
        std::fs::copy(path, copy).unwrap();
        let db = Database::create(copy).unwrap();
        let txn = db.begin_read().unwrap();
        let Ok(table) = txn.open_table(T) else {
            return Vec::new();
        };
        table
            .iter()
            .unwrap()
            .map(|entry| entry.unwrap().0.value())
            .collect()
    }

    /// A deferred commit is visible at once, and a crash rolls it back. The
    /// durable commit after it persists both.
    #[test]
    fn a_deferred_commit_persists_only_with_a_durable_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("gated.redb");
        let db = GatedDatabase::new(Database::create(&path).unwrap());
        put(&db, 1);
        db.defer();
        put(&db, 2);
        {
            let read = db.begin_read().unwrap();
            assert_eq!(
                read.open_table(T)
                    .unwrap()
                    .get(2)
                    .unwrap()
                    .map(|v| v.value()),
                Some(2),
                "a deferred commit is visible to reads"
            );
        }
        assert_eq!(
            keys_after_crash(&path, &dir.path().join("crash-1.redb")),
            [1],
            "a crash rolls a deferred commit back"
        );

        db.persist().unwrap();
        db.resume();
        assert!(!db.is_deferred());
        assert_eq!(
            keys_after_crash(&path, &dir.path().join("crash-2.redb")),
            [1, 2]
        );
    }
}
