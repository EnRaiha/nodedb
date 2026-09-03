// SPDX-License-Identifier: BUSL-1.1

//! Sequence metadata operations for the system catalog.
//!
//! Both `_system.sequences` (the definition) and `_system.sequence_state` (the
//! durable counter) key on `"{database_id}:{tenant_id}:{name}"`. The two tables
//! move together: the counter belongs to the definition that names it.
//!
//! The database segment scopes the row. Two databases in one tenant can hold a
//! same-named sequence, and a shared key makes both hand out one counter.

use super::sequence_types::{SequenceState, StoredSequence};
use super::types::{SEQUENCE_STATE, SEQUENCES, SystemCatalog, catalog_err};
use redb::{ReadableDatabase, ReadableTable};

impl SystemCatalog {
    /// Store a sequence definition.
    ///
    /// The key comes from the entry, so the row can never land under a
    /// database the entry does not name.
    pub fn put_sequence(&self, seq: &StoredSequence) -> crate::Result<()> {
        let key = sequence_key(seq.database_id, seq.tenant_id, &seq.name);
        let bytes =
            zerompk::to_msgpack_vec(seq).map_err(|e| catalog_err("serialize sequence", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SEQUENCES)
                .map_err(|e| catalog_err("open sequences", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert sequence", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Get a sequence definition by database, tenant, and name.
    pub fn get_sequence(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<StoredSequence>> {
        let key = sequence_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get sequence", e))?
        {
            Some(value) => {
                let seq = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize sequence", e))?;
                Ok(Some(seq))
            }
            None => Ok(None),
        }
    }

    /// Delete a sequence definition and its counter. Returns true if the
    /// definition existed.
    ///
    /// The counter row goes with it. A counter that outlives its definition
    /// hands a recreated sequence of the same name the old numbers.
    pub fn delete_sequence(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<bool> {
        let key = sequence_key(database_id, tenant_id, name);
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        let existed;
        {
            let mut table = write_txn
                .open_table(SEQUENCES)
                .map_err(|e| catalog_err("open sequences", e))?;
            existed = table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete sequence", e))?
                .is_some();
            let mut state_table = write_txn
                .open_table(SEQUENCE_STATE)
                .map_err(|e| catalog_err("open sequence_state", e))?;
            state_table
                .remove(key.as_str())
                .map_err(|e| catalog_err("delete sequence state", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))?;
        Ok(existed)
    }

    /// Load every sequence of one tenant in one database.
    ///
    /// The scan is bounded to the tenant's key range, so a node holding many
    /// tenants reads only the rows it returns.
    pub fn load_sequences_for_tenant(
        &self,
        database_id: u64,
        tenant_id: u64,
    ) -> crate::Result<Vec<StoredSequence>> {
        let lower = format!("{database_id}:{tenant_id}:");
        let upper = tenant_upper_bound(database_id, tenant_id);
        self.range_sequences(&lower, &upper)
    }

    /// Load every sequence of one database, across every tenant.
    pub fn load_sequences_in_database(
        &self,
        database_id: u64,
    ) -> crate::Result<Vec<StoredSequence>> {
        let lower = format!("{database_id}:");
        let upper = database_upper_bound(database_id);
        self.range_sequences(&lower, &upper)
    }

    /// Load every sequence across all databases and tenants.
    ///
    /// The registry loads every database on startup, so this stays a full-table
    /// scan.
    pub fn load_all_sequences(&self) -> crate::Result<Vec<StoredSequence>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;

        let mut sequences = Vec::new();
        let mut range = table
            .range(..)
            .map_err(|e| catalog_err("range sequences", e))?;
        while let Some(Ok((_key, value))) = range.next() {
            if let Ok(seq) = zerompk::from_msgpack::<StoredSequence>(value.value()) {
                sequences.push(seq);
            }
        }
        Ok(sequences)
    }

    /// Store sequence runtime state (current value, epoch).
    ///
    /// The key comes from the state, so the counter can never land under a
    /// database the state does not name.
    pub fn put_sequence_state(&self, state: &SequenceState) -> crate::Result<()> {
        let key = sequence_key(state.database_id, state.tenant_id, &state.name);
        let bytes = zerompk::to_msgpack_vec(state)
            .map_err(|e| catalog_err("serialize sequence state", e))?;
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| catalog_err("write txn", e))?;
        {
            let mut table = write_txn
                .open_table(SEQUENCE_STATE)
                .map_err(|e| catalog_err("open sequence_state", e))?;
            table
                .insert(key.as_str(), bytes.as_slice())
                .map_err(|e| catalog_err("insert sequence state", e))?;
        }
        write_txn.commit().map_err(|e| catalog_err("commit", e))
    }

    /// Load sequence runtime state.
    pub fn get_sequence_state(
        &self,
        database_id: u64,
        tenant_id: u64,
        name: &str,
    ) -> crate::Result<Option<SequenceState>> {
        let key = sequence_key(database_id, tenant_id, name);
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCE_STATE)
            .map_err(|e| catalog_err("open sequence_state", e))?;
        match table
            .get(key.as_str())
            .map_err(|e| catalog_err("get sequence state", e))?
        {
            Some(value) => {
                let state = zerompk::from_msgpack(value.value())
                    .map_err(|e| catalog_err("deserialize sequence state", e))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    /// Decode every sequence definition in one key range.
    fn range_sequences(&self, lower: &str, upper: &str) -> crate::Result<Vec<StoredSequence>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| catalog_err("read txn", e))?;
        let table = read_txn
            .open_table(SEQUENCES)
            .map_err(|e| catalog_err("open sequences", e))?;

        let mut sequences = Vec::new();
        for item in table
            .range(lower..upper)
            .map_err(|e| catalog_err("range sequences", e))?
        {
            let (_, value) = item.map_err(|e| catalog_err("read sequence", e))?;
            let seq: StoredSequence = zerompk::from_msgpack(value.value())
                .map_err(|e| catalog_err("deserialize sequence", e))?;
            sequences.push(seq);
        }
        Ok(sequences)
    }
}

fn sequence_key(database_id: u64, tenant_id: u64, name: &str) -> String {
    format!("{database_id}:{tenant_id}:{name}")
}

/// Exclusive upper bound for one database's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every tenant of the database.
fn database_upper_bound(database_id: u64) -> String {
    format!("{database_id};")
}

/// Exclusive upper bound for one tenant's key prefix.
///
/// The prefix ends with `:`. The next byte after `:` is `;`, so this key sorts
/// immediately past every sequence of the tenant.
fn tenant_upper_bound(database_id: u64, tenant_id: u64) -> String {
    format!("{database_id}:{tenant_id};")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::security::catalog::types::SystemCatalog;

    fn make_catalog() -> (tempfile::TempDir, SystemCatalog) {
        let dir = tempfile::tempdir().unwrap();
        let catalog = SystemCatalog::open(&dir.path().join("system.redb")).unwrap();
        (dir, catalog)
    }

    fn seq(database_id: u64, tenant_id: u64, name: &str) -> StoredSequence {
        StoredSequence::new(database_id, tenant_id, name.into(), "admin".into())
    }

    #[test]
    fn put_get_sequence() {
        let (_dir, cat) = make_catalog();
        cat.put_sequence(&seq(2, 1, "order_seq")).unwrap();

        let loaded = cat.get_sequence(2, 1, "order_seq").unwrap().unwrap();
        assert_eq!(loaded.name, "order_seq");
        assert_eq!(loaded.increment, 1);
        assert_eq!(loaded.start_value, 1);
    }

    #[test]
    fn delete_sequence() {
        let (_dir, cat) = make_catalog();
        cat.put_sequence(&seq(2, 1, "s1")).unwrap();
        assert!(cat.delete_sequence(2, 1, "s1").unwrap());
        assert!(!cat.delete_sequence(2, 1, "s1").unwrap());
        assert!(cat.get_sequence(2, 1, "s1").unwrap().is_none());
    }

    #[test]
    fn delete_sequence_removes_the_counter_too() {
        let (_dir, cat) = make_catalog();
        cat.put_sequence(&seq(2, 1, "s1")).unwrap();
        cat.put_sequence_state(&SequenceState::new(2, 1, "s1".into(), 90, 1))
            .unwrap();

        cat.delete_sequence(2, 1, "s1").unwrap();
        assert!(
            cat.get_sequence_state(2, 1, "s1").unwrap().is_none(),
            "a counter outliving its definition rewinds a recreated sequence"
        );
    }

    #[test]
    fn load_for_tenant() {
        let (_dir, cat) = make_catalog();
        for i in 0..3 {
            cat.put_sequence(&seq(2, 1, &format!("s{i}"))).unwrap();
        }
        cat.put_sequence(&seq(2, 2, "other")).unwrap();

        assert_eq!(cat.load_sequences_for_tenant(2, 1).unwrap().len(), 3);
        assert_eq!(cat.load_all_sequences().unwrap().len(), 4);
    }

    #[test]
    fn load_in_database_excludes_another_database() {
        let (_dir, cat) = make_catalog();
        cat.put_sequence(&seq(2, 1, "order_seq")).unwrap();
        cat.put_sequence(&seq(2, 5, "order_seq")).unwrap();
        cat.put_sequence(&seq(3, 1, "order_seq")).unwrap();

        assert_eq!(cat.load_sequences_in_database(2).unwrap().len(), 2);
        assert_eq!(cat.load_sequences_in_database(3).unwrap().len(), 1);
    }

    #[test]
    fn sequence_state_roundtrip() {
        let (_dir, cat) = make_catalog();
        let state = SequenceState::new(2, 1, "s1".into(), 1, 1);
        cat.put_sequence_state(&state).unwrap();

        let loaded = cat.get_sequence_state(2, 1, "s1").unwrap().unwrap();
        assert_eq!(loaded.current_value, 1);
        assert!(!loaded.is_called);
    }

    /// One name, two databases, one tenant: each database keeps its own
    /// definition and its own counter, and a delete in one leaves the other
    /// whole. Drop the `database_id` segment from the key and this fails.
    #[test]
    fn sequences_of_one_database_survive_a_delete_in_another() {
        let (_dir, cat) = make_catalog();

        let mut first = seq(1, 7, "order_id");
        first.start_value = 100;
        let mut second = seq(2, 7, "order_id");
        second.start_value = 500;
        cat.put_sequence(&first).unwrap();
        cat.put_sequence(&second).unwrap();
        cat.put_sequence_state(&SequenceState::new(1, 7, "order_id".into(), 140, 1))
            .unwrap();
        cat.put_sequence_state(&SequenceState::new(2, 7, "order_id".into(), 560, 1))
            .unwrap();

        assert_eq!(
            cat.get_sequence(1, 7, "order_id")
                .unwrap()
                .unwrap()
                .start_value,
            100
        );
        assert_eq!(
            cat.get_sequence(2, 7, "order_id")
                .unwrap()
                .unwrap()
                .start_value,
            500
        );
        assert_eq!(
            cat.get_sequence_state(1, 7, "order_id")
                .unwrap()
                .unwrap()
                .current_value,
            140
        );
        assert_eq!(
            cat.get_sequence_state(2, 7, "order_id")
                .unwrap()
                .unwrap()
                .current_value,
            560
        );

        assert!(cat.delete_sequence(1, 7, "order_id").unwrap());

        assert!(cat.get_sequence(1, 7, "order_id").unwrap().is_none());
        assert!(cat.get_sequence_state(1, 7, "order_id").unwrap().is_none());
        assert_eq!(
            cat.get_sequence(2, 7, "order_id")
                .unwrap()
                .unwrap()
                .start_value,
            500,
            "the other database keeps its definition"
        );
        assert_eq!(
            cat.get_sequence_state(2, 7, "order_id")
                .unwrap()
                .unwrap()
                .current_value,
            560,
            "the other database keeps its counter"
        );
    }
}
