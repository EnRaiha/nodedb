// SPDX-License-Identifier: Apache-2.0

//! Collection-tombstone replay filter.
//!
//! A [`TombstoneSet`] records, per `(database_id, tenant_id, collection)` tuple,
//! the highest `purge_lsn` observed in any `RecordType::CollectionTombstoned`
//! record. Replay consumers query [`TombstoneSet::is_tombstoned`] after
//! decoding the collection field from their payload; any record with
//! `lsn < purge_lsn` for a tombstoned pair MUST be skipped.
//!
//! Rationale: the WAL crate is payload-schema agnostic. Each engine's
//! payload (vector / KV / document / graph / ...) has its own MessagePack
//! shape, so collection extraction lives in the engine-side replay code.
//! This module supplies only the ordered predicate.

use std::collections::HashMap;

use crate::record::{RecordType, WalRecord};
use crate::tombstone::CollectionTombstonePayload;

/// In-memory index of active collection tombstones.
///
/// Keyed by `(database_id, tenant_id, collection)`; value is the `purge_lsn`
/// written at tombstone time. If the same object is tombstoned more than once
/// (re-create → re-drop in the same log), the highest `purge_lsn` wins.
#[derive(Debug, Default, Clone)]
pub struct TombstoneSet {
    entries: HashMap<(u64, u64, String), u64>,
}

impl TombstoneSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Bind replay checks to one database while preserving the concise
    /// `(tenant, collection, lsn)` predicate used inside engine replay loops.
    pub fn for_database(&self, database_id: u64) -> DatabaseTombstones<'_> {
        DatabaseTombstones {
            set: self,
            database_id,
        }
    }

    /// Record a tombstone. If the object already has a higher `purge_lsn`,
    /// the existing value is kept (idempotent, order-independent).
    pub fn insert(&mut self, database_id: u64, tenant_id: u64, collection: String, purge_lsn: u64) {
        self.entries
            .entry((database_id, tenant_id, collection))
            .and_modify(|existing| {
                if purge_lsn > *existing {
                    *existing = purge_lsn;
                }
            })
            .or_insert(purge_lsn);
    }

    /// Return `true` iff a write at `lsn` for the database-scoped object
    /// is shadowed by a later tombstone and therefore must be skipped.
    pub fn is_tombstoned(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        lsn: u64,
    ) -> bool {
        self.entries
            .get(&(database_id, tenant_id, collection.to_string()))
            .is_some_and(|&purge_lsn| lsn < purge_lsn)
    }

    /// Return the `purge_lsn` for an object, if any. Primarily used by redb
    /// persistence to serialize the current set after a replay pass.
    pub fn purge_lsn(&self, database_id: u64, tenant_id: u64, collection: &str) -> Option<u64> {
        self.entries
            .get(&(database_id, tenant_id, collection.to_string()))
            .copied()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over every `(database_id, tenant_id, collection, purge_lsn)`.
    pub fn iter(&self) -> impl Iterator<Item = (u64, u64, &str, u64)> + '_ {
        self.entries
            .iter()
            .map(|((db, tid, name), lsn)| (*db, *tid, name.as_str(), *lsn))
    }

    /// Merge another tombstone set into this one. Used when loading
    /// persisted tombstones from redb at startup before a fresh WAL pass.
    pub fn extend(&mut self, other: TombstoneSet) {
        for ((db, tid, name), lsn) in other.entries {
            self.insert(db, tid, name, lsn);
        }
    }
}

/// Database-bound view over a [`TombstoneSet`].
pub struct DatabaseTombstones<'a> {
    set: &'a TombstoneSet,
    database_id: u64,
}

impl DatabaseTombstones<'_> {
    pub fn is_tombstoned(&self, tenant_id: u64, collection: &str, lsn: u64) -> bool {
        self.set
            .is_tombstoned(self.database_id, tenant_id, collection, lsn)
    }
}

impl std::ops::Deref for DatabaseTombstones<'_> {
    type Target = TombstoneSet;

    fn deref(&self) -> &Self::Target {
        self.set
    }
}

/// Single-pass extraction: walk `records`, decode every
/// `CollectionTombstoned` entry, and return the resulting set.
///
/// A malformed tombstone fails extraction. Skipping one would replay writes
/// below an unknown purge boundary and can resurrect a dropped collection.
///
/// Records must already be plaintext. Every record type is encrypted when a key
/// is configured, tombstones included, and the replay drivers in
/// [`crate::segmented`] / [`crate::mmap_reader`] decrypt before returning. A
/// record that still carries `ENCRYPTED_FLAG` here means it reached this
/// function around those drivers; decoding its ciphertext would either error
/// confusingly or, worse, parse into a bogus purge boundary, so it is rejected.
pub fn extract_tombstones(records: &[WalRecord]) -> crate::Result<TombstoneSet> {
    let mut set = TombstoneSet::new();
    for record in records {
        let Some(kind) = RecordType::from_raw(record.logical_record_type()) else {
            continue;
        };
        if kind != RecordType::CollectionTombstoned {
            continue;
        }
        if record.is_encrypted() {
            const SITE: &str = "collection-tombstone extraction";
            let err = crate::WalError::EncryptedRecordWithoutKey {
                lsn: record.header.lsn,
                context: SITE,
            };
            crate::diag::encrypted_record_without_key(&err, record.header.lsn, SITE);
            return Err(err);
        }
        let payload = CollectionTombstonePayload::from_bytes(&record.payload)?;
        set.insert(
            record.header.database_id,
            record.header.tenant_id,
            payload.collection,
            payload.purge_lsn,
        );
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{WalRecord, WalRecordArgs};

    fn tombstone_record(
        database: u64,
        tenant: u64,
        name: &str,
        purge_lsn: u64,
        record_lsn: u64,
    ) -> WalRecord {
        let payload = CollectionTombstonePayload::new(name, purge_lsn)
            .to_bytes()
            .unwrap();
        WalRecord::new(WalRecordArgs {
            record_type: RecordType::CollectionTombstoned as u32,
            lsn: record_lsn,
            tenant_id: tenant,
            vshard_id: 0,
            database_id: database,
            payload,
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }

    #[test]
    fn is_tombstoned_shadows_older_writes() {
        let mut set = TombstoneSet::new();
        set.insert(7, 1, "users".into(), 100);
        assert!(set.is_tombstoned(7, 1, "users", 50));
        assert!(!set.is_tombstoned(7, 1, "users", 100));
        assert!(!set.is_tombstoned(7, 1, "users", 200));
        assert!(!set.is_tombstoned(7, 1, "other", 50));
        assert!(!set.is_tombstoned(7, 2, "users", 50));
        assert!(!set.is_tombstoned(8, 1, "users", 50));
    }

    #[test]
    fn insert_keeps_highest_purge_lsn() {
        let mut set = TombstoneSet::new();
        set.insert(7, 1, "users".into(), 100);
        set.insert(7, 1, "users".into(), 50);
        assert_eq!(set.purge_lsn(7, 1, "users"), Some(100));
        set.insert(7, 1, "users".into(), 200);
        assert_eq!(set.purge_lsn(7, 1, "users"), Some(200));
    }

    #[test]
    fn extract_from_record_stream() {
        let records = vec![
            tombstone_record(7, 1, "users", 100, 10),
            tombstone_record(7, 1, "orders", 150, 11),
            tombstone_record(8, 1, "users", 200, 12),
        ];
        let set = extract_tombstones(&records).unwrap();
        assert_eq!(set.len(), 3);
        assert_eq!(set.purge_lsn(7, 1, "users"), Some(100));
        assert_eq!(set.purge_lsn(7, 1, "orders"), Some(150));
        assert_eq!(set.purge_lsn(8, 1, "users"), Some(200));
    }

    #[test]
    fn extract_ignores_non_tombstone_records() {
        let records = vec![
            tombstone_record(0, 1, "users", 100, 10),
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::Put as u32,
                lsn: 11,
                tenant_id: 1,
                vshard_id: 0,
                database_id: 0,
                payload: b"junk".to_vec(),
                encryption_key: None,
                preamble_bytes: None,
            })
            .unwrap(),
            WalRecord::new(WalRecordArgs {
                record_type: RecordType::Noop as u32,
                lsn: 12,
                tenant_id: 1,
                vshard_id: 0,
                database_id: 0,
                payload: vec![],
                encryption_key: None,
                preamble_bytes: None,
            })
            .unwrap(),
        ];
        let set = extract_tombstones(&records).unwrap();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn extract_rejects_corrupt_payload() {
        // Build a tombstone-typed record whose payload is too short to decode.
        let bogus = WalRecord::new(WalRecordArgs {
            record_type: RecordType::CollectionTombstoned as u32,
            lsn: 5,
            tenant_id: 1,
            vshard_id: 0,
            database_id: 0,
            payload: vec![0xFF, 0xFF, 0xFF], // truncated name_len, no body
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap();
        let records = vec![bogus, tombstone_record(0, 1, "users", 100, 10)];
        assert!(
            extract_tombstones(&records).is_err(),
            "malformed tombstone must fail replay rather than lose a purge boundary"
        );
    }

    #[test]
    fn extend_merges_sets() {
        let mut a = TombstoneSet::new();
        a.insert(7, 1, "users".into(), 100);
        let mut b = TombstoneSet::new();
        b.insert(7, 1, "users".into(), 150);
        b.insert(8, 1, "orders".into(), 200);
        a.extend(b);
        assert_eq!(a.purge_lsn(7, 1, "users"), Some(150));
        assert_eq!(a.purge_lsn(8, 1, "orders"), Some(200));
    }
}
