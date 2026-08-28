// SPDX-License-Identifier: BUSL-1.1

//! Node-local table of in-flight `DdlPendingPropose` records.
//!
//! Rebuilt entirely by metadata Raft log replay, the same way
//! `SharedState::metadata_ddl_owner` and `MetadataCache` are — never
//! persisted on its own. A record is inserted on `DdlPendingPropose`
//! apply and removed on the matching `DdlPendingFinalize` /
//! `DdlPendingCancel` apply (see
//! `control::cluster::metadata_applier::pending_ddl`).

use std::collections::HashMap;
use std::sync::Mutex;

use nodedb_cluster::PendingDdlObject;
use nodedb_types::Hlc;

/// One in-flight pending DDL propose.
#[derive(Debug, Clone)]
pub struct PendingDdlRecord {
    pub objects: Vec<PendingDdlObject>,
    pub proposed_at: Hlc,
}

/// Node-local table of pending DDL records, keyed by fencing token.
#[derive(Default)]
pub struct PendingDdlTable {
    records: Mutex<HashMap<u64, PendingDdlRecord>>,
}

impl PendingDdlTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert (or replace) the pending record for `token`.
    pub fn insert(&self, token: u64, objects: Vec<PendingDdlObject>, proposed_at: Hlc) {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                token,
                PendingDdlRecord {
                    objects,
                    proposed_at,
                },
            );
    }

    /// Clone the pending record for `token`, if any, without removing it.
    /// Finalize peeks via this before replaying host-side effects, so a
    /// mid-replay failure leaves the record in place for retry.
    pub fn get(&self, token: u64) -> Option<PendingDdlRecord> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get(&token)
            .cloned()
    }

    /// Remove and return the pending record for `token`, if any.
    pub fn take(&self, token: u64) -> Option<PendingDdlRecord> {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&token)
    }

    /// True when a pending record exists for `token`.
    pub fn contains(&self, token: u64) -> bool {
        self.records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .contains_key(&token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_take_round_trip() {
        let table = PendingDdlTable::new();
        table.insert(7, Vec::new(), Hlc::default());
        assert!(table.contains(7));
        let record = table.take(7).expect("record present");
        assert!(record.objects.is_empty());
        assert!(!table.contains(7));
    }

    #[test]
    fn take_unknown_token_is_none() {
        let table = PendingDdlTable::new();
        assert!(table.take(99).is_none());
    }

    #[test]
    fn get_does_not_remove() {
        let table = PendingDdlTable::new();
        table.insert(3, Vec::new(), Hlc::default());
        assert!(table.get(3).is_some());
        assert!(table.contains(3), "get must not remove the record");
    }
}
