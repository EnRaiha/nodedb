// SPDX-License-Identifier: Apache-2.0

//! Aborted-write replay filter.
//!
//! An [`AbortedWrites`] set holds the LSNs named by every
//! [`RecordType::WriteAborted`](crate::record::RecordType::WriteAborted) record
//! in a replay stream. A record whose own LSN is in the set was refused by the
//! executing engine AFTER its forward record was already appended, so replaying
//! it would resurrect a write the client was told did not happen.
//!
//! Unlike the collection-tombstone filter, this predicate needs no payload
//! knowledge at all: the key is the record header's LSN, which is why the whole
//! engine-facing replay stream can be filtered once at its source rather than
//! re-checked inside every engine's replay arm.

use std::collections::HashSet;

/// In-memory index of aborted write LSNs.
#[derive(Debug, Default, Clone)]
pub struct AbortedWrites {
    lsns: HashSet<u64>,
}

impl AbortedWrites {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that the write at `lsn` was refused and must not be replayed.
    pub fn insert(&mut self, lsn: u64) {
        self.lsns.insert(lsn);
    }

    /// Return `true` iff the record at `lsn` names a refused write.
    pub fn contains(&self, lsn: u64) -> bool {
        self.lsns.contains(&lsn)
    }

    pub fn len(&self) -> usize {
        self.lsns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lsns.is_empty()
    }

    /// Iterate over every aborted LSN.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.lsns.iter().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn contains_only_inserted_lsns() {
        let mut set = AbortedWrites::new();
        set.insert(42);
        assert!(set.contains(42));
        assert!(!set.contains(41));
        assert!(!set.contains(43));
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn duplicate_inserts_collapse() {
        let mut set = AbortedWrites::new();
        set.insert(7);
        set.insert(7);
        assert_eq!(set.len(), 1);
    }
}
