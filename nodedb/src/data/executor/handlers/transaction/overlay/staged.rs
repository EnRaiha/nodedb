// SPDX-License-Identifier: BUSL-1.1

//! Per-transaction staging overlay data types.
//!
//! This is pure scaffolding for an in-progress transaction-execution
//! redesign: the types and data-only accessors below are not yet populated
//! or read by any handler. Nothing in this file changes existing behavior.
//!
//! Keying rationale: the real storage key for a document is the SURROGATE
//! (`u32`) — `apply_point_put` keys `sparse.versioned_put_in_txn` by
//! surrogate. `doc_id_to_surrogate` lets later units resolve a doc_id to a
//! staged surrogate for not-yet-persisted inserts (a doc_id that has no
//! durable surrogate yet because the insert itself is only staged).

use std::collections::HashMap;

use crate::types::{DatabaseId, TenantId};

/// A single staged mutation for one surrogate row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Staged {
    /// A staged insert/update: the new encoded row body.
    Put(Vec<u8>),
    /// A staged delete.
    Tombstone,
}

/// Staged mutations for a single collection within one transaction.
#[derive(Debug, Default)]
pub struct CollectionOverlay {
    /// Staged mutation per surrogate — the authoritative storage key.
    by_surrogate: HashMap<u32, Staged>,
    /// Resolves a doc_id to its staged surrogate, for inserts that have not
    /// yet been made durable (and therefore have no other way to be looked
    /// up by doc_id).
    doc_id_to_surrogate: HashMap<String, u32>,
}

/// Per-transaction staging overlay: holds not-yet-durable writes for every
/// collection touched by the transaction, keyed by
/// `(DatabaseId, TenantId, collection)`.
#[derive(Debug, Default)]
pub struct TxnOverlay {
    collections: HashMap<(DatabaseId, TenantId, String), CollectionOverlay>,
}

impl TxnOverlay {
    /// Create an empty overlay.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stage a put (insert/update) for `surrogate` in the given collection.
    pub fn insert_put(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &str,
        body: Vec<u8>,
    ) {
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.by_surrogate.insert(surrogate, Staged::Put(body));
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.to_string(), surrogate);
    }

    /// Stage a tombstone (delete) for `surrogate` in the given collection.
    pub fn insert_tombstone(
        &mut self,
        coll_key: (DatabaseId, TenantId, String),
        surrogate: u32,
        doc_id: &str,
    ) {
        let overlay = self.collections.entry(coll_key).or_default();
        overlay.by_surrogate.insert(surrogate, Staged::Tombstone);
        overlay
            .doc_id_to_surrogate
            .insert(doc_id.to_string(), surrogate);
    }

    /// Look up the staged mutation for `surrogate` in the given collection.
    pub fn get(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        surrogate: u32,
    ) -> Option<&Staged> {
        self.collections
            .get(coll_key)
            .and_then(|overlay| overlay.by_surrogate.get(&surrogate))
    }

    /// Look up the staged mutation for `doc_id` in the given collection,
    /// resolving through `doc_id_to_surrogate` first.
    pub fn get_by_doc_id(
        &self,
        coll_key: &(DatabaseId, TenantId, String),
        doc_id: &str,
    ) -> Option<&Staged> {
        let overlay = self.collections.get(coll_key)?;
        let surrogate = overlay.doc_id_to_surrogate.get(doc_id)?;
        overlay.by_surrogate.get(surrogate)
    }

    /// Iterate all staged `(surrogate, Staged)` pairs for a collection.
    /// Yields nothing if the collection has no overlay entries.
    pub fn iter_for_collection<'a>(
        &'a self,
        coll_key: &(DatabaseId, TenantId, String),
    ) -> impl Iterator<Item = (u32, &'a Staged)> {
        self.collections
            .get(coll_key)
            .into_iter()
            .flat_map(|overlay| overlay.by_surrogate.iter().map(|(k, v)| (*k, v)))
    }

    /// True if no collection has any staged mutation.
    pub fn is_empty(&self) -> bool {
        self.collections
            .values()
            .all(|overlay| overlay.by_surrogate.is_empty())
    }

    /// Total number of staged mutations across all collections.
    pub fn len(&self) -> usize {
        self.collections
            .values()
            .map(|overlay| overlay.by_surrogate.len())
            .sum()
    }

    /// Sum of staged `Put` body byte lengths across all collections.
    ///
    /// Placeholder for a future memory cap — not enforced here.
    pub fn memory_size_estimate(&self) -> usize {
        self.collections
            .values()
            .flat_map(|overlay| overlay.by_surrogate.values())
            .map(|staged| match staged {
                Staged::Put(body) => body.len(),
                Staged::Tombstone => 0,
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(coll: &str) -> (DatabaseId, TenantId, String) {
        (DatabaseId::new(1), TenantId::new(1), coll.to_string())
    }

    #[test]
    fn empty_overlay_has_no_entries() {
        let overlay = TxnOverlay::new();
        assert!(overlay.is_empty());
        assert_eq!(overlay.len(), 0);
        assert_eq!(overlay.memory_size_estimate(), 0);
        assert!(overlay.get(&key("users"), 1).is_none());
        assert!(overlay.get_by_doc_id(&key("users"), "abc").is_none());
        assert_eq!(overlay.iter_for_collection(&key("users")).count(), 0);
    }

    #[test]
    fn insert_put_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_put(key("users"), 7, "doc-1", vec![1, 2, 3]);

        assert!(!overlay.is_empty());
        assert_eq!(overlay.len(), 1);
        assert_eq!(overlay.memory_size_estimate(), 3);
        assert_eq!(
            overlay.get(&key("users"), 7),
            Some(&Staged::Put(vec![1, 2, 3]))
        );
        assert_eq!(
            overlay.get_by_doc_id(&key("users"), "doc-1"),
            Some(&Staged::Put(vec![1, 2, 3]))
        );
        let collected: Vec<_> = overlay.iter_for_collection(&key("users")).collect();
        assert_eq!(collected.len(), 1);
    }

    #[test]
    fn insert_tombstone_and_lookup() {
        let mut overlay = TxnOverlay::new();
        overlay.insert_tombstone(key("users"), 9, "doc-2");

        assert_eq!(overlay.get(&key("users"), 9), Some(&Staged::Tombstone));
        assert_eq!(overlay.memory_size_estimate(), 0);
    }
}
