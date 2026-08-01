// SPDX-License-Identifier: Apache-2.0

//! CrdtState core: document handle, row CRUD, uniqueness probes.

use std::cell::Cell;
use std::collections::HashSet;
use std::marker::PhantomData;

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};
use crate::row_lookup::RowLookup;
use crate::validator::bitemporal::{VALID_UNTIL, VALID_UNTIL_OPEN};

use super::document_cell::DocumentCell;

/// A row is live when its `_ts_valid_until` field is absent, null, or the
/// open sentinel (`i64::MAX`). Rows with any finite `_ts_valid_until` are
/// treated as superseded, independent of wall-clock time — the write path
/// sets finite `_ts_valid_until` only when explicitly terminating a version.
fn row_is_live(row: &LoroMap) -> bool {
    match row.get(VALID_UNTIL) {
        None => true,
        Some(ValueOrContainer::Value(LoroValue::Null)) => true,
        Some(ValueOrContainer::Value(LoroValue::I64(n))) => n == VALID_UNTIL_OPEN,
        _ => true,
    }
}

/// True when `key` currently holds a container (map/list/text/etc.) value
/// on `row`, rather than a plain scalar. Shared by `upsert`'s delete-set
/// filter and its scalar-write guard so both use one definition of
/// "container-valued key".
fn key_is_container(row: &LoroMap, key: &str) -> bool {
    matches!(row.get(key), Some(ValueOrContainer::Container(_)))
}

/// A CRDT state for a single collection — owns one `LoroDoc`.
///
/// Container naming inside the doc still uses `doc.get_map(collection)` so the
/// on-the-wire container layout matches across Origin and Lite and a raw Loro
/// `import` of a peer's delta merges into the same container.
pub struct CrdtState {
    /// Kept private to this state module so callers cannot clone or share a
    /// raw Loro handle around the preview's pending-check/fork critical section.
    pub(in crate::state) doc: DocumentCell,
    pub(super) peer_id: u64,
    /// Loro auto-commit state must stay single-owner. This makes accidental
    /// cross-thread sharing of a `CrdtState` a compile error.
    pub(super) _single_owner: PhantomData<Cell<()>>,
}

impl CrdtState {
    /// Create a new empty state for the given peer.
    pub fn new(peer_id: u64) -> Result<Self> {
        Ok(Self {
            doc: DocumentCell::new(Self::new_doc(peer_id)?),
            peer_id,
            _single_owner: PhantomData,
        })
    }

    /// Load a state from an encoded document this process produced itself — a
    /// snapshot read back from durable storage, or a pre-image exported moments
    /// earlier for transaction rollback.
    ///
    /// Pairs `new` with [`CrdtState::import_local`], because those two steps
    /// belong together: a caller that assembles them by hand has to know that
    /// its own bytes must not go through the peer ceilings, and a caller that
    /// gets that wrong writes a document it can no longer open. See
    /// `import_local` for why the ceilings do not apply here.
    pub fn from_local_snapshot(peer_id: u64, snapshot: &[u8]) -> Result<Self> {
        let state = Self::new(peer_id)?;
        state.import_local(snapshot)?;
        Ok(state)
    }

    /// A fresh Loro document bound to `peer_id`.
    ///
    /// Shared by `new` and by the compaction paths, which need the same
    /// peer-bound document before loading a shallow snapshot into it.
    pub(in crate::state) fn new_doc(peer_id: u64) -> Result<LoroDoc> {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        Ok(doc)
    }

    /// Fetch a row's existing `LoroMap` container, or create one if absent.
    /// Shared by `upsert` and `set_fields` — both need the same row handle
    /// before diverging on prune-vs-preserve semantics.
    fn row_container(&self, collection: &str, row_id: &str) -> Result<LoroMap> {
        let coll = self.doc.get_map(collection);
        match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => Ok(m),
            _ => coll
                .insert_container(row_id, LoroMap::new())
                .map_err(|e| CrdtError::Loro(e.to_string())),
        }
    }

    /// Write `fields` onto `row_container` as scalar LWW inserts, rejecting
    /// any key that currently holds a container value. Shared by `upsert`
    /// and `set_fields` — both write the same way, only the prune step
    /// (upsert-only) differs.
    fn write_scalar_fields(
        row_container: &LoroMap,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        for (field, value) in fields {
            // A container-valued key can never legitimately appear in the
            // incoming scalar projection. Overwriting one would destroy the
            // nested container; skipping it would silently discard the
            // caller's write. Reject instead of doing either.
            if key_is_container(row_container, field) {
                return Err(CrdtError::ScalarFieldShadowsContainer {
                    collection: collection.to_string(),
                    row_id: row_id.to_string(),
                    field: (*field).to_string(),
                });
            }
            row_container
                .insert(field, value.clone())
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }
        Ok(())
    }

    /// Insert or update a row in a collection.
    ///
    /// This is a REPLACE for scalar fields — every caller passes the
    /// complete scalar projection, and any current scalar key absent from
    /// `fields` is deleted. It reuses the row's existing `LoroMap` rather
    /// than destroying and recreating it, because container-valued keys
    /// (e.g. the Notion-style block list in `list_ops.rs`, stored as a
    /// container-valued key inside this same row map) cannot be expressed in
    /// `fields: &[(&str, LoroValue)]` at all — they are structurally out of
    /// scope for this replace and must survive across every call.
    pub fn upsert(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        let row_container = self.row_container(collection, row_id)?;

        let incoming_keys: HashSet<&str> = fields.iter().map(|(field, _)| *field).collect();

        // Full-projection replace, computed from the row's current live
        // keys on every call — never assumed from caller discipline.
        // Container-valued keys are excluded: they are never part of the
        // scalar projection callers pass, so deleting them here would
        // silently discard nested CRDT state (e.g. a row's block list).
        let keys_to_delete: Vec<String> = row_container
            .keys()
            .filter(|key| {
                !incoming_keys.contains(key.as_ref()) && !key_is_container(&row_container, key)
            })
            .map(|key| key.to_string())
            .collect();
        for key in &keys_to_delete {
            row_container
                .delete(key)
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }

        Self::write_scalar_fields(&row_container, collection, row_id, fields)
    }

    /// Partial-merge write: set exactly the provided scalar `fields` on a row
    /// (LWW-per-field), creating the row if absent, leaving every untouched
    /// key intact. This is `upsert` WITHOUT the full-projection prune step —
    /// the UPDATE-SET semantic for `CrdtOp::DocUpsert { partial: true }`.
    pub fn set_fields(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        let row_container = self.row_container(collection, row_id)?;
        Self::write_scalar_fields(&row_container, collection, row_id, fields)
    }

    /// Delete a row from a collection.
    pub fn delete(&self, collection: &str, row_id: &str) -> Result<()> {
        let coll = self.doc.get_map(collection);
        coll.delete(row_id)
            .map_err(|e| CrdtError::Loro(e.to_string()))?;
        Ok(())
    }

    /// Insert a block-map into one row's movable list and populate scalar fields.
    /// The raw document handle never leaves this state object.
    pub fn list_insert_fields(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
        fields: &[(String, LoroValue)],
    ) -> Result<()> {
        let block = crate::list_ops::list_insert_container(
            &self.doc, collection, row_id, list_path, index,
        )?;
        for (key, value) in fields {
            block
                .insert(key.as_str(), value.clone())
                .map_err(|error| CrdtError::Loro(error.to_string()))?;
        }
        Ok(())
    }

    /// Delete one block from a row-owned movable list.
    pub fn list_delete(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<()> {
        crate::list_ops::list_delete(&self.doc, collection, row_id, list_path, index)
    }

    /// Move one block within a row-owned movable list.
    pub fn list_move(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> Result<()> {
        crate::list_ops::list_move(
            &self.doc, collection, row_id, list_path, from_index, to_index,
        )
    }

    /// Return one row-owned movable list's length.
    pub fn list_length(&self, collection: &str, row_id: &str, list_path: &str) -> Result<usize> {
        crate::list_ops::list_length(&self.doc, collection, row_id, list_path)
    }

    /// Read one value from a row-owned movable list without exposing its doc.
    pub fn list_get(
        &self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> Result<Option<LoroValue>> {
        crate::list_ops::list_get(&self.doc, collection, row_id, list_path, index)
    }

    /// Delete all rows in a collection. Returns the number of rows deleted.
    pub fn clear_collection(&self, collection: &str) -> Result<usize> {
        let coll = self.doc.get_map(collection);
        let keys: Vec<String> = coll.keys().map(|k| k.to_string()).collect();
        let count = keys.len();
        for key in &keys {
            coll.delete(key)
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }
        Ok(count)
    }

    /// Read a single row's fields as a `LoroValue::Map`.
    ///
    /// Navigates via `LoroMap::get()` to avoid the expensive recursive
    /// `get_deep_value()` clone on the entire row container.
    pub fn read_row(&self, collection: &str, row_id: &str) -> Option<LoroValue> {
        let coll = self.doc.get_map(collection);
        match coll.get(row_id)? {
            ValueOrContainer::Container(loro::Container::Map(m)) => Some(m.get_value()),
            ValueOrContainer::Container(loro::Container::List(l)) => Some(l.get_value()),
            ValueOrContainer::Container(_) => Some(LoroValue::Null),
            ValueOrContainer::Value(v) => Some(v),
        }
    }

    /// Read a single field from a row without cloning the entire row.
    ///
    /// This is the fast path for KV-style access where only one field
    /// is needed. Avoids allocating a full Map for single-field reads.
    ///
    /// Shares the same `doc.get_map(collection).get(row_id)` lookup pattern
    /// as `read_row`, but returns a single field value instead of the whole
    /// row map — different return granularity, intentionally kept separate.
    pub fn read_field(&self, collection: &str, row_id: &str, field: &str) -> Option<LoroValue> {
        let coll = self.doc.get_map(collection);
        let row_map = match coll.get(row_id)? {
            ValueOrContainer::Container(loro::Container::Map(m)) => m,
            ValueOrContainer::Value(v) => return Some(v),
            _ => return None,
        };
        match row_map.get(field)? {
            ValueOrContainer::Value(v) => Some(v),
            ValueOrContainer::Container(loro::Container::Map(m)) => Some(m.get_value()),
            ValueOrContainer::Container(loro::Container::List(l)) => Some(l.get_value()),
            ValueOrContainer::Container(_) => Some(LoroValue::Null),
        }
    }

    /// Check if a row exists in this collection's Loro document.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        let coll = self.doc.get_map(collection);
        coll.get(row_id).is_some()
    }

    /// List all collection names (top-level map keys in the Loro doc).
    ///
    /// Reads the shallow value: the keys are at the top level, and
    /// `get_deep_value` would materialise every row and field of every
    /// collection to reach them — O(document) for a list whose size is the
    /// number of collections. Name resolution calls this per query.
    pub fn collection_names(&self) -> Vec<String> {
        match self.doc.get_value() {
            LoroValue::Map(map) => map.keys().map(|k| k.to_string()).collect(),
            _ => Vec::new(),
        }
    }

    /// Get all row IDs in a collection.
    pub fn row_ids(&self, collection: &str) -> Vec<String> {
        let coll = self.doc.get_map(collection);
        coll.keys().map(|k| k.to_string()).collect()
    }

    /// Check if a value exists for the given field across all rows in a collection.
    /// Used for UNIQUE constraint checking.
    ///
    /// When `exclude_row_id` is `Some`, the row with that id is skipped so a row
    /// does not collide with its own already-committed version. `None` scans
    /// every row.
    pub fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            if exclude_row_id == Some(key.as_ref()) {
                continue;
            }
            // Reached through the row container rather than a built path.
            // `get_by_str_path` formats a `collection/row/field` string and
            // re-resolves it from the document root for every row, which turns
            // one UNIQUE probe into a per-row allocation plus a path parse and
            // walk — the dominant cost of validating a single row against a
            // large collection. The containers here are already in hand.
            let Some(ValueOrContainer::Container(loro::Container::Map(row))) = coll.get(&key)
            else {
                continue;
            };
            if let Some(ValueOrContainer::Value(field_val)) = row.get(field)
                && &field_val == value
            {
                return true;
            }
        }
        false
    }

    /// Bitemporal variant of [`field_value_exists`]: only considers rows
    /// whose `_ts_valid_until` is open (absent or `i64::MAX`).
    ///
    /// A UNIQUE collision between a superseded version and a new live row
    /// is not a violation — both may share the same value because they
    /// represent the same logical entity at different valid-times.
    pub fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            if exclude_row_id == Some(key.as_ref()) {
                continue;
            }
            let row_map = match coll.get(&key) {
                Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
                _ => continue,
            };
            if !row_is_live(&row_map) {
                continue;
            }
            let field_val = match row_map.get(field) {
                Some(ValueOrContainer::Value(v)) => v,
                _ => continue,
            };
            if &field_val == value {
                return true;
            }
        }
        false
    }

    /// Return row IDs currently "live" in a bitemporal collection
    /// (rows whose `_ts_valid_until` is open). For non-bitemporal
    /// collections every row is returned.
    pub fn live_row_ids(&self, collection: &str) -> Vec<String> {
        let coll = self.doc.get_map(collection);
        let mut out = Vec::new();
        for key in coll.keys() {
            let row_map = match coll.get(&key) {
                Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
                _ => continue,
            };
            if row_is_live(&row_map) {
                out.push(key.to_string());
            }
        }
        out
    }

    /// Peer ID of this state.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }
}

impl RowLookup for CrdtState {
    fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        self.row_exists(collection, row_id)
    }

    fn field_value_exists(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.field_value_exists(collection, field, value, exclude_row_id)
    }

    fn field_value_exists_live(
        &self,
        collection: &str,
        field: &str,
        value: &LoroValue,
        exclude_row_id: Option<&str>,
    ) -> bool {
        self.field_value_exists_live(collection, field, value, exclude_row_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const COLL: &str = "c";
    const ROW: &str = "r";

    #[test]
    fn set_fields_preserves_untouched_keys_and_upsert_prunes() {
        let state = CrdtState::new(0).expect("state");

        // Full projection {a:1, b:2}.
        state
            .upsert(
                COLL,
                ROW,
                &[("a", LoroValue::I64(1)), ("b", LoroValue::I64(2))],
            )
            .expect("upsert");

        // Partial-merge {b:9}: `a` must survive untouched, `b` overwritten.
        state
            .set_fields(COLL, ROW, &[("b", LoroValue::I64(9))])
            .expect("set_fields");
        assert_eq!(
            state.read_field(COLL, ROW, "a"),
            Some(LoroValue::I64(1)),
            "set_fields must leave the untouched key `a` intact"
        );
        assert_eq!(
            state.read_field(COLL, ROW, "b"),
            Some(LoroValue::I64(9)),
            "set_fields must overwrite `b` to 9"
        );

        // Full-projection replace {a:5}: absent key `b` must be pruned.
        state
            .upsert(COLL, ROW, &[("a", LoroValue::I64(5))])
            .expect("upsert replace");
        assert_eq!(
            state.read_field(COLL, ROW, "a"),
            Some(LoroValue::I64(5)),
            "upsert must set `a` to 5"
        );
        assert_eq!(
            state.read_field(COLL, ROW, "b"),
            None,
            "upsert must prune key `b` absent from the projection"
        );
    }
}
