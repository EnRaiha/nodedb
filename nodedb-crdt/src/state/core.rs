// SPDX-License-Identifier: Apache-2.0

//! CrdtState core: document handle, row CRUD, uniqueness probes.

use std::collections::HashSet;

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};
use crate::validator::bitemporal::{VALID_UNTIL, VALID_UNTIL_OPEN};

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

/// A CRDT state for a single tenant/namespace.
pub struct CrdtState {
    pub(super) doc: LoroDoc,
    pub(super) peer_id: u64,
    /// Array surrogate IDs that are considered "live" referents for
    /// `BiTemporalFK` / `ForeignKey` constraint checks. Populated by the
    /// caller from the array catalog before validation. Empty by default,
    /// which means array-surrogate references are invisible to the constraint
    /// checker — callers must register them for cross-engine FK validation.
    array_surrogate_ids: HashSet<String>,
}

impl CrdtState {
    /// Create a new empty state for the given peer.
    pub fn new(peer_id: u64) -> Result<Self> {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        Ok(Self {
            doc,
            peer_id,
            array_surrogate_ids: HashSet::new(),
        })
    }

    /// Register an array-engine surrogate ID as a valid referent for FK checks.
    ///
    /// Call this before running constraint validation whenever the referent
    /// collection is backed by the array engine rather than the document/graph
    /// engines. The ID must be the string form of the surrogate (e.g., the
    /// decimal representation of the `Surrogate` value or the composite key
    /// used by the array catalog).
    pub fn register_array_surrogate(&mut self, id: String) {
        self.array_surrogate_ids.insert(id);
    }

    /// Insert or update a row in a collection.
    pub fn upsert(
        &self,
        collection: &str,
        row_id: &str,
        fields: &[(&str, LoroValue)],
    ) -> Result<()> {
        let coll = self.doc.get_map(collection);
        let row_container = coll
            .insert_container(row_id, LoroMap::new())
            .map_err(|e| CrdtError::Loro(e.to_string()))?;
        for (field, value) in fields {
            row_container
                .insert(field, value.clone())
                .map_err(|e| CrdtError::Loro(e.to_string()))?;
        }
        Ok(())
    }

    /// Delete a row from a collection.
    pub fn delete(&self, collection: &str, row_id: &str) -> Result<()> {
        let coll = self.doc.get_map(collection);
        coll.delete(row_id)
            .map_err(|e| CrdtError::Loro(e.to_string()))?;
        Ok(())
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

    /// Check if a row exists in a collection.
    ///
    /// Checks the Loro-backed document/graph collections first. If not found
    /// there, falls back to the registered array surrogate set so that
    /// `BiTemporalFK` and `ForeignKey` constraints can reference array-engine
    /// cells as valid referents without requiring a full cross-engine query.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        let coll = self.doc.get_map(collection);
        if coll.get(row_id).is_some() {
            return true;
        }
        self.array_surrogate_ids.contains(row_id)
    }

    /// List all collection names (top-level map keys in the Loro doc).
    pub fn collection_names(&self) -> Vec<String> {
        let root = self.doc.get_deep_value();
        match root {
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
    pub fn field_value_exists(&self, collection: &str, field: &str, value: &LoroValue) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
            let path = format!("{collection}/{key}/{field}");
            if let Some(voc) = self.doc.get_by_str_path(&path) {
                let field_val = match voc {
                    ValueOrContainer::Value(v) => v,
                    ValueOrContainer::Container(_) => {
                        continue;
                    }
                };
                if &field_val == value {
                    return true;
                }
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
    ) -> bool {
        let coll = self.doc.get_map(collection);
        for key in coll.keys() {
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

    /// Get the underlying LoroDoc for advanced operations.
    pub fn doc(&self) -> &LoroDoc {
        &self.doc
    }

    /// Peer ID of this state.
    pub fn peer_id(&self) -> u64 {
        self.peer_id
    }
}
