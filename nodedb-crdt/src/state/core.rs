// SPDX-License-Identifier: Apache-2.0

//! CrdtState core: document handle, row CRUD, uniqueness probes.

use std::collections::HashSet;

use loro::{LoroDoc, LoroMap, LoroValue, ValueOrContainer};

use crate::error::{CrdtError, Result};
use crate::row_lookup::RowLookup;
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
    pub(super) doc: LoroDoc,
    pub(super) peer_id: u64,
}

impl CrdtState {
    /// Create a new empty state for the given peer.
    pub fn new(peer_id: u64) -> Result<Self> {
        let doc = LoroDoc::new();
        doc.set_peer_id(peer_id)
            .map_err(|e| CrdtError::Loro(format!("failed to set peer_id {peer_id}: {e}")))?;
        Ok(Self { doc, peer_id })
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
        let coll = self.doc.get_map(collection);
        let row_container = match coll.get(row_id) {
            Some(ValueOrContainer::Container(loro::Container::Map(m))) => m,
            _ => coll
                .insert_container(row_id, LoroMap::new())
                .map_err(|e| CrdtError::Loro(e.to_string()))?,
        };

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

        for (field, value) in fields {
            // A container-valued key can never legitimately appear in the
            // incoming scalar projection. Overwriting one would destroy the
            // nested container; skipping it would silently discard the
            // caller's write. Reject instead of doing either.
            if key_is_container(&row_container, field) {
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

    /// Check if a row exists in this collection's Loro document.
    pub fn row_exists(&self, collection: &str, row_id: &str) -> bool {
        let coll = self.doc.get_map(collection);
        coll.get(row_id).is_some()
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

    /// Get the underlying LoroDoc for advanced operations.
    pub fn doc(&self) -> &LoroDoc {
        &self.doc
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
