// SPDX-License-Identifier: BUSL-1.1

//! Safe per-collection block-list operations.
//!
//! These methods intentionally delegate through `CrdtState` rather than
//! returning Loro's raw document handle. That keeps auto-commit state owned by
//! the Data Plane engine and prevents callers from racing delta preview's
//! pending-transaction check with an out-of-band document mutation.

use loro::LoroValue;

use super::TenantCrdtEngine;

impl TenantCrdtEngine {
    /// Insert a populated block map into a row-owned movable list.
    pub fn list_insert_fields(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
        fields: &[(String, LoroValue)],
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_insert_fields(collection, row_id, list_path, index, fields)
            .map_err(crate::Error::Crdt)
    }

    /// Delete one block from a row-owned movable list.
    pub fn list_delete(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_delete(collection, row_id, list_path, index)
            .map_err(crate::Error::Crdt)
    }

    /// Move one block within a row-owned movable list.
    pub fn list_move(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        from_index: usize,
        to_index: usize,
    ) -> crate::Result<()> {
        self.state_mut(collection)?
            .list_move(collection, row_id, list_path, from_index, to_index)
            .map_err(crate::Error::Crdt)
    }

    /// Return one row-owned movable list's length without exposing its raw doc.
    pub fn list_length(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
    ) -> crate::Result<usize> {
        self.state_mut(collection)?
            .list_length(collection, row_id, list_path)
            .map_err(crate::Error::Crdt)
    }

    /// Read one row-owned movable-list value without exposing its raw doc.
    pub fn list_get(
        &mut self,
        collection: &str,
        row_id: &str,
        list_path: &str,
        index: usize,
    ) -> crate::Result<Option<LoroValue>> {
        self.state_mut(collection)?
            .list_get(collection, row_id, list_path, index)
            .map_err(crate::Error::Crdt)
    }
}
