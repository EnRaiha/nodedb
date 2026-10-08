// SPDX-License-Identifier: BUSL-1.1

//! Undo of one CRDT scalar write to a document row.
//!
//! A scalar write changes only the row's scalar fields. The pre-image is
//! those fields, and the undo writes them back with new Loro operations.
//! This costs one row, where the collection snapshot costs the whole
//! collection.

use nodedb_crdt::state::RowImage;

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

use super::{UndoEntry, UndoError};

/// The scalar state of one CRDT document row before a write.
pub(in crate::data::executor) struct CrdtRowUndo {
    pub database_id: DatabaseId,
    pub tenant_id: TenantId,
    pub collection: String,
    pub row_id: String,
    /// The row before the write. `None`: the collection had no Loro state.
    pub image: Option<RowImage>,
}

impl CoreLoop {
    /// Capture the state of `row_id` that a CRDT scalar write can change.
    /// Opens the tenant's CRDT engine when it is not open.
    pub(in crate::data::executor) fn capture_crdt_row_undo(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        collection: &str,
        row_id: &str,
    ) -> crate::Result<UndoEntry> {
        let image = self
            .get_crdt_engine(database_id, tenant_id)?
            .doc_row_image(collection, row_id)?;
        Ok(UndoEntry::CrdtRow(Box::new(CrdtRowUndo {
            database_id,
            tenant_id,
            collection: collection.to_string(),
            row_id: row_id.to_string(),
            image,
        })))
    }

    /// Put the row's scalar fields back.
    pub(super) fn apply_undo_crdt_row(
        &mut self,
        entry_index: usize,
        undo: CrdtRowUndo,
    ) -> Result<(), UndoError> {
        let Some(engine) = self
            .crdt_engines
            .get_mut(&(undo.database_id, undo.tenant_id))
        else {
            return Err(UndoError::mismatch(
                entry_index,
                format!(
                    "the CRDT engine of '{}' vanished before its row '{}' was rolled back",
                    undo.collection, undo.row_id
                ),
            ));
        };
        engine
            .restore_doc_row(&undo.collection, &undo.row_id, undo.image.as_ref())
            .map_err(|e| {
                UndoError::failed(
                    entry_index,
                    format!(
                        "restoring the CRDT row '{}' of '{}'",
                        undo.row_id, undo.collection
                    ),
                    e,
                )
            })
    }
}
