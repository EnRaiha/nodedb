// SPDX-License-Identifier: BUSL-1.1

//! CRDT write keys.
//!
//! A CRDT document is keyed by its document id, bound or not: a delete
//! planned before a concurrent upsert bound the document locks the key the
//! upsert locks. A write of the whole collection locks the collection key.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::CrdtOp;

use super::plan::not_a_write;
use super::set::WriteKeys;

/// Add the write keys of CRDT op `op` to `keys`.
pub(super) fn add_keys(keys: &mut WriteKeys, op: &CrdtOp) -> crate::Result<()> {
    match op {
        CrdtOp::Apply {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ApplyAuthenticated {
            collection,
            document_id,
            ..
        }
        | CrdtOp::RestoreToVersion {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListInsert {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListDelete {
            collection,
            document_id,
            ..
        }
        | CrdtOp::ListMove {
            collection,
            document_id,
            ..
        }
        | CrdtOp::DocUpsert {
            collection,
            document_id,
            ..
        }
        | CrdtOp::DocDelete {
            collection,
            document_id,
            ..
        } => keys.row_id(collection.as_str(), document_id),
        CrdtOp::ImportSnapshot { collection, .. }
        | CrdtOp::SetConstraints { collection, .. }
        | CrdtOp::DropConstraints { collection, .. } => keys.whole_collection(collection.as_str()),
        CrdtOp::Read { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::PreviewApply { .. } => {
            return Err(not_a_write("a CRDT read, policy or compaction op"));
        }
    }
    Ok(())
}
