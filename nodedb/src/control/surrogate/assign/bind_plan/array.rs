// SPDX-License-Identifier: BUSL-1.1

//! Array ops: a put binds every cell's coordinate tuple. The key is
//! `zerompk(coord)`, the same bytes the planner assigned under.

use nodedb_physical::physical_plan::ArrayOp;

use super::binder::IdentityBinder;
use crate::engine::array::wal::ArrayPutCell;

pub(super) fn bind(binder: &IdentityBinder<'_>, op: &mut ArrayOp) -> crate::Result<()> {
    match op {
        ArrayOp::Put {
            array_id,
            cells_msgpack,
            ..
        } => {
            let mut cells: Vec<ArrayPutCell> =
                zerompk::from_msgpack(cells_msgpack).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array cell put decode: {e}"),
                })?;
            let pks = cells
                .iter()
                .map(|cell| {
                    zerompk::to_msgpack_vec(&cell.coord).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("array coord pk encode: {e}"),
                    })
                })
                .collect::<crate::Result<Vec<Vec<u8>>>>()?;
            let key = binder.bare_key(&array_id.name);
            let identities: Vec<_> = pks
                .iter()
                .zip(&cells)
                .map(|(pk_bytes, cell)| (key, pk_bytes.as_slice(), cell.surrogate))
                .collect();
            // Every cell binds in one catalog transaction.
            let bound = binder.resolve_many(&identities)?;
            let mut rewritten = false;
            for (cell, surrogate) in cells.iter_mut().zip(bound) {
                if surrogate != cell.surrogate {
                    cell.surrogate = surrogate;
                    rewritten = true;
                }
            }
            // First-wins can hand back an earlier binding; the cells must
            // carry it, or the tile stores a row under an identity no catalog
            // resolves.
            if rewritten {
                *cells_msgpack =
                    zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("array cell put re-encode: {e}"),
                    })?;
            }
            Ok(())
        }
        // Deletes are keyed by exact coordinate and carry no surrogate; the
        // rest read, reshape or maintain the array.
        ArrayOp::Delete { .. }
        | ArrayOp::OpenArray { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Project { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::Flush { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::SurrogateBitmapScan { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. } => Ok(()),
    }
}
