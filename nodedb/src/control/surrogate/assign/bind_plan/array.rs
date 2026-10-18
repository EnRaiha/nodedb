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
            // Every cell's key goes into one buffer: one allocation per put.
            let mut pk_buf: Vec<u8> = Vec::new();
            let mut pk_ends: Vec<usize> = Vec::with_capacity(cells.len());
            for cell in &cells {
                zerompk::write_msgpack(&mut pk_buf, &cell.coord).map_err(|e| {
                    crate::Error::Serialization {
                        format: "msgpack".into(),
                        detail: format!("array coord pk encode: {e}"),
                    }
                })?;
                pk_ends.push(pk_buf.len());
            }
            let key = binder.bare_key(&array_id.name);
            let mut start = 0;
            let identities: Vec<_> = pk_ends
                .iter()
                .zip(&cells)
                .map(|(&end, cell)| {
                    let pk_bytes = &pk_buf[start..end];
                    start = end;
                    (key, pk_bytes, cell.surrogate)
                })
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
