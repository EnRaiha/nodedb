// SPDX-License-Identifier: Apache-2.0

//! The `ArrayPutBatch` request payload, and the cell type a client writes.

use serde::{Deserialize, Serialize};

use crate::error::{NodeDbError, NodeDbResult};
use crate::value::Value;

/// One cell to write into an array: one coordinate per dim and one value per
/// attr, each in the array's schema order. `Value::Null` in an attr writes
/// NULL, which only a nullable attr accepts.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayCellWrite {
    pub coords: Vec<Value>,
    pub attrs: Vec<Value>,
}

/// The `payload` of an `ArrayPutBatch` request: the cells as columns. `dims`
/// holds one column per dim and `attrs` one column per attr, in schema
/// order. Every column holds one value per cell, in cell order.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayPutBatch {
    pub dims: Vec<Vec<Value>>,
    pub attrs: Vec<Vec<Value>>,
}

impl ArrayPutBatch {
    /// The columns of `cells`. Every cell must have the first cell's number
    /// of coordinates and of attrs.
    pub fn from_cells(cells: &[ArrayCellWrite]) -> NodeDbResult<Self> {
        let Some(first) = cells.first() else {
            return Ok(Self::default());
        };
        let mut dims: Vec<Vec<Value>> = (0..first.coords.len())
            .map(|_| Vec::with_capacity(cells.len()))
            .collect();
        let mut attrs: Vec<Vec<Value>> = (0..first.attrs.len())
            .map(|_| Vec::with_capacity(cells.len()))
            .collect();
        for (index, cell) in cells.iter().enumerate() {
            if cell.coords.len() != dims.len() || cell.attrs.len() != attrs.len() {
                return Err(NodeDbError::bad_request(format!(
                    "array cell {index} has {} coords and {} attrs; cell 0 has {} and {}",
                    cell.coords.len(),
                    cell.attrs.len(),
                    dims.len(),
                    attrs.len()
                )));
            }
            for (column, value) in dims.iter_mut().zip(&cell.coords) {
                column.push(value.clone());
            }
            for (column, value) in attrs.iter_mut().zip(&cell.attrs) {
                column.push(value.clone());
            }
        }
        Ok(Self { dims, attrs })
    }

    /// The number of cells.
    pub fn len(&self) -> usize {
        self.dims.first().or(self.attrs.first()).map_or(0, Vec::len)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cell(y: i64, x: i64, v: f64) -> ArrayCellWrite {
        ArrayCellWrite {
            coords: vec![Value::Integer(y), Value::Integer(x)],
            attrs: vec![Value::Float(v)],
        }
    }

    #[test]
    fn cells_become_columns_and_roundtrip() {
        let batch = ArrayPutBatch::from_cells(&[cell(0, 1, 0.5), cell(2, 3, 1.5)]).expect("batch");
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.dims[1], vec![Value::Integer(1), Value::Integer(3)]);
        assert_eq!(batch.attrs[0], vec![Value::Float(0.5), Value::Float(1.5)]);
        let bytes = zerompk::to_msgpack_vec(&batch).expect("encode");
        let back: ArrayPutBatch = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, batch);
    }

    #[test]
    fn a_ragged_cell_is_refused() {
        let mut short = cell(0, 0, 0.0);
        short.coords.pop();
        assert!(ArrayPutBatch::from_cells(&[cell(0, 1, 0.5), short]).is_err());
    }
}
