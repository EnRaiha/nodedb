// SPDX-License-Identifier: Apache-2.0

//! Tile keys of a batch of cells, one curve encode per run of cells in one
//! tile.
//!
//! A cell's tile key is the space-filling-curve encode of its tile indices.
//! The indices are a subtract and a divide per dim, and the encode costs far
//! more. Consecutive cells of one tile share the indices, so a batch keeps
//! the last tile's indices and key and encodes only when the tile changes.
//! A batch in row-major or tile order encodes once per tile row segment.

use crate::coord::normalize::{MAX_DIMS, bits_per_dim, check_coord_domain};
use crate::error::{ArrayError, ArrayResult};
use crate::schema::ArraySchema;
use crate::types::TileId;
use crate::types::coord::value::CoordValue;

use super::layout::{tile_index, tile_indices_for_cell, tile_prefix};

/// The last tile one batch keyed: its indices and its key.
///
/// One memo serves one schema. A batch builds its own memo and drops it.
#[derive(Debug, Default)]
pub struct TilePrefixMemo {
    last: Option<([u64; MAX_DIMS], u64)>,
}

impl TilePrefixMemo {
    pub fn new() -> Self {
        Self::default()
    }

    /// [`super::tile_id_for_cell`], encoding the curve only when the tile
    /// differs from the last cell's.
    pub fn tile_id(
        &mut self,
        schema: &ArraySchema,
        coord: &[CoordValue],
        system_from_ms: i64,
    ) -> ArrayResult<TileId> {
        Ok(TileId::new(self.prefix(schema, coord)?, system_from_ms))
    }

    /// [`super::cell_tile_prefix`], encoding the curve only when the tile
    /// differs from the last cell's. Fails when `coord` lies outside the
    /// schema's domain.
    pub fn cell_prefix(&mut self, schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
        check_coord_domain(schema, coord, bits_per_dim(schema.arity()))?;
        self.prefix(schema, coord)
    }

    /// The tile key of `coord`, as [`tile_prefix`] over its tile indices.
    fn prefix(&mut self, schema: &ArraySchema, coord: &[CoordValue]) -> ArrayResult<u64> {
        let n = schema.arity();
        if coord.len() != n {
            return Err(ArrayError::CoordArityMismatch {
                array: schema.name.clone(),
                expected: n,
                got: coord.len(),
            });
        }
        if n > MAX_DIMS {
            // No curve encodes this arity: the unmemoized path reports why.
            return tile_prefix(schema, &tile_indices_for_cell(schema, coord)?);
        }
        let mut indices = [0u64; MAX_DIMS];
        for (i, cell) in coord.iter().enumerate() {
            indices[i] = tile_index(schema, i, cell)?;
        }
        if let Some((last, key)) = &self.last
            && last[..n] == indices[..n]
        {
            return Ok(*key);
        }
        let key = tile_prefix(schema, &indices[..n])?;
        self.last = Some((indices, key));
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ArraySchemaBuilder;
    use crate::schema::attr_spec::{AttrSpec, AttrType};
    use crate::schema::dim_spec::{DimSpec, DimType};
    use crate::tile::{cell_tile_prefix, tile_id_for_cell};
    use crate::types::domain::{Domain, DomainBound};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("g")
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(250)),
            ))
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(255)),
            ))
            .attr(AttrSpec::new("v", AttrType::Int64, false))
            .tile_extents(vec![64, 64])
            .build()
            .unwrap()
    }

    /// Every cell of a row-major sweep, back and forth across tile edges,
    /// keys as the unmemoized functions key it.
    #[test]
    fn a_memo_agrees_with_the_unmemoized_keys() {
        let s = schema();
        let mut memo = TilePrefixMemo::new();
        let cells = (0..=250i64)
            .step_by(7)
            .flat_map(|y| (0..256i64).step_by(5).map(move |x| (y, x)))
            .chain([(3, 200), (200, 3), (3, 201)]);
        for (y, x) in cells {
            let coord = [CoordValue::Int64(y), CoordValue::Int64(x)];
            assert_eq!(
                memo.cell_prefix(&s, &coord).unwrap(),
                cell_tile_prefix(&s, &coord).unwrap(),
                "({y}, {x})"
            );
            assert_eq!(
                memo.tile_id(&s, &coord, 9).unwrap(),
                tile_id_for_cell(&s, &coord, 9).unwrap()
            );
        }
    }

    /// A cell past the domain inside the last tile shares that tile's
    /// indices, and the memo still refuses it.
    #[test]
    fn a_memo_hit_still_checks_the_domain() {
        let s = schema();
        let mut memo = TilePrefixMemo::new();
        memo.cell_prefix(&s, &[CoordValue::Int64(250), CoordValue::Int64(0)])
            .unwrap();
        assert!(
            memo.cell_prefix(&s, &[CoordValue::Int64(251), CoordValue::Int64(0)])
                .is_err()
        );
        assert!(memo.cell_prefix(&s, &[CoordValue::Int64(1)]).is_err());
    }
}
