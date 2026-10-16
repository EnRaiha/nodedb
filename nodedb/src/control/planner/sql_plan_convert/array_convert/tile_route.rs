// SPDX-License-Identifier: BUSL-1.1

//! The vShard an array cell routes to, from its coordinate literals.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_tile_prefix;
use nodedb_sql::types_array::ArrayCoordLiteral;
use nodedb_types::array_route::array_vshard_for_tile;

use super::helpers::coerce_coords;

/// The vShard that owns the tile of the cell at `coords`. The cluster write
/// path routes each cell of a put by this same key, so cells bucketed by it
/// reach one shard.
pub(crate) fn cell_vshard(
    coords: &[ArrayCoordLiteral],
    schema: &ArraySchema,
    prefix_bits: u8,
) -> crate::Result<u32> {
    let coord = coerce_coords(coords, schema)?;
    let tile_key = cell_tile_prefix(schema, &coord).map_err(|e| crate::Error::PlanError {
        detail: format!("array cell tile key: {e}"),
    })?;
    array_vshard_for_tile(tile_key, prefix_bits).map_err(|e| crate::Error::PlanError {
        detail: e.to_string(),
    })
}
