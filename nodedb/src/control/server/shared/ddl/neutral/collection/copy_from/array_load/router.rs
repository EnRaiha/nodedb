// SPDX-License-Identifier: BUSL-1.1

//! Picks the chunk bucket of each row of an array bulk load.

use nodedb_array::schema::ArraySchema;
use nodedb_sql::types_array::ArrayCoordLiteral;

use crate::control::planner::sql_plan_convert::array_convert::cell_vshard;
use crate::control::server::shared::ddl::result::DdlError;

use super::dispatch::ArrayLoadCtx;

/// Picks each row's chunk bucket: its tile's vShard in a cluster, so each
/// chunk reaches one shard. A node with no cluster topology holds every
/// tile in one vShard, so every row shares one bucket.
pub(super) enum CellRouter {
    Single,
    Cluster {
        schema: Box<ArraySchema>,
        prefix_bits: u8,
    },
}

impl CellRouter {
    pub(super) fn for_array(ctx: &ArrayLoadCtx<'_>, array: &str) -> Result<Self, DdlError> {
        if ctx.state.cluster_topology.is_none() {
            return Ok(CellRouter::Single);
        }
        let entry = {
            let catalog = ctx
                .state
                .array_catalog
                .read()
                .map_err(|_| DdlError::internal("array catalog lock poisoned"))?;
            catalog
                .lookup_by_name_in_database(ctx.identity.tenant_id, ctx.database_id, array)
                .ok_or_else(|| {
                    DdlError::new("42P01", format!("COPY: array '{array}' does not exist"))
                })?
        };
        let schema: ArraySchema = zerompk::from_msgpack(&entry.schema_msgpack)
            .map_err(|e| DdlError::internal(format!("COPY: array '{array}' schema decode: {e}")))?;
        Ok(CellRouter::Cluster {
            schema: Box::new(schema),
            prefix_bits: entry.prefix_bits,
        })
    }

    pub(super) fn vshard(&self, coords: &[ArrayCoordLiteral]) -> crate::Result<u32> {
        match self {
            CellRouter::Single => Ok(0),
            CellRouter::Cluster {
                schema,
                prefix_bits,
            } => cell_vshard(coords, schema, *prefix_bits),
        }
    }
}
