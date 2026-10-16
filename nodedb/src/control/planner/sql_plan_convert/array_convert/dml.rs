// SPDX-License-Identifier: BUSL-1.1

//! `INSERT INTO ARRAY` / `DELETE FROM ARRAY` lowering to `PhysicalTask`.

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_tile_prefix;
use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_sql::types_array::{ArrayCoordLiteral, ArrayInsertRow};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::types::TenantId;
use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp};

use super::super::convert::ConvertContext;
use super::helpers::{coerce_attrs, coerce_coords};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// The catalog entry and decoded schema of the array `name` an
/// `INSERT INTO ARRAY` writes.
fn insert_target(
    name: &str,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<(ArrayCatalogEntry, ArraySchema)> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "INSERT INTO ARRAY: no array catalog wired into convert context".into(),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("INSERT INTO ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;
    Ok((entry, schema))
}

/// The coerced coordinate and identity key of every cell an
/// `INSERT INTO ARRAY` writes, in row order. The bind step reads them once,
/// and conversion reuses them.
#[derive(Debug)]
pub(in super::super) struct ArrayCellKeys {
    pub coords: Vec<Vec<CoordValue>>,
    /// Each coordinate, msgpack-encoded: the cell's surrogate key.
    pub pks: Vec<Vec<u8>>,
}

/// The [`ArrayCellKeys`] of the rows an `INSERT INTO ARRAY` writes.
pub(in super::super) fn insert_array_cell_keys(
    name: &str,
    rows: &[ArrayInsertRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<ArrayCellKeys> {
    let (_, schema) = insert_target(name, tenant_id, ctx)?;
    let mut coords = Vec::with_capacity(rows.len());
    let mut pks = Vec::with_capacity(rows.len());
    for row in rows {
        let coord = coerce_coords(&row.coords, &schema)?;
        pks.push(coord_pk(&coord)?);
        coords.push(coord);
    }
    Ok(ArrayCellKeys { coords, pks })
}

/// Row `index`'s coerced coordinate and identity key: the bind step's when
/// it kept them, else coerced and encoded here.
fn cell_key<'k>(
    kept: Option<&'k ArrayCellKeys>,
    index: usize,
    row: &ArrayInsertRow,
    schema: &ArraySchema,
) -> crate::Result<(Vec<CoordValue>, std::borrow::Cow<'k, [u8]>)> {
    if let Some(keys) = kept
        && let (Some(coord), Some(pk)) = (keys.coords.get(index), keys.pks.get(index))
    {
        return Ok((coord.clone(), std::borrow::Cow::Borrowed(pk.as_slice())));
    }
    let coord = coerce_coords(&row.coords, schema)?;
    let pk = coord_pk(&coord)?;
    Ok((coord, std::borrow::Cow::Owned(pk)))
}

pub(in super::super) fn convert_insert_array(
    name: &str,
    rows: &[ArrayInsertRow],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let (entry, schema) = insert_target(name, tenant_id, ctx)?;

    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let key = ctx.collection_key(name);
    let vshard = key.vshard();
    let system_now_ms = chrono::Utc::now().timestamp_millis();
    let kept = ctx.prefetched.array_keys(key, rows);

    if ctx.cluster_enabled {
        let mut partitioned: Vec<(u64, Vec<u8>)> = Vec::with_capacity(rows.len());
        for (index, row) in rows.iter().enumerate() {
            let (coord, pk_bytes) = cell_key(kept, index, row, &schema)?;
            let attrs = coerce_attrs(&row.attrs, &schema)?;
            let surrogate = ctx.surrogate_for_pk(key, &pk_bytes)?;
            // A cell routes by its tile's key, the key a slice's shard
            // fan-out and each shard's tile filter use.
            let tile_key =
                cell_tile_prefix(&schema, &coord).map_err(|e| crate::Error::PlanError {
                    detail: format!("INSERT INTO ARRAY {name}: tile key: {e}"),
                })?;
            let cell = ArrayPutCell {
                coord,
                attrs,
                surrogate,
                system_from_ms: system_now_ms,
                valid_from_ms: system_now_ms,
                valid_until_ms: i64::MAX,
            };
            let cell_bytes =
                zerompk::to_msgpack_vec(&cell).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array put cell encode: {e}"),
                })?;
            partitioned.push((tile_key, cell_bytes));
        }
        let array_id_msgpack =
            zerompk::to_msgpack_vec(&aid).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array id encode: {e}"),
            })?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
                array_id: aid,
                array_id_msgpack,
                cells: partitioned,
                // A routing wrapper carries no LSN of its own: the coordinator
                // fans these cells out to the owning shards and each owner's
                // apply mints the redo — and the LSN — for the cells it holds.
                wal_lsn: 0,
                prefix_bits: entry.prefix_bits,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // Single-node path: bundle all cells into one msgpack blob.
    let mut cells: Vec<ArrayPutCell> = Vec::with_capacity(rows.len());
    for (index, row) in rows.iter().enumerate() {
        let (coord, pk_bytes) = cell_key(kept, index, row, &schema)?;
        let attrs = coerce_attrs(&row.attrs, &schema)?;
        let surrogate = ctx.surrogate_for_pk(key, &pk_bytes)?;
        cells.push(ArrayPutCell {
            coord,
            attrs,
            surrogate,
            system_from_ms: system_now_ms,
            valid_from_ms: system_now_ms,
            valid_until_ms: i64::MAX,
        });
    }
    let cells_msgpack =
        zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array put cells encode: {e}"),
        })?;

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Put {
            array_id: aid,
            cells_msgpack,
            // Stamped by the write funnel with the LSN of the redo record it
            // appends for this write, so the live tile version equals the one
            // replay rebuilds from that record's header. The planner must not
            // allocate one: it appends nothing, so any LSN it picked will name
            // a record that does not exist.
            wal_lsn: 0,
            provenance: None,
            // Single node: the task's vShard holds every tile.
            vshard_id: vshard.as_u32(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in super::super) fn convert_delete_array(
    name: &str,
    coords: &[Vec<ArrayCoordLiteral>],
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DELETE FROM ARRAY: no array catalog wired into convert context".into(),
        })?;
    let entry = {
        let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("DELETE FROM ARRAY {name}: not found"),
            })?
    };
    let schema: ArraySchema =
        zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema decode: {e}"),
        })?;

    let aid = ArrayId::in_database(tenant_id, ctx.database_id, name);
    let vshard = ctx.collection_key(name).vshard();
    let system_now_ms = chrono::Utc::now().timestamp_millis();

    if ctx.cluster_enabled {
        let mut partitioned: Vec<(u64, Vec<u8>)> = Vec::with_capacity(coords.len());
        for row in coords {
            let typed = coerce_coords(row, &schema)?;
            let tile_key =
                cell_tile_prefix(&schema, &typed).map_err(|e| crate::Error::PlanError {
                    detail: format!("DELETE FROM ARRAY {name}: tile key: {e}"),
                })?;
            let cell = ArrayDeleteCell {
                coord: typed,
                system_from_ms: system_now_ms,
                erasure: false,
            };
            let cell_bytes =
                zerompk::to_msgpack_vec(&cell).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("array delete cell encode: {e}"),
                })?;
            partitioned.push((tile_key, cell_bytes));
        }
        let array_id_msgpack =
            zerompk::to_msgpack_vec(&aid).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("array id encode: {e}"),
            })?;
        return Ok(vec![PhysicalTask {
            tenant_id,
            vshard_id: vshard,
            database_id: ctx.database_id,
            plan: PhysicalPlan::ClusterArray(ClusterArrayOp::Delete {
                array_id: aid,
                array_id_msgpack,
                coords: partitioned,
                // See `convert_insert_array`: the owning shard's apply mints the
                // redo, so the routing wrapper carries no LSN.
                wal_lsn: 0,
                prefix_bits: entry.prefix_bits,
            }),
            post_set_op: PostSetOp::None,
            txn_id: None,
        }]);
    }

    // Single-node path: bundle all cells into one msgpack blob.
    let mut cells: Vec<ArrayDeleteCell> = Vec::with_capacity(coords.len());
    for row in coords {
        cells.push(ArrayDeleteCell {
            coord: coerce_coords(row, &schema)?,
            system_from_ms: system_now_ms,
            erasure: false,
        });
    }
    let coords_msgpack =
        zerompk::to_msgpack_vec(&cells).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array delete cells encode: {e}"),
        })?;
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::Delete {
            array_id: aid,
            coords_msgpack,
            // Stamped by the write funnel — see `convert_insert_array`.
            wal_lsn: 0,
            provenance: None,
            // Single node: the task's vShard holds every tile.
            vshard_id: vshard.as_u32(),
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

/// The identity key of an array cell: its coerced coordinate, msgpack-encoded.
fn coord_pk(coord: &[CoordValue]) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(&coord).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array coord pk encode: {e}"),
    })
}
