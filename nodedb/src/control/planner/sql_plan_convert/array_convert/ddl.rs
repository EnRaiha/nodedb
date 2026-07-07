// SPDX-License-Identifier: BUSL-1.1

//! `CREATE ARRAY` / `DROP ARRAY` lowering to `PhysicalTask`.

use std::sync::Arc;

use nodedb_types::config::retention::BitemporalRetention;

use nodedb_array::schema::{
    ArraySchema, ArraySchemaBuilder, AttrSpec, AttrType as EngineAttrType, CellOrder, DimSpec,
    DimType as EngineDimType, TileOrder,
};
use nodedb_array::types::ArrayId;
use nodedb_array::types::domain::{Domain, DomainBound};
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrType, ArrayCellOrderAst, ArrayDimAst, ArrayDimType, ArrayDomainBound,
    ArrayTileOrderAst,
};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::ArrayOp;

use super::super::convert::ConvertContext;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// All inputs for `CREATE ARRAY` lowering, bundled to stay under
/// the 7-parameter clippy limit.
pub(in super::super) struct CreateArrayArgs<'a> {
    pub name: &'a str,
    pub dims: &'a [ArrayDimAst],
    pub attrs: &'a [ArrayAttrAst],
    pub tile_extents: &'a [i64],
    pub cell_order: ArrayCellOrderAst,
    pub tile_order: ArrayTileOrderAst,
    pub prefix_bits: u8,
    pub audit_retain_ms: Option<u64>,
    pub minimum_audit_retain_ms: Option<u64>,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in super::super) fn convert_create_array(
    args: CreateArrayArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let CreateArrayArgs {
        name,
        dims,
        attrs,
        tile_extents,
        cell_order,
        tile_order,
        prefix_bits,
        audit_retain_ms,
        minimum_audit_retain_ms,
        tenant_id,
        ctx,
    } = args;
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "CREATE ARRAY: no array catalog wired into convert context".into(),
        })?;
    let credentials = ctx
        .credentials
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "CREATE ARRAY: no credential store wired into convert context".into(),
        })?;

    // 1a. Validate retention policy before touching shared state.
    if audit_retain_ms.is_some() || minimum_audit_retain_ms.is_some() {
        let retention = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: audit_retain_ms.unwrap_or(0),
            minimum_audit_retain_ms: minimum_audit_retain_ms.unwrap_or(0),
        };
        retention.validate().map_err(|e| crate::Error::PlanError {
            detail: format!("CREATE ARRAY {name}: {e}"),
        })?;
    }

    // 1. Build typed schema.
    let schema = build_schema(name, dims, attrs, tile_extents, cell_order, tile_order)?;

    // 2. Encode + hash.
    let schema_msgpack =
        zerompk::to_msgpack_vec(&*schema).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("array schema encode: {e}"),
        })?;
    let schema_hash = stable_schema_hash(&schema.content_msgpack());

    // 3. Persist + register. Reject duplicates with a typed error.
    let aid = ArrayId::new(tenant_id, name);
    let entry = ArrayCatalogEntry {
        array_id: aid.clone(),
        name: name.to_string(),
        schema_msgpack: schema_msgpack.clone(),
        schema_hash,
        created_at_ms: now_epoch_ms(),
        prefix_bits,
        audit_retain_ms: audit_retain_ms.map(|ms| ms as i64),
        minimum_audit_retain_ms,
    };
    {
        let mut cat = array_catalog.write().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        if cat.lookup_by_name(name).is_some() {
            return Err(crate::Error::PlanError {
                detail: format!("CREATE ARRAY {name}: already exists"),
            });
        }
        cat.register(entry.clone())
            .map_err(|e| crate::Error::PlanError {
                detail: format!("array catalog register: {e}"),
            })?;
    }
    {
        let catalog = credentials.catalog();
        crate::control::array_catalog::persist::persist(catalog, &entry).map_err(|e| {
            crate::Error::PlanError {
                detail: format!("array catalog persist: {e}"),
            }
        })?;
    }

    // Register with the bitemporal retention registry.
    if let Some(registry) = &ctx.bitemporal_retention_registry
        && let Some(retain_ms) = audit_retain_ms
    {
        let retention = BitemporalRetention {
            data_retain_ms: 0,
            audit_retain_ms: retain_ms,
            minimum_audit_retain_ms: minimum_audit_retain_ms.unwrap_or(0),
        };
        registry
            .register(
                ctx.database_id,
                TenantId::new(0),
                name,
                crate::engine::bitemporal::BitemporalEngineKind::Array,
                retention,
            )
            .map_err(|e| crate::Error::PlanError {
                detail: format!("CREATE ARRAY {name}: registry register: {e}"),
            })?;
    }

    // 4. Emit OpenArray so the Data Plane opens the engine side.
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id: aid,
            schema_msgpack,
            schema_hash,
            prefix_bits,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

pub(in super::super) fn convert_drop_array(
    name: &str,
    if_exists: bool,
    tenant_id: TenantId,
    ctx: &ConvertContext,
) -> crate::Result<Vec<PhysicalTask>> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DROP ARRAY: no array catalog wired into convert context".into(),
        })?;
    let credentials = ctx
        .credentials
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: "DROP ARRAY: no credential store wired into convert context".into(),
        })?;
    let removed_array_id: Option<ArrayId> = {
        let mut cat = array_catalog.write().map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
        let aid = cat.lookup_by_name(name).map(|e| e.array_id.clone());
        if aid.is_some() {
            cat.unregister(name);
        }
        aid
    };
    if removed_array_id.is_none() && !if_exists {
        return Err(crate::Error::PlanError {
            detail: format!("DROP ARRAY {name}: not found"),
        });
    }
    {
        let catalog = credentials.catalog();
        if let Err(e) = crate::control::array_catalog::persist::remove(catalog, name) {
            return Err(crate::Error::PlanError {
                detail: format!("array catalog remove: {e}"),
            });
        }
        if let Err(e) =
            catalog.delete_all_surrogates_for_collection(ctx.database_id, ctx.tenant_id, name)
        {
            return Err(crate::Error::PlanError {
                detail: format!("array surrogate-map cleanup: {e}"),
            });
        }
    }
    let Some(aid) = removed_array_id else {
        return Ok(Vec::new());
    };
    let vshard = VShardId::from_collection_in_database(ctx.database_id, name);
    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Array(ArrayOp::DropArray { array_id: aid }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

// ── Schema construction helpers ──────────────────────────────────────

pub(super) fn build_schema(
    name: &str,
    dims: &[ArrayDimAst],
    attrs: &[ArrayAttrAst],
    tile_extents: &[i64],
    cell_order: ArrayCellOrderAst,
    tile_order: ArrayTileOrderAst,
) -> crate::Result<Arc<ArraySchema>> {
    let mut builder = ArraySchemaBuilder::new(name);
    for d in dims {
        let dtype = match d.dtype {
            ArrayDimType::Int64 => EngineDimType::Int64,
            ArrayDimType::Float64 => EngineDimType::Float64,
            ArrayDimType::TimestampMs => EngineDimType::TimestampMs,
            ArrayDimType::String => EngineDimType::String,
        };
        let lo = bound_to_engine(&d.lo);
        let hi = bound_to_engine(&d.hi);
        builder = builder.dim(DimSpec::new(d.name.clone(), dtype, Domain::new(lo, hi)));
    }
    for a in attrs {
        let dtype = match a.dtype {
            ArrayAttrType::Int64 => EngineAttrType::Int64,
            ArrayAttrType::Float64 => EngineAttrType::Float64,
            ArrayAttrType::String => EngineAttrType::String,
            ArrayAttrType::Bytes => EngineAttrType::Bytes,
        };
        builder = builder.attr(AttrSpec::new(a.name.clone(), dtype, a.nullable));
    }
    let extents: Vec<u64> = tile_extents.iter().map(|n| *n as u64).collect();
    builder = builder
        .tile_extents(extents)
        .cell_order(map_cell_order(cell_order))
        .tile_order(map_tile_order(tile_order));
    let schema = builder.build().map_err(|e| crate::Error::PlanError {
        detail: format!("CREATE ARRAY {name}: {e}"),
    })?;
    Ok(Arc::new(schema))
}

fn bound_to_engine(b: &ArrayDomainBound) -> DomainBound {
    match b {
        ArrayDomainBound::Int64(v) => DomainBound::Int64(*v),
        ArrayDomainBound::Float64(v) => DomainBound::Float64(*v),
        ArrayDomainBound::TimestampMs(v) => DomainBound::TimestampMs(*v),
        ArrayDomainBound::String(v) => DomainBound::String(v.clone()),
    }
}

fn map_cell_order(o: ArrayCellOrderAst) -> CellOrder {
    match o {
        ArrayCellOrderAst::RowMajor => CellOrder::RowMajor,
        ArrayCellOrderAst::ColMajor => CellOrder::ColMajor,
        ArrayCellOrderAst::Hilbert => CellOrder::Hilbert,
        ArrayCellOrderAst::ZOrder => CellOrder::ZOrder,
    }
}

fn map_tile_order(o: ArrayTileOrderAst) -> TileOrder {
    match o {
        ArrayTileOrderAst::RowMajor => TileOrder::RowMajor,
        ArrayTileOrderAst::ColMajor => TileOrder::ColMajor,
        ArrayTileOrderAst::Hilbert => TileOrder::Hilbert,
        ArrayTileOrderAst::ZOrder => TileOrder::ZOrder,
    }
}

/// Deterministic schema hash: CRC32C of msgpack bytes split into two halves, OR'd into u64.
pub(super) fn stable_schema_hash(bytes: &[u8]) -> u64 {
    if bytes.len() <= 4 {
        return crc32c::crc32c(bytes) as u64;
    }
    let mid = bytes.len() / 2;
    let lo = crc32c::crc32c(&bytes[..mid]) as u64;
    let hi = crc32c::crc32c(&bytes[mid..]) as u64;
    (hi << 32) | lo
}

pub(super) fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
