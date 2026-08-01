// SPDX-License-Identifier: BUSL-1.1

//! Shared helpers for array fn → PhysicalTask lowering.

use nodedb_array::schema::{ArraySchema, AttrType as EngineAttrType, DimType as EngineDimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::temporal::{TemporalScope, ValidTime};
use nodedb_sql::types_array::{ArrayBinaryOpAst, ArrayCoordLiteral, ArrayReducerAst};

use crate::control::array_catalog::ArrayCatalogEntry;
use nodedb_physical::physical_plan::{ArrayBinaryOp, ArrayReducer};

use super::super::convert::ConvertContext;

/// Load the full catalog entry for an array by name.
///
/// Used by converters that need both the schema *and* catalog metadata
/// (e.g. `prefix_bits` for cluster routing).
pub(super) fn load_entry(
    name: &str,
    tenant_id: nodedb_types::TenantId,
    ctx: &ConvertContext,
) -> crate::Result<ArrayCatalogEntry> {
    let array_catalog = ctx
        .array_catalog
        .as_ref()
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_*: no array catalog wired into convert context for '{name}'"),
        })?;
    let cat = array_catalog.read().map_err(|_| crate::Error::PlanError {
        detail: "array catalog lock poisoned".into(),
    })?;
    cat.lookup_by_name_in_database(tenant_id, ctx.database_id, name)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_*: array '{name}' not found"),
        })
}

pub(super) fn load_schema(
    name: &str,
    tenant_id: nodedb_types::TenantId,
    ctx: &ConvertContext,
) -> crate::Result<ArraySchema> {
    let entry = load_entry(name, tenant_id, ctx)?;
    zerompk::from_msgpack(&entry.schema_msgpack).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array schema decode: {e}"),
    })
}

pub(super) fn resolve_attr_indices(
    name: &str,
    attrs: &[String],
    schema: &ArraySchema,
) -> crate::Result<Vec<u32>> {
    let mut out = Vec::with_capacity(attrs.len());
    for a in attrs {
        let idx = schema
            .attrs
            .iter()
            .position(|s| &s.name == a)
            .ok_or_else(|| crate::Error::PlanError {
                detail: format!("ARRAY_*: array '{name}' has no attr '{a}'"),
            })?;
        out.push(idx as u32);
    }
    Ok(out)
}

pub(crate) fn coerce_bound(
    lit: &ArrayCoordLiteral,
    dtype: EngineDimType,
    dim: &str,
) -> crate::Result<DomainBound> {
    match (lit, dtype) {
        (ArrayCoordLiteral::Int64(v), EngineDimType::Int64) => Ok(DomainBound::Int64(*v)),
        (ArrayCoordLiteral::Int64(v), EngineDimType::TimestampMs) => {
            Ok(DomainBound::TimestampMs(*v))
        }
        (ArrayCoordLiteral::Int64(v), EngineDimType::Float64) => {
            Ok(DomainBound::Float64(*v as f64))
        }
        (ArrayCoordLiteral::Float64(v), EngineDimType::Float64) => Ok(DomainBound::Float64(*v)),
        (ArrayCoordLiteral::String(v), EngineDimType::String) => Ok(DomainBound::String(v.clone())),
        (got, want) => Err(crate::Error::PlanError {
            detail: format!(
                "ARRAY_SLICE bound for dim `{dim}`: got {got:?}, expected dim type {want:?}"
            ),
        }),
    }
}

pub(super) fn map_reducer(r: ArrayReducerAst) -> ArrayReducer {
    match r {
        ArrayReducerAst::Sum => ArrayReducer::Sum,
        ArrayReducerAst::Count => ArrayReducer::Count,
        ArrayReducerAst::Min => ArrayReducer::Min,
        ArrayReducerAst::Max => ArrayReducer::Max,
        ArrayReducerAst::Mean => ArrayReducer::Mean,
    }
}

pub(super) fn map_binary_op(o: ArrayBinaryOpAst) -> ArrayBinaryOp {
    match o {
        ArrayBinaryOpAst::Add => ArrayBinaryOp::Add,
        ArrayBinaryOpAst::Sub => ArrayBinaryOp::Sub,
        ArrayBinaryOpAst::Mul => ArrayBinaryOp::Mul,
        ArrayBinaryOpAst::Div => ArrayBinaryOp::Div,
    }
}

/// Resolve a [`TemporalScope`] into the `(system_as_of, valid_at_ms)` pair
/// expected by `ArrayOp::Aggregate` (and `ClusterArrayOp::Agg`).
///
/// Default semantics when neither clause was given: both `None` — the Data
/// Plane handler treats this as the live-state fast path (equivalent to
/// `system_as_of = i64::MAX`, no valid-time filter). This avoids allocating
/// any bitemporal bookkeeping for the overwhelmingly common non-temporal case.
///
/// When `AS OF SYSTEM TIME` is given: `system_as_of = Some(t)`, the DP
/// handler applies the Ceiling resolver to reconstruct the array's state at
/// the named system timestamp. `valid_at_ms` remains `None` unless
/// `AS OF VALID TIME` is also present.
///
/// When `AS OF VALID TIME` is given: `valid_at_ms = Some(v)`. Only the
/// `ValidTime::At(v)` (point-in-time) form is supported by the array engine;
/// a range predicate (`ValidTime::Range`) is rejected here with a typed error
/// because array cells store a single valid-time point, not an interval.
///
/// `AS OF SYSTEM TIME NULL` (AllVersions) is rejected for aggregates because
/// summing across all historical versions is not meaningful. Use
/// `NDARRAY_SLICE` for audit-log reads instead.
pub(super) fn resolve_array_temporal(
    temporal: TemporalScope,
    context: &str,
) -> crate::Result<(Option<i64>, Option<i64>)> {
    let system_as_of = match temporal.system_time {
        nodedb_types::SystemTimeScope::Current => None,
        nodedb_types::SystemTimeScope::AsOf(ms) => Some(ms),
        nodedb_types::SystemTimeScope::AllVersions => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "{context}: AS OF SYSTEM TIME NULL is not meaningful for NDARRAY_AGG; \
                     use a point-in-time AS OF SYSTEM TIME <ms>, or NDARRAY_SLICE for the audit log"
                ),
            });
        }
    };
    let valid_at_ms = match temporal.valid_time {
        ValidTime::Any => None,
        ValidTime::At(ms) => Some(ms),
        ValidTime::Range(lo, _hi) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "{context}: AS OF VALID TIME range predicates are not supported on array reads; \
                     use AS OF VALID TIME <ms> (point-in-time). Got FOR VALID_TIME FROM {lo} ..."
                ),
            });
        }
    };
    Ok((system_as_of, valid_at_ms))
}

/// Resolve a [`TemporalScope`] into the `(system_time, valid_at_ms)` pair
/// expected by `ArrayOp::Slice` (and `ClusterArrayOp::Slice`).
///
/// Unlike `resolve_array_temporal` (used for AGG), this variant passes
/// `SystemTimeScope::AllVersions` through unchanged so that
/// `AS OF SYSTEM TIME NULL` triggers the audit-log scan path in the
/// Data Plane handler.
///
/// `ValidTime::Range` is still rejected — array cells carry a single
/// valid-time point, not an interval, so range predicates are nonsensical.
pub(super) fn resolve_array_temporal_scope(
    temporal: TemporalScope,
    context: &str,
) -> crate::Result<(nodedb_types::SystemTimeScope, Option<i64>)> {
    let system_time = temporal.system_time; // all three variants pass through
    let valid_at_ms = match temporal.valid_time {
        ValidTime::Any => None,
        ValidTime::At(ms) => Some(ms),
        ValidTime::Range(lo, _hi) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "{context}: AS OF VALID TIME range predicates are not supported on array reads; \
                     use AS OF VALID TIME <ms> (point-in-time). Got FOR VALID_TIME FROM {lo} ..."
                ),
            });
        }
    };
    Ok((system_time, valid_at_ms))
}

const _UNUSED_ATTR_TYPE: Option<EngineAttrType> = None;
