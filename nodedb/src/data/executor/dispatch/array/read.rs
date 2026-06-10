// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Slice` and `ArrayOp::Project` handlers.
//!
//! Both are read-only fan-outs over the engine's tile scan. Slice
//! prunes by per-dim coord ranges and (optionally) projects an attribute
//! subset; Project is a pure attribute projection over every cell.
//!
//! Decoded slice payloads ride as zerompk bytes — matching the
//! contract documented on `ArrayOp::Slice::slice_msgpack`.

use nodedb_array::query::project::{Projection, project_sparse};
use nodedb_array::query::slice::{Slice, slice_sparse, tile_overlaps_slice};
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::ArrayId;
use nodedb_types::{ArrayCell, SurrogateBitmap, SystemTimeScope, Value};

/// Slice parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct SliceParams<'a> {
    pub array_id: &'a ArrayId,
    pub slice_msgpack: &'a [u8],
    pub attr_projection: &'a [u32],
    pub limit: u32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    /// Optional Hilbert-prefix range `[lo, hi]` for shard-level partitioning.
    /// When set, only tiles whose Hilbert prefix falls within this range are
    /// included. Used by the distributed shard handler to prevent duplicate
    /// rows when all vShards share a single Data Plane.
    pub hilbert_range: Option<(u64, u64)>,
    /// Bitemporal system-time scope.
    ///
    /// - `Current`: live read (effective cutoff `i64::MAX`).
    /// - `AsOf(t)`: point-in-time snapshot at `t`.
    /// - `AllVersions`: audit-log — every live cell-version, sorted ascending
    ///   by `system_from_ms`, limit bounds total versions.
    pub system_time: SystemTimeScope,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::convert::{cell_value_to_value, coord_value_to_value, sparse_tile_to_array_cells};
use super::encode::encode_value_rows;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_slice(
        &mut self,
        task: &ExecutionTask,
        p: SliceParams<'_>,
    ) -> Response {
        let SliceParams {
            array_id,
            slice_msgpack,
            attr_projection,
            limit,
            cell_filter,
            hilbert_range,
            system_time,
            valid_at_ms,
        } = p;

        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
        let slice: Slice = match zerompk::from_msgpack(slice_msgpack) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array slice decode: {e}"),
                    },
                );
            }
        };

        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        let proj = if attr_projection.is_empty() {
            None
        } else {
            Some(Projection::new(
                attr_projection.iter().map(|&i| i as usize).collect(),
            ))
        };
        let cap = limit as usize;

        match system_time {
            SystemTimeScope::AllVersions => {
                // Audit-log path: emit one row per live cell-version.
                let store = match self.array_engine.store(array_id) {
                    Ok(s) => s,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array '{}' not open: {e}", array_id.name),
                            },
                        );
                    }
                };
                let mut versions = match store.scan_tiles_all_versions(valid_at_ms) {
                    Ok(v) => v,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array audit-log scan: {e}"),
                            },
                        );
                    }
                };

                // Apply Hilbert-range and slice coord filters.
                versions.retain(|(hp, coord, _sys_ms, _payload)| {
                    if let Some((lo, hi)) = hilbert_range
                        && (*hp < lo || *hp > hi)
                    {
                        return false;
                    }
                    // Check coord against slice predicate dim-by-dim.
                    slice_coord_matches(coord, &slice)
                });

                // Sort ascending by system_from_ms, ties by coord lexicographic order.
                versions.sort_by(|(_, coord_a, sys_a, _), (_, coord_b, sys_b, _)| {
                    sys_a.cmp(sys_b).then_with(|| {
                        coord_a
                            .partial_cmp(coord_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                });

                // Collect rows.
                let mut rows: Vec<Value> = Vec::new();
                'av_outer: for (_, coord, sys_ms, payload) in versions {
                    // Apply cell_filter via surrogate.
                    if let Some(f) = cell_filter
                        && !f.contains(payload.surrogate)
                    {
                        continue;
                    }
                    // Build coords from CoordValue.
                    let coords: Vec<Value> = coord.iter().map(coord_value_to_value).collect();
                    // Build attrs from payload, applying projection.
                    let all_attrs: Vec<Value> =
                        payload.attrs.iter().map(cell_value_to_value).collect();
                    let attrs = if let Some(p) = proj.as_ref() {
                        p.attr_indices
                            .iter()
                            .map(|&i| all_attrs.get(i).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        all_attrs
                    };
                    rows.push(Value::ArrayCell(ArrayCell {
                        coords,
                        attrs,
                        system_time: Some(sys_ms),
                    }));
                    if cap > 0 && rows.len() >= cap {
                        break 'av_outer;
                    }
                }

                encode_slice_rows(self, task, rows, false)
            }

            SystemTimeScope::Current | SystemTimeScope::AsOf(_) => {
                // Point-in-time / live path — use existing Ceiling resolver.
                let cutoff = match system_time {
                    SystemTimeScope::AsOf(t) => t,
                    SystemTimeScope::Current => i64::MAX,
                    SystemTimeScope::AllVersions => unreachable!(),
                };
                let store = match self.array_engine.store(array_id) {
                    Ok(s) => s,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array '{}' not open: {e}", array_id.name),
                            },
                        );
                    }
                };
                let (resolved_tiles, truncated_before_horizon) =
                    match store.scan_tiles_at(cutoff, valid_at_ms) {
                        Ok(r) => r,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("array bitemporal scan: {e}"),
                                },
                            );
                        }
                    };

                let mut rows: Vec<Value> = Vec::new();
                'outer: for (hp, sparse) in resolved_tiles {
                    if let Some((lo, hi)) = hilbert_range
                        && (hp < lo || hp > hi)
                    {
                        continue;
                    }
                    if !tile_overlaps_slice(&sparse.mbr.dim_mins, &sparse.mbr.dim_maxs, &slice) {
                        continue;
                    }
                    let filtered = match slice_sparse(&schema, &sparse, &slice) {
                        Ok(t) => t,
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("array slice filter: {e}"),
                                },
                            );
                        }
                    };
                    let final_tile = match proj.as_ref() {
                        Some(p) => match project_sparse(&filtered, p) {
                            Ok(t) => t,
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("array slice project: {e}"),
                                    },
                                );
                            }
                        },
                        None => filtered,
                    };
                    for (row_idx, cell) in sparse_tile_to_array_cells(&schema, &final_tile)
                        .into_iter()
                        .enumerate()
                    {
                        if let Some(f) = cell_filter {
                            let sur = final_tile
                                .surrogates
                                .get(row_idx)
                                .copied()
                                .unwrap_or(nodedb_types::Surrogate::ZERO);
                            if !f.contains(sur) {
                                continue;
                            }
                        }
                        rows.push(Value::ArrayCell(cell));
                        if cap > 0 && rows.len() >= cap {
                            break 'outer;
                        }
                    }
                }

                encode_slice_rows(self, task, rows, truncated_before_horizon)
            }
        }
    }
}

/// Check whether a coordinate tuple matches a slice predicate.
///
/// Returns `true` if for every constrained dimension the coord value falls
/// within `[lo, hi]`. Unconstrained dims (None) are always included.
fn slice_coord_matches(
    coord: &[nodedb_array::types::coord::value::CoordValue],
    slice: &Slice,
) -> bool {
    for (i, dr_opt) in slice.dim_ranges.iter().enumerate() {
        let Some(dr) = dr_opt else { continue };
        let Some(cv) = coord.get(i) else { return false };
        // Compare coord value against [lo, hi] domain bounds.
        if !coord_in_dim_range(cv, dr) {
            return false;
        }
    }
    true
}

fn coord_in_dim_range(
    cv: &nodedb_array::types::coord::value::CoordValue,
    dr: &nodedb_array::query::slice::DimRange,
) -> bool {
    // Convert DomainBound to CoordValue for comparison.
    let lo_cv = domain_bound_to_coord_value(&dr.lo);
    let hi_cv = domain_bound_to_coord_value(&dr.hi);
    let (Some(lo), Some(hi)) = (lo_cv, hi_cv) else {
        return true;
    };
    cv >= &lo && cv <= &hi
}

fn domain_bound_to_coord_value(
    b: &nodedb_array::types::domain::DomainBound,
) -> Option<nodedb_array::types::coord::value::CoordValue> {
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::DomainBound;
    match b {
        DomainBound::Int64(v) => Some(CoordValue::Int64(*v)),
        DomainBound::Float64(v) => Some(CoordValue::Float64(*v)),
        DomainBound::TimestampMs(v) => Some(CoordValue::TimestampMs(*v)),
        DomainBound::String(v) => Some(CoordValue::String(v.clone())),
    }
}

/// Encode the slice result rows into an `ArraySliceResponse` and return a `Response`.
fn encode_slice_rows(
    core: &mut crate::data::executor::core_loop::CoreLoop,
    task: &crate::data::executor::task::ExecutionTask,
    rows: Vec<Value>,
    truncated_before_horizon: bool,
) -> crate::bridge::envelope::Response {
    let rows_msgpack = {
        let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 64);
        let n = rows.len();
        if n < 16 {
            buf.push(0x90 | n as u8);
        } else if n <= u16::MAX as usize {
            buf.push(0xDC);
            buf.extend_from_slice(&(n as u16).to_be_bytes());
        } else {
            buf.push(0xDD);
            buf.extend_from_slice(&(n as u32).to_be_bytes());
        }
        for row in &rows {
            match nodedb_types::value_to_msgpack(row) {
                Ok(b) => buf.extend_from_slice(&b),
                Err(e) => {
                    return core.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("array response encode: {e}"),
                        },
                    );
                }
            }
        }
        buf
    };
    let resp = crate::data::executor::response_codec::ArraySliceResponse {
        rows_msgpack,
        truncated_before_horizon,
    };
    match zerompk::to_msgpack_vec(&resp) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            crate::bridge::envelope::ErrorCode::Internal {
                detail: format!("array slice response encode: {e}"),
            },
        ),
    }
}

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_project(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        attr_indices: &[u32],
    ) -> Response {
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        let tiles = match self
            .array_engine
            .scan_tiles(array_id, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array project scan: {e}"),
                    },
                );
            }
        };

        let proj = Projection::new(attr_indices.iter().map(|&i| i as usize).collect());

        let mut rows: Vec<Value> = Vec::new();
        for tile in tiles {
            let sparse: SparseTile = match tile {
                TilePayload::Sparse(s) => s,
                TilePayload::Dense(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: "dense tile payload in project".to_string(),
                        },
                    );
                }
            };
            let projected = match project_sparse(&sparse, &proj) {
                Ok(t) => t,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array project: {e}"),
                        },
                    );
                }
            };
            for cell in sparse_tile_to_array_cells(&schema, &projected) {
                rows.push(Value::ArrayCell(cell));
            }
        }

        encode_value_rows(self, task, &rows)
    }
}
