// SPDX-License-Identifier: BUSL-1.1

//! The region an `ARRAY_AGG` reads, lowered to the engine [`ArrayRegion`].
//!
//! The box comes from the slice argument. A zone also narrows the box on its
//! two dims to the bounding box of its polygons, so tile pruning and the
//! cluster fan-out see the zone's extent. An INT64 dim takes the ceiling of
//! the low edge and the floor of the high edge.

use nodedb_array::query::region::{ArrayRegion, PolygonZone, ZonePolygon};
use nodedb_array::query::slice::DimRange;
use nodedb_array::schema::{ArraySchema, DimType};
use nodedb_array::types::domain::DomainBound;
use nodedb_sql::types_array::{ArraySliceAst, ArrayZoneAst};

use super::helpers::resolve_slice;

/// The region of an `ARRAY_AGG` over array `name`, or `None` when it reads
/// the whole array.
pub(super) fn resolve_region(
    name: &str,
    slice: &ArraySliceAst,
    zone: Option<&ArrayZoneAst>,
    schema: &ArraySchema,
) -> crate::Result<Option<ArrayRegion>> {
    if slice.dim_ranges.is_empty() && zone.is_none() {
        return Ok(None);
    }
    let mut engine_slice = resolve_slice("ARRAY_AGG", name, slice, schema)?;
    let zone = match zone {
        None => None,
        Some(zone) => {
            let x = zone_axis(name, &zone.x_dim, schema)?;
            let y = zone_axis(name, &zone.y_dim, schema)?;
            let lowered = lower_zone(zone, x.dim, y.dim)?;
            if let Some([x0, y0, x1, y1]) = zone_bbox(&lowered.polygons) {
                for (axis, lo, hi) in [(x, x0, x1), (y, y0, y1)] {
                    let (lo, hi) = match axis.kind {
                        AxisKind::Int64 => (
                            DomainBound::Int64(lo.ceil() as i64),
                            DomainBound::Int64(hi.floor() as i64),
                        ),
                        AxisKind::Float64 => (DomainBound::Float64(lo), DomainBound::Float64(hi)),
                    };
                    narrow(&mut engine_slice.dim_ranges[axis.dim as usize], lo, hi);
                }
            }
            Some(lowered)
        }
    };
    Ok(Some(ArrayRegion::new(engine_slice, zone)))
}

/// The numeric type of a zone dim.
#[derive(Clone, Copy)]
enum AxisKind {
    Int64,
    Float64,
}

/// One zone dim: its schema position and its numeric type.
#[derive(Clone, Copy)]
struct ZoneAxis {
    dim: u32,
    kind: AxisKind,
}

fn zone_axis(name: &str, dim: &str, schema: &ArraySchema) -> crate::Result<ZoneAxis> {
    let idx = schema
        .dims
        .iter()
        .position(|d| d.name == dim)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ARRAY_AGG: array '{name}' has no dim '{dim}'"),
        })?;
    let kind = match schema.dims[idx].dtype {
        DimType::Int64 => AxisKind::Int64,
        DimType::Float64 => AxisKind::Float64,
        other @ (DimType::TimestampMs | DimType::String) => {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "ARRAY_AGG: zone dim '{dim}' is {other:?}; a zone needs an INT64 or FLOAT64 \
                     dim"
                ),
            });
        }
    };
    let dim = u32::try_from(idx).map_err(|_| crate::Error::PlanError {
        detail: format!("ARRAY_AGG: zone dim '{dim}' is past the dim index range"),
    })?;
    Ok(ZoneAxis { dim, kind })
}

fn lower_zone(zone: &ArrayZoneAst, x_dim: u32, y_dim: u32) -> crate::Result<PolygonZone> {
    let polygons = zone
        .polygons
        .iter()
        .map(|rings| {
            let (exterior, holes) = rings.split_first().ok_or_else(|| crate::Error::PlanError {
                detail: "ARRAY_AGG: a zone polygon has no exterior ring".into(),
            })?;
            Ok(ZonePolygon {
                exterior: exterior.clone(),
                holes: holes.to_vec(),
            })
        })
        .collect::<crate::Result<Vec<_>>>()?;
    Ok(PolygonZone {
        x_dim,
        y_dim,
        polygons,
    })
}

/// `[min_x, min_y, max_x, max_y]` over every exterior ring, or `None` when
/// no ring holds a position.
fn zone_bbox(polygons: &[ZonePolygon]) -> Option<[f64; 4]> {
    let mut points = polygons.iter().flat_map(|p| p.exterior.iter());
    let first = points.next()?;
    let mut b = [first[0], first[1], first[0], first[1]];
    for p in points {
        b[0] = b[0].min(p[0]);
        b[1] = b[1].min(p[1]);
        b[2] = b[2].max(p[0]);
        b[3] = b[3].max(p[1]);
    }
    Some(b)
}

/// Intersect `range` with `[lo, hi]`. The bounds share the dim's type.
fn narrow(range: &mut Option<DimRange>, lo: DomainBound, hi: DomainBound) {
    *range = Some(match range.take() {
        None => DimRange::new(lo, hi),
        Some(r) => DimRange::new(
            if lo > r.lo { lo } else { r.lo },
            if hi < r.hi { hi } else { r.hi },
        ),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::schema::{ArraySchemaBuilder, AttrSpec, AttrType, DimSpec};
    use nodedb_array::types::domain::Domain;
    use nodedb_sql::types_array::{ArrayCoordLiteral, NamedDimRange};

    fn schema() -> ArraySchema {
        ArraySchemaBuilder::new("grid")
            .dim(DimSpec::new(
                "lon".to_string(),
                DimType::Float64,
                Domain::new(DomainBound::Float64(0.0), DomainBound::Float64(100.0)),
            ))
            .dim(DimSpec::new(
                "row".to_string(),
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(99)),
            ))
            .dim(DimSpec::new(
                "ts".to_string(),
                DimType::TimestampMs,
                Domain::new(DomainBound::TimestampMs(0), DomainBound::TimestampMs(1_000)),
            ))
            .attr(AttrSpec::new("v".to_string(), AttrType::Float64, true))
            .tile_extents(vec![10, 10, 100])
            .build()
            .expect("schema build")
    }

    fn zone(x: &str, y: &str) -> ArrayZoneAst {
        ArrayZoneAst {
            polygons: vec![vec![vec![
                [1.5, 2.5],
                [7.25, 2.5],
                [7.25, 6.5],
                [1.5, 6.5],
                [1.5, 2.5],
            ]]],
            x_dim: x.into(),
            y_dim: y.into(),
        }
    }

    fn range(r: &ArrayRegion, dim: usize) -> Option<(DomainBound, DomainBound)> {
        r.slice.dim_ranges[dim]
            .as_ref()
            .map(|r| (r.lo.clone(), r.hi.clone()))
    }

    #[test]
    fn the_whole_array_has_no_region() {
        let region = resolve_region("grid", &ArraySliceAst::default(), None, &schema()).unwrap();
        assert!(region.is_none());
    }

    #[test]
    fn a_zone_narrows_the_box_to_its_bounding_box() {
        let region = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "row")),
            &schema(),
        )
        .unwrap()
        .expect("region");
        assert_eq!(
            range(&region, 0),
            Some((DomainBound::Float64(1.5), DomainBound::Float64(7.25)))
        );
        assert_eq!(
            range(&region, 1),
            Some((DomainBound::Int64(3), DomainBound::Int64(6))),
            "an INT64 dim takes the ceiling of the low edge and the floor of the high edge"
        );
        assert!(range(&region, 2).is_none());
        let zone = region.zone.expect("zone");
        assert_eq!((zone.x_dim, zone.y_dim), (0, 1));
        assert_eq!(zone.polygons.len(), 1);
    }

    #[test]
    fn the_zone_box_intersects_the_slice_box() {
        let slice = ArraySliceAst {
            dim_ranges: vec![NamedDimRange {
                dim: "lon".into(),
                lo: ArrayCoordLiteral::Int64(5),
                hi: ArrayCoordLiteral::Int64(50),
            }],
        };
        let region = resolve_region("grid", &slice, Some(&zone("lon", "row")), &schema())
            .unwrap()
            .expect("region");
        assert_eq!(
            range(&region, 0),
            Some((DomainBound::Float64(5.0), DomainBound::Float64(7.25)))
        );
    }

    #[test]
    fn a_zone_on_a_timestamp_dim_is_refused() {
        let err = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "ts")),
            &schema(),
        )
        .expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("zone dim 'ts'")),
            "{err:?}"
        );
    }

    #[test]
    fn a_zone_on_an_unknown_dim_is_refused() {
        let err = resolve_region(
            "grid",
            &ArraySliceAst::default(),
            Some(&zone("lon", "lat")),
            &schema(),
        )
        .expect_err("refused");
        assert!(
            matches!(&err, crate::Error::PlanError { detail } if detail.contains("no dim 'lat'")),
            "{err:?}"
        );
    }
}
