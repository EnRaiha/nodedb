// SPDX-License-Identifier: BUSL-1.1

//! Spatial scan plan builder.

use nodedb_types::protocol::TextFields;

use crate::bridge::envelope::PhysicalPlan;
use nodedb_physical::physical_plan::{SpatialOp, SpatialPredicate};

pub(crate) fn build_scan(fields: &TextFields, collection: &str) -> crate::Result<PhysicalPlan> {
    let raw_bytes = fields
        .query_geometry
        .as_ref()
        .ok_or_else(|| crate::Error::BadRequest {
            detail: "missing 'query_geometry'".to_string(),
        })?;

    let geometry: nodedb_types::geometry::Geometry =
        crate::util::bounded_json::from_slice(raw_bytes).map_err(|e| crate::Error::BadRequest {
            detail: format!("invalid query geometry: {e}"),
        })?;

    let issues = nodedb_spatial::validate::validate_geometry(&geometry);
    if !issues.is_empty() {
        return Err(crate::Error::BadRequest {
            detail: format!("invalid query geometry: {}", issues.join("; ")),
        });
    }

    let predicate_str = fields.spatial_predicate.as_deref().unwrap_or("dwithin");
    let predicate = match predicate_str.to_lowercase().as_str() {
        "dwithin" => SpatialPredicate::DWithin,
        "contains" => SpatialPredicate::Contains,
        "intersects" => SpatialPredicate::Intersects,
        "within" => SpatialPredicate::Within,
        other => {
            return Err(crate::Error::BadRequest {
                detail: format!("unknown spatial predicate: {other}"),
            });
        }
    };

    let distance_meters = fields.distance_meters.unwrap_or(0.0);
    let field = fields.field.clone().unwrap_or_else(|| "geom".to_string());
    let limit = fields.limit.unwrap_or(1000) as usize;

    Ok(PhysicalPlan::Spatial(SpatialOp::Scan {
        collection: collection.to_string(),
        field,
        predicate,
        query_geometry: geometry,
        distance_meters,
        attribute_filters: Vec::new(),
        limit,
        projection: Vec::new(),
        rls_filters: Vec::new(),
        prefilter: None,
    }))
}
