// SPDX-License-Identifier: Apache-2.0

//! Argument coercion shared by the geo function evaluators.

use crate::value_ops::value_to_f64;
use nodedb_types::Value;
use nodedb_types::geometry::Geometry;

pub(super) fn str_arg(args: &[Value], idx: usize) -> Option<String> {
    args.get(idx)?.as_str().map(|s| s.to_string())
}

pub(super) fn num_arg(args: &[Value], idx: usize) -> Option<f64> {
    args.get(idx).and_then(|v| value_to_f64(v, true))
}

/// Extract a geometry argument.
///
/// Accepts every representation geometry takes on its way through the system:
/// a native `Value::Geometry`, a GeoJSON object decoded from a document, a
/// GeoJSON string (the form a constant-folded geometry is stored and passed
/// as), and a WKT string. Accepting WKT here is what makes a bare
/// `'POINT(1 2)'` literal usable wherever a geometry is expected, matching
/// PostGIS's treatment of an unknown-typed literal in geometry position.
pub(super) fn geom_arg(args: &[Value], idx: usize) -> Option<Geometry> {
    match args.get(idx)? {
        Value::Geometry(g) => Some(g.clone()),
        Value::String(s) => geometry_from_text(s),
        other => {
            // GeoJSON object: convert Value → JSON → Geometry.
            let json = serde_json::Value::from(other.clone());
            serde_json::from_value(json).ok()
        }
    }
}

/// Parse a textual geometry: GeoJSON first, then WKT.
///
/// The two are unambiguous — GeoJSON always starts with `{`, WKT with a type
/// keyword — so trying them in order cannot misread one as the other.
pub fn geometry_from_text(text: &str) -> Option<Geometry> {
    let trimmed = text.trim();
    if trimmed.starts_with('{') {
        nodedb_types::geometry::from_geojson_str(trimmed)
    } else {
        nodedb_spatial::geometry_from_wkt(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geojson_and_wkt_strings_both_parse() {
        let geojson = geometry_from_text(r#"{"type":"Point","coordinates":[1.0,2.0]}"#);
        let wkt = geometry_from_text("POINT(1 2)");
        assert_eq!(geojson, wkt);
        assert_eq!(geojson, Some(Geometry::point(1.0, 2.0)));
    }

    #[test]
    fn surrounding_whitespace_does_not_defeat_parsing() {
        assert!(geometry_from_text("  POINT(1 2)  ").is_some());
        assert!(geometry_from_text("  {\"type\":\"Point\",\"coordinates\":[1,2]}  ").is_some());
    }

    #[test]
    fn non_geometry_text_is_rejected() {
        assert!(geometry_from_text("not a geometry").is_none());
        assert!(geometry_from_text("").is_none());
    }
}
