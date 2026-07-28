// SPDX-License-Identifier: Apache-2.0

//! Geodesic measures over geometry: area and centroid.
//!
//! Area uses the spherical-excess formula rather than a projected shoelace,
//! so it stays correct for polygons spanning arbitrary extents — the same
//! basis as [`crate::st_distance`], which is haversine. Centroid follows OGC
//! semantics: the result is the centroid of the highest-dimension components
//! present, so a `GeometryCollection` of a polygon and a stray point returns
//! the polygon's centroid, not a blend of the two.

use nodedb_types::geometry::Geometry;

/// Mean Earth radius in meters, matching `nodedb_types::geometry`'s haversine.
const EARTH_RADIUS_M: f64 = 6_371_000.0;

/// Geodesic area of a geometry in square meters.
///
/// Areal geometries (`Polygon`, `MultiPolygon`) measure their exterior ring
/// minus their holes. Point and linear geometries have zero area, matching
/// PostGIS. A `GeometryCollection` sums the areas of its members.
pub fn st_area(geom: &Geometry) -> f64 {
    match geom {
        Geometry::Polygon { coordinates } => polygon_area(coordinates),
        Geometry::MultiPolygon { coordinates } => {
            coordinates.iter().map(|rings| polygon_area(rings)).sum()
        }
        Geometry::GeometryCollection { geometries } => geometries.iter().map(st_area).sum(),
        Geometry::Point { .. }
        | Geometry::LineString { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::MultiLineString { .. } => 0.0,
        // `Geometry` is `#[non_exhaustive]`. A geometry kind added upstream is
        // not known to be areal, and reporting a fabricated area for it would
        // be worse than reporting none.
        _ => 0.0,
    }
}

/// Area of one polygon: exterior ring less every hole.
fn polygon_area(rings: &[Vec<[f64; 2]>]) -> f64 {
    let mut iter = rings.iter();
    let Some(exterior) = iter.next() else {
        return 0.0;
    };
    let holes: f64 = iter.map(|ring| ring_area(ring)).sum();
    (ring_area(exterior) - holes).max(0.0)
}

/// Unsigned area of a single closed ring via spherical excess.
///
/// For a ring of vertices `(λᵢ, φᵢ)` in radians the enclosed area is
/// `R²/2 · |Σ (λᵢ₊₁ − λᵢ)(2 + sin φᵢ + sin φᵢ₊₁)|`. Longitude deltas are
/// normalized into `[-π, π]` so a ring crossing the antimeridian measures its
/// true extent instead of wrapping the long way around the globe.
fn ring_area(ring: &[[f64; 2]]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    let mut total = 0.0;
    for i in 0..ring.len() {
        let a = ring[i];
        let b = ring[(i + 1) % ring.len()];
        let mut dlng = (b[0] - a[0]).to_radians();
        if dlng > std::f64::consts::PI {
            dlng -= std::f64::consts::TAU;
        } else if dlng < -std::f64::consts::PI {
            dlng += std::f64::consts::TAU;
        }
        total += dlng * (2.0 + a[1].to_radians().sin() + b[1].to_radians().sin());
    }
    (total * EARTH_RADIUS_M * EARTH_RADIUS_M / 2.0).abs()
}

/// Centroid of a geometry, as a `Point`.
///
/// Follows OGC dimension precedence: areal components win over linear, which
/// win over punctual. Returns `None` only when the geometry carries no
/// vertices at all, or when every component of its winning dimension is
/// degenerate (zero area / zero length) — those fall back to the next
/// dimension down rather than returning nothing.
pub fn st_centroid(geom: &Geometry) -> Option<Geometry> {
    let [lng, lat] = centroid_coords(geom)?;
    Some(Geometry::point(lng, lat))
}

/// A weighted point accumulator: `(Σ w·lng, Σ w·lat, Σ w)`.
#[derive(Default, Clone, Copy)]
struct Weighted {
    lng: f64,
    lat: f64,
    weight: f64,
}

impl Weighted {
    fn add(&mut self, point: [f64; 2], weight: f64) {
        self.lng += point[0] * weight;
        self.lat += point[1] * weight;
        self.weight += weight;
    }

    fn merge(&mut self, other: Self) {
        self.lng += other.lng;
        self.lat += other.lat;
        self.weight += other.weight;
    }

    fn resolve(self) -> Option<[f64; 2]> {
        if self.weight > 0.0 {
            Some([self.lng / self.weight, self.lat / self.weight])
        } else {
            None
        }
    }
}

fn centroid_coords(geom: &Geometry) -> Option<[f64; 2]> {
    areal_centroid(geom)
        .and_then(Weighted::resolve)
        .or_else(|| linear_centroid(geom).and_then(Weighted::resolve))
        .or_else(|| punctual_centroid(geom).and_then(Weighted::resolve))
}

/// Area-weighted centroid of every areal component, or `None` if there are no
/// areal components at all.
fn areal_centroid(geom: &Geometry) -> Option<Weighted> {
    match geom {
        Geometry::Polygon { coordinates } => Some(polygon_centroid(coordinates)),
        Geometry::MultiPolygon { coordinates } => {
            let mut acc = Weighted::default();
            for rings in coordinates {
                acc.merge(polygon_centroid(rings));
            }
            Some(acc)
        }
        Geometry::GeometryCollection { geometries } => collect(geometries, areal_centroid),
        Geometry::Point { .. }
        | Geometry::LineString { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::MultiLineString { .. } => None,
        // `Geometry` is `#[non_exhaustive]`; an unknown kind is not areal, so
        // it falls through to the linear and punctual dimensions below.
        _ => None,
    }
}

/// Length-weighted centroid of every linear component.
fn linear_centroid(geom: &Geometry) -> Option<Weighted> {
    match geom {
        Geometry::LineString { coordinates } => Some(line_centroid(coordinates)),
        Geometry::MultiLineString { coordinates } => {
            let mut acc = Weighted::default();
            for line in coordinates {
                acc.merge(line_centroid(line));
            }
            Some(acc)
        }
        Geometry::GeometryCollection { geometries } => collect(geometries, linear_centroid),
        Geometry::Point { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::Polygon { .. }
        | Geometry::MultiPolygon { .. } => None,
        // `Geometry` is `#[non_exhaustive]`; an unknown kind is not linear, so
        // it falls through to the punctual dimension.
        _ => None,
    }
}

/// Arithmetic mean of every vertex, the last-resort dimension. Every geometry
/// contributes here, so a degenerate polygon still yields a centroid.
fn punctual_centroid(geom: &Geometry) -> Option<Weighted> {
    let mut acc = Weighted::default();
    for point in vertices(geom) {
        acc.add(point, 1.0);
    }
    Some(acc)
}

/// Merge the per-component results of `f` across a collection, yielding `None`
/// when no member contributed at that dimension.
fn collect(geometries: &[Geometry], f: fn(&Geometry) -> Option<Weighted>) -> Option<Weighted> {
    let mut acc = Weighted::default();
    let mut any = false;
    for member in geometries {
        if let Some(part) = f(member) {
            acc.merge(part);
            any = true;
        }
    }
    any.then_some(acc)
}

/// Centroid of one polygon, weighting the exterior ring positively and each
/// hole negatively so the hole's mass is removed from the result.
fn polygon_centroid(rings: &[Vec<[f64; 2]>]) -> Weighted {
    let mut acc = Weighted::default();
    let mut iter = rings.iter();
    let Some(exterior) = iter.next() else {
        return acc;
    };
    if let Some(point) = ring_centroid(exterior) {
        acc.add(point, ring_area(exterior));
    }
    for hole in iter {
        if let Some(point) = ring_centroid(hole) {
            acc.add(point, -ring_area(hole));
        }
    }
    // Holes can only ever cancel the exterior down to nothing; a negative
    // total weight would mean malformed input (holes larger than the shell),
    // which must not flip the centroid to the far side of the polygon.
    if acc.weight <= 0.0 {
        return Weighted::default();
    }
    acc
}

/// Area-weighted centroid of a closed ring via the shoelace formula. Falls
/// back to the vertex mean when the ring encloses no area (collinear points).
fn ring_centroid(ring: &[[f64; 2]]) -> Option<[f64; 2]> {
    if ring.is_empty() {
        return None;
    }
    let mut twice_area = 0.0;
    let mut lng = 0.0;
    let mut lat = 0.0;
    for i in 0..ring.len() {
        let a = ring[i];
        let b = ring[(i + 1) % ring.len()];
        let cross = a[0] * b[1] - b[0] * a[1];
        twice_area += cross;
        lng += (a[0] + b[0]) * cross;
        lat += (a[1] + b[1]) * cross;
    }
    if twice_area.abs() < f64::EPSILON {
        let n = ring.len() as f64;
        return Some([
            ring.iter().map(|c| c[0]).sum::<f64>() / n,
            ring.iter().map(|c| c[1]).sum::<f64>() / n,
        ]);
    }
    Some([lng / (3.0 * twice_area), lat / (3.0 * twice_area)])
}

/// Length-weighted centroid of a polyline: each segment contributes its
/// midpoint weighted by its geodesic length.
fn line_centroid(coords: &[[f64; 2]]) -> Weighted {
    let mut acc = Weighted::default();
    for pair in coords.windows(2) {
        let (a, b) = (pair[0], pair[1]);
        let length = nodedb_types::geometry::haversine_distance(a[0], a[1], b[0], b[1]);
        acc.add([(a[0] + b[0]) / 2.0, (a[1] + b[1]) / 2.0], length);
    }
    acc
}

/// Every vertex of a geometry, in document order.
fn vertices(geom: &Geometry) -> Vec<[f64; 2]> {
    match geom {
        Geometry::Point { coordinates } => vec![*coordinates],
        Geometry::LineString { coordinates } | Geometry::MultiPoint { coordinates } => {
            coordinates.clone()
        }
        Geometry::Polygon { coordinates } | Geometry::MultiLineString { coordinates } => {
            coordinates.iter().flatten().copied().collect()
        }
        Geometry::MultiPolygon { coordinates } => {
            coordinates.iter().flatten().flatten().copied().collect()
        }
        Geometry::GeometryCollection { geometries } => {
            geometries.iter().flat_map(vertices).collect()
        }
        // `Geometry` is `#[non_exhaustive]`; an unknown kind exposes no
        // coordinates this crate can read, so it contributes no vertices and
        // the centroid of a geometry made only of such parts is `None`.
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One degree of latitude at the equator is ~111.19 km; a 1°×1° cell is
    /// therefore ~1.236e10 m². Spherical excess must land within a fraction of
    /// a percent of that.
    #[test]
    fn area_of_unit_degree_cell_at_equator() {
        let poly = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let area = st_area(&poly);
        let expected = 1.236e10;
        assert!(
            (area - expected).abs() / expected < 0.01,
            "expected ~{expected} m², got {area}"
        );
    }

    /// Winding order must not change the magnitude of the result.
    #[test]
    fn area_is_orientation_independent() {
        let ccw = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [1.0, 0.0],
            [1.0, 1.0],
            [0.0, 1.0],
            [0.0, 0.0],
        ]]);
        let cw = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [0.0, 1.0],
            [1.0, 1.0],
            [1.0, 0.0],
            [0.0, 0.0],
        ]]);
        assert!((st_area(&ccw) - st_area(&cw)).abs() < 1.0);
    }

    #[test]
    fn area_subtracts_holes() {
        let solid = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]]);
        let with_hole = Geometry::polygon(vec![
            vec![[0.0, 0.0], [4.0, 0.0], [4.0, 4.0], [0.0, 4.0], [0.0, 0.0]],
            vec![[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 2.0], [1.0, 1.0]],
        ]);
        assert!(
            st_area(&with_hole) < st_area(&solid),
            "hole must reduce the measured area"
        );
    }

    #[test]
    fn area_of_non_areal_geometry_is_zero() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]);
        assert_eq!(st_area(&line), 0.0);
        assert_eq!(st_area(&Geometry::point(1.0, 2.0)), 0.0);
    }

    #[test]
    fn centroid_of_point_is_itself() {
        let p = Geometry::point(10.0, 20.0);
        assert_eq!(st_centroid(&p), Some(Geometry::point(10.0, 20.0)));
    }

    #[test]
    fn centroid_of_square_is_its_center() {
        let square = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [2.0, 0.0],
            [2.0, 2.0],
            [0.0, 2.0],
            [0.0, 0.0],
        ]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&square) else {
            panic!("expected a Point centroid");
        };
        assert!((coordinates[0] - 1.0).abs() < 1e-9);
        assert!((coordinates[1] - 1.0).abs() < 1e-9);
    }

    /// An L-shape's centroid must sit at the area-weighted center, which the
    /// vertex mean would get wrong.
    #[test]
    fn centroid_is_area_weighted_not_vertex_mean() {
        let l_shape = Geometry::polygon(vec![vec![
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 1.0],
            [1.0, 1.0],
            [1.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&l_shape) else {
            panic!("expected a Point centroid");
        };
        // A 4x1 arm (area 4, centroid (2, 0.5)) plus a 1x3 arm (area 3,
        // centroid (0.5, 2.5)) gives 9.5/7 ≈ 1.357 on both axes. The vertex
        // mean would be 10/6 ≈ 1.667, so this discriminates the two.
        let expected = 9.5 / 7.0;
        assert!(
            (coordinates[0] - expected).abs() < 0.01 && (coordinates[1] - expected).abs() < 0.01,
            "expected ~({expected}, {expected}), got {coordinates:?}"
        );
    }

    /// A collection holding both a polygon and a distant point must report the
    /// polygon's centroid — areal dimension outranks punctual.
    #[test]
    fn centroid_of_collection_prefers_areal_components() {
        let collection = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::polygon(vec![vec![
                    [0.0, 0.0],
                    [2.0, 0.0],
                    [2.0, 2.0],
                    [0.0, 2.0],
                    [0.0, 0.0],
                ]]),
                Geometry::point(100.0, 100.0),
            ],
        };
        let Some(Geometry::Point { coordinates }) = st_centroid(&collection) else {
            panic!("expected a Point centroid");
        };
        assert!(
            (coordinates[0] - 1.0).abs() < 1e-6,
            "stray point must not pull the centroid, got {coordinates:?}"
        );
    }

    /// A zero-area polygon has no areal mass; the centroid must fall back
    /// rather than divide by zero and vanish.
    #[test]
    fn centroid_of_degenerate_polygon_falls_back() {
        let collapsed =
            Geometry::polygon(vec![vec![[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]]);
        let centroid = st_centroid(&collapsed);
        assert!(
            centroid.is_some(),
            "a collinear ring must still yield a centroid"
        );
    }

    #[test]
    fn centroid_of_linestring_is_length_weighted() {
        // A long western segment and a short eastern one: the centroid must
        // sit inside the long segment, not at the vertex mean.
        let line = Geometry::line_string(vec![[0.0, 0.0], [10.0, 0.0], [11.0, 0.0]]);
        let Some(Geometry::Point { coordinates }) = st_centroid(&line) else {
            panic!("expected a Point centroid");
        };
        assert!(
            coordinates[0] < 6.0,
            "length weighting must bias toward the long segment, got {coordinates:?}"
        );
    }

    #[test]
    fn centroid_of_empty_geometry_is_none() {
        let empty = Geometry::GeometryCollection { geometries: vec![] };
        assert_eq!(st_centroid(&empty), None);
    }
}
