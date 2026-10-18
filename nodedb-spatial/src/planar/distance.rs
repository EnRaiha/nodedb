// SPDX-License-Identifier: Apache-2.0

//! Planar distance and length of projected geometries, in metres.
//!
//! Every measure is Euclidean in coordinate units, which for a projected SRID
//! are metres on the grid.

use nodedb_types::geometry::{Geometry, point_in_polygon};
use nodedb_types::{BboxSpace, geometry_bbox_in};

use crate::predicates::edge::{point_on_ring_boundary, ring_edges, segment_to_segment_dist_sq};

/// The pieces of a geometry a distance is measured between.
#[derive(Default)]
struct Parts<'a> {
    /// Every edge, with a lone point as a zero-length edge.
    segments: Vec<([f64; 2], [f64; 2])>,
    /// Every vertex.
    vertices: Vec<[f64; 2]>,
    /// Every polygon, as its rings.
    polygons: Vec<&'a [Vec<[f64; 2]>]>,
}

impl<'a> Parts<'a> {
    fn of(geom: &'a Geometry) -> Self {
        let mut parts = Parts::default();
        parts.add(geom);
        parts
    }

    fn add(&mut self, geom: &'a Geometry) {
        match geom {
            Geometry::Point { coordinates } => self.add_point(*coordinates),
            Geometry::MultiPoint { coordinates } => {
                coordinates.iter().for_each(|c| self.add_point(*c));
            }
            Geometry::LineString { coordinates } => self.add_path(coordinates),
            Geometry::MultiLineString { coordinates } => {
                coordinates.iter().for_each(|line| self.add_path(line));
            }
            Geometry::Polygon { coordinates } => self.add_polygon(coordinates),
            Geometry::MultiPolygon { coordinates } => {
                coordinates.iter().for_each(|rings| self.add_polygon(rings));
            }
            Geometry::GeometryCollection { geometries } => {
                geometries.iter().for_each(|g| self.add(g));
            }
            // `Geometry` is `#[non_exhaustive]`; a kind added upstream exposes
            // no coordinates this crate can read.
            _ => {}
        }
    }

    fn add_point(&mut self, c: [f64; 2]) {
        self.segments.push((c, c));
        self.vertices.push(c);
    }

    fn add_path(&mut self, path: &[[f64; 2]]) {
        match path {
            [] => {}
            [only] => self.add_point(*only),
            _ => {
                self.segments.extend(path.windows(2).map(|w| (w[0], w[1])));
                self.vertices.extend_from_slice(path);
            }
        }
    }

    fn add_polygon(&mut self, rings: &'a [Vec<[f64; 2]>]) {
        for ring in rings {
            self.segments.extend(ring_edges(ring));
            self.vertices.extend_from_slice(ring);
        }
        self.polygons.push(rings);
    }

    /// Whether any polygon of `self` covers any vertex of `other`.
    fn covers_any_vertex_of(&self, other: &Parts<'_>) -> bool {
        self.polygons
            .iter()
            .any(|rings| other.vertices.iter().any(|v| polygon_covers(rings, *v)))
    }
}

/// Whether `pt` lies inside or on a polygon, outside every hole interior.
fn polygon_covers(rings: &[Vec<[f64; 2]>], pt: [f64; 2]) -> bool {
    let Some(exterior) = rings.first() else {
        return false;
    };
    if !point_in_polygon(pt[0], pt[1], exterior) && !point_on_ring_boundary(pt, exterior) {
        return false;
    }
    !rings[1..]
        .iter()
        .any(|hole| point_in_polygon(pt[0], pt[1], hole) && !point_on_ring_boundary(pt, hole))
}

/// Minimum Euclidean distance between two geometries, in coordinate units.
///
/// Zero when one geometry's polygon covers a vertex of the other, or when
/// any edges cross. Otherwise the minimum edge-to-edge distance. An empty
/// geometry is infinitely far from everything.
pub fn planar_distance(a: &Geometry, b: &Geometry) -> f64 {
    let pa = Parts::of(a);
    let pb = Parts::of(b);
    if pa.covers_any_vertex_of(&pb) || pb.covers_any_vertex_of(&pa) {
        return 0.0;
    }
    let mut min_sq = f64::INFINITY;
    for &(a1, a2) in &pa.segments {
        for &(b1, b2) in &pb.segments {
            min_sq = min_sq.min(segment_to_segment_dist_sq(a1, a2, b1, b2));
        }
    }
    min_sq.sqrt()
}

/// Whether two geometries lie within `distance` coordinate units.
///
/// A planar bbox pre-filter rejects clearly distant pairs before the exact
/// distance runs.
pub fn planar_dwithin(a: &Geometry, b: &Geometry, distance: f64) -> bool {
    let a_bb = geometry_bbox_in(a, BboxSpace::Planar).expand_meters_in(distance, BboxSpace::Planar);
    let b_bb = geometry_bbox_in(b, BboxSpace::Planar);
    if !a_bb.intersects(&b_bb) {
        return false;
    }
    planar_distance(a, b) <= distance
}

fn path_length(path: &[[f64; 2]]) -> f64 {
    path.windows(2)
        .map(|w| (w[1][0] - w[0][0]).hypot(w[1][1] - w[0][1]))
        .sum()
}

fn closed_ring_length(ring: &[[f64; 2]]) -> f64 {
    ring_edges(ring)
        .iter()
        .map(|(a, b)| (b[0] - a[0]).hypot(b[1] - a[1]))
        .sum()
}

/// Total Euclidean length of every linear component. Areal geometries and
/// points have no length, matching PostGIS.
pub fn planar_length(geom: &Geometry) -> f64 {
    match geom {
        Geometry::LineString { coordinates } => path_length(coordinates),
        Geometry::MultiLineString { coordinates } => {
            coordinates.iter().map(|line| path_length(line)).sum()
        }
        Geometry::GeometryCollection { geometries } => geometries.iter().map(planar_length).sum(),
        Geometry::Point { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::Polygon { .. }
        | Geometry::MultiPolygon { .. } => 0.0,
        // `Geometry` is `#[non_exhaustive]`; a kind added upstream is not
        // known to be linear.
        _ => 0.0,
    }
}

/// Total Euclidean length of every areal component's rings, holes included.
/// An unclosed ring is closed implicitly.
pub fn planar_perimeter(geom: &Geometry) -> f64 {
    match geom {
        Geometry::Polygon { coordinates } => coordinates
            .iter()
            .map(|ring| closed_ring_length(ring))
            .sum(),
        Geometry::MultiPolygon { coordinates } => coordinates
            .iter()
            .flat_map(|rings| rings.iter())
            .map(|ring| closed_ring_length(ring))
            .sum(),
        Geometry::GeometryCollection { geometries } => {
            geometries.iter().map(planar_perimeter).sum()
        }
        Geometry::Point { .. }
        | Geometry::MultiPoint { .. }
        | Geometry::LineString { .. }
        | Geometry::MultiLineString { .. } => 0.0,
        // `Geometry` is `#[non_exhaustive]`; a kind added upstream is not
        // known to be areal.
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn square(x0: f64, y0: f64, side: f64) -> Geometry {
        Geometry::polygon(vec![vec![
            [x0, y0],
            [x0 + side, y0],
            [x0 + side, y0 + side],
            [x0, y0 + side],
            [x0, y0],
        ]])
    }

    #[test]
    fn point_to_point_is_euclidean() {
        let d = planar_distance(&Geometry::point(0.0, 0.0), &Geometry::point(300.0, 400.0));
        assert!((d - 500.0).abs() < 1e-9);
    }

    #[test]
    fn point_inside_polygon_is_zero() {
        let d = planar_distance(&square(0.0, 0.0, 100.0), &Geometry::point(50.0, 50.0));
        assert_eq!(d, 0.0);
    }

    #[test]
    fn point_to_polygon_edge() {
        let d = planar_distance(&square(0.0, 0.0, 100.0), &Geometry::point(150.0, 50.0));
        assert!((d - 50.0).abs() < 1e-9);
    }

    #[test]
    fn crossing_line_is_zero() {
        let line = Geometry::line_string(vec![[-10.0, 50.0], [110.0, 50.0]]);
        assert_eq!(planar_distance(&square(0.0, 0.0, 100.0), &line), 0.0);
    }

    #[test]
    fn point_in_hole_measures_to_hole_edge() {
        let poly = Geometry::polygon(vec![
            vec![
                [0.0, 0.0],
                [100.0, 0.0],
                [100.0, 100.0],
                [0.0, 100.0],
                [0.0, 0.0],
            ],
            vec![
                [40.0, 40.0],
                [60.0, 40.0],
                [60.0, 60.0],
                [40.0, 60.0],
                [40.0, 40.0],
            ],
        ]);
        let d = planar_distance(&poly, &Geometry::point(50.0, 50.0));
        assert!((d - 10.0).abs() < 1e-9);
    }

    #[test]
    fn dwithin_respects_threshold() {
        let a = Geometry::point(500_000.0, 0.0);
        let b = Geometry::point(500_300.0, 400.0);
        assert!(planar_dwithin(&a, &b, 500.0));
        assert!(!planar_dwithin(&a, &b, 499.0));
    }

    #[test]
    fn length_and_perimeter() {
        let line = Geometry::line_string(vec![[0.0, 0.0], [3.0, 4.0], [3.0, 10.0]]);
        assert!((planar_length(&line) - 11.0).abs() < 1e-9);
        assert!((planar_perimeter(&square(0.0, 0.0, 100.0)) - 400.0).abs() < 1e-9);
        assert_eq!(planar_length(&square(0.0, 0.0, 1.0)), 0.0);
    }
}
