// SPDX-License-Identifier: Apache-2.0

//! Planar buffer of projected geometries, by a distance in metres.

use nodedb_types::BboxSpace;
use nodedb_types::geometry::Geometry;

use crate::operations::st_buffer_in;

/// Buffer `geom` by `distance` coordinate units (metres for a projected
/// SRID), approximating curves with `segments` sides.
///
/// Uses the same construction as the geographic `st_buffer`, with metres
/// added to coordinates directly instead of converted to degrees.
pub fn planar_buffer(geom: &Geometry, distance: f64, segments: usize) -> Geometry {
    st_buffer_in(geom, distance, segments, BboxSpace::Planar)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planar::area::planar_area;

    #[test]
    fn buffered_point_area_approaches_circle() {
        let segments = 64;
        let circle = planar_buffer(&Geometry::point(500_000.0, 200_000.0), 100.0, segments);
        let area = planar_area(&circle);
        // A regular n-gon inscribed in radius r has area n r² sin(2π/n) / 2.
        let n = segments as f64;
        let expected = n * 100.0 * 100.0 * (std::f64::consts::TAU / n).sin() / 2.0;
        assert!(
            (area - expected).abs() < 1e-6,
            "area={area} expected={expected}"
        );
    }
}
