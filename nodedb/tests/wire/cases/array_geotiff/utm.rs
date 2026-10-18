// SPDX-License-Identifier: BUSL-1.1

//! An independent answer for zone queries: a UTM inverse from Snyder,
//! "Map Projections: A Working Manual" (USGS PP 1395, 1987), eqs. 8-18 to
//! 8-25, an even-odd point-in-polygon test, and a point-to-edge distance.
//! None of it shares code with the engine.

/// WGS 84 semi-major axis, metres.
const A: f64 = 6_378_137.0;
/// WGS 84 flattening.
const F: f64 = 1.0 / 298.257_223_563;
/// UTM scale factor on the central meridian.
const K0: f64 = 0.9996;
/// UTM false easting, metres.
const FALSE_EASTING: f64 = 500_000.0;
/// Central meridian of UTM zone 47, degrees.
const ZONE_47_MERIDIAN: f64 = 99.0;

/// `(lon, lat)` in degrees of the EPSG:32647 point `(easting, northing)`.
pub fn utm47n_to_lonlat(easting: f64, northing: f64) -> (f64, f64) {
    let e2 = F * (2.0 - F);
    let ep2 = e2 / (1.0 - e2);
    let m = northing / K0;
    let mu = m / (A * (1.0 - e2 / 4.0 - 3.0 * e2.powi(2) / 64.0 - 5.0 * e2.powi(3) / 256.0));
    let e1 = (1.0 - (1.0 - e2).sqrt()) / (1.0 + (1.0 - e2).sqrt());
    let phi1 = mu
        + (3.0 * e1 / 2.0 - 27.0 * e1.powi(3) / 32.0) * (2.0 * mu).sin()
        + (21.0 * e1.powi(2) / 16.0 - 55.0 * e1.powi(4) / 32.0) * (4.0 * mu).sin()
        + (151.0 * e1.powi(3) / 96.0) * (6.0 * mu).sin()
        + (1097.0 * e1.powi(4) / 512.0) * (8.0 * mu).sin();
    let (sin1, cos1, tan1) = (phi1.sin(), phi1.cos(), phi1.tan());
    let c1 = ep2 * cos1.powi(2);
    let t1 = tan1.powi(2);
    let n1 = A / (1.0 - e2 * sin1.powi(2)).sqrt();
    let r1 = A * (1.0 - e2) / (1.0 - e2 * sin1.powi(2)).powf(1.5);
    let d = (easting - FALSE_EASTING) / (n1 * K0);
    let phi = phi1
        - (n1 * tan1 / r1)
            * (d.powi(2) / 2.0
                - (5.0 + 3.0 * t1 + 10.0 * c1 - 4.0 * c1.powi(2) - 9.0 * ep2) * d.powi(4) / 24.0
                + (61.0 + 90.0 * t1 + 298.0 * c1 + 45.0 * t1.powi(2)
                    - 252.0 * ep2
                    - 3.0 * c1.powi(2))
                    * d.powi(6)
                    / 720.0);
    let lambda = (d - (1.0 + 2.0 * t1 + c1) * d.powi(3) / 6.0
        + (5.0 - 2.0 * c1 + 28.0 * t1 - 3.0 * c1.powi(2) + 8.0 * ep2 + 24.0 * t1.powi(2))
            * d.powi(5)
            / 120.0)
        / cos1;
    (ZONE_47_MERIDIAN + lambda.to_degrees(), phi.to_degrees())
}

/// Even-odd test of `point` against the closed `ring`, both `(lon, lat)`.
pub fn inside(point: (f64, f64), ring: &[(f64, f64)]) -> bool {
    let (x, y) = point;
    let mut odd = false;
    for (i, &(x1, y1)) in ring.iter().enumerate() {
        let (x2, y2) = ring[(i + 1) % ring.len()];
        if (y1 > y) != (y2 > y) {
            let crossing = x1 + (y - y1) * (x2 - x1) / (y2 - y1);
            if x < crossing {
                odd = !odd;
            }
        }
    }
    odd
}

/// Metres from `point` to the nearest edge of `ring`, with lon/lat scaled
/// by the WGS 84 radii of curvature at the point's latitude.
pub fn edge_distance_m(point: (f64, f64), ring: &[(f64, f64)]) -> f64 {
    let e2 = F * (2.0 - F);
    let lat = point.1.to_radians();
    let w = (1.0 - e2 * lat.sin().powi(2)).sqrt();
    let meridional = A * (1.0 - e2) / w.powi(3);
    let normal = A / w;
    let kx = normal * lat.cos() * std::f64::consts::PI / 180.0;
    let ky = meridional * std::f64::consts::PI / 180.0;
    let scale = |(x, y): (f64, f64)| (x * kx, y * ky);
    let (px, py) = scale(point);
    let mut nearest = f64::INFINITY;
    for (i, &a) in ring.iter().enumerate() {
        let (ax, ay) = scale(a);
        let (bx, by) = scale(ring[(i + 1) % ring.len()]);
        let (dx, dy) = (bx - ax, by - ay);
        let t = (((px - ax) * dx + (py - ay) * dy) / (dx * dx + dy * dy)).clamp(0.0, 1.0);
        nearest = nearest.min((px - ax - t * dx).hypot(py - ay - t * dy));
    }
    nearest
}

#[test]
fn inverse_matches_a_known_point() {
    // The zone origin row: easting 500000 lies on the central meridian.
    let (lon, lat) = utm47n_to_lonlat(500_000.0, 0.0);
    assert!(
        (lon - 99.0).abs() < 1e-12 && lat.abs() < 1e-12,
        "({lon}, {lat})"
    );
    // The WGS 84 meridian arc from 0 to 1 degree north is 110574.389 m.
    let (lon, lat) = utm47n_to_lonlat(500_000.0, 110_574.389 * K0);
    assert!((lon - 99.0).abs() < 1e-12, "{lon}");
    assert!((lat - 1.0).abs() < 1e-7, "{lat}");
}
