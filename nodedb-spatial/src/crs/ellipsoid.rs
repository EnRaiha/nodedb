// SPDX-License-Identifier: Apache-2.0

//! Reference ellipsoids.

/// An ellipsoid of revolution, defined by its semi-major axis and inverse
/// flattening.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Ellipsoid {
    /// Semi-major axis in metres.
    pub a: f64,
    /// Inverse flattening, 1/f.
    pub inv_f: f64,
}

/// GRS 1980 (EPSG:7019), the ellipsoid of GDM2000.
pub const GRS80: Ellipsoid = Ellipsoid {
    a: 6_378_137.0,
    inv_f: 298.257_222_101,
};

/// WGS 84 (EPSG:7030).
pub const WGS84: Ellipsoid = Ellipsoid {
    a: 6_378_137.0,
    inv_f: 298.257_223_563,
};

impl Ellipsoid {
    /// Flattening f.
    pub fn f(&self) -> f64 {
        1.0 / self.inv_f
    }

    /// First eccentricity squared, e² = f(2 − f).
    pub fn e2(&self) -> f64 {
        let f = self.f();
        f * (2.0 - f)
    }

    /// First eccentricity e.
    pub fn e(&self) -> f64 {
        self.e2().sqrt()
    }

    /// Third flattening n = f / (2 − f).
    pub fn n(&self) -> f64 {
        let f = self.f();
        f / (2.0 - f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grs80_and_wgs84_differ_only_in_flattening() {
        assert_eq!(GRS80.a, WGS84.a);
        assert!((GRS80.e2() - 0.006_694_380_022_90).abs() < 1e-14);
        assert!((WGS84.e2() - 0.006_694_379_990_14).abs() < 1e-14);
    }

    #[test]
    fn third_flattening_matches_definition() {
        let n = WGS84.n();
        let f = WGS84.f();
        assert!((n - f / (2.0 - f)).abs() < 1e-18);
        assert!((WGS84.e() * WGS84.e() - WGS84.e2()).abs() < 1e-18);
    }
}
