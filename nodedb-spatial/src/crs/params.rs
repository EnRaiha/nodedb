// SPDX-License-Identifier: Apache-2.0

//! Defining parameters of every supported projected SRID.
//!
//! Values are the EPSG definitions as PROJ publishes them.
//!
//! Datum bound: GDM2000 (EPSG:3375, 3376) is ITRF2000 at epoch 2000.0. Its
//! published `towgs84` is all zeros, so WGS 84 → GDM2000 is taken as the
//! identity. GDM2000 is fixed to the plate while WGS 84 tracks ITRF, so the
//! identity drifts with Sundaland plate motion: about 1 m by 2026. Results in
//! 3375 / 3376 are exact to the projection, not to that datum offset.

use super::ellipsoid::{GRS80, WGS84};
use super::hom::{HomParams, HomVariant};
use super::tm::TmParams;

/// UTM scale factor on the central meridian.
const UTM_K0: f64 = 0.9996;

/// UTM false easting, metres.
const UTM_FALSE_EASTING: f64 = 500_000.0;

/// UTM false northing in the northern hemisphere, metres.
const UTM_FALSE_NORTHING_NORTH: f64 = 0.0;

/// RSO scale factor on the initial line, both Malaysian RSO grids.
const RSO_K: f64 = 0.99984;

/// EPSG:32647, UTM zone 47N on WGS 84.
pub const UTM_47N: TmParams = TmParams {
    lon0_deg: 99.0,
    k0: UTM_K0,
    false_easting: UTM_FALSE_EASTING,
    false_northing: UTM_FALSE_NORTHING_NORTH,
    ellipsoid: WGS84,
};

/// EPSG:32648, UTM zone 48N on WGS 84.
pub const UTM_48N: TmParams = TmParams {
    lon0_deg: 105.0,
    k0: UTM_K0,
    false_easting: UTM_FALSE_EASTING,
    false_northing: UTM_FALSE_NORTHING_NORTH,
    ellipsoid: WGS84,
};

/// EPSG:3375, GDM2000 Peninsula RSO.
/// `+proj=omerc +no_uoff +lat_0=4 +lonc=102.25 +alpha=323.025796466667
/// +gamma=323.130102361111 +k=0.99984 +x_0=804671 +y_0=0 +ellps=GRS80`.
pub const PENINSULAR_RSO: HomParams = HomParams {
    variant: HomVariant::A,
    lat_c_deg: 4.0,
    lon_c_deg: 102.25,
    alpha_c_deg: 323.025_796_466_667,
    gamma_c_deg: 323.130_102_361_111,
    k_c: RSO_K,
    false_easting: 804_671.0,
    false_northing: 0.0,
    ellipsoid: GRS80,
};

/// EPSG:3376, GDM2000 East Malaysia (Borneo) RSO.
/// `+proj=omerc +no_uoff +lat_0=4 +lonc=115 +alpha=53.31580995
/// +gamma=53.1301023611111 +k=0.99984 +x_0=0 +y_0=0 +ellps=GRS80`.
pub const BORNEO_RSO: HomParams = HomParams {
    variant: HomVariant::A,
    lat_c_deg: 4.0,
    lon_c_deg: 115.0,
    alpha_c_deg: 53.315_809_95,
    gamma_c_deg: 53.130_102_361_111_1,
    k_c: RSO_K,
    false_easting: 0.0,
    false_northing: 0.0,
    ellipsoid: GRS80,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn utm_zones_differ_only_in_central_meridian() {
        assert_eq!(UTM_47N.lon0_deg, 99.0);
        assert_eq!(UTM_48N.lon0_deg, 105.0);
        assert_eq!(
            TmParams {
                lon0_deg: UTM_48N.lon0_deg,
                ..UTM_47N
            },
            UTM_48N
        );
    }

    #[test]
    fn rso_grids_are_variant_a_on_grs80() {
        for p in [PENINSULAR_RSO, BORNEO_RSO] {
            assert_eq!(p.variant, HomVariant::A);
            assert_eq!(p.ellipsoid, GRS80);
            assert_eq!(p.k_c, RSO_K);
            assert_eq!(p.lat_c_deg, 4.0);
        }
    }
}
