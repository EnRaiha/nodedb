// SPDX-License-Identifier: Apache-2.0

//! Hotine Oblique Mercator, by IOGP Guidance Note 7-2 §3.2.4.
//!
//! Variant A (EPSG method 9812) measures u from the natural origin, the PROJ
//! `+no_uoff` form. Variant B (EPSG method 9815) measures u from the
//! projection centre.

use std::f64::consts::FRAC_PI_4;

use nodedb_types::geometry::Srid;

use super::ellipsoid::Ellipsoid;
use super::error::{CrsError, DomainReason, check_finite, check_lon_lat};

/// |cos αc| below which the initial-line azimuth counts as 90°, where
/// GN7-2 replaces the uc formula with uc = A (λc − λ0).
const AZIMUTH_RIGHT_ANGLE_EPS: f64 = 1e-12;

/// Where u is measured from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HomVariant {
    /// EPSG 9812: from the natural origin. False easting and northing apply
    /// at the natural origin.
    A,
    /// EPSG 9815: from the projection centre. False easting and northing
    /// apply at the projection centre.
    B,
}

/// Defining parameters of one Hotine Oblique Mercator projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HomParams {
    pub variant: HomVariant,
    /// Latitude of the projection centre φc, degrees.
    pub lat_c_deg: f64,
    /// Longitude of the projection centre λc, degrees.
    pub lon_c_deg: f64,
    /// Azimuth of the initial line αc, degrees.
    pub alpha_c_deg: f64,
    /// Angle from the rectified grid to the skew grid γc, degrees.
    pub gamma_c_deg: f64,
    /// Scale factor on the initial line.
    pub k_c: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
    pub ellipsoid: Ellipsoid,
}

/// A Hotine Oblique Mercator projection with its constants computed.
#[derive(Debug, Clone)]
pub struct HotineObliqueMercator {
    srid: Srid,
    params: HomParams,
    e: f64,
    e2: f64,
    b: f64,
    a: f64,
    h: f64,
    sin_g0: f64,
    cos_g0: f64,
    sin_gc: f64,
    cos_gc: f64,
    /// λ0 in radians.
    lon0: f64,
    /// Offset of u for variant B: |uc| · sign(φc). Zero for variant A.
    u_offset: f64,
}

impl HotineObliqueMercator {
    pub fn new(srid: Srid, params: HomParams) -> Self {
        let el = params.ellipsoid;
        let (e, e2) = (el.e(), el.e2());
        let phi_c = params.lat_c_deg.to_radians();
        let lam_c = params.lon_c_deg.to_radians();
        let alpha_c = params.alpha_c_deg.to_radians();
        let gamma_c = params.gamma_c_deg.to_radians();
        let (sin_pc, cos_pc) = phi_c.sin_cos();
        let sign_pc = if phi_c < 0.0 { -1.0 } else { 1.0 };

        let b = (1.0 + e2 * cos_pc.powi(4) / (1.0 - e2)).sqrt();
        let a = el.a * b * params.k_c * (1.0 - e2).sqrt() / (1.0 - e2 * sin_pc * sin_pc);
        let t0 = isometric_t(phi_c, e);
        let d = b * (1.0 - e2).sqrt() / (cos_pc * (1.0 - e2 * sin_pc * sin_pc).sqrt());
        // GN7-2: when D < 1, take D² = 1 so F stays real.
        let d2 = (d * d).max(1.0);
        let f = d + (d2 - 1.0).sqrt() * sign_pc;
        let h = f * t0.powf(b);
        let g = (f - 1.0 / f) / 2.0;
        let gamma0 = (alpha_c.sin() / d).asin();
        let lon0 = lam_c - (g * gamma0.tan()).asin() / b;
        let u_offset = match params.variant {
            HomVariant::A => 0.0,
            HomVariant::B => {
                let uc = if alpha_c.cos().abs() < AZIMUTH_RIGHT_ANGLE_EPS {
                    a * (lam_c - lon0)
                } else {
                    a / b * ((d2 - 1.0).sqrt() / alpha_c.cos()).atan() * sign_pc
                };
                uc.abs() * sign_pc
            }
        };
        let (sin_g0, cos_g0) = gamma0.sin_cos();
        let (sin_gc, cos_gc) = gamma_c.sin_cos();
        Self {
            srid,
            params,
            e,
            e2,
            b,
            a,
            h,
            sin_g0,
            cos_g0,
            sin_gc,
            cos_gc,
            lon0,
            u_offset,
        }
    }

    /// Project `[lon, lat]` degrees to `[easting, northing]` metres.
    pub fn forward(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        check_lon_lat(self.srid, coord)?;
        let phi = coord[1].to_radians();
        let lam = coord[0].to_radians();
        let t = isometric_t(phi, self.e);
        let q = self.h / t.powf(self.b);
        let s = (q - 1.0 / q) / 2.0;
        let tt = (q + 1.0 / q) / 2.0;
        let (v_sin, b_dlam_cos) = (self.b * (lam - self.lon0)).sin_cos();
        let u_big = (-v_sin * self.cos_g0 + s * self.sin_g0) / tt;
        if !u_big.is_finite() || u_big.abs() >= 1.0 {
            return Err(CrsError::out_of_domain(
                self.srid,
                coord,
                DomainReason::ObliqueSingularity,
            ));
        }
        let v = self.a * ((1.0 - u_big) / (1.0 + u_big)).ln() / (2.0 * self.b);
        let u = self.a * (s * self.cos_g0 + v_sin * self.sin_g0).atan2(b_dlam_cos) / self.b
            - self.u_offset;
        let out = [
            v * self.cos_gc + u * self.sin_gc + self.params.false_easting,
            u * self.cos_gc - v * self.sin_gc + self.params.false_northing,
        ];
        check_finite(self.srid, out)?;
        Ok(out)
    }

    /// Unproject `[easting, northing]` metres to `[lon, lat]` degrees.
    pub fn inverse(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        check_finite(self.srid, coord)?;
        let de = coord[0] - self.params.false_easting;
        let dn = coord[1] - self.params.false_northing;
        let v = de * self.cos_gc - dn * self.sin_gc;
        let u = dn * self.cos_gc + de * self.sin_gc + self.u_offset;
        let q = (-self.b * v / self.a).exp();
        let s = (q - 1.0 / q) / 2.0;
        let tt = (q + 1.0 / q) / 2.0;
        let (v_sin, bu_cos) = (self.b * u / self.a).sin_cos();
        let u_big = (v_sin * self.cos_g0 + s * self.sin_g0) / tt;
        if !u_big.is_finite() || u_big.abs() >= 1.0 {
            return Err(CrsError::out_of_domain(
                self.srid,
                coord,
                DomainReason::ObliqueSingularity,
            ));
        }
        let t = (self.h / ((1.0 + u_big) / (1.0 - u_big)).sqrt()).powf(1.0 / self.b);
        let chi = std::f64::consts::FRAC_PI_2 - 2.0 * t.atan();
        let phi = latitude_from_conformal(chi, self.e2);
        let lam = self.lon0 - (s * self.cos_g0 - v_sin * self.sin_g0).atan2(bu_cos) / self.b;
        let out = [lam.to_degrees(), phi.to_degrees()];
        check_finite(self.srid, out)?;
        Ok(out)
    }
}

/// The GN7-2 t function: tan(π/4 − φ/2) / [(1 − e sin φ)/(1 + e sin φ)]^(e/2).
fn isometric_t(phi: f64, e: f64) -> f64 {
    let es = e * phi.sin();
    (FRAC_PI_4 - phi / 2.0).tan() / ((1.0 - es) / (1.0 + es)).powf(e / 2.0)
}

/// Geodetic latitude from conformal latitude χ, the GN7-2 series in e².
/// The first omitted term is of order e¹⁰ ≈ 1.4e-11 rad, under 0.1 mm.
fn latitude_from_conformal(chi: f64, e2: f64) -> f64 {
    let e4 = e2 * e2;
    let e6 = e4 * e2;
    let e8 = e6 * e2;
    chi + (2.0 * chi).sin() * (e2 / 2.0 + 5.0 * e4 / 24.0 + e6 / 12.0 + 13.0 * e8 / 360.0)
        + (4.0 * chi).sin() * (7.0 * e4 / 48.0 + 29.0 * e6 / 240.0 + 811.0 * e8 / 11520.0)
        + (6.0 * chi).sin() * (7.0 * e6 / 120.0 + 81.0 * e8 / 1120.0)
        + (8.0 * chi).sin() * (4279.0 * e8 / 161_280.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::params::{BORNEO_RSO, PENINSULAR_RSO};

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    fn rso_peninsular() -> HotineObliqueMercator {
        HotineObliqueMercator::new(Srid::PeninsularRso, PENINSULAR_RSO)
    }

    fn rso_borneo() -> HotineObliqueMercator {
        HotineObliqueMercator::new(Srid::BorneoRso, BORNEO_RSO)
    }

    fn assert_close(got: [f64; 2], want: [f64; 2]) {
        assert!(
            (got[0] - want[0]).abs() < MM && (got[1] - want[1]).abs() < MM,
            "got {got:?}, want {want:?}"
        );
    }

    // Reference values: PROJ 9.8.1 via pyproj, EPSG:4326 → target, always_xy.

    #[test]
    fn peninsular_rso_klcc() {
        let got = rso_peninsular().forward([101.7117, 3.1579]).unwrap();
        assert_close(got, [412_834.468_1, 349_472.917_7]);
    }

    #[test]
    fn peninsular_rso_105_5() {
        let got = rso_peninsular().forward([105.0, 5.0]).unwrap();
        assert_close(got, [778_104.319_5, 553_022.237_0]);
    }

    #[test]
    fn borneo_rso_kota_kinabalu() {
        let got = rso_borneo().forward([116.0735, 5.9804]).unwrap();
        assert_close(got, [708_697.150_6, 662_326.916_8]);
    }

    #[test]
    fn borneo_rso_105_5() {
        let got = rso_borneo().forward([105.0, 5.0]).unwrap();
        assert_close(got, [-521_596.204_7, 554_604.058_6]);
    }

    #[test]
    fn inverse_recovers_input_to_a_millimetre() {
        for hom in [rso_peninsular(), rso_borneo()] {
            for input in [[101.7117, 3.1579], [116.0735, 5.9804], [105.0, 5.0]] {
                let projected = hom.forward(input).unwrap();
                let back = hom.inverse(projected).unwrap();
                let again = hom.forward(back).unwrap();
                assert_close(again, projected);
            }
        }
    }

    /// Variant B moves u to the projection centre: the centre itself maps to
    /// the false origin.
    #[test]
    fn variant_b_maps_centre_to_false_origin() {
        let params = HomParams {
            variant: HomVariant::B,
            false_easting: 0.0,
            false_northing: 0.0,
            ..PENINSULAR_RSO
        };
        let hom = HotineObliqueMercator::new(Srid::PeninsularRso, params);
        let centre = hom.forward([params.lon_c_deg, params.lat_c_deg]).unwrap();
        assert_close(centre, [0.0, 0.0]);
        let back = hom.inverse(centre).unwrap();
        assert!((back[0] - params.lon_c_deg).abs() < 1e-9);
        assert!((back[1] - params.lat_c_deg).abs() < 1e-9);
    }

    #[test]
    fn pole_is_out_of_domain() {
        assert!(rso_peninsular().forward([102.0, 90.0]).is_err());
        assert!(rso_peninsular().forward([102.0, 90.5]).is_err());
    }
}
