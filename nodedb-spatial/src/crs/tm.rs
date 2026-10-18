// SPDX-License-Identifier: Apache-2.0

//! Transverse Mercator, by the Krüger series to n⁶.
//!
//! Follows C. F. F. Karney, "Transverse Mercator with an accuracy of a few
//! nanometers", J. Geodesy 85 (2011), eqs. 7–9 and 35–36. Within 35° of the
//! central meridian the series errors stay far below a millimetre.

use nodedb_types::geometry::Srid;

use super::ellipsoid::Ellipsoid;
use super::error::{CrsError, DomainReason, check_finite, check_lon_lat};

/// Largest |λ − λ0| in degrees that the projection accepts. Karney 2011
/// bounds the n⁶ series error below 5 nm inside 35° of the central meridian.
pub const TM_MAX_LON_OFFSET_DEG: f64 = 35.0;

/// Newton iterations allowed when inverting τ' → τ. Karney 2011 §2.2 reports
/// convergence to double precision in two; the cap only bounds bad input.
pub const TAU_NEWTON_MAX_ITERS: usize = 10;

/// Newton step on τ below which the inverse stops. A step this small moves
/// latitude by under 1e-14 rad, about 64 nm on the ground.
pub const TAU_TOLERANCE: f64 = 1e-14;

/// Half a turn in degrees, for wrapping a longitude difference.
const HALF_TURN_DEG: f64 = 180.0;

/// A full turn in degrees.
const FULL_TURN_DEG: f64 = 360.0;

/// Defining parameters of one Transverse Mercator projection.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TmParams {
    /// Central meridian λ0, degrees.
    pub lon0_deg: f64,
    /// Scale factor on the central meridian.
    pub k0: f64,
    /// False easting, metres.
    pub false_easting: f64,
    /// False northing, metres.
    pub false_northing: f64,
    pub ellipsoid: Ellipsoid,
}

/// A Transverse Mercator projection with its series coefficients computed.
#[derive(Debug, Clone)]
pub struct TransverseMercator {
    srid: Srid,
    params: TmParams,
    e: f64,
    e2: f64,
    /// k0 · A, where A is the rectifying radius.
    k0_a: f64,
    /// Krüger α coefficients (forward).
    alpha: [f64; 6],
    /// Krüger β coefficients (inverse).
    beta: [f64; 6],
}

impl TransverseMercator {
    pub fn new(srid: Srid, params: TmParams) -> Self {
        let n = params.ellipsoid.n();
        let n2 = n * n;
        let n3 = n2 * n;
        let n4 = n3 * n;
        let n5 = n4 * n;
        let n6 = n5 * n;
        // Rectifying radius A, Karney 2011 eq. 14.
        let a_rect = params.ellipsoid.a / (1.0 + n) * (1.0 + n2 / 4.0 + n4 / 64.0 + n6 / 256.0);
        // Karney 2011 eq. 35.
        let alpha = [
            n / 2.0 - 2.0 / 3.0 * n2 + 5.0 / 16.0 * n3 + 41.0 / 180.0 * n4 - 127.0 / 288.0 * n5
                + 7891.0 / 37800.0 * n6,
            13.0 / 48.0 * n2 - 3.0 / 5.0 * n3 + 557.0 / 1440.0 * n4 + 281.0 / 630.0 * n5
                - 1_983_433.0 / 1_935_360.0 * n6,
            61.0 / 240.0 * n3 - 103.0 / 140.0 * n4
                + 15061.0 / 26880.0 * n5
                + 167_603.0 / 181_440.0 * n6,
            49561.0 / 161_280.0 * n4 - 179.0 / 168.0 * n5 + 6_601_661.0 / 7_257_600.0 * n6,
            34729.0 / 80640.0 * n5 - 3_418_889.0 / 1_995_840.0 * n6,
            212_378_941.0 / 319_334_400.0 * n6,
        ];
        // Karney 2011 eq. 36.
        let beta = [
            n / 2.0 - 2.0 / 3.0 * n2 + 37.0 / 96.0 * n3 - 1.0 / 360.0 * n4 - 81.0 / 512.0 * n5
                + 96199.0 / 604_800.0 * n6,
            1.0 / 48.0 * n2 + 1.0 / 15.0 * n3 - 437.0 / 1440.0 * n4 + 46.0 / 105.0 * n5
                - 1_118_711.0 / 3_870_720.0 * n6,
            17.0 / 480.0 * n3 - 37.0 / 840.0 * n4 - 209.0 / 4480.0 * n5 + 5569.0 / 90720.0 * n6,
            4397.0 / 161_280.0 * n4 - 11.0 / 504.0 * n5 - 830_251.0 / 7_257_600.0 * n6,
            4583.0 / 161_280.0 * n5 - 108_847.0 / 3_991_680.0 * n6,
            20_648_693.0 / 638_668_800.0 * n6,
        ];
        Self {
            srid,
            params,
            e: params.ellipsoid.e(),
            e2: params.ellipsoid.e2(),
            k0_a: params.k0 * a_rect,
            alpha,
            beta,
        }
    }

    /// Project `[lon, lat]` degrees to `[easting, northing]` metres.
    pub fn forward(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        check_lon_lat(self.srid, coord)?;
        let [lon, lat] = coord;
        let dlon_deg = wrap_lon_offset(lon - self.params.lon0_deg);
        if dlon_deg.abs() > TM_MAX_LON_OFFSET_DEG {
            return Err(CrsError::out_of_domain(
                self.srid,
                coord,
                DomainReason::CentralMeridianOffset,
            ));
        }
        let dlon = dlon_deg.to_radians();
        let tau_p = conformal_tau(lat.to_radians().tan(), self.e);
        let xi_p = tau_p.atan2(dlon.cos());
        let eta_p = (dlon.sin() / tau_p.hypot(dlon.cos())).asinh();
        let mut xi = xi_p;
        let mut eta = eta_p;
        for (j, a) in self.alpha.iter().enumerate() {
            let k = 2.0 * (j + 1) as f64;
            xi += a * (k * xi_p).sin() * (k * eta_p).cosh();
            eta += a * (k * xi_p).cos() * (k * eta_p).sinh();
        }
        let out = [
            self.params.false_easting + self.k0_a * eta,
            self.params.false_northing + self.k0_a * xi,
        ];
        check_finite(self.srid, out)?;
        Ok(out)
    }

    /// Unproject `[easting, northing]` metres to `[lon, lat]` degrees.
    pub fn inverse(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        check_finite(self.srid, coord)?;
        let xi = (coord[1] - self.params.false_northing) / self.k0_a;
        let eta = (coord[0] - self.params.false_easting) / self.k0_a;
        let mut xi_p = xi;
        let mut eta_p = eta;
        for (j, b) in self.beta.iter().enumerate() {
            let k = 2.0 * (j + 1) as f64;
            xi_p -= b * (k * xi).sin() * (k * eta).cosh();
            eta_p -= b * (k * xi).cos() * (k * eta).sinh();
        }
        let tau_p = xi_p.sin() / eta_p.sinh().hypot(xi_p.cos());
        let dlon = eta_p.sinh().atan2(xi_p.cos());
        let tau = self.tau_from_conformal(tau_p);
        let dlon_deg = dlon.to_degrees();
        if dlon_deg.abs() > TM_MAX_LON_OFFSET_DEG {
            return Err(CrsError::out_of_domain(
                self.srid,
                coord,
                DomainReason::CentralMeridianOffset,
            ));
        }
        let out = [self.params.lon0_deg + dlon_deg, tau.atan().to_degrees()];
        check_finite(self.srid, out)?;
        Ok(out)
    }

    /// Solve τ' = τ'(τ) for τ by Newton's method, Karney 2011 eqs. 19–21.
    fn tau_from_conformal(&self, tau_p: f64) -> f64 {
        let one_minus_e2 = 1.0 - self.e2;
        let mut tau = tau_p;
        for _ in 0..TAU_NEWTON_MAX_ITERS {
            let tau_i_p = conformal_tau(tau, self.e);
            let step = (tau_p - tau_i_p) / (1.0 + tau_i_p * tau_i_p).sqrt()
                * (1.0 + one_minus_e2 * tau * tau)
                / (one_minus_e2 * (1.0 + tau * tau).sqrt());
            tau += step;
            if step.abs() < TAU_TOLERANCE {
                break;
            }
        }
        tau
    }
}

/// Conformal-latitude tangent τ' from geodetic τ = tan φ, Karney 2011 eq. 7.
fn conformal_tau(tau: f64, e: f64) -> f64 {
    let tau1 = (1.0 + tau * tau).sqrt();
    let sigma = (e * (e * tau / tau1).atanh()).sinh();
    tau * (1.0 + sigma * sigma).sqrt() - sigma * tau1
}

/// Wrap a longitude difference into [-180°, 180°].
fn wrap_lon_offset(dlon_deg: f64) -> f64 {
    let d = dlon_deg % FULL_TURN_DEG;
    if d > HALF_TURN_DEG {
        d - FULL_TURN_DEG
    } else if d < -HALF_TURN_DEG {
        d + FULL_TURN_DEG
    } else {
        d
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::params::{UTM_47N, UTM_48N};

    /// One millimetre, the tolerance for every PROJ reference value.
    const MM: f64 = 1e-3;

    fn utm47() -> TransverseMercator {
        TransverseMercator::new(Srid::Utm47N, UTM_47N)
    }

    fn utm48() -> TransverseMercator {
        TransverseMercator::new(Srid::Utm48N, UTM_48N)
    }

    fn assert_close(got: [f64; 2], want: [f64; 2]) {
        assert!(
            (got[0] - want[0]).abs() < MM && (got[1] - want[1]).abs() < MM,
            "got {got:?}, want {want:?}"
        );
    }

    // Reference values: PROJ 9.8.1 via pyproj, EPSG:4326 → target, always_xy.

    #[test]
    fn utm47n_klcc() {
        let got = utm47().forward([101.7117, 3.1579]).unwrap();
        assert_close(got, [801_401.794_9, 349_439.510_6]);
    }

    #[test]
    fn utm48n_klcc() {
        let got = utm48().forward([101.7117, 3.1579]).unwrap();
        assert_close(got, [134_445.592_4, 349_624.727_8]);
    }

    #[test]
    fn utm48n_origin_on_central_meridian() {
        assert_close(utm48().forward([105.0, 0.0]).unwrap(), [500_000.0, 0.0]);
    }

    #[test]
    fn utm48n_central_meridian_five_north() {
        let got = utm48().forward([105.0, 5.0]).unwrap();
        assert_close(got, [500_000.0, 552_664.296_9]);
    }

    #[test]
    fn inverse_recovers_input() {
        let tm = utm48();
        for input in [
            [101.7117, 3.1579],
            [105.0, 0.0],
            [109.5, 6.2],
            [100.0, -2.0],
        ] {
            let back = tm.inverse(tm.forward(input).unwrap()).unwrap();
            assert!(
                (back[0] - input[0]).abs() < 1e-9 && (back[1] - input[1]).abs() < 1e-9,
                "{input:?} -> {back:?}"
            );
        }
    }

    #[test]
    fn far_from_central_meridian_is_out_of_domain() {
        let err = utm48().forward([150.0, 3.0]).unwrap_err();
        assert!(matches!(
            err,
            CrsError::OutOfDomain {
                reason: DomainReason::CentralMeridianOffset,
                ..
            }
        ));
    }

    #[test]
    fn bad_latitude_and_non_finite_are_out_of_domain() {
        assert!(utm48().forward([105.0, 91.0]).is_err());
        assert!(utm48().forward([f64::NAN, 1.0]).is_err());
        assert!(utm48().inverse([f64::INFINITY, 1.0]).is_err());
    }

    #[test]
    fn wrap_keeps_offset_in_half_turn() {
        assert_eq!(wrap_lon_offset(200.0), -160.0);
        assert_eq!(wrap_lon_offset(-200.0), 160.0);
        assert_eq!(wrap_lon_offset(10.0), 10.0);
    }
}
