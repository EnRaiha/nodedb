// SPDX-License-Identifier: Apache-2.0

//! Coordinate transformation errors.

use nodedb_types::geometry::Srid;

/// A coordinate a projection cannot map.
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum CrsError {
    /// The coordinate lies outside the region where the projection is
    /// defined or accurate.
    #[error("coordinate ({x}, {y}) is outside the domain of SRID {srid}: {reason}")]
    OutOfDomain {
        srid: Srid,
        x: f64,
        y: f64,
        reason: DomainReason,
    },
}

/// Why a coordinate is outside a projection's domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainReason {
    /// An ordinate is NaN or infinite.
    NonFinite,
    /// Latitude is beyond ±90°.
    LatitudeRange,
    /// Longitude is too far from a Transverse Mercator central meridian for
    /// the Krüger series to hold.
    CentralMeridianOffset,
    /// The Hotine Oblique Mercator U term reaches ±1, where the projection
    /// diverges.
    ObliqueSingularity,
}

impl std::fmt::Display for DomainReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            DomainReason::NonFinite => "non-finite ordinate",
            DomainReason::LatitudeRange => "latitude beyond ±90°",
            DomainReason::CentralMeridianOffset => "too far from the central meridian",
            DomainReason::ObliqueSingularity => "on the oblique projection's singular line",
        })
    }
}

/// Largest absolute latitude, in degrees.
const MAX_LATITUDE_DEG: f64 = 90.0;

/// Fail unless `[lon, lat]` is a finite geographic coordinate with latitude
/// inside ±90°.
pub(crate) fn check_lon_lat(srid: Srid, coord: [f64; 2]) -> Result<(), CrsError> {
    let [lon, lat] = coord;
    if !lon.is_finite() || !lat.is_finite() {
        return Err(CrsError::out_of_domain(
            srid,
            coord,
            DomainReason::NonFinite,
        ));
    }
    if lat.abs() > MAX_LATITUDE_DEG {
        return Err(CrsError::out_of_domain(
            srid,
            coord,
            DomainReason::LatitudeRange,
        ));
    }
    Ok(())
}

/// Fail unless both ordinates are finite.
pub(crate) fn check_finite(srid: Srid, coord: [f64; 2]) -> Result<(), CrsError> {
    if coord[0].is_finite() && coord[1].is_finite() {
        Ok(())
    } else {
        Err(CrsError::out_of_domain(
            srid,
            coord,
            DomainReason::NonFinite,
        ))
    }
}

impl CrsError {
    pub(crate) fn out_of_domain(srid: Srid, [x, y]: [f64; 2], reason: DomainReason) -> Self {
        CrsError::OutOfDomain { srid, x, y, reason }
    }

    /// The SRID whose projection rejected the coordinate.
    pub fn srid(&self) -> Srid {
        match self {
            CrsError::OutOfDomain { srid, .. } => *srid,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lon_lat_checks() {
        assert!(check_lon_lat(Srid::Utm48N, [105.0, 90.0]).is_ok());
        assert_eq!(
            check_lon_lat(Srid::Utm48N, [105.0, 90.5]),
            Err(CrsError::out_of_domain(
                Srid::Utm48N,
                [105.0, 90.5],
                DomainReason::LatitudeRange
            ))
        );
        assert!(check_lon_lat(Srid::Utm48N, [f64::NAN, 0.0]).is_err());
        assert!(check_finite(Srid::BorneoRso, [0.0, f64::INFINITY]).is_err());
    }

    #[test]
    fn message_names_srid_and_reason() {
        let e = CrsError::out_of_domain(
            Srid::Utm47N,
            [150.0, 3.0],
            DomainReason::CentralMeridianOffset,
        );
        assert_eq!(e.srid(), Srid::Utm47N);
        let text = e.to_string();
        assert!(
            text.contains("32647") && text.contains("central meridian"),
            "{text}"
        );
    }
}
