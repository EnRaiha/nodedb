// SPDX-License-Identifier: Apache-2.0

//! Coordinate transformation between supported SRIDs.
//!
//! Every path runs through WGS 84 geographic coordinates: the inverse of the
//! source projection (none for 4326), then the forward of the target. The
//! WGS 84 → GDM2000 datum step is the identity (see `params`).

use nodedb_types::geometry::{Srid, SridGeometry};

use super::error::CrsError;
use super::hom::HotineObliqueMercator;
use super::params::{BORNEO_RSO, PENINSULAR_RSO, UTM_47N, UTM_48N};
use super::tm::TransverseMercator;

/// The projection behind one projected SRID.
#[derive(Debug, Clone)]
pub enum Projection {
    TransverseMercator(TransverseMercator),
    HotineObliqueMercator(HotineObliqueMercator),
}

impl Projection {
    /// The projection of `srid`, or `None` for the geographic WGS 84.
    pub fn for_srid(srid: Srid) -> Option<Projection> {
        match srid {
            Srid::Wgs84 => None,
            Srid::Utm47N => Some(Projection::TransverseMercator(TransverseMercator::new(
                srid, UTM_47N,
            ))),
            Srid::Utm48N => Some(Projection::TransverseMercator(TransverseMercator::new(
                srid, UTM_48N,
            ))),
            Srid::PeninsularRso => Some(Projection::HotineObliqueMercator(
                HotineObliqueMercator::new(srid, PENINSULAR_RSO),
            )),
            Srid::BorneoRso => Some(Projection::HotineObliqueMercator(
                HotineObliqueMercator::new(srid, BORNEO_RSO),
            )),
        }
    }

    /// `[lon, lat]` degrees to `[easting, northing]` metres.
    pub fn forward(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        match self {
            Projection::TransverseMercator(p) => p.forward(coord),
            Projection::HotineObliqueMercator(p) => p.forward(coord),
        }
    }

    /// `[easting, northing]` metres to `[lon, lat]` degrees.
    pub fn inverse(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        match self {
            Projection::TransverseMercator(p) => p.inverse(coord),
            Projection::HotineObliqueMercator(p) => p.inverse(coord),
        }
    }
}

/// A prepared `from → to` coordinate transform.
#[derive(Debug, Clone)]
pub struct Transformer {
    from: Srid,
    source: Option<Projection>,
    target: Option<Projection>,
}

impl Transformer {
    pub fn new(from: Srid, to: Srid) -> Self {
        // A same-SRID transform maps nothing.
        if from == to {
            return Self {
                from,
                source: None,
                target: None,
            };
        }
        Self {
            from,
            source: Projection::for_srid(from),
            target: Projection::for_srid(to),
        }
    }

    /// Map one coordinate.
    pub fn apply(&self, coord: [f64; 2]) -> Result<[f64; 2], CrsError> {
        let geographic = match &self.source {
            Some(p) => p.inverse(coord)?,
            None => {
                super::error::check_finite(self.from, coord)?;
                coord
            }
        };
        match &self.target {
            Some(p) => p.forward(geographic),
            None => Ok(geographic),
        }
    }
}

/// Transform every coordinate of `geom` into `target`.
///
/// A geometry already in `target` comes back unchanged. Any coordinate a
/// projection cannot map fails the whole call with
/// [`CrsError::OutOfDomain`].
pub fn transform_geometry(geom: &SridGeometry, target: Srid) -> Result<SridGeometry, CrsError> {
    if geom.srid == target {
        return Ok(geom.clone());
    }
    let transformer = Transformer::new(geom.srid, target);
    let geometry = geom
        .geometry
        .try_map_coords(&mut |c| transformer.apply(c))?;
    Ok(SridGeometry::new(target, geometry))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::geometry::Geometry;

    /// Largest round-trip error accepted, metres.
    const ROUND_TRIP_TOLERANCE_M: f64 = 1e-3;

    /// Metres per degree of latitude, rounded up. Converts a metre tolerance
    /// to a conservative degree tolerance.
    const METRES_PER_DEGREE: f64 = 111_320.0;

    fn point(srid: Srid, x: f64, y: f64) -> SridGeometry {
        SridGeometry::new(srid, Geometry::point(x, y))
    }

    fn coords(g: &SridGeometry) -> [f64; 2] {
        match g.geometry {
            Geometry::Point { coordinates } => coordinates,
            _ => panic!("expected a point"),
        }
    }

    #[test]
    fn wgs84_round_trip_through_every_srid() {
        let inputs = [[101.7117, 3.1579], [116.0735, 5.9804], [103.5, 1.5]];
        for srid in &Srid::SUPPORTED[1..] {
            for input in inputs {
                let src = point(Srid::Wgs84, input[0], input[1]);
                let projected = transform_geometry(&src, *srid).unwrap();
                assert_eq!(projected.srid, *srid);
                let back = transform_geometry(&projected, Srid::Wgs84).unwrap();
                let [lon, lat] = coords(&back);
                let tol = ROUND_TRIP_TOLERANCE_M / METRES_PER_DEGREE;
                assert!(
                    (lon - input[0]).abs() < tol && (lat - input[1]).abs() < tol,
                    "{srid:?}: {input:?} -> {:?}",
                    [lon, lat]
                );
            }
        }
    }

    #[test]
    fn peninsular_rso_through_utm47n_round_trips() {
        let src = point(Srid::PeninsularRso, 412_834.468_1, 349_472.917_7);
        let utm = transform_geometry(&src, Srid::Utm47N).unwrap();
        let back = transform_geometry(&utm, Srid::PeninsularRso).unwrap();
        let [x, y] = coords(&back);
        assert!(
            (x - 412_834.468_1).abs() < ROUND_TRIP_TOLERANCE_M
                && (y - 349_472.917_7).abs() < ROUND_TRIP_TOLERANCE_M,
            "{x} {y}"
        );
        let [ux, uy] = coords(&utm);
        assert!((ux - 801_401.794_9).abs() < ROUND_TRIP_TOLERANCE_M, "{ux}");
        assert!((uy - 349_439.510_6).abs() < ROUND_TRIP_TOLERANCE_M, "{uy}");
    }

    #[test]
    fn same_srid_is_identity() {
        let src = point(Srid::Utm48N, 1.0, 2.0);
        assert_eq!(transform_geometry(&src, Srid::Utm48N), Ok(src));
    }

    #[test]
    fn every_coordinate_is_mapped() {
        let src = SridGeometry::wgs84(Geometry::polygon(vec![vec![
            [101.0, 3.0],
            [102.0, 3.0],
            [102.0, 4.0],
            [101.0, 3.0],
        ]]));
        let out = transform_geometry(&src, Srid::Utm47N).unwrap();
        let Geometry::Polygon { coordinates } = out.geometry else {
            panic!("shape changed");
        };
        assert_eq!(coordinates[0].len(), 4);
        assert!(coordinates[0].iter().all(|c| c[0] > 100_000.0));
    }

    #[test]
    fn out_of_domain_fails_the_call() {
        let src = point(Srid::Wgs84, 179.0, 3.0);
        assert!(matches!(
            transform_geometry(&src, Srid::Utm48N),
            Err(CrsError::OutOfDomain { .. })
        ));
    }
}
