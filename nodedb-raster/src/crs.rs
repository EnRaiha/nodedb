// SPDX-License-Identifier: Apache-2.0

//! The raster CRS, from the GeoKey directory.
//!
//! A projected model (1) reads `ProjectedCRSGeoKey` (3072), a geographic
//! model (2) reads `GeodeticCRSGeoKey` (2048). With no model key, 3072 wins
//! over 2048. The EPSG code then maps through [`Srid::from_code`].

use nodedb_types::geometry::Srid;

use crate::error::RasterError;
use crate::geokeys::{GEOGRAPHIC_TYPE, GT_MODEL_TYPE, GeoKeyDirectory, PROJECTED_CS_TYPE};

const MODEL_PROJECTED: u16 = 1;
const MODEL_GEOGRAPHIC: u16 = 2;
/// GeoKey value of a CRS defined by parameters, not by an EPSG code.
const USER_DEFINED: u16 = 32767;
/// GeoKey value of an undefined CRS.
const UNDEFINED: u16 = 0;

/// The SRID the GeoKeys name, or `None` when they name no CRS.
pub fn srid_from_geokeys(keys: Option<&GeoKeyDirectory>) -> Result<Option<Srid>, RasterError> {
    let Some(keys) = keys else {
        return Ok(None);
    };
    let key = match keys.short(GT_MODEL_TYPE) {
        Some(MODEL_PROJECTED) => PROJECTED_CS_TYPE,
        Some(MODEL_GEOGRAPHIC) => GEOGRAPHIC_TYPE,
        Some(model) => return Err(RasterError::UnsupportedModelType { model }),
        None if keys.short(PROJECTED_CS_TYPE).is_some() => PROJECTED_CS_TYPE,
        None if keys.short(GEOGRAPHIC_TYPE).is_some() => GEOGRAPHIC_TYPE,
        None => return Ok(None),
    };
    let model = keys.short(GT_MODEL_TYPE).unwrap_or(UNDEFINED);
    match keys.short(key) {
        None | Some(UNDEFINED) => Err(RasterError::MissingCrsKey { model, key }),
        Some(USER_DEFINED) => Err(RasterError::UserDefinedCrs { key }),
        Some(code) => Ok(Some(Srid::from_code(i64::from(code))?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(entries: &[(u16, u16)]) -> GeoKeyDirectory {
        let count = u16::try_from(entries.len()).expect("few keys");
        let mut dir = vec![1, 1, 0, count];
        for (key, value) in entries {
            dir.extend([*key, 0, 1, *value]);
        }
        GeoKeyDirectory::parse(&dir, None, None).expect("valid directory")
    }

    #[test]
    fn no_directory_is_no_crs() {
        assert_eq!(srid_from_geokeys(None).ok(), Some(None));
    }

    #[test]
    fn projected_model_reads_3072() {
        let dir = keys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32647)]);
        assert_eq!(srid_from_geokeys(Some(&dir)).ok(), Some(Some(Srid::Utm47N)));
    }

    #[test]
    fn geographic_model_reads_2048() {
        let dir = keys(&[(GT_MODEL_TYPE, 2), (GEOGRAPHIC_TYPE, 4326)]);
        assert_eq!(srid_from_geokeys(Some(&dir)).ok(), Some(Some(Srid::Wgs84)));
    }

    #[test]
    fn model_key_picks_the_crs_key() {
        let dir = keys(&[
            (GT_MODEL_TYPE, 2),
            (GEOGRAPHIC_TYPE, 4326),
            (PROJECTED_CS_TYPE, 32648),
        ]);
        assert_eq!(srid_from_geokeys(Some(&dir)).ok(), Some(Some(Srid::Wgs84)));
    }

    #[test]
    fn without_a_model_key_3072_wins() {
        let dir = keys(&[(GEOGRAPHIC_TYPE, 4326), (PROJECTED_CS_TYPE, 3375)]);
        assert_eq!(
            srid_from_geokeys(Some(&dir)).ok(),
            Some(Some(Srid::PeninsularRso))
        );
        let none = keys(&[(1025, 1)]);
        assert_eq!(srid_from_geokeys(Some(&none)).ok(), Some(None));
    }

    #[test]
    fn unmappable_crs_are_typed_errors() {
        let geocentric = keys(&[(GT_MODEL_TYPE, 3)]);
        assert!(matches!(
            srid_from_geokeys(Some(&geocentric)),
            Err(RasterError::UnsupportedModelType { model: 3 })
        ));
        let missing = keys(&[(GT_MODEL_TYPE, 1)]);
        assert!(matches!(
            srid_from_geokeys(Some(&missing)),
            Err(RasterError::MissingCrsKey {
                model: 1,
                key: 3072
            })
        ));
        let user = keys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32767)]);
        assert!(matches!(
            srid_from_geokeys(Some(&user)),
            Err(RasterError::UserDefinedCrs { key: 3072 })
        ));
        let other = keys(&[(GT_MODEL_TYPE, 1), (PROJECTED_CS_TYPE, 32633)]);
        assert!(matches!(
            srid_from_geokeys(Some(&other)),
            Err(RasterError::UnsupportedCrs(_))
        ));
    }
}
