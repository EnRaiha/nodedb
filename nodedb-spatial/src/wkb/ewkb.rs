// SPDX-License-Identifier: Apache-2.0

//! PostGIS EWKB writer: ISO WKB with the SRID in the top-level header.

use nodedb_types::geometry::SridGeometry;

use super::format::{EWKB_SRID_FLAG, MAX_WKB_BYTES};
use super::write::{geometry_to_wkb, write_u32};

/// Length of the WKB byte-order marker plus type word.
const HEADER_LEN: usize = 5;

/// Serialize a geometry to little-endian EWKB with its SRID.
///
/// The top-level type word gains [`EWKB_SRID_FLAG`] and the SRID code
/// follows it. Nested geometries stay plain WKB, as PostGIS writes them.
/// Returns `None` under the same limits as `geometry_to_wkb`.
pub fn geometry_to_ewkb(geom: &SridGeometry) -> Option<Vec<u8>> {
    let iso = geometry_to_wkb(&geom.geometry)?;
    if iso.len() + 4 > MAX_WKB_BYTES || iso.len() < HEADER_LEN {
        return None;
    }
    let type_word = u32::from_le_bytes(iso[1..HEADER_LEN].try_into().ok()?);
    let mut out = Vec::with_capacity(iso.len() + 4);
    out.push(iso[0]);
    write_u32(&mut out, type_word | EWKB_SRID_FLAG);
    write_u32(&mut out, geom.srid.code());
    out.extend_from_slice(&iso[HEADER_LEN..]);
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::super::read::geometry_from_wkb;
    use super::*;
    use nodedb_types::geometry::{Geometry, Srid};

    #[test]
    fn ewkb_round_trip_keeps_srid() {
        for srid in Srid::SUPPORTED {
            let g = SridGeometry::new(
                srid,
                Geometry::polygon(vec![vec![
                    [0.0, 0.0],
                    [100.0, 0.0],
                    [100.0, 100.0],
                    [0.0, 0.0],
                ]]),
            );
            let bytes = geometry_to_ewkb(&g).unwrap();
            let (geometry, code) = geometry_from_wkb(&bytes).unwrap();
            assert_eq!(geometry, g.geometry);
            assert_eq!(code, Some(srid.code()));
        }
    }

    #[test]
    fn iso_wkb_carries_no_srid() {
        let iso = geometry_to_wkb(&Geometry::point(1.0, 2.0)).unwrap();
        assert_eq!(geometry_from_wkb(&iso).map(|(_, s)| s), Some(None));
    }
}
