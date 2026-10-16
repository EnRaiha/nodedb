// SPDX-License-Identifier: Apache-2.0

//! WKB / EWKB reader.
//!
//! Accepts ISO WKB and PostGIS EWKB in either byte order. An EWKB type word
//! with [`EWKB_SRID_FLAG`] set carries a 4-byte SRID after it. The reader
//! masks the flag, and reports the top-level SRID to the caller.

use nodedb_types::geometry::Geometry;

use super::format::{
    EWKB_SRID_FLAG, MAX_WKB_BYTES, MAX_WKB_DEPTH, MAX_WKB_ELEMENTS, WKB_GEOMETRYCOLLECTION,
    WKB_LINESTRING, WKB_MULTILINESTRING, WKB_MULTIPOINT, WKB_MULTIPOLYGON, WKB_POINT, WKB_POLYGON,
};

/// Deserialize a Geometry from WKB or EWKB bytes.
///
/// Returns the geometry and the SRID an EWKB header declares, `None` for
/// ISO WKB. The SRID is the raw code: the caller decides whether it is
/// supported. Returns `None` overall if the bytes are malformed or truncated.
pub fn geometry_from_wkb(data: &[u8]) -> Option<(Geometry, Option<u32>)> {
    if data.len() > MAX_WKB_BYTES {
        return None;
    }
    let mut cursor = 0;
    let (geometry, srid) = read_geometry(data, &mut cursor, MAX_WKB_DEPTH)?;
    Some((geometry, srid))
}

/// Extract bounding box from WKB, in geographic space.
pub fn wkb_bbox(data: &[u8]) -> Option<nodedb_types::BoundingBox> {
    let (geom, _) = geometry_from_wkb(data)?;
    Some(nodedb_types::geometry_bbox(&geom))
}

/// One geometry and the SRID its own header declared.
fn read_geometry(
    data: &[u8],
    cursor: &mut usize,
    depth_remaining: usize,
) -> Option<(Geometry, Option<u32>)> {
    if depth_remaining == 0 {
        return None;
    }
    let byte_order = read_u8(data, cursor)?;
    let is_le = byte_order == 1;
    let raw_type = read_u32(data, cursor, is_le)?;
    let srid = if raw_type & EWKB_SRID_FLAG != 0 {
        Some(read_u32(data, cursor, is_le)?)
    } else {
        None
    };
    let wkb_type = raw_type & !EWKB_SRID_FLAG;
    let child = |data: &[u8], cursor: &mut usize| {
        read_geometry(data, cursor, depth_remaining - 1).map(|(g, _)| g)
    };

    let geometry = match wkb_type {
        WKB_POINT => {
            let x = read_f64(data, cursor, is_le)?;
            let y = read_f64(data, cursor, is_le)?;
            Geometry::Point {
                coordinates: [x, y],
            }
        }
        WKB_LINESTRING => {
            let count = read_u32(data, cursor, is_le)? as usize;
            Geometry::LineString {
                coordinates: read_coords(data, cursor, count, is_le)?,
            }
        }
        WKB_POLYGON => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 4)?;
            let mut rings = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let ring_count = read_u32(data, cursor, is_le)? as usize;
                rings.push(read_coords(data, cursor, ring_count, is_le)?);
            }
            Geometry::Polygon { coordinates: rings }
        }
        WKB_MULTIPOINT => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 21)?;
            let mut coords = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let Geometry::Point { coordinates } = child(data, cursor)? else {
                    return None;
                };
                coords.push(coordinates);
            }
            Geometry::MultiPoint {
                coordinates: coords,
            }
        }
        WKB_MULTILINESTRING => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut lines = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let Geometry::LineString { coordinates } = child(data, cursor)? else {
                    return None;
                };
                lines.push(coordinates);
            }
            Geometry::MultiLineString { coordinates: lines }
        }
        WKB_MULTIPOLYGON => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut polys = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let Geometry::Polygon { coordinates } = child(data, cursor)? else {
                    return None;
                };
                polys.push(coordinates);
            }
            Geometry::MultiPolygon { coordinates: polys }
        }
        WKB_GEOMETRYCOLLECTION => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut geoms = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                geoms.push(child(data, cursor)?);
            }
            Geometry::GeometryCollection { geometries: geoms }
        }
        _ => return None,
    };
    Some((geometry, srid))
}

/// Validate an untrusted count before using it as a vector capacity.
///
/// Every encoded element must occupy at least `min_encoded_bytes` after the
/// current cursor. This makes a truncated count fail before allocation, while
/// the independent element cap bounds work on large valid inputs.
fn checked_wkb_capacity(
    data: &[u8],
    cursor: &usize,
    count: usize,
    min_encoded_bytes: usize,
) -> Option<usize> {
    if count > MAX_WKB_ELEMENTS {
        return None;
    }
    let required = count.checked_mul(min_encoded_bytes)?;
    let remaining = data.len().checked_sub(*cursor)?;
    (required <= remaining).then_some(count)
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Option<u8> {
    let val = *data.get(*cursor)?;
    *cursor += 1;
    Some(val)
}

fn read_u32(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<u32> {
    let bytes: [u8; 4] = data.get(*cursor..*cursor + 4)?.try_into().ok()?;
    *cursor += 4;
    Some(if is_le {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}

fn read_f64(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<f64> {
    let bytes: [u8; 8] = data.get(*cursor..*cursor + 8)?.try_into().ok()?;
    *cursor += 8;
    Some(if is_le {
        f64::from_le_bytes(bytes)
    } else {
        f64::from_be_bytes(bytes)
    })
}

fn read_coords(
    data: &[u8],
    cursor: &mut usize,
    count: usize,
    is_le: bool,
) -> Option<Vec<[f64; 2]>> {
    let capacity = checked_wkb_capacity(data, cursor, count, 16)?;
    let mut coords = Vec::with_capacity(capacity);
    for _ in 0..capacity {
        let x = read_f64(data, cursor, is_le)?;
        let y = read_f64(data, cursor, is_le)?;
        coords.push([x, y]);
    }
    Some(coords)
}

#[cfg(test)]
mod tests {
    use super::super::write::{geometry_to_wkb as encode, write_header, write_u32};
    use super::*;

    fn geometry_to_wkb(geometry: &Geometry) -> Vec<u8> {
        encode(geometry).expect("test geometry must fit WKB limits")
    }

    fn decode(wkb: &[u8]) -> Option<Geometry> {
        geometry_from_wkb(wkb).map(|(g, _)| g)
    }

    #[test]
    fn point_roundtrip() {
        let geom = Geometry::point(-73.9857, 40.7484);
        let wkb = geometry_to_wkb(&geom);
        assert_eq!(geometry_from_wkb(&wkb), Some((geom, None)));
    }

    #[test]
    fn linestring_roundtrip() {
        let geom = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]);
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn polygon_roundtrip() {
        let geom = Geometry::polygon(vec![
            vec![
                [0.0, 0.0],
                [10.0, 0.0],
                [10.0, 10.0],
                [0.0, 10.0],
                [0.0, 0.0],
            ],
            vec![[2.0, 2.0], [3.0, 2.0], [3.0, 3.0], [2.0, 3.0], [2.0, 2.0]], // hole
        ]);
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn multipoint_roundtrip() {
        let geom = Geometry::MultiPoint {
            coordinates: vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        };
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn multilinestring_roundtrip() {
        let geom = Geometry::MultiLineString {
            coordinates: vec![
                vec![[0.0, 0.0], [1.0, 1.0]],
                vec![[2.0, 2.0], [3.0, 3.0], [4.0, 2.0]],
            ],
        };
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn multipolygon_roundtrip() {
        let geom = Geometry::MultiPolygon {
            coordinates: vec![
                vec![vec![
                    [0.0, 0.0],
                    [1.0, 0.0],
                    [1.0, 1.0],
                    [0.0, 1.0],
                    [0.0, 0.0],
                ]],
                vec![vec![
                    [5.0, 5.0],
                    [6.0, 5.0],
                    [6.0, 6.0],
                    [5.0, 6.0],
                    [5.0, 5.0],
                ]],
            ],
        };
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn geometry_collection_roundtrip() {
        let geom = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            ],
        };
        assert_eq!(decode(&geometry_to_wkb(&geom)), Some(geom));
    }

    #[test]
    fn parsed_max_counts_are_rejected_before_allocation() {
        for geometry_type in [
            WKB_LINESTRING,
            WKB_POLYGON,
            WKB_MULTIPOINT,
            WKB_MULTILINESTRING,
            WKB_MULTIPOLYGON,
            WKB_GEOMETRYCOLLECTION,
        ] {
            let mut wkb = Vec::new();
            write_header(&mut wkb, geometry_type);
            write_u32(&mut wkb, u32::MAX);
            assert!(decode(&wkb).is_none(), "type={geometry_type}");
        }
    }

    #[test]
    fn truncated_count_bodies_are_rejected_before_allocation() {
        for geometry_type in [
            WKB_LINESTRING,
            WKB_POLYGON,
            WKB_MULTIPOINT,
            WKB_MULTILINESTRING,
            WKB_MULTIPOLYGON,
            WKB_GEOMETRYCOLLECTION,
        ] {
            let mut wkb = Vec::new();
            write_header(&mut wkb, geometry_type);
            write_u32(&mut wkb, 1);
            assert!(decode(&wkb).is_none(), "type={geometry_type}");
        }
    }

    #[test]
    fn geometry_collection_depth_is_bounded() {
        let mut valid = Geometry::point(1.0, 2.0);
        for _ in 0..MAX_WKB_DEPTH - 1 {
            valid = Geometry::GeometryCollection {
                geometries: vec![valid],
            };
        }
        let valid_wkb = geometry_to_wkb(&valid);
        assert_eq!(decode(&valid_wkb), Some(valid));

        let mut too_deep = Geometry::point(1.0, 2.0);
        for _ in 0..MAX_WKB_DEPTH {
            too_deep = Geometry::GeometryCollection {
                geometries: vec![too_deep],
            };
        }
        assert!(encode(&too_deep).is_none());
    }

    #[test]
    fn truncated_data_returns_none() {
        let wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        assert!(decode(&wkb[..3]).is_none());
        assert!(decode(&[]).is_none());
    }

    #[test]
    fn invalid_type_returns_none() {
        let mut wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        wkb[1] = 99; // invalid WKB type
        assert!(decode(&wkb).is_none());
    }

    #[test]
    fn ewkb_srid_flag_is_masked_and_reported() {
        // EWKB POINT(1 2) SRID=32648, little-endian.
        let mut wkb = Vec::new();
        write_header(&mut wkb, WKB_POINT | EWKB_SRID_FLAG);
        write_u32(&mut wkb, 32648);
        wkb.extend_from_slice(&1.0f64.to_le_bytes());
        wkb.extend_from_slice(&2.0f64.to_le_bytes());
        assert_eq!(
            geometry_from_wkb(&wkb),
            Some((Geometry::point(1.0, 2.0), Some(32648)))
        );
    }

    #[test]
    fn wkb_bbox_extraction() {
        let geom = Geometry::polygon(vec![vec![
            [-10.0, -5.0],
            [10.0, -5.0],
            [10.0, 5.0],
            [-10.0, 5.0],
            [-10.0, -5.0],
        ]]);
        let wkb = geometry_to_wkb(&geom);
        let bb = wkb_bbox(&wkb).unwrap();
        assert_eq!(bb.min_lng, -10.0);
        assert_eq!(bb.max_lng, 10.0);
        assert_eq!(bb.min_lat, -5.0);
        assert_eq!(bb.max_lat, 5.0);
    }
}
