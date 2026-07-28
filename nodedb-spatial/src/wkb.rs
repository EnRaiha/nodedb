// SPDX-License-Identifier: Apache-2.0

//! Well-Known Binary (WKB) serialization for Geometry types.
//!
//! WKB is the standard binary format for geometry interchange (ISO 13249).
//! Used as the Arrow `DataType::Binary` backing for spatial columns —
//! avoids JSON parse overhead during DataFusion query execution.
//!
//! Format (little-endian):
//! ```text
//! [byte_order: u8] [type: u32] [coordinates...]
//! ```
//!
//! Byte order: 1 = little-endian (NDR), 0 = big-endian (XDR). We always
//! write little-endian and accept both on read.

use nodedb_types::geometry::Geometry;

// WKB geometry type codes (ISO 13249 / OGC SFA).
const WKB_POINT: u32 = 1;
const WKB_LINESTRING: u32 = 2;
const WKB_POLYGON: u32 = 3;
const WKB_MULTIPOINT: u32 = 4;
const WKB_MULTILINESTRING: u32 = 5;
const WKB_MULTIPOLYGON: u32 = 6;
const WKB_GEOMETRYCOLLECTION: u32 = 7;

const BYTE_ORDER_LE: u8 = 1;

// WKB count fields are attacker-controlled. The bound caps allocation and
// parsing work even when the encoded input itself is very large.
const MAX_WKB_ELEMENTS: usize = 1_000_000;
const MAX_WKB_BYTES: usize = 64 * 1024 * 1024;
// Nested geometry collections are recursive on the stack. Keep the budget
// explicit so hostile nesting cannot exhaust the decoder thread's stack.
const MAX_WKB_DEPTH: usize = 64;

/// Serialize a Geometry to WKB (little-endian).
///
/// Returns `None` when the geometry exceeds the decoder's element or nesting
/// limits, ensuring every emitted WKB value remains decodable.
pub fn geometry_to_wkb(geom: &Geometry) -> Option<Vec<u8>> {
    let encoded_size = checked_encoded_size(geom, MAX_WKB_DEPTH)?;
    let mut buf = Vec::with_capacity(encoded_size);
    write_geometry(&mut buf, geom, MAX_WKB_DEPTH)?;
    debug_assert_eq!(buf.len(), encoded_size);
    Some(buf)
}

/// Deserialize a Geometry from WKB bytes.
///
/// Returns `None` if the bytes are malformed or truncated.
pub fn geometry_from_wkb(data: &[u8]) -> Option<Geometry> {
    if data.len() > MAX_WKB_BYTES {
        return None;
    }
    let mut cursor = 0;
    read_geometry(data, &mut cursor, MAX_WKB_DEPTH)
}

/// Extract bounding box from WKB without full deserialization.
///
/// Scans coordinate values to find min/max. Faster than full deserialize
/// when only the bbox is needed (e.g., R-tree insertion from Arrow batch).
pub fn wkb_bbox(data: &[u8]) -> Option<nodedb_types::BoundingBox> {
    let geom = geometry_from_wkb(data)?;
    Some(nodedb_types::geometry_bbox(&geom))
}

// ── Write helpers ──

fn checked_encoded_size(geom: &Geometry, depth_remaining: usize) -> Option<usize> {
    if depth_remaining == 0 {
        return None;
    }
    let size = match geom {
        Geometry::Point { .. } => 21,
        Geometry::LineString { coordinates } => {
            checked_count(coordinates.len())?;
            bounded_add(9, coordinates.len().checked_mul(16)?)?
        }
        Geometry::Polygon { coordinates } => {
            checked_count(coordinates.len())?;
            let mut size = 9usize;
            for ring in coordinates {
                checked_count(ring.len())?;
                size = bounded_add(size, bounded_add(4, ring.len().checked_mul(16)?)?)?;
            }
            size
        }
        Geometry::MultiPoint { coordinates } => {
            checked_count(coordinates.len())?;
            bounded_add(9, coordinates.len().checked_mul(21)?)?
        }
        Geometry::MultiLineString { coordinates } => {
            if depth_remaining == 1 {
                return None;
            }
            checked_count(coordinates.len())?;
            let mut size = 9usize;
            for line in coordinates {
                checked_count(line.len())?;
                size = bounded_add(size, bounded_add(9, line.len().checked_mul(16)?)?)?;
            }
            size
        }
        Geometry::MultiPolygon { coordinates } => {
            if depth_remaining == 1 {
                return None;
            }
            checked_count(coordinates.len())?;
            let mut size = 9usize;
            for polygon in coordinates {
                checked_count(polygon.len())?;
                let mut polygon_size = 9usize;
                for ring in polygon {
                    checked_count(ring.len())?;
                    polygon_size =
                        bounded_add(polygon_size, bounded_add(4, ring.len().checked_mul(16)?)?)?;
                }
                size = bounded_add(size, polygon_size)?;
            }
            size
        }
        Geometry::GeometryCollection { geometries } => {
            checked_count(geometries.len())?;
            let mut size = 9usize;
            for geometry in geometries {
                size = bounded_add(size, checked_encoded_size(geometry, depth_remaining - 1)?)?;
            }
            size
        }
        _ => 9,
    };
    (size <= MAX_WKB_BYTES).then_some(size)
}

fn checked_count(count: usize) -> Option<()> {
    (count <= MAX_WKB_ELEMENTS && u32::try_from(count).is_ok()).then_some(())
}

fn bounded_add(left: usize, right: usize) -> Option<usize> {
    left.checked_add(right)
        .filter(|&size| size <= MAX_WKB_BYTES)
}

fn write_geometry(buf: &mut Vec<u8>, geom: &Geometry, depth_remaining: usize) -> Option<()> {
    if depth_remaining == 0 {
        return None;
    }
    match geom {
        Geometry::Point { coordinates } => {
            write_header(buf, WKB_POINT);
            write_f64(buf, coordinates[0]);
            write_f64(buf, coordinates[1]);
        }
        Geometry::LineString { coordinates } => {
            write_header(buf, WKB_LINESTRING);
            write_count(buf, coordinates.len())?;
            for c in coordinates {
                write_f64(buf, c[0]);
                write_f64(buf, c[1]);
            }
        }
        Geometry::Polygon { coordinates } => {
            write_header(buf, WKB_POLYGON);
            write_count(buf, coordinates.len())?;
            for ring in coordinates {
                write_count(buf, ring.len())?;
                for c in ring {
                    write_f64(buf, c[0]);
                    write_f64(buf, c[1]);
                }
            }
        }
        Geometry::MultiPoint { coordinates } => {
            write_header(buf, WKB_MULTIPOINT);
            write_count(buf, coordinates.len())?;
            for c in coordinates {
                write_header(buf, WKB_POINT);
                write_f64(buf, c[0]);
                write_f64(buf, c[1]);
            }
        }
        Geometry::MultiLineString { coordinates } => {
            write_header(buf, WKB_MULTILINESTRING);
            write_count(buf, coordinates.len())?;
            for ls in coordinates {
                write_geometry(
                    buf,
                    &Geometry::LineString {
                        coordinates: ls.clone(),
                    },
                    depth_remaining - 1,
                )?;
            }
        }
        Geometry::MultiPolygon { coordinates } => {
            write_header(buf, WKB_MULTIPOLYGON);
            write_count(buf, coordinates.len())?;
            for poly in coordinates {
                write_geometry(
                    buf,
                    &Geometry::Polygon {
                        coordinates: poly.clone(),
                    },
                    depth_remaining - 1,
                )?;
            }
        }
        Geometry::GeometryCollection { geometries } => {
            write_header(buf, WKB_GEOMETRYCOLLECTION);
            write_count(buf, geometries.len())?;
            for geometry in geometries {
                write_geometry(buf, geometry, depth_remaining - 1)?;
            }
        }
        _ => {
            write_header(buf, WKB_GEOMETRYCOLLECTION);
            write_u32(buf, 0);
        }
    }
    Some(())
}

fn write_count(buf: &mut Vec<u8>, count: usize) -> Option<()> {
    if count > MAX_WKB_ELEMENTS {
        return None;
    }
    write_u32(buf, u32::try_from(count).ok()?);
    Some(())
}

fn write_header(buf: &mut Vec<u8>, wkb_type: u32) {
    buf.push(BYTE_ORDER_LE);
    write_u32(buf, wkb_type);
}

fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_f64(buf: &mut Vec<u8>, val: f64) {
    buf.extend_from_slice(&val.to_le_bytes());
}

// ── Read helpers ──

fn read_geometry(data: &[u8], cursor: &mut usize, depth_remaining: usize) -> Option<Geometry> {
    if depth_remaining == 0 {
        return None;
    }
    let byte_order = read_u8(data, cursor)?;
    let is_le = byte_order == 1;
    let wkb_type = read_u32(data, cursor, is_le)?;

    match wkb_type {
        WKB_POINT => {
            let x = read_f64(data, cursor, is_le)?;
            let y = read_f64(data, cursor, is_le)?;
            Some(Geometry::Point {
                coordinates: [x, y],
            })
        }
        WKB_LINESTRING => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let coords = read_coords(data, cursor, count, is_le)?;
            Some(Geometry::LineString {
                coordinates: coords,
            })
        }
        WKB_POLYGON => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 4)?;
            let mut rings = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let ring_count = read_u32(data, cursor, is_le)? as usize;
                let ring = read_coords(data, cursor, ring_count, is_le)?;
                rings.push(ring);
            }
            Some(Geometry::Polygon { coordinates: rings })
        }
        WKB_MULTIPOINT => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 21)?;
            let mut coords = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let inner = read_geometry(data, cursor, depth_remaining - 1)?;
                if let Geometry::Point { coordinates } = inner {
                    coords.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiPoint {
                coordinates: coords,
            })
        }
        WKB_MULTILINESTRING => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut lines = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let inner = read_geometry(data, cursor, depth_remaining - 1)?;
                if let Geometry::LineString { coordinates } = inner {
                    lines.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiLineString { coordinates: lines })
        }
        WKB_MULTIPOLYGON => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut polys = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                let inner = read_geometry(data, cursor, depth_remaining - 1)?;
                if let Geometry::Polygon { coordinates } = inner {
                    polys.push(coordinates);
                } else {
                    return None;
                }
            }
            Some(Geometry::MultiPolygon { coordinates: polys })
        }
        WKB_GEOMETRYCOLLECTION => {
            let count = read_u32(data, cursor, is_le)? as usize;
            let capacity = checked_wkb_capacity(data, cursor, count, 5)?;
            let mut geoms = Vec::with_capacity(capacity);
            for _ in 0..capacity {
                geoms.push(read_geometry(data, cursor, depth_remaining - 1)?);
            }
            Some(Geometry::GeometryCollection { geometries: geoms })
        }
        _ => None,
    }
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
    if *cursor >= data.len() {
        return None;
    }
    let val = data[*cursor];
    *cursor += 1;
    Some(val)
}

fn read_u32(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<u32> {
    if *cursor + 4 > data.len() {
        return None;
    }
    let bytes: [u8; 4] = [
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
    ];
    *cursor += 4;
    Some(if is_le {
        u32::from_le_bytes(bytes)
    } else {
        u32::from_be_bytes(bytes)
    })
}

fn read_f64(data: &[u8], cursor: &mut usize, is_le: bool) -> Option<f64> {
    if *cursor + 8 > data.len() {
        return None;
    }
    let bytes: [u8; 8] = [
        data[*cursor],
        data[*cursor + 1],
        data[*cursor + 2],
        data[*cursor + 3],
        data[*cursor + 4],
        data[*cursor + 5],
        data[*cursor + 6],
        data[*cursor + 7],
    ];
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
    use super::*;

    fn geometry_to_wkb(geometry: &Geometry) -> Vec<u8> {
        super::geometry_to_wkb(geometry).expect("test geometry must fit WKB limits")
    }

    #[test]
    fn point_roundtrip() {
        let geom = Geometry::point(-73.9857, 40.7484);
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn linestring_roundtrip() {
        let geom = Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0], [2.0, 0.0]]);
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
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
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn multipoint_roundtrip() {
        let geom = Geometry::MultiPoint {
            coordinates: vec![[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn multilinestring_roundtrip() {
        let geom = Geometry::MultiLineString {
            coordinates: vec![
                vec![[0.0, 0.0], [1.0, 1.0]],
                vec![[2.0, 2.0], [3.0, 3.0], [4.0, 2.0]],
            ],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
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
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
    }

    #[test]
    fn geometry_collection_roundtrip() {
        let geom = Geometry::GeometryCollection {
            geometries: vec![
                Geometry::point(1.0, 2.0),
                Geometry::line_string(vec![[0.0, 0.0], [1.0, 1.0]]),
            ],
        };
        let wkb = geometry_to_wkb(&geom);
        let decoded = geometry_from_wkb(&wkb).unwrap();
        assert_eq!(geom, decoded);
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
            assert!(geometry_from_wkb(&wkb).is_none(), "type={geometry_type}");
        }
    }

    #[test]
    fn encoder_rejects_total_output_above_byte_limit_before_writing() {
        let geometry = Geometry::MultiLineString {
            coordinates: (0..5).map(|_| vec![[0.0, 0.0]; 840_000]).collect(),
        };
        assert!(checked_encoded_size(&geometry, MAX_WKB_DEPTH).is_none());
        assert!(super::geometry_to_wkb(&geometry).is_none());
    }

    #[test]
    fn encoder_rejects_element_count_above_decoder_limit() {
        let geometry = Geometry::LineString {
            coordinates: vec![[0.0, 0.0]; MAX_WKB_ELEMENTS + 1],
        };
        assert!(super::geometry_to_wkb(&geometry).is_none());
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
            assert!(geometry_from_wkb(&wkb).is_none(), "type={geometry_type}");
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
        assert_eq!(geometry_from_wkb(&valid_wkb), Some(valid));

        let mut too_deep = Geometry::point(1.0, 2.0);
        for _ in 0..MAX_WKB_DEPTH {
            too_deep = Geometry::GeometryCollection {
                geometries: vec![too_deep],
            };
        }
        assert!(super::geometry_to_wkb(&too_deep).is_none());
    }

    #[test]
    fn truncated_data_returns_none() {
        let wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        assert!(geometry_from_wkb(&wkb[..3]).is_none());
        assert!(geometry_from_wkb(&[]).is_none());
    }

    #[test]
    fn invalid_type_returns_none() {
        let mut wkb = geometry_to_wkb(&Geometry::point(1.0, 2.0));
        wkb[1] = 99; // invalid WKB type
        assert!(geometry_from_wkb(&wkb).is_none());
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

    #[test]
    fn point_wkb_size() {
        let wkb = geometry_to_wkb(&Geometry::point(0.0, 0.0));
        // 1 (byte order) + 4 (type) + 8 (x) + 8 (y) = 21 bytes
        assert_eq!(wkb.len(), 21);
    }
}
