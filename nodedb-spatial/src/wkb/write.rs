// SPDX-License-Identifier: Apache-2.0

//! ISO WKB writer (little-endian, no SRID).

use nodedb_types::geometry::Geometry;

use super::format::{
    BYTE_ORDER_LE, MAX_WKB_BYTES, MAX_WKB_DEPTH, MAX_WKB_ELEMENTS, WKB_GEOMETRYCOLLECTION,
    WKB_LINESTRING, WKB_MULTILINESTRING, WKB_MULTIPOINT, WKB_MULTIPOLYGON, WKB_POINT, WKB_POLYGON,
};

/// Serialize a Geometry to ISO WKB (little-endian). Carries no SRID.
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

pub(super) fn checked_encoded_size(geom: &Geometry, depth_remaining: usize) -> Option<usize> {
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

pub(super) fn write_header(buf: &mut Vec<u8>, wkb_type: u32) {
    buf.push(BYTE_ORDER_LE);
    write_u32(buf, wkb_type);
}

pub(super) fn write_u32(buf: &mut Vec<u8>, val: u32) {
    buf.extend_from_slice(&val.to_le_bytes());
}

fn write_f64(buf: &mut Vec<u8>, val: f64) {
    buf.extend_from_slice(&val.to_le_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoder_rejects_total_output_above_byte_limit_before_writing() {
        let geometry = Geometry::MultiLineString {
            coordinates: (0..5).map(|_| vec![[0.0, 0.0]; 840_000]).collect(),
        };
        assert!(checked_encoded_size(&geometry, MAX_WKB_DEPTH).is_none());
        assert!(geometry_to_wkb(&geometry).is_none());
    }

    #[test]
    fn encoder_rejects_element_count_above_decoder_limit() {
        let geometry = Geometry::LineString {
            coordinates: vec![[0.0, 0.0]; MAX_WKB_ELEMENTS + 1],
        };
        assert!(geometry_to_wkb(&geometry).is_none());
    }

    #[test]
    fn point_wkb_size() {
        let wkb = geometry_to_wkb(&Geometry::point(0.0, 0.0)).unwrap();
        // 1 (byte order) + 4 (type) + 8 (x) + 8 (y) = 21 bytes
        assert_eq!(wkb.len(), 21);
    }
}
