// SPDX-License-Identifier: Apache-2.0

//! Geohash and H3 cell encoding / decoding.

use std::collections::HashMap;

use nodedb_types::Value;

use super::helpers::{num_arg, str_arg};

/// Geohash precision used when the caller does not specify one — ~1.2 km
/// cells, the conventional default for point-of-interest bucketing.
const DEFAULT_GEOHASH_PRECISION: u8 = 6;

/// H3 resolution used when the caller does not specify one — ~5 km hexes.
const DEFAULT_H3_RESOLUTION: u8 = 7;

pub(super) fn eval(canonical: &str, args: &[Value]) -> Option<Value> {
    let result = match canonical {
        "st_geohash" | "geo_h3" => {
            let (Some(lng), Some(lat)) = (num_arg(args, 0), num_arg(args, 1)) else {
                return Some(Value::Null);
            };
            if canonical == "st_geohash" {
                let precision = precision_arg(args, 2, DEFAULT_GEOHASH_PRECISION);
                Value::String(nodedb_spatial::geohash_encode(lng, lat, precision))
            } else {
                let resolution = precision_arg(args, 2, DEFAULT_H3_RESOLUTION);
                h3_cell(lng, lat, resolution)
            }
        }
        // Latitude first, unlike `geo_h3` — see the catalog note on why these
        // two are separate entries rather than aliases.
        "h3_latlngtocell" => {
            let (Some(lat), Some(lng)) = (num_arg(args, 0), num_arg(args, 1)) else {
                return Some(Value::Null);
            };
            let resolution = precision_arg(args, 2, DEFAULT_H3_RESOLUTION);
            h3_cell(lng, lat, resolution)
        }
        "st_geohashdecode" => {
            let Some(hash) = str_arg(args, 0) else {
                return Some(Value::Null);
            };
            match nodedb_spatial::geohash_decode(&hash) {
                Some(bbox) => Value::Object(HashMap::from([
                    ("min_lng".to_string(), Value::Float(bbox.min_lng)),
                    ("min_lat".to_string(), Value::Float(bbox.min_lat)),
                    ("max_lng".to_string(), Value::Float(bbox.max_lng)),
                    ("max_lat".to_string(), Value::Float(bbox.max_lat)),
                ])),
                None => Value::Null,
            }
        }
        "geo_geohash_neighbors" => {
            let Some(hash) = str_arg(args, 0) else {
                return Some(Value::Null);
            };
            Value::Array(
                nodedb_spatial::geohash_neighbors(&hash)
                    .into_iter()
                    .map(|(direction, neighbor)| {
                        Value::Object(HashMap::from([
                            (
                                "direction".to_string(),
                                Value::String(format!("{direction:?}")),
                            ),
                            ("hash".to_string(), Value::String(neighbor)),
                        ]))
                    })
                    .collect(),
            )
        }
        "h3_celltolatlng" => match h3_index(args) {
            Some(index) => match nodedb_spatial::h3::h3_to_center(index) {
                Some((lng, lat)) => Value::Object(HashMap::from([
                    ("lat".to_string(), Value::Float(lat)),
                    ("lng".to_string(), Value::Float(lng)),
                ])),
                None => Value::Null,
            },
            None => Value::Null,
        },
        "geo_h3_to_boundary" => match h3_index(args) {
            Some(index) => match nodedb_spatial::h3::h3_to_boundary(index) {
                Some(geom) => Value::Geometry(geom),
                None => Value::Null,
            },
            None => Value::Null,
        },
        "geo_h3_resolution" => match h3_index(args) {
            Some(index) => match nodedb_spatial::h3::h3_resolution(index) {
                Some(resolution) => Value::Integer(resolution as i64),
                None => Value::Null,
            },
            None => Value::Null,
        },
        _ => return None,
    };
    Some(result)
}

fn h3_cell(lng: f64, lat: f64, resolution: u8) -> Value {
    match nodedb_spatial::h3::h3_encode_string(lng, lat, resolution) {
        Some(cell) => Value::String(cell),
        None => Value::Null,
    }
}

/// Parse and validate a hex H3 cell argument. An unparseable or invalid cell
/// yields `None` so the caller returns NULL, rather than being coerced to
/// index 0 and silently answering about a different cell.
fn h3_index(args: &[Value]) -> Option<u64> {
    let text = str_arg(args, 0)?;
    let index = u64::from_str_radix(text.trim(), 16).ok()?;
    nodedb_spatial::h3::h3_is_valid(index).then_some(index)
}

/// A precision / resolution argument, ignoring non-finite or negative values
/// in favour of the default rather than wrapping them into a `u8`.
fn precision_arg(args: &[Value], idx: usize, default: u8) -> u8 {
    num_arg(args, idx)
        .filter(|n| n.is_finite() && *n >= 0.0 && *n <= u8::MAX as f64)
        .map_or(default, |n| n as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geohash_encodes_and_decodes() {
        let args = vec![Value::Float(-122.4), Value::Float(37.8), Value::Integer(6)];
        let Some(Value::String(hash)) = eval("st_geohash", &args) else {
            panic!("expected a geohash");
        };
        let decoded = eval("st_geohashdecode", &[Value::String(hash)]);
        assert!(matches!(decoded, Some(Value::Object(_))));
    }

    #[test]
    fn geohash_precision_defaults_when_absent() {
        let args = vec![Value::Float(-122.4), Value::Float(37.8)];
        let Some(Value::String(hash)) = eval("st_geohash", &args) else {
            panic!("expected a geohash");
        };
        assert_eq!(hash.len(), DEFAULT_GEOHASH_PRECISION as usize);
    }

    /// `geo_h3` is lng-first and `h3_latlngtocell` is lat-first; the same
    /// location expressed each way must produce the same cell.
    #[test]
    fn h3_argument_orders_describe_the_same_cell() {
        let lng_first = eval(
            "geo_h3",
            &[Value::Float(-122.4), Value::Float(37.8), Value::Integer(7)],
        );
        let lat_first = eval(
            "h3_latlngtocell",
            &[Value::Float(37.8), Value::Float(-122.4), Value::Integer(7)],
        );
        assert_eq!(lng_first, lat_first);
        assert!(matches!(lng_first, Some(Value::String(_))));
    }

    /// An unparseable cell must not be coerced to index 0 and answered about.
    #[test]
    fn invalid_h3_cell_yields_null() {
        let bad = vec![Value::String("not-a-cell".into())];
        assert_eq!(eval("h3_celltolatlng", &bad), Some(Value::Null));
        assert_eq!(eval("geo_h3_resolution", &bad), Some(Value::Null));
        assert_eq!(eval("geo_h3_to_boundary", &bad), Some(Value::Null));
    }

    #[test]
    fn unknown_name_falls_through() {
        assert_eq!(eval("st_x", &[]), None);
    }
}
