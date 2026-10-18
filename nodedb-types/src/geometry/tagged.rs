// SPDX-License-Identifier: Apache-2.0

//! A geometry tagged with its spatial reference system, and its GeoJSON form.
//!
//! GeoJSON (RFC 7946) fixes the reference system to WGS 84. A geometry in any
//! other SRID carries the 2008 GeoJSON `crs` member:
//! `"crs":{"type":"name","properties":{"name":"EPSG:<code>"}}`. A WGS 84
//! geometry writes no `crs` member, so its text stays plain RFC 7946.

use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::shape::Geometry;
use super::srid::{Srid, UnsupportedSrid};
use crate::bbox::{BoundingBox, geometry_bbox_in};

/// A geometry and the SRID its coordinates are in.
#[derive(Debug, Clone, PartialEq)]
pub struct SridGeometry {
    pub srid: Srid,
    pub geometry: Geometry,
}

impl SridGeometry {
    pub fn new(srid: Srid, geometry: Geometry) -> Self {
        Self { srid, geometry }
    }

    /// A geometry in WGS 84, the SRID of untagged GeoJSON.
    pub fn wgs84(geometry: Geometry) -> Self {
        Self::new(Srid::Wgs84, geometry)
    }

    /// The same SRID around a different geometry.
    pub fn with_geometry(&self, geometry: Geometry) -> Self {
        Self::new(self.srid, geometry)
    }

    /// Bounding box in the coordinate space of this SRID.
    pub fn bbox(&self) -> BoundingBox {
        geometry_bbox_in(&self.geometry, self.srid.bbox_space())
    }

    /// Read a geometry out of a document field value.
    ///
    /// Accepts the three stored forms: a native `Value::Geometry`, a GeoJSON
    /// string, and a GeoJSON object decoded from a document. Any other value,
    /// or GeoJSON that does not parse, gives `None`.
    pub fn from_value(value: &crate::Value) -> Option<SridGeometry> {
        match value {
            crate::Value::Geometry(g) => Some(g.clone()),
            crate::Value::String(s) => from_geojson_str(s),
            crate::Value::Object(_) => {
                let json = serde_json::Value::from(value.clone());
                serde_json::from_value(json).ok()
            }
            _ => None,
        }
    }
}

impl From<Geometry> for SridGeometry {
    fn from(geometry: Geometry) -> Self {
        Self::wgs84(geometry)
    }
}

/// A total, deterministic text form. A WGS 84 geometry renders as its shape.
/// Any other SRID gets an EWKT-style `SRID=<code>;` prefix.
impl std::fmt::Display for SridGeometry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.srid {
            Srid::Wgs84 => write!(f, "{:?}", self.geometry),
            Srid::Utm47N | Srid::Utm48N | Srid::PeninsularRso | Srid::BorneoRso => {
                write!(f, "SRID={};{:?}", self.srid.code(), self.geometry)
            }
        }
    }
}

/// Why a GeoJSON text does not yield a [`SridGeometry`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeoJsonError {
    #[error("malformed GeoJSON geometry")]
    Malformed,
    #[error("unrecognized GeoJSON crs name {name:?}")]
    UnknownCrsName { name: String },
    #[error(transparent)]
    UnsupportedSrid(#[from] UnsupportedSrid),
}

/// The GeoJSON 2008 named `crs` member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrsMember {
    #[serde(rename = "type")]
    pub kind: String,
    pub properties: CrsProperties,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrsProperties {
    pub name: String,
}

/// The `type` value of a named `crs` member.
const CRS_KIND_NAME: &str = "name";

/// OGC URN prefix that ends in a version segment and then an EPSG code.
const OGC_EPSG_URN_PREFIX: &str = "urn:ogc:def:crs:epsg:";

/// The OGC URNs for WGS 84 in longitude / latitude order.
const OGC_CRS84_URNS: [&str; 2] = [
    "urn:ogc:def:crs:ogc:1.3:crs84",
    "urn:ogc:def:crs:ogc::crs84",
];

impl CrsMember {
    fn for_srid(srid: Srid) -> Self {
        Self {
            kind: CRS_KIND_NAME.to_string(),
            properties: CrsProperties {
                name: format!("EPSG:{}", srid.code()),
            },
        }
    }
}

/// Resolve an optional `crs` member to an SRID. No member means WGS 84.
///
/// Accepts `EPSG:<n>`, `urn:ogc:def:crs:EPSG:[version]:<n>` and the OGC
/// CRS84 URNs. Names compare without regard to ASCII case.
pub fn srid_from_crs(crs: Option<&CrsMember>) -> Result<Srid, GeoJsonError> {
    let Some(member) = crs else {
        return Ok(Srid::Wgs84);
    };
    let name = member.properties.name.trim();
    let unknown = || GeoJsonError::UnknownCrsName {
        name: name.to_string(),
    };
    if !member.kind.eq_ignore_ascii_case(CRS_KIND_NAME) {
        return Err(unknown());
    }
    let lower = name.to_ascii_lowercase();
    if OGC_CRS84_URNS.contains(&lower.as_str()) {
        return Ok(Srid::Wgs84);
    }
    let code_text = if let Some(code) = lower.strip_prefix("epsg:") {
        code
    } else if let Some(rest) = lower.strip_prefix(OGC_EPSG_URN_PREFIX) {
        // `rest` is `<version>:<code>`. The version can be empty.
        rest.rsplit_once(':')
            .map(|(_, code)| code)
            .ok_or_else(unknown)?
    } else {
        return Err(unknown());
    };
    let code: i64 = code_text.parse().map_err(|_| unknown())?;
    Ok(Srid::from_code(code)?)
}

/// Only the `crs` member of a GeoJSON object. Every other key is skipped
/// without being built, so probing costs one shallow pass.
#[derive(Deserialize)]
struct CrsProbe {
    #[serde(default)]
    crs: Option<CrsMember>,
}

/// Parse GeoJSON text into a [`SridGeometry`].
///
/// The geometry parses through its own derive. The `crs` member is then read
/// by a separate shallow probe, so the common untagged case never buffers
/// the document. Uses `sonic_rs` per workspace policy.
pub fn parse_geojson(text: &str) -> Result<SridGeometry, GeoJsonError> {
    let geometry: Geometry = sonic_rs::from_str(text).map_err(|_| GeoJsonError::Malformed)?;
    let probe: CrsProbe = sonic_rs::from_str(text).map_err(|_| GeoJsonError::Malformed)?;
    let srid = srid_from_crs(probe.crs.as_ref())?;
    Ok(SridGeometry::new(srid, geometry))
}

/// [`parse_geojson`] over raw UTF-8 bytes.
pub fn parse_geojson_slice(bytes: &[u8]) -> Result<SridGeometry, GeoJsonError> {
    let text = std::str::from_utf8(bytes).map_err(|_| GeoJsonError::Malformed)?;
    parse_geojson(text)
}

/// Parse a GeoJSON string into a [`SridGeometry`].
///
/// Shared by every storage/read path that meets geometry stored as a JSON
/// string rather than a native value: SQL `ST_Point(...)` inserts serialize
/// to a GeoJSON string, while schemaless document writes and
/// `Value::Geometry` keep the native form. The document index build, the
/// spatial read path and the columnar geometry index all read through here.
///
/// Returns `None` on malformed JSON, non-geometry JSON, or a `crs` member
/// naming an unsupported SRID.
pub fn from_geojson_str(s: &str) -> Option<SridGeometry> {
    parse_geojson(s).ok()
}

#[derive(Serialize)]
struct SridGeometryOut<'a> {
    #[serde(flatten)]
    geometry: &'a Geometry,
    #[serde(skip_serializing_if = "Option::is_none")]
    crs: Option<CrsMember>,
}

#[derive(Deserialize)]
struct SridGeometryIn {
    #[serde(flatten)]
    geometry: Geometry,
    #[serde(default)]
    crs: Option<CrsMember>,
}

impl Serialize for SridGeometry {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let crs = match self.srid {
            Srid::Wgs84 => None,
            Srid::Utm47N | Srid::Utm48N | Srid::PeninsularRso | Srid::BorneoRso => {
                Some(CrsMember::for_srid(self.srid))
            }
        };
        SridGeometryOut {
            geometry: &self.geometry,
            crs,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for SridGeometry {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = SridGeometryIn::deserialize(deserializer)?;
        let srid = srid_from_crs(raw.crs.as_ref()).map_err(D::Error::custom)?;
        Ok(SridGeometry::new(srid, raw.geometry))
    }
}

/// MessagePack form: `[srid_code_u32, geometry]`.
impl zerompk::ToMessagePack for SridGeometry {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_array_len(2)?;
        self.srid.write(writer)?;
        self.geometry.write(writer)
    }
}

impl<'a> zerompk::FromMessagePack<'a> for SridGeometry {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let len = reader.read_array_len()?;
        if len != 2 {
            return Err(zerompk::Error::ArrayLengthMismatch {
                expected: 2,
                actual: len,
            });
        }
        let srid = Srid::read(reader)?;
        let geometry = Geometry::read(reader)?;
        Ok(SridGeometry::new(srid, geometry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn crs(name: &str) -> CrsMember {
        CrsMember {
            kind: "name".into(),
            properties: CrsProperties { name: name.into() },
        }
    }

    #[test]
    fn wgs84_writes_no_crs_member() {
        let g = SridGeometry::wgs84(Geometry::point(1.0, 2.0));
        let json = sonic_rs::to_string(&g).unwrap();
        assert!(!json.contains("crs"), "{json}");
        assert!(json.contains("\"type\":\"Point\""), "{json}");
    }

    #[test]
    fn projected_geojson_round_trip_keeps_srid() {
        let g = SridGeometry::new(Srid::Utm48N, Geometry::point(134445.5, 349624.7));
        let json = sonic_rs::to_string(&g).unwrap();
        assert!(json.contains("\"name\":\"EPSG:32648\""), "{json}");
        assert_eq!(parse_geojson(&json), Ok(g.clone()));
        let derived: SridGeometry = sonic_rs::from_str(&json).unwrap();
        assert_eq!(derived, g);
    }

    #[test]
    fn missing_crs_member_is_wgs84() {
        let g = parse_geojson(r#"{"type":"Point","coordinates":[1.0,2.0]}"#).unwrap();
        assert_eq!(g.srid, Srid::Wgs84);
    }

    #[test]
    fn every_accepted_crs_spelling_resolves() {
        assert_eq!(
            srid_from_crs(Some(&crs("EPSG:3375"))),
            Ok(Srid::PeninsularRso)
        );
        assert_eq!(
            srid_from_crs(Some(&crs("urn:ogc:def:crs:EPSG::3376"))),
            Ok(Srid::BorneoRso)
        );
        assert_eq!(
            srid_from_crs(Some(&crs("urn:ogc:def:crs:OGC:1.3:CRS84"))),
            Ok(Srid::Wgs84)
        );
        assert_eq!(srid_from_crs(Some(&crs("epsg:32647"))), Ok(Srid::Utm47N));
    }

    #[test]
    fn unsupported_and_unknown_crs_names_fail() {
        assert_eq!(
            srid_from_crs(Some(&crs("EPSG:2154"))),
            Err(GeoJsonError::UnsupportedSrid(UnsupportedSrid {
                code: 2154
            }))
        );
        assert!(matches!(
            srid_from_crs(Some(&crs("urn:x:y"))),
            Err(GeoJsonError::UnknownCrsName { .. })
        ));
    }

    #[test]
    fn from_value_reads_every_stored_form() {
        let g = SridGeometry::new(Srid::Utm47N, Geometry::point(1.0, 2.0));
        let text = sonic_rs::to_string(&g).unwrap();
        assert_eq!(
            SridGeometry::from_value(&crate::Value::Geometry(g.clone())),
            Some(g.clone())
        );
        assert_eq!(
            SridGeometry::from_value(&crate::Value::String(text.clone())),
            Some(g.clone())
        );
        let json: serde_json::Value = sonic_rs::from_str(&text).unwrap();
        let obj = crate::Value::from(json);
        assert_eq!(SridGeometry::from_value(&obj), Some(g));
    }

    #[test]
    fn msgpack_round_trip_keeps_srid() {
        let g = SridGeometry::new(Srid::BorneoRso, Geometry::point(1.0, 2.0));
        let bytes = zerompk::to_msgpack_vec(&g).unwrap();
        let back: SridGeometry = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, g);
    }

    #[test]
    fn display_prefixes_non_wgs84() {
        let g = SridGeometry::new(Srid::Utm48N, Geometry::point(1.0, 2.0));
        assert_eq!(
            g.to_string(),
            "SRID=32648;Point { coordinates: [1.0, 2.0] }"
        );
        assert_eq!(
            SridGeometry::wgs84(Geometry::point(1.0, 2.0)).to_string(),
            "Point { coordinates: [1.0, 2.0] }"
        );
    }
}
