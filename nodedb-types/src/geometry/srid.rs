// SPDX-License-Identifier: Apache-2.0

//! The closed set of coordinate reference systems NodeDB supports.

use serde::{Deserialize, Serialize};

use crate::bbox::BboxSpace;

/// Every supported SRID code, comma-separated, for error messages.
pub const SUPPORTED_SRID_CODES: &str = "4326, 32647, 32648, 3375, 3376";

/// A spatial reference system identifier, restricted to the systems NodeDB
/// can transform between.
///
/// The set is closed: an EPSG code outside it is rejected with
/// [`UnsupportedSrid`], never stored. Serialized as its numeric EPSG code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(into = "u32", try_from = "u32")]
pub enum Srid {
    /// EPSG:4326, WGS 84 geographic. x is longitude, y is latitude, degrees.
    #[default]
    Wgs84,
    /// EPSG:32647, UTM zone 47N on WGS 84. Metres.
    Utm47N,
    /// EPSG:32648, UTM zone 48N on WGS 84. Metres.
    Utm48N,
    /// EPSG:3375, GDM2000 Peninsula RSO. Metres.
    PeninsularRso,
    /// EPSG:3376, GDM2000 East Malaysia (Borneo) RSO. Metres.
    BorneoRso,
}

/// An SRID code outside the supported set.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unsupported SRID {code}; supported: {SUPPORTED_SRID_CODES}")]
pub struct UnsupportedSrid {
    pub code: i64,
}

impl Srid {
    /// Every supported SRID, WGS 84 first.
    pub const SUPPORTED: [Srid; 5] = [
        Srid::Wgs84,
        Srid::Utm47N,
        Srid::Utm48N,
        Srid::PeninsularRso,
        Srid::BorneoRso,
    ];

    /// The EPSG code.
    pub const fn code(self) -> u32 {
        match self {
            Srid::Wgs84 => 4326,
            Srid::Utm47N => 32647,
            Srid::Utm48N => 32648,
            Srid::PeninsularRso => 3375,
            Srid::BorneoRso => 3376,
        }
    }

    /// Resolve an EPSG code. Any code outside [`Srid::SUPPORTED`] fails.
    pub fn from_code(code: i64) -> Result<Srid, UnsupportedSrid> {
        Srid::SUPPORTED
            .into_iter()
            .find(|srid| i64::from(srid.code()) == code)
            .ok_or(UnsupportedSrid { code })
    }

    /// Whether coordinates are longitude / latitude degrees. A `false` SRID
    /// is projected, with coordinates in metres.
    pub const fn is_geographic(self) -> bool {
        match self {
            Srid::Wgs84 => true,
            Srid::Utm47N | Srid::Utm48N | Srid::PeninsularRso | Srid::BorneoRso => false,
        }
    }

    /// The coordinate space bounding boxes in this SRID live in.
    pub const fn bbox_space(self) -> BboxSpace {
        if self.is_geographic() {
            BboxSpace::Geographic
        } else {
            BboxSpace::Planar
        }
    }
}

impl From<Srid> for u32 {
    fn from(srid: Srid) -> u32 {
        srid.code()
    }
}

impl TryFrom<u32> for Srid {
    type Error = UnsupportedSrid;

    fn try_from(code: u32) -> Result<Srid, UnsupportedSrid> {
        Srid::from_code(i64::from(code))
    }
}

impl std::fmt::Display for Srid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl zerompk::ToMessagePack for Srid {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_u32(self.code())
    }
}

impl<'a> zerompk::FromMessagePack<'a> for Srid {
    fn read<R: zerompk::Read<'a>>(reader: &mut R) -> zerompk::Result<Self> {
        let code = reader.read_u32()?;
        Srid::try_from(code).map_err(|e| zerompk::Error::UnknownKey(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_supported_code_round_trips() {
        for srid in Srid::SUPPORTED {
            assert_eq!(Srid::from_code(i64::from(srid.code())), Ok(srid));
        }
    }

    #[test]
    fn unsupported_code_names_every_supported_code() {
        let err = Srid::from_code(2154).unwrap_err();
        let text = err.to_string();
        for srid in Srid::SUPPORTED {
            assert!(text.contains(&srid.code().to_string()), "{text}");
        }
        assert!(text.contains("2154"), "{text}");
    }

    #[test]
    fn only_wgs84_is_geographic() {
        assert!(Srid::Wgs84.is_geographic());
        assert_eq!(Srid::Wgs84.bbox_space(), BboxSpace::Geographic);
        for srid in &Srid::SUPPORTED[1..] {
            assert!(!srid.is_geographic());
            assert_eq!(srid.bbox_space(), BboxSpace::Planar);
        }
    }

    #[test]
    fn default_is_wgs84() {
        assert_eq!(Srid::default(), Srid::Wgs84);
    }

    #[test]
    fn msgpack_round_trip() {
        let bytes = zerompk::to_msgpack_vec(&Srid::BorneoRso).unwrap();
        let back: Srid = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(back, Srid::BorneoRso);
    }

    #[test]
    fn msgpack_rejects_unknown_code() {
        let bytes = zerompk::to_msgpack_vec(&2154u32).unwrap();
        assert!(zerompk::from_msgpack::<Srid>(&bytes).is_err());
    }
}
