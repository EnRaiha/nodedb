// SPDX-License-Identifier: Apache-2.0

//! Well-Known Binary (WKB) serialization for Geometry types.
//!
//! WKB is the standard binary format for geometry interchange (ISO 13249).
//! Used as the Arrow `DataType::Binary` backing for spatial columns, which
//! avoids JSON parse overhead during query execution.
//!
//! Format: `[byte_order: u8] [type: u32] [coordinates...]`. Byte order 1 is
//! little-endian (NDR), 0 is big-endian (XDR). Writers always emit
//! little-endian. The reader accepts both, plus PostGIS EWKB.

pub mod ewkb;
pub mod format;
pub mod read;
pub mod write;

pub use ewkb::geometry_to_ewkb;
pub use format::EWKB_SRID_FLAG;
pub use read::{geometry_from_wkb, wkb_bbox};
pub use write::geometry_to_wkb;
