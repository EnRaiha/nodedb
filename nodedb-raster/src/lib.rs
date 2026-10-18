// SPDX-License-Identifier: Apache-2.0

//! Bounded GeoTIFF decoding for NodeDB array bulk loads.
//!
//! [`GeoTiffReader`] opens IFD 0 of a GeoTIFF and decodes it one strip or
//! tile at a time into typed per-band samples, under a caller-given byte
//! cap. It reads the georeference (geotransform, CRS, `GDAL_NODATA`) from
//! the GeoTIFF tags.

pub mod block;
pub mod crs;
pub mod error;
pub mod geokeys;
pub mod layout;
pub mod limits;
pub mod nodata;
mod preflight;
pub mod reader;
pub mod sample;
pub mod transform;

pub use block::{BandSamples, RasterBlock};
pub use crs::srid_from_geokeys;
pub use error::RasterError;
pub use geokeys::{GeoKeyDirectory, GeoKeyError, GeoKeyValue};
pub use layout::{BlockKind, BlockWindow, Interleave, RasterLayout};
pub use limits::RasterLimits;
pub use nodata::parse_gdal_nodata;
pub use reader::GeoTiffReader;
pub use sample::{SampleFormatCode, SampleType};
pub use transform::{GeoTransform, ModelTags};
