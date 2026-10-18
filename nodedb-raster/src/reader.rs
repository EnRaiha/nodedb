// SPDX-License-Identifier: Apache-2.0

//! `GeoTiffReader`: IFD 0 of a GeoTIFF, decoded one block at a time.
//!
//! Open bounds every IFD 0 tag value by the file size first. It then reads
//! the tags once and refuses what the reader cannot decode exactly: an
//! overview as IFD 0, an unsupported codec, sample type, or photometric
//! interpretation, and a block larger than the caller's cap. A block read
//! then allocates at most that cap.

use std::io::{Read, Seek};
use std::mem::size_of;

use nodedb_types::decode_bounds::checked_decode_capacity;
use nodedb_types::geometry::Srid;
use tiff::decoder::{Decoder, DecodingResult};
use tiff::tags::Tag;

use crate::block::{BandSamples, ChunkShape, RasterBlock, chunky_bands, planar_band};
use crate::crs::srid_from_geokeys;
use crate::error::RasterError;
use crate::geokeys::{GT_RASTER_TYPE, GeoKeyDirectory};
use crate::layout::{Interleave, RasterLayout};
use crate::limits::RasterLimits;
use crate::nodata::parse_gdal_nodata;
use crate::preflight::check_ifd0;
use crate::sample::SampleType;
use crate::transform::{GeoTransform, ModelTags};

/// Compression codes the decoder reads: none, LZW, deflate (both codes),
/// PackBits.
const SUPPORTED_COMPRESSION: [u16; 5] = [1, 5, 8, 32946, 32773];
/// `NewSubfileType` bit of a reduced-resolution image.
const REDUCED_RESOLUTION_BIT: u32 = 1;
/// `SubfileType` value of a reduced-resolution image.
const SUBFILE_REDUCED_RESOLUTION: u32 = 2;

/// A GeoTIFF opened at IFD 0.
pub struct GeoTiffReader<R: Read + Seek> {
    decoder: Decoder<R>,
    layout: RasterLayout,
    limits: RasterLimits,
    transform: Option<GeoTransform>,
    geokeys: Option<GeoKeyDirectory>,
    nodata: Option<f64>,
}

impl<R: Read + Seek> GeoTiffReader<R> {
    /// Open `reader` and read the tags of IFD 0.
    pub fn open(mut reader: R, limits: RasterLimits) -> Result<Self, RasterError> {
        let cap = limits.block_bytes;
        let tiff = |e| RasterError::from_tiff(e, cap);
        check_ifd0(&mut reader)?;
        let mut decoder = Decoder::new(reader)
            .map_err(tiff)?
            .with_limits(limits.tiff_limits());
        refuse_reduced_resolution(&mut decoder, cap)?;
        let compression = decoder
            .find_tag_unsigned::<u16>(Tag::Compression)
            .map_err(tiff)?
            .unwrap_or(1);
        if !SUPPORTED_COMPRESSION.contains(&compression) {
            return Err(RasterError::UnsupportedCompression { code: compression });
        }
        let layout = RasterLayout::read(&mut decoder, cap)?;
        let needed_bytes = layout.block_bytes();
        if needed_bytes > cap as u64 {
            return Err(RasterError::BlockTooLarge {
                needed_bytes,
                limit_bytes: cap,
            });
        }
        let geokeys = read_geokeys(&mut decoder, cap)?;
        let pixel_scale = f64_tag(&mut decoder, Tag::ModelPixelScaleTag, cap)?;
        let tiepoints = f64_tag(&mut decoder, Tag::ModelTiepointTag, cap)?;
        let transformation = f64_tag(&mut decoder, Tag::ModelTransformationTag, cap)?;
        let transform = GeoTransform::from_tags(ModelTags {
            pixel_scale: pixel_scale.as_deref(),
            tiepoints: tiepoints.as_deref(),
            transformation: transformation.as_deref(),
            raster_type: geokeys.as_ref().and_then(|keys| keys.short(GT_RASTER_TYPE)),
        })?;
        let nodata = decoder
            .find_tag(Tag::GdalNodata)
            .map_err(tiff)?
            .map(|value| value.into_string().map_err(tiff))
            .transpose()?
            .map(|text| parse_gdal_nodata(&text))
            .transpose()?;
        Ok(Self {
            decoder,
            layout,
            limits,
            transform,
            geokeys,
            nodata,
        })
    }

    /// The block grid and band layout.
    pub fn layout(&self) -> &RasterLayout {
        &self.layout
    }

    /// Image width in pixels.
    pub fn width(&self) -> u32 {
        self.layout.width
    }

    /// Image height in pixels.
    pub fn height(&self) -> u32 {
        self.layout.height
    }

    /// Number of bands.
    pub fn band_count(&self) -> u16 {
        self.layout.bands
    }

    /// The sample type every band shares.
    pub fn sample_type(&self) -> SampleType {
        self.layout.sample_type
    }

    /// Blocks per band plane. Block indexes run `0..block_count()`.
    pub fn block_count(&self) -> u32 {
        self.layout.block_count()
    }

    /// The pixel-corner geotransform, PixelIsPoint already shifted.
    pub fn transform(&self) -> Option<GeoTransform> {
        self.transform
    }

    /// The GeoKey directory, when the file has one.
    pub fn geokeys(&self) -> Option<&GeoKeyDirectory> {
        self.geokeys.as_ref()
    }

    /// The SRID the GeoKeys name, or `None` when they name no CRS.
    pub fn srid(&self) -> Result<Option<Srid>, RasterError> {
        srid_from_geokeys(self.geokeys.as_ref())
    }

    /// The `GDAL_NODATA` value, shared by every band.
    pub fn nodata(&self) -> Option<f64> {
        self.nodata
    }

    /// Decode block `index`: one sample vector per band, cropped to the
    /// image.
    pub fn read_block(&mut self, index: u32) -> Result<RasterBlock, RasterError> {
        let window = self.layout.window(index)?;
        let shape = ChunkShape {
            index,
            pixels: window.pixels(),
            bands: usize::from(self.layout.bands),
            limit_bytes: self.limits.block_bytes,
        };
        let bands = match self.layout.interleave {
            Interleave::Chunky => chunky_bands(self.read_chunk(index)?, shape)?,
            Interleave::Planar => self.read_planes(index, shape)?,
        };
        Ok(RasterBlock {
            index,
            window,
            bands,
        })
    }

    /// Read block `index` of every band plane. Plane `b` holds its blocks
    /// at chunk indexes `b * block_count() ..`.
    fn read_planes(
        &mut self,
        index: u32,
        shape: ChunkShape,
    ) -> Result<Vec<BandSamples>, RasterError> {
        let count = self.layout.block_count();
        let capacity = checked_decode_capacity(
            shape.bands,
            size_of::<BandSamples>(),
            shape.bands,
            1,
            usize::from(u16::MAX),
            shape.limit_bytes,
        )
        .ok_or(RasterError::BlockTooLarge {
            needed_bytes: self.layout.block_bytes(),
            limit_bytes: shape.limit_bytes,
        })?;
        let mut bands = Vec::with_capacity(capacity);
        for band in 0..u32::from(self.layout.bands) {
            let chunk = band
                .checked_mul(count)
                .and_then(|start| start.checked_add(index))
                .ok_or(RasterError::BlockOutOfRange { index, count })?;
            bands.push(planar_band(self.read_chunk(chunk)?, shape)?);
        }
        Ok(bands)
    }

    fn read_chunk(&mut self, chunk: u32) -> Result<DecodingResult, RasterError> {
        let cap = self.limits.block_bytes;
        self.decoder
            .read_chunk(chunk)
            .map_err(|e| RasterError::from_tiff(e, cap))
    }
}

/// Refuse an IFD 0 flagged as an overview by `NewSubfileType` or the
/// older `SubfileType`.
fn refuse_reduced_resolution<R: Read + Seek>(
    decoder: &mut Decoder<R>,
    cap: usize,
) -> Result<(), RasterError> {
    let tiff = |e| RasterError::from_tiff(e, cap);
    let new_subfile = decoder
        .find_tag_unsigned::<u32>(Tag::NewSubfileType)
        .map_err(tiff)?
        .unwrap_or(0);
    let subfile = decoder
        .find_tag_unsigned::<u32>(Tag::SubfileType)
        .map_err(tiff)?
        .unwrap_or(0);
    if new_subfile & REDUCED_RESOLUTION_BIT != 0 || subfile == SUBFILE_REDUCED_RESOLUTION {
        return Err(RasterError::ReducedResolutionFirstImage);
    }
    Ok(())
}

/// The GeoKey directory with its parameter arrays, when the file has one.
fn read_geokeys<R: Read + Seek>(
    decoder: &mut Decoder<R>,
    cap: usize,
) -> Result<Option<GeoKeyDirectory>, RasterError> {
    let tiff = |e| RasterError::from_tiff(e, cap);
    let Some(directory) = decoder
        .find_tag(Tag::GeoKeyDirectoryTag)
        .map_err(tiff)?
        .map(|value| value.into_u16_vec().map_err(tiff))
        .transpose()?
    else {
        return Ok(None);
    };
    let doubles = f64_tag(decoder, Tag::GeoDoubleParamsTag, cap)?;
    let ascii = decoder
        .find_tag(Tag::GeoAsciiParamsTag)
        .map_err(tiff)?
        .map(|value| value.into_string().map_err(tiff))
        .transpose()?;
    let keys = GeoKeyDirectory::parse(&directory, doubles.as_deref(), ascii.as_deref())?;
    Ok(Some(keys))
}

/// The doubles of `tag`, when present.
fn f64_tag<R: Read + Seek>(
    decoder: &mut Decoder<R>,
    tag: Tag,
    cap: usize,
) -> Result<Option<Vec<f64>>, RasterError> {
    let tiff = |e| RasterError::from_tiff(e, cap);
    decoder
        .find_tag(tag)
        .map_err(tiff)?
        .map(|value| value.into_f64_vec().map_err(tiff))
        .transpose()
}
