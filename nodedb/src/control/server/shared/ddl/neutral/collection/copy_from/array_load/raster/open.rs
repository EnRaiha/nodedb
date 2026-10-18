// SPDX-License-Identifier: BUSL-1.1

//! The open-time checks of a GeoTIFF load. Every check runs before the
//! first block decodes, so a refused file commits no row.
//!
//! - The array must have a GEOREF (`55000`), and the file CRS must be its
//!   SRID (`22023`).
//! - Bands map onto attrs by the lossless type rule (`42804`), and an attr
//!   no band fills must be nullable (`23502`).
//! - The file grid must align with the array (`22023`), and its footprint
//!   must fit the dim domains (`22003`).
//! - `TIME` fills the one `TIMESTAMP` dim. Any other dim has no source.
//! - The nodata value must be a value of the sample type (`22023`).

use nodedb_raster::{BandSamples, SampleType};
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::ddl_ast::statement::CopyRasterOptions;
use nodedb_sql::types_array::{ArrayDimAst, ArrayDimType, ArrayDomainBound, ArrayGeorefAst};
use nodedb_types::geometry::Srid;

use super::super::decode::{RowError, bound_text};
use super::bands::{AttrSource, map_bands};
use super::grid::{CellGrid, GridInput, plan_grid};

/// What a GeoTIFF's tags say, read on the decoder thread.
#[derive(Debug, Clone)]
pub(super) struct RasterHeader {
    pub width: u32,
    pub height: u32,
    pub bands: u16,
    pub sample_type: SampleType,
    /// The pixel-corner geotransform. `None` when the file has none.
    pub transform: Option<[f64; 6]>,
    /// The CRS the GeoKeys name. `None` when they name none.
    pub srid: Option<Srid>,
    /// The `GDAL_NODATA` value.
    pub nodata: Option<f64>,
}

/// The source of one dim's coordinate.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum CoordSource {
    /// The GEOREF x axis.
    X,
    /// The GEOREF y axis.
    Y,
    /// The `TIME` instant, in ms since the Unix epoch.
    Time(i64),
}

/// The nodata value, typed by the sample type.
#[derive(Debug, Clone, Copy)]
pub(super) enum Nodata {
    Int(i64),
    F32(f32),
    F64(f64),
}

impl Nodata {
    /// Whether sample `index` of `band` is nodata. NaN matches NaN.
    pub(super) fn matches(self, band: &BandSamples, index: usize) -> bool {
        match (self, band) {
            (Nodata::Int(n), BandSamples::U8(v)) => {
                v.get(index).is_some_and(|s| i64::from(*s) == n)
            }
            (Nodata::Int(n), BandSamples::U16(v)) => {
                v.get(index).is_some_and(|s| i64::from(*s) == n)
            }
            (Nodata::Int(n), BandSamples::I16(v)) => {
                v.get(index).is_some_and(|s| i64::from(*s) == n)
            }
            (Nodata::Int(n), BandSamples::U32(v)) => {
                v.get(index).is_some_and(|s| i64::from(*s) == n)
            }
            (Nodata::F32(n), BandSamples::F32(v)) => v
                .get(index)
                .is_some_and(|s| *s == n || (s.is_nan() && n.is_nan())),
            (Nodata::F64(n), BandSamples::F64(v)) => v
                .get(index)
                .is_some_and(|s| *s == n || (s.is_nan() && n.is_nan())),
            (
                Nodata::Int(_) | Nodata::F32(_) | Nodata::F64(_),
                BandSamples::U8(_)
                | BandSamples::U16(_)
                | BandSamples::I16(_)
                | BandSamples::U32(_)
                | BandSamples::F32(_)
                | BandSamples::F64(_),
            ) => false,
        }
    }
}

/// How every pixel of an accepted file becomes a row.
#[derive(Debug)]
pub(super) struct RasterPlan {
    pub(super) width: u32,
    pub(super) bands: u16,
    /// One source per dim, in schema order.
    pub(super) coords: Vec<CoordSource>,
    /// One source per attr, in schema order.
    pub(super) attrs: Vec<AttrSource>,
    /// The file bands that load. A pixel is nodata when all of them are.
    pub(super) loaded: Vec<usize>,
    pub(super) grid: CellGrid,
    pub(super) nodata: Option<Nodata>,
}

impl RasterPlan {
    /// Run every open-time check of `header` against `view`.
    pub(super) fn new(
        view: &ArrayCatalogView,
        georef: &ArrayGeorefAst,
        header: &RasterHeader,
        options: &CopyRasterOptions,
    ) -> Result<Self, RowError> {
        check_srid(view, georef, header)?;
        let map = map_bands(
            view,
            header.bands,
            header.sample_type,
            options.bands.as_deref(),
        )?;
        let (x_index, x_dim) = georef_axis(view, &georef.x_dim)?;
        let (y_index, y_dim) = georef_axis(view, &georef.y_dim)?;
        let file_gt = header.transform.ok_or_else(|| {
            RowError::new(
                0,
                "22023",
                "the file has no geotransform, so no pixel has a world position".to_string(),
            )
        })?;
        let grid = plan_grid(&GridInput {
            view,
            x_dim,
            y_dim,
            array_gt: georef.transform,
            file_gt,
            width: header.width,
            height: header.height,
        })?;
        let coords = plan_coords(view, x_index, y_index, options.time_ms)?;
        let nodata = options
            .nodata
            .or(header.nodata)
            .map(|value| typed_nodata(value, header.sample_type))
            .transpose()?;
        Ok(Self {
            width: header.width,
            bands: header.bands,
            coords,
            attrs: map.attrs,
            loaded: map.loaded,
            grid,
            nodata,
        })
    }
}

fn check_srid(
    view: &ArrayCatalogView,
    georef: &ArrayGeorefAst,
    header: &RasterHeader,
) -> Result<(), RowError> {
    match header.srid {
        Some(srid) if srid.code() == georef.srid => Ok(()),
        Some(srid) => Err(RowError::new(
            0,
            "22023",
            format!(
                "the file CRS is EPSG:{}, array '{}' GEOREF is SRID {}; \
                 reproject outside the engine",
                srid.code(),
                view.name,
                georef.srid
            ),
        )),
        None => Err(RowError::new(
            0,
            "22023",
            format!(
                "the file GeoKeys name no CRS, array '{}' GEOREF is SRID {}",
                view.name, georef.srid
            ),
        )),
    }
}

/// The index and spec of the dim a GEOREF axis names.
fn georef_axis<'v>(
    view: &'v ArrayCatalogView,
    name: &str,
) -> Result<(usize, &'v ArrayDimAst), RowError> {
    view.dims
        .iter()
        .enumerate()
        .find(|(_, dim)| dim.name.eq_ignore_ascii_case(name))
        .ok_or_else(|| {
            RowError::new(
                0,
                "XX000",
                format!(
                    "array '{}' GEOREF names dim '{name}', which it lacks",
                    view.name
                ),
            )
        })
}

/// The source of every dim: the GEOREF axes, then `TIME` for the one
/// `TIMESTAMP` dim. Any other dim has no source in a raster (`0A000`).
fn plan_coords(
    view: &ArrayCatalogView,
    x_index: usize,
    y_index: usize,
    time_ms: Option<i64>,
) -> Result<Vec<CoordSource>, RowError> {
    let mut time_used = false;
    let mut coords = Vec::with_capacity(view.dims.len());
    for (index, dim) in view.dims.iter().enumerate() {
        let source = if index == x_index {
            CoordSource::X
        } else if index == y_index {
            CoordSource::Y
        } else if dim.dtype == ArrayDimType::TimestampMs && !time_used {
            time_used = true;
            CoordSource::Time(time_coord(dim, time_ms)?)
        } else {
            return Err(RowError::new(
                0,
                "0A000",
                format!(
                    "dim '{}' has no source in a GeoTIFF; a raster fills the GEOREF axes \
                     and one TIMESTAMP dim from TIME",
                    dim.name
                ),
            ));
        };
        coords.push(source);
    }
    if time_ms.is_some() && !time_used {
        return Err(RowError::new(
            0,
            "42601",
            format!(
                "TIME is given, and array '{}' has no TIMESTAMP dim",
                view.name
            ),
        ));
    }
    Ok(coords)
}

/// The `TIME` instant for `dim`, checked against its domain.
fn time_coord(dim: &ArrayDimAst, time_ms: Option<i64>) -> Result<i64, RowError> {
    let time = time_ms.ok_or_else(|| {
        RowError::new(
            0,
            "42601",
            format!(
                "dim '{}' is a TIMESTAMP; give the scene instant with WITH (TIME '<iso>')",
                dim.name
            ),
        )
    })?;
    let inside = match (&dim.lo, &dim.hi) {
        (
            ArrayDomainBound::TimestampMs(lo) | ArrayDomainBound::Int64(lo),
            ArrayDomainBound::TimestampMs(hi) | ArrayDomainBound::Int64(hi),
        ) => *lo <= time && time <= *hi,
        _ => false,
    };
    if inside {
        return Ok(time);
    }
    Err(RowError::new(
        0,
        "22003",
        format!(
            "TIME {time} is outside dim '{}' domain [{}..{}]",
            dim.name,
            bound_text(&dim.lo),
            bound_text(&dim.hi)
        ),
    ))
}

/// `value` as a nodata of `sample`. An integer type needs a whole value in
/// its range. `f32` takes the nearest `f32`, as GDAL compares.
fn typed_nodata(value: f64, sample: SampleType) -> Result<Nodata, RowError> {
    let range = match sample {
        SampleType::U8 => Some((f64::from(u8::MIN), f64::from(u8::MAX))),
        SampleType::U16 => Some((f64::from(u16::MIN), f64::from(u16::MAX))),
        SampleType::I16 => Some((f64::from(i16::MIN), f64::from(i16::MAX))),
        SampleType::U32 => Some((f64::from(u32::MIN), f64::from(u32::MAX))),
        SampleType::F32 if value.is_finite() && value.abs() > f64::from(f32::MAX) => {
            return Err(unrepresentable(value, sample));
        }
        SampleType::F32 => return Ok(Nodata::F32(value as f32)),
        SampleType::F64 => return Ok(Nodata::F64(value)),
    };
    match range {
        Some((lo, hi)) if value.fract() == 0.0 && lo <= value && value <= hi => {
            Ok(Nodata::Int(value as i64))
        }
        _ => Err(unrepresentable(value, sample)),
    }
}

fn unrepresentable(value: f64, sample: SampleType) -> RowError {
    RowError::new(
        0,
        "22023",
        format!("nodata {value} is not a {sample} value, so no sample can match it"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_nodata_must_be_a_sample_value() {
        assert!(matches!(
            typed_nodata(0.0, SampleType::U16),
            Ok(Nodata::Int(0))
        ));
        assert!(matches!(
            typed_nodata(-9999.0, SampleType::I16),
            Ok(Nodata::Int(-9999))
        ));
        for (value, sample) in [
            (-1.0, SampleType::U16),
            (65536.0, SampleType::U16),
            (0.5, SampleType::U8),
            (f64::NAN, SampleType::U32),
            (1e39, SampleType::F32),
        ] {
            let err = typed_nodata(value, sample).expect_err("unrepresentable");
            assert_eq!(err.sqlstate, "22023", "{value} as {sample}");
        }
    }

    #[test]
    fn nan_nodata_matches_nan_samples() {
        let band = BandSamples::F32(vec![f32::NAN, 1.0]);
        let nodata = typed_nodata(f64::NAN, SampleType::F32).expect("NaN is an f32 value");
        assert!(nodata.matches(&band, 0));
        assert!(!nodata.matches(&band, 1));
        let ints = BandSamples::U16(vec![0, 7]);
        assert!(Nodata::Int(0).matches(&ints, 0));
        assert!(!Nodata::Int(0).matches(&ints, 1));
    }

    fn dim(
        name: &str,
        dtype: ArrayDimType,
        lo: ArrayDomainBound,
        hi: ArrayDomainBound,
    ) -> ArrayDimAst {
        ArrayDimAst {
            name: name.into(),
            dtype,
            lo,
            hi,
        }
    }

    fn view(dims: Vec<ArrayDimAst>) -> ArrayCatalogView {
        ArrayCatalogView {
            name: "scene".into(),
            dims,
            attrs: Vec::new(),
            tile_extents: Vec::new(),
            georef: None,
        }
    }

    #[test]
    fn time_fills_the_timestamp_dim() {
        let ts = || {
            dim(
                "t",
                ArrayDimType::TimestampMs,
                ArrayDomainBound::TimestampMs(0),
                ArrayDomainBound::TimestampMs(1_000),
            )
        };
        let axis = |name: &str| {
            dim(
                name,
                ArrayDimType::Int64,
                ArrayDomainBound::Int64(0),
                ArrayDomainBound::Int64(9),
            )
        };
        let v = view(vec![ts(), axis("row"), axis("col")]);
        let coords = plan_coords(&v, 2, 1, Some(500)).expect("time given");
        assert_eq!(
            coords,
            vec![CoordSource::Time(500), CoordSource::Y, CoordSource::X]
        );
        assert_eq!(
            plan_coords(&v, 2, 1, None).expect_err("no TIME").sqlstate,
            "42601"
        );
        assert_eq!(
            plan_coords(&v, 2, 1, Some(5_000))
                .expect_err("past")
                .sqlstate,
            "22003"
        );

        let plain = view(vec![axis("row"), axis("col")]);
        assert_eq!(
            plan_coords(&plain, 1, 0, Some(5))
                .expect_err("no ts")
                .sqlstate,
            "42601"
        );
        let band = view(vec![axis("band"), axis("row"), axis("col")]);
        assert_eq!(
            plan_coords(&band, 2, 1, None)
                .expect_err("band dim")
                .sqlstate,
            "0A000"
        );
    }
}
