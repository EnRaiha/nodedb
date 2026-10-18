// SPDX-License-Identifier: BUSL-1.1

//! The rows of one decoded GeoTIFF block, read one pixel at a time.
//!
//! A row's number is the pixel's row-major ordinal in the image plus one.
//! A pixel whose loaded bands all hold nodata is skipped. A kept pixel
//! with one nodata band writes NULL there, or is refused with `23502` when
//! the attr is NOT NULL. Open checked the types and the footprint, so no
//! other check runs per pixel.
//!
//! Each kept pixel allocates its coordinate and attr vectors, sized
//! exactly. A skipped pixel allocates nothing.

use nodedb_raster::{BandSamples, RasterBlock};
use nodedb_sql::types_array::{ArrayAttrLiteral, ArrayCoordLiteral, ArrayInsertRow};

use super::super::decode::RowError;
use super::bands::AttrSource;
use super::open::{CoordSource, RasterPlan};

/// The rows of one block in pixel order within the block.
pub(super) struct BlockRows<'a> {
    plan: &'a RasterPlan,
    block: &'a RasterBlock,
    next: usize,
    pixels: usize,
}

impl<'a> BlockRows<'a> {
    /// Read `block` under `plan`. A block with the wrong band count or a
    /// short band is a malformed file (`22P04`).
    pub(super) fn new(plan: &'a RasterPlan, block: &'a RasterBlock) -> Result<Self, RowError> {
        let pixels = block.window.pixels();
        if block.bands.len() != usize::from(plan.bands) {
            return Err(RowError::new(
                0,
                "22P04",
                format!(
                    "GeoTIFF block {} decoded {} bands, the file declares {}",
                    block.index,
                    block.bands.len(),
                    plan.bands
                ),
            ));
        }
        if let Some(short) = block.bands.iter().find(|band| band.len() < pixels) {
            return Err(RowError::new(
                0,
                "22P04",
                format!(
                    "GeoTIFF block {} decoded {} samples, its window holds {pixels}",
                    block.index,
                    short.len()
                ),
            ));
        }
        Ok(Self {
            plan,
            block,
            next: 0,
            pixels,
        })
    }

    /// Whether every loaded band of pixel `index` holds nodata.
    fn is_nodata(&self, index: usize) -> bool {
        let Some(nodata) = self.plan.nodata else {
            return false;
        };
        self.plan.loaded.iter().all(|&band| {
            self.block
                .bands
                .get(band)
                .is_some_and(|samples| nodata.matches(samples, index))
        })
    }

    /// The row of pixel `index` of the block.
    fn row(&self, index: usize) -> Result<(usize, ArrayInsertRow), RowError> {
        let window = &self.block.window;
        let local_row = index / window.width as usize;
        let local_col = index % window.width as usize;
        // The window lies inside the image, so both fit `u32`.
        let row = window.row_off + local_row as u32;
        let col = window.col_off + local_col as u32;
        let row_no = row as usize * self.plan.width as usize + col as usize + 1;
        let (x, y) = self.plan.grid.cell(col, row);
        let mut coords = Vec::with_capacity(self.plan.coords.len());
        for source in &self.plan.coords {
            coords.push(match source {
                CoordSource::X => x.clone(),
                CoordSource::Y => y.clone(),
                CoordSource::Time(ms) => ArrayCoordLiteral::Int64(*ms),
            });
        }
        let mut attrs = Vec::with_capacity(self.plan.attrs.len());
        for source in &self.plan.attrs {
            let AttrSource::Band {
                band,
                float,
                nullable,
                name,
            } = source
            else {
                attrs.push(ArrayAttrLiteral::Null);
                continue;
            };
            let samples = self
                .block
                .bands
                .get(*band)
                .ok_or_else(|| missing_band(*band, row_no))?;
            let is_nodata = self.plan.nodata.is_some_and(|n| n.matches(samples, index));
            let literal = match (is_nodata, *nullable) {
                (true, true) => ArrayAttrLiteral::Null,
                (true, false) => {
                    return Err(RowError::new(
                        row_no,
                        "23502",
                        format!(
                            "attr '{name}' is NOT NULL, and band {} holds nodata at \
                             (pixel row {row}, col {col})",
                            band + 1
                        ),
                    ));
                }
                (false, _) => sample_literal(samples, index, *float)
                    .ok_or_else(|| missing_band(*band, row_no))?,
            };
            attrs.push(literal);
        }
        Ok((row_no, ArrayInsertRow { coords, attrs }))
    }
}

impl Iterator for BlockRows<'_> {
    type Item = Result<(usize, ArrayInsertRow), RowError>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.next < self.pixels {
            let index = self.next;
            self.next += 1;
            if !self.is_nodata(index) {
                return Some(self.row(index));
            }
        }
        None
    }
}

/// Sample `index` as an attr literal. Open admitted only lossless pairs,
/// so an integer sample widens to `f64` exactly when the attr is a float.
fn sample_literal(samples: &BandSamples, index: usize, float: bool) -> Option<ArrayAttrLiteral> {
    let int = |n: i64| {
        if float {
            ArrayAttrLiteral::Float64(n as f64)
        } else {
            ArrayAttrLiteral::Int64(n)
        }
    };
    match samples {
        BandSamples::U8(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::U16(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::I16(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::U32(v) => v.get(index).map(|s| int(i64::from(*s))),
        BandSamples::F32(v) => v
            .get(index)
            .map(|s| ArrayAttrLiteral::Float64(f64::from(*s))),
        BandSamples::F64(v) => v.get(index).map(|s| ArrayAttrLiteral::Float64(*s)),
    }
}

fn missing_band(band: usize, row_no: usize) -> RowError {
    RowError::new(
        row_no,
        "22P04",
        format!("GeoTIFF band {} has no sample for this pixel", band + 1),
    )
}

#[cfg(test)]
mod tests {
    use super::super::grid::CellGrid;
    use super::super::open::Nodata;
    use super::*;
    use nodedb_raster::BlockWindow;

    fn plan(nullable: bool) -> RasterPlan {
        RasterPlan {
            width: 4,
            bands: 2,
            coords: vec![CoordSource::Y, CoordSource::X],
            attrs: vec![
                AttrSource::Band {
                    band: 0,
                    float: false,
                    nullable,
                    name: "b1".into(),
                },
                AttrSource::Band {
                    band: 1,
                    float: true,
                    nullable: true,
                    name: "b2".into(),
                },
                AttrSource::Null,
            ],
            loaded: vec![0, 1],
            grid: CellGrid::Pixel { col0: 10, row0: 20 },
            nodata: Some(Nodata::Int(0)),
        }
    }

    fn block() -> RasterBlock {
        RasterBlock {
            index: 1,
            window: BlockWindow {
                col_off: 2,
                row_off: 0,
                width: 2,
                height: 2,
            },
            bands: vec![
                BandSamples::U16(vec![5, 0, 0, 7]),
                BandSamples::U16(vec![6, 0, 9, 8]),
            ],
        }
    }

    #[test]
    fn rows_skip_all_nodata_pixels_and_null_partial_ones() {
        let (plan, block) = (plan(true), block());
        let rows: Vec<_> = BlockRows::new(&plan, &block)
            .expect("block fits")
            .collect::<Result<_, _>>()
            .expect("all rows load");
        let numbers: Vec<usize> = rows.iter().map(|(n, _)| *n).collect();
        // Pixel (0, 3) is nodata in both bands. Row-major ordinals: 2, 6, 7.
        assert_eq!(numbers, vec![3, 7, 8]);
        let (_, first) = &rows[0];
        assert_eq!(
            first.coords,
            vec![ArrayCoordLiteral::Int64(20), ArrayCoordLiteral::Int64(12)]
        );
        assert_eq!(
            first.attrs,
            vec![
                ArrayAttrLiteral::Int64(5),
                ArrayAttrLiteral::Float64(6.0),
                ArrayAttrLiteral::Null
            ]
        );
        let (_, partial) = &rows[1];
        assert_eq!(partial.attrs[0], ArrayAttrLiteral::Null);
        assert_eq!(partial.attrs[1], ArrayAttrLiteral::Float64(9.0));
    }

    #[test]
    fn a_nodata_band_in_a_not_null_attr_names_the_pixel() {
        let (plan, block) = (plan(false), block());
        let err = BlockRows::new(&plan, &block)
            .expect("block fits")
            .find_map(Result::err)
            .expect("pixel (1, 2) is refused");
        assert_eq!(err.sqlstate, "23502");
        assert_eq!(err.row, 7);
        assert!(
            err.message.contains("(pixel row 1, col 2)"),
            "{}",
            err.message
        );
    }

    #[test]
    fn a_short_block_is_malformed() {
        let plan = plan(true);
        let mut block = block();
        block.bands[1] = BandSamples::U16(vec![1]);
        let err = BlockRows::new(&plan, &block).err().expect("short band");
        assert_eq!(err.sqlstate, "22P04");
    }
}
