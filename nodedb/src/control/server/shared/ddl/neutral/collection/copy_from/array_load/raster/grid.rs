// SPDX-License-Identifier: BUSL-1.1

//! Where each GeoTIFF pixel lands in an array: the cell coordinates of its
//! GEOREF axes, checked against the dim domains once at open.
//!
//! A pixel-mode array stores pixel indexes. The file grid must be the
//! array grid shifted by whole pixels, so file pixel `(col, row)` is cell
//! `(col + col0, row + row0)`. A world-mode array stores pixel-centre
//! world coordinates, so the cell is the file geotransform at
//! `(col + 0.5, row + 0.5)`.
//!
//! The pixel-to-cell map is affine and each coordinate is monotone in the
//! pixel index. So the four corner pixels bound every cell, and a footprint
//! inside the dim domains puts every cell inside them.

use nodedb_array::schema::GeoTransform;
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::types_array::{ArrayCoordLiteral, ArrayDimAst, ArrayDimType, ArrayDomainBound};

use super::super::decode::{RowError, bound_text};

/// Largest relative difference between the file and array linear terms.
const LINEAR_TOLERANCE: f64 = 1e-9;
/// Largest distance from a whole number, in pixels or INT64 units.
const INTEGRAL_TOLERANCE: f64 = 1e-6;
/// Largest pixel offset kept as `i64`, so `offset + u32` never overflows.
const MAX_OFFSET: f64 = (1u64 << 62) as f64;

/// The cell coordinates of one pixel on the array's GEOREF axes.
#[derive(Debug, Clone, Copy)]
pub(super) enum CellGrid {
    /// File pixel `(col, row)` is cell `(col + col0, row + row0)`.
    Pixel { col0: i64, row0: i64 },
    /// The cell is the pixel centre in world coordinates. An `INT64` axis
    /// holds the rounded centre, which open checks to be whole.
    World {
        gt: GeoTransform,
        x_int: bool,
        y_int: bool,
    },
}

impl CellGrid {
    /// The `(x, y)` cell coordinates of file pixel `(col, row)`.
    pub(super) fn cell(&self, col: u32, row: u32) -> (ArrayCoordLiteral, ArrayCoordLiteral) {
        match *self {
            CellGrid::Pixel { col0, row0 } => (
                ArrayCoordLiteral::Int64(i64::from(col) + col0),
                ArrayCoordLiteral::Int64(i64::from(row) + row0),
            ),
            CellGrid::World { gt, x_int, y_int } => {
                let (x, y) = gt.pixel_to_world(f64::from(col) + 0.5, f64::from(row) + 0.5);
                (world_coord(x, x_int), world_coord(y, y_int))
            }
        }
    }
}

fn world_coord(value: f64, int: bool) -> ArrayCoordLiteral {
    if int {
        // Open proved every centre on this axis whole and inside the domain.
        ArrayCoordLiteral::Int64(value.round() as i64)
    } else {
        ArrayCoordLiteral::Float64(value)
    }
}

/// The file facts the grid checks read.
pub(super) struct GridInput<'a> {
    pub view: &'a ArrayCatalogView,
    pub x_dim: &'a ArrayDimAst,
    pub y_dim: &'a ArrayDimAst,
    /// The array GEOREF transform. `None` is a world-mode array.
    pub array_gt: Option<[f64; 6]>,
    pub file_gt: [f64; 6],
    pub width: u32,
    pub height: u32,
}

/// Check the file grid against the array and its footprint against the
/// dim domains. Misalignment is `22023`, a footprint outside a domain
/// `22003`.
pub(super) fn plan_grid(input: &GridInput<'_>) -> Result<CellGrid, RowError> {
    match input.array_gt {
        Some(array_gt) => pixel_grid(input, array_gt),
        None => world_grid(input),
    }
}

fn pixel_grid(input: &GridInput<'_>, array_gt: [f64; 6]) -> Result<CellGrid, RowError> {
    let file = input.file_gt;
    let scale = [1, 2, 4, 5]
        .iter()
        .map(|&k| array_gt[k].abs())
        .fold(0.0_f64, f64::max);
    let linear_equal = [1, 2, 4, 5]
        .iter()
        .all(|&k| (file[k] - array_gt[k]).abs() <= LINEAR_TOLERANCE * scale);
    if !linear_equal {
        return Err(misaligned(
            input,
            format!(
                "the file pixel size and rotation {:?} differ from the array TRANSFORM {:?}",
                [file[1], file[2], file[4], file[5]],
                [array_gt[1], array_gt[2], array_gt[4], array_gt[5]]
            ),
        ));
    }
    let origin = GeoTransform(array_gt).world_to_pixel(file[0], file[3]);
    let Some((u0, v0)) = origin else {
        return Err(misaligned(
            input,
            "the array TRANSFORM has no inverse".into(),
        ));
    };
    let (Some(col0), Some(row0)) = (whole_offset(u0), whole_offset(v0)) else {
        return Err(misaligned(
            input,
            format!("the file origin falls at array pixel ({u0}, {v0}), not on a whole pixel"),
        ));
    };
    let last_col = col0 + i64::from(input.width) - 1;
    let last_row = row0 + i64::from(input.height) - 1;
    check_int_span(input.x_dim, col0, last_col)?;
    check_int_span(input.y_dim, row0, last_row)?;
    Ok(CellGrid::Pixel { col0, row0 })
}

/// `value` rounded to `i64` when it is within the tolerance of a whole
/// number and inside the offset range.
fn whole_offset(value: f64) -> Option<i64> {
    let rounded = value.round();
    let whole = (value - rounded).abs() <= INTEGRAL_TOLERANCE;
    (whole && rounded.abs() <= MAX_OFFSET).then_some(rounded as i64)
}

fn world_grid(input: &GridInput<'_>) -> Result<CellGrid, RowError> {
    let gt = GeoTransform(input.file_gt);
    let (w, h) = (f64::from(input.width), f64::from(input.height));
    let corners = [
        (0.5, 0.5),
        (w - 0.5, 0.5),
        (0.5, h - 0.5),
        (w - 0.5, h - 0.5),
    ]
    .map(|(u, v)| gt.pixel_to_world(u, v));
    let xs = corners.map(|(x, _)| x);
    let ys = corners.map(|(_, y)| y);
    let x_int = int_axis(input.x_dim)?;
    let y_int = int_axis(input.y_dim)?;
    let file = input.file_gt;
    if x_int {
        check_whole_axis(input.x_dim, &xs, [file[1], file[2]])?;
    }
    if y_int {
        check_whole_axis(input.y_dim, &ys, [file[4], file[5]])?;
    }
    check_world_span(input.x_dim, &xs)?;
    check_world_span(input.y_dim, &ys)?;
    Ok(CellGrid::World { gt, x_int, y_int })
}

/// Whether a world-mode axis is `INT64`. GEOREF admits `INT64` and
/// `FLOAT64` axes only.
fn int_axis(dim: &ArrayDimAst) -> Result<bool, RowError> {
    match dim.dtype {
        ArrayDimType::Int64 => Ok(true),
        ArrayDimType::Float64 => Ok(false),
        ArrayDimType::TimestampMs | ArrayDimType::String => Err(RowError::new(
            0,
            "22023",
            format!("GEOREF axis '{}' cannot hold world coordinates", dim.name),
        )),
    }
}

/// Every pixel centre on an `INT64` world axis must be whole: the corner
/// centres and the per-pixel steps.
fn check_whole_axis(
    dim: &ArrayDimAst,
    corners: &[f64; 4],
    steps: [f64; 2],
) -> Result<(), RowError> {
    let whole = |v: &f64| (v - v.round()).abs() <= INTEGRAL_TOLERANCE;
    if corners.iter().all(whole) && steps.iter().all(whole) {
        return Ok(());
    }
    Err(RowError::new(
        0,
        "22023",
        format!(
            "pixel centres on INT64 axis '{}' are not whole numbers; \
             declare it FLOAT64 or resample outside the engine",
            dim.name
        ),
    ))
}

fn check_world_span(dim: &ArrayDimAst, corners: &[f64; 4]) -> Result<(), RowError> {
    let lo = corners.iter().copied().fold(f64::INFINITY, f64::min);
    let hi = corners.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    match dim.dtype {
        ArrayDimType::Int64 => check_int_span(dim, lo.round() as i64, hi.round() as i64),
        ArrayDimType::Float64 | ArrayDimType::TimestampMs | ArrayDimType::String => {
            let inside = match (&dim.lo, &dim.hi) {
                (ArrayDomainBound::Float64(d_lo), ArrayDomainBound::Float64(d_hi)) => {
                    *d_lo <= lo && hi <= *d_hi
                }
                _ => false,
            };
            if inside {
                Ok(())
            } else {
                Err(outside(dim, &lo.to_string(), &hi.to_string()))
            }
        }
    }
}

/// Check the integer span `[lo, hi]` of an axis against its domain.
fn check_int_span(dim: &ArrayDimAst, lo: i64, hi: i64) -> Result<(), RowError> {
    let inside = match (&dim.lo, &dim.hi) {
        (ArrayDomainBound::Int64(d_lo), ArrayDomainBound::Int64(d_hi)) => {
            *d_lo <= lo && hi <= *d_hi
        }
        _ => false,
    };
    if inside {
        return Ok(());
    }
    Err(outside(dim, &lo.to_string(), &hi.to_string()))
}

fn outside(dim: &ArrayDimAst, lo: &str, hi: &str) -> RowError {
    RowError::new(
        0,
        "22003",
        format!(
            "the raster footprint spans [{lo}..{hi}] on dim '{}', outside its domain [{}..{}]",
            dim.name,
            bound_text(&dim.lo),
            bound_text(&dim.hi)
        ),
    )
}

fn misaligned(input: &GridInput<'_>, why: String) -> RowError {
    RowError::new(
        0,
        "22023",
        format!(
            "raster grid does not align with array '{}': {why}; resample outside the engine",
            input.view.name
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn int_dim(name: &str, lo: i64, hi: i64) -> ArrayDimAst {
        ArrayDimAst {
            name: name.into(),
            dtype: ArrayDimType::Int64,
            lo: ArrayDomainBound::Int64(lo),
            hi: ArrayDomainBound::Int64(hi),
        }
    }

    fn float_dim(name: &str, lo: f64, hi: f64) -> ArrayDimAst {
        ArrayDimAst {
            name: name.into(),
            dtype: ArrayDimType::Float64,
            lo: ArrayDomainBound::Float64(lo),
            hi: ArrayDomainBound::Float64(hi),
        }
    }

    fn view() -> ArrayCatalogView {
        ArrayCatalogView {
            name: "scene".into(),
            dims: Vec::new(),
            attrs: Vec::new(),
            tile_extents: Vec::new(),
            georef: None,
        }
    }

    const GT: [f64; 6] = [600_000.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];

    fn input<'a>(
        view: &'a ArrayCatalogView,
        x: &'a ArrayDimAst,
        y: &'a ArrayDimAst,
        array_gt: Option<[f64; 6]>,
        file_gt: [f64; 6],
    ) -> GridInput<'a> {
        GridInput {
            view,
            x_dim: x,
            y_dim: y,
            array_gt,
            file_gt,
            width: 256,
            height: 192,
        }
    }

    #[test]
    fn a_shifted_file_lands_at_its_pixel_offset() {
        let (v, x, y) = (view(), int_dim("col", 0, 1000), int_dim("row", 0, 1000));
        let file = [
            600_000.0 + 30.0 * 10.0,
            30.0,
            0.0,
            700_000.0 - 30.0 * 4.0,
            0.0,
            -30.0,
        ];
        let grid = plan_grid(&input(&v, &x, &y, Some(GT), file)).expect("aligned");
        let (cx, cy) = grid.cell(0, 0);
        assert_eq!(cx, ArrayCoordLiteral::Int64(10));
        assert_eq!(cy, ArrayCoordLiteral::Int64(4));
    }

    #[test]
    fn a_half_pixel_shift_is_misaligned() {
        let (v, x, y) = (view(), int_dim("col", 0, 1000), int_dim("row", 0, 1000));
        let file = [600_015.0, 30.0, 0.0, 700_000.0, 0.0, -30.0];
        let err = plan_grid(&input(&v, &x, &y, Some(GT), file)).expect_err("half pixel");
        assert_eq!(err.sqlstate, "22023");
        assert!(
            err.message.contains("resample outside the engine"),
            "{}",
            err.message
        );
        let scaled = [600_000.0, 10.0, 0.0, 700_000.0, 0.0, -10.0];
        let err = plan_grid(&input(&v, &x, &y, Some(GT), scaled)).expect_err("pixel size");
        assert_eq!(err.sqlstate, "22023");
    }

    #[test]
    fn a_footprint_past_the_domain_is_22003() {
        let (v, x, y) = (view(), int_dim("col", 0, 255), int_dim("row", 0, 99));
        let err = plan_grid(&input(&v, &x, &y, Some(GT), GT)).expect_err("rows past 99");
        assert_eq!(err.sqlstate, "22003");
    }

    #[test]
    fn world_cells_are_pixel_centres() {
        let v = view();
        let x = float_dim("x", 600_000.0, 607_680.0);
        let y = float_dim("y", 694_240.0, 700_000.0);
        let grid = plan_grid(&input(&v, &x, &y, None, GT)).expect("inside");
        let (cx, cy) = grid.cell(2, 1);
        assert_eq!(cx, ArrayCoordLiteral::Float64(600_075.0));
        assert_eq!(cy, ArrayCoordLiteral::Float64(699_955.0));
    }

    #[test]
    fn int_world_axes_need_whole_centres() {
        let v = view();
        let x = int_dim("x", 0, 10_000_000);
        let y = int_dim("y", 0, 10_000_000);
        let quarter = [600_000.0, 25.0, 0.0, 700_000.0, 0.0, -25.0];
        let err = plan_grid(&input(&v, &x, &y, None, quarter)).expect_err("centres at 12.5 m");
        assert_eq!(err.sqlstate, "22023");
        let shifted = [599_985.0, 30.0, 0.0, 700_015.0, 0.0, -30.0];
        let grid = plan_grid(&input(&v, &x, &y, None, shifted)).expect("whole centres");
        let (cx, cy) = grid.cell(1, 1);
        assert_eq!(cx, ArrayCoordLiteral::Int64(600_030));
        assert_eq!(cy, ArrayCoordLiteral::Int64(699_970));
    }
}
