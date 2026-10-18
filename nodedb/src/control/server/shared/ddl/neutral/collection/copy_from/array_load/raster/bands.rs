// SPDX-License-Identifier: BUSL-1.1

//! Which array attr each GeoTIFF band loads into, and the type rule.
//!
//! `BANDS (a, SKIP, b)` maps file bands in order: band 0 to `a`, band 1
//! nowhere, band 2 to `b`. Without `BANDS`, band `i` loads into attr `i`.
//! A band loads only into an attr that holds every value of its sample type
//! exactly. An attr no band loads into must be nullable, and every cell
//! holds NULL there.

use nodedb_raster::SampleType;
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::ddl_ast::statement::CopyBand;
use nodedb_sql::types_array::ArrayAttrType;

use super::super::decode::RowError;

/// Where one attr's value comes from.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum AttrSource {
    /// No band loads into the attr. It is nullable.
    Null,
    /// File band `band` loads into the attr.
    Band {
        band: usize,
        /// Whether the attr holds floats, so integer samples widen to `f64`.
        float: bool,
        nullable: bool,
        name: String,
    },
}

/// The attr sources in schema order, and the file bands that load.
#[derive(Debug)]
pub(super) struct BandMap {
    pub attrs: Vec<AttrSource>,
    pub loaded: Vec<usize>,
}

/// Map `band_count` file bands of `sample` onto the attrs of `view`.
pub(super) fn map_bands(
    view: &ArrayCatalogView,
    band_count: u16,
    sample: SampleType,
    bands: Option<&[CopyBand]>,
) -> Result<BandMap, RowError> {
    let targets = band_targets(view, band_count, bands)?;
    let mut attrs = vec![AttrSource::Null; view.attrs.len()];
    let mut loaded = Vec::with_capacity(targets.len());
    for (band, target) in targets.into_iter().enumerate() {
        let Some(index) = target else { continue };
        let Some(attr) = view.attrs.get(index) else {
            continue;
        };
        if !widens(sample, attr.dtype) {
            return Err(RowError::new(
                0,
                "42804",
                format!(
                    "band {} holds {sample} samples, which attr '{}' ({}) cannot hold exactly; \
                     declare the attr as a type that widens {sample}",
                    band + 1,
                    attr.name,
                    attr.dtype.sql_name()
                ),
            ));
        }
        if let Some(slot) = attrs.get_mut(index) {
            *slot = AttrSource::Band {
                band,
                float: attr.dtype.is_float(),
                nullable: attr.nullable,
                name: attr.name.clone(),
            };
        }
        loaded.push(band);
    }
    if loaded.is_empty() {
        return Err(RowError::new(
            0,
            "22023",
            "no file band loads into an attr".to_string(),
        ));
    }
    for (attr, source) in view.attrs.iter().zip(&attrs) {
        if *source == AttrSource::Null && !attr.nullable {
            return Err(RowError::new(
                0,
                "23502",
                format!(
                    "attr '{}' is NOT NULL, and no file band loads into it; map a band to it \
                     with BANDS (...)",
                    attr.name
                ),
            ));
        }
    }
    Ok(BandMap { attrs, loaded })
}

/// The attr index each file band loads into, `None` for a skipped band.
fn band_targets(
    view: &ArrayCatalogView,
    band_count: u16,
    bands: Option<&[CopyBand]>,
) -> Result<Vec<Option<usize>>, RowError> {
    let count = usize::from(band_count);
    let Some(bands) = bands else {
        if count > view.attrs.len() {
            return Err(RowError::new(
                0,
                "22023",
                format!(
                    "the file has {count} bands, array '{}' has {} attrs; \
                     name each band's attr or SKIP with BANDS (...)",
                    view.name,
                    view.attrs.len()
                ),
            ));
        }
        return Ok((0..count).map(Some).collect());
    };
    if bands.len() != count {
        return Err(RowError::new(
            0,
            "22023",
            format!(
                "BANDS lists {} entries, the file has {count} bands; list one entry per band",
                bands.len()
            ),
        ));
    }
    bands
        .iter()
        .map(|band| match band {
            CopyBand::Skip => Ok(None),
            CopyBand::Attr(name) => view
                .attrs
                .iter()
                .position(|attr| attr.name.eq_ignore_ascii_case(name))
                .map(Some)
                .ok_or_else(|| {
                    RowError::new(
                        0,
                        "42703",
                        format!(
                            "BANDS names '{name}', which array '{}' has no attr",
                            view.name
                        ),
                    )
                }),
        })
        .collect()
}

/// Whether every `sample` value converts to `attr` without loss.
fn widens(sample: SampleType, attr: ArrayAttrType) -> bool {
    use ArrayAttrType as A;
    match sample {
        SampleType::U8 => matches!(
            attr,
            A::UInt8
                | A::Int16
                | A::UInt16
                | A::Int32
                | A::UInt32
                | A::Int64
                | A::Float32
                | A::Float64
        ),
        SampleType::U16 => matches!(
            attr,
            A::UInt16 | A::Int32 | A::UInt32 | A::Int64 | A::Float32 | A::Float64
        ),
        SampleType::I16 => matches!(
            attr,
            A::Int16 | A::Int32 | A::Int64 | A::Float32 | A::Float64
        ),
        SampleType::U32 => matches!(attr, A::UInt32 | A::Int64 | A::Float64),
        SampleType::F32 => matches!(attr, A::Float32 | A::Float64),
        SampleType::F64 => matches!(attr, A::Float64),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types_array::ArrayAttrAst;

    fn view(attrs: &[(&str, ArrayAttrType, bool)]) -> ArrayCatalogView {
        ArrayCatalogView {
            name: "scene".into(),
            dims: Vec::new(),
            attrs: attrs
                .iter()
                .map(|(name, dtype, nullable)| ArrayAttrAst {
                    name: (*name).into(),
                    dtype: *dtype,
                    nullable: *nullable,
                })
                .collect(),
            tile_extents: Vec::new(),
            georef: None,
        }
    }

    #[test]
    fn widening_is_lossless_only() {
        use ArrayAttrType as A;
        assert!(widens(SampleType::U8, A::UInt8));
        assert!(widens(SampleType::U16, A::Float32));
        assert!(!widens(SampleType::U16, A::Int16));
        assert!(!widens(SampleType::I16, A::UInt16));
        assert!(!widens(SampleType::U32, A::Float32));
        assert!(!widens(SampleType::F32, A::Int64));
        assert!(!widens(SampleType::F64, A::Float32));
        assert!(!widens(SampleType::U8, A::String));
    }

    #[test]
    fn bands_load_in_order_without_a_list() {
        let v = view(&[
            ("b1", ArrayAttrType::UInt16, false),
            ("b2", ArrayAttrType::Float64, true),
            ("extra", ArrayAttrType::Int64, true),
        ]);
        let map = map_bands(&v, 2, SampleType::U16, None).expect("maps");
        assert_eq!(map.loaded, vec![0, 1]);
        assert!(matches!(
            map.attrs[1],
            AttrSource::Band {
                band: 1,
                float: true,
                ..
            }
        ));
        assert_eq!(map.attrs[2], AttrSource::Null);
    }

    #[test]
    fn a_band_list_skips_and_reorders() {
        let v = view(&[
            ("red", ArrayAttrType::UInt16, true),
            ("nir", ArrayAttrType::UInt16, true),
        ]);
        let list = [
            CopyBand::Attr("nir".into()),
            CopyBand::Skip,
            CopyBand::Attr("red".into()),
        ];
        let map = map_bands(&v, 3, SampleType::U16, Some(&list)).expect("maps");
        assert_eq!(map.loaded, vec![0, 2]);
        assert!(matches!(map.attrs[0], AttrSource::Band { band: 2, .. }));
        assert!(matches!(map.attrs[1], AttrSource::Band { band: 0, .. }));
    }

    #[test]
    fn refusals_carry_their_sqlstate() {
        let narrow = view(&[("b1", ArrayAttrType::UInt8, true)]);
        let err = map_bands(&narrow, 1, SampleType::U16, None).expect_err("u16 into UINT8");
        assert_eq!(err.sqlstate, "42804");

        let strict = view(&[
            ("b1", ArrayAttrType::UInt16, true),
            ("qa", ArrayAttrType::UInt16, false),
        ]);
        let err = map_bands(&strict, 1, SampleType::U16, None).expect_err("qa unmapped");
        assert_eq!(err.sqlstate, "23502");

        let one = view(&[("b1", ArrayAttrType::UInt16, true)]);
        let err = map_bands(&one, 2, SampleType::U16, None).expect_err("2 bands, 1 attr");
        assert_eq!(err.sqlstate, "22023");
        let short = [CopyBand::Attr("b1".into())];
        let err = map_bands(&one, 2, SampleType::U16, Some(&short)).expect_err("short list");
        assert_eq!(err.sqlstate, "22023");
        let unknown = [CopyBand::Attr("b9".into())];
        let err = map_bands(&one, 1, SampleType::U16, Some(&unknown)).expect_err("no attr b9");
        assert_eq!(err.sqlstate, "42703");
    }
}
