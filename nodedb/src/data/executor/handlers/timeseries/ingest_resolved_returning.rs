// SPDX-License-Identifier: BUSL-1.1

//! The `RETURNING` rows of a resolved timeseries ingest, decoded from the
//! landed rows' images.
//!
//! Each landed row's image is the row as a scan reads it. An image that
//! does not decode is corruption of the resolved record. The statement is
//! refused with the corruption error and one corruption report. It never
//! returns fewer rows than it stored.

/// The site name the corruption report carries.
const SITE: &str = "timeseries_resolved_returning";

/// Decode every landed row's image, in row order.
///
/// `Err(SegmentCorrupted)` for the first image that does not decode, after
/// filing its corruption report.
pub(super) fn decode_returning_images(
    collection: &str,
    images: &[&[u8]],
) -> crate::Result<Vec<rmpv::Value>> {
    images
        .iter()
        .enumerate()
        .map(|(row, image)| {
            crate::util::bounded_msgpack::read_value(image).map_err(|e| {
                let err = crate::Error::SegmentCorrupted {
                    detail: format!(
                        "timeseries '{collection}': the image of landed row {row} does not \
                         decode: {e}"
                    ),
                };
                crate::diag::timeseries_row_image_undecodable(&err, collection, SITE);
                err
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(value: &rmpv::Value) -> Vec<u8> {
        let mut bytes = Vec::new();
        rmpv::encode::write_value(&mut bytes, value).expect("encode image");
        bytes
    }

    #[test]
    fn every_image_decodes_in_row_order() {
        let first = rmpv::Value::Map(vec![("v".into(), 1.into())]);
        let second = rmpv::Value::Map(vec![("v".into(), 2.into())]);
        let images = [image(&first), image(&second)];
        let refs: Vec<&[u8]> = images.iter().map(Vec::as_slice).collect();

        let rows = decode_returning_images("ts", &refs).expect("decode");

        assert_eq!(rows, vec![first, second]);
    }

    #[test]
    fn an_undecodable_image_refuses_the_statement() {
        let good = image(&rmpv::Value::Map(vec![("v".into(), 1.into())]));
        // 0xc1 is a reserved MessagePack marker, never a value.
        let bad: &[u8] = &[0xc1];
        let refs: Vec<&[u8]> = vec![good.as_slice(), bad];

        let err = decode_returning_images("ts", &refs).expect_err("row 1 does not decode");

        assert!(
            matches!(&err, crate::Error::SegmentCorrupted { detail } if detail.contains("row 1")),
            "{err:?}"
        );
    }

    #[test]
    fn an_empty_image_is_not_a_row() {
        let refs: Vec<&[u8]> = vec![&[]];

        let err = decode_returning_images("ts", &refs).expect_err("an empty image is no row");

        assert!(
            matches!(err, crate::Error::SegmentCorrupted { .. }),
            "{err:?}"
        );
    }
}
