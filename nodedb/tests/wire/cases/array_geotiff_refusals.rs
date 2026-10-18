// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM '<path>.tif'` refusals. Every open-time check runs
//! before the first block decodes, so a refused load commits no row.

use crate::harness::TestServer;

use super::array_geotiff::fixture::{scene_file, scene_tiff, write_tiff};
use super::array_geotiff::support::{
    PIXEL_GEOREF, SCENE_ATTRS, assert_refused, create, pixel_array,
};

/// One refused array: its name, DDL, COPY options, and SQLSTATE.
struct Case {
    name: &'static str,
    ddl: String,
    extra: &'static str,
    code: &'static str,
}

fn cases() -> Vec<Case> {
    let case = |name, ddl, extra, code| Case {
        name,
        ddl,
        extra,
        code,
    };
    vec![
        // u16 samples do not fit UINT8.
        case(
            "geo_narrow",
            pixel_array(
                "geo_narrow",
                "0..191",
                "b1 UINT8, b2 UINT16, b3 UINT16",
                PIXEL_GEOREF,
            ),
            "",
            "42804",
        ),
        // The file is EPSG:32647.
        case(
            "geo_srid",
            pixel_array(
                "geo_srid",
                "0..191",
                SCENE_ATTRS,
                "GEOREF (SRID 32648, X col, Y row, TRANSFORM (600000, 30, 0, 700000, 0, -30))",
            ),
            "",
            "22023",
        ),
        // Half a pixel east of the file grid.
        case(
            "geo_shift",
            pixel_array(
                "geo_shift",
                "0..191",
                SCENE_ATTRS,
                "GEOREF (SRID 32647, X col, Y row, TRANSFORM (600015, 30, 0, 700000, 0, -30))",
            ),
            "",
            "22023",
        ),
        // 192 rows do not fit [0..99].
        case(
            "geo_small",
            pixel_array("geo_small", "0..99", SCENE_ATTRS, PIXEL_GEOREF),
            "",
            "22003",
        ),
        // No GEOREF places no pixel.
        case(
            "geo_plain",
            pixel_array("geo_plain", "0..191", SCENE_ATTRS, ""),
            "",
            "55000",
        ),
        // No band fills a NOT NULL attr.
        case(
            "geo_qa",
            pixel_array(
                "geo_qa",
                "0..191",
                "b1 UINT16, b2 UINT16, b3 UINT16, qa UINT8 NOT NULL",
                PIXEL_GEOREF,
            ),
            "",
            "23502",
        ),
        // A TIMESTAMP dim needs TIME.
        case(
            "geo_untimed",
            format!(
                "CREATE ARRAY geo_untimed \
                 DIMS (t TIMESTAMP_MS [1735689600000..1798761600000], \
                       row INT64 [0..191], col INT64 [0..255]) \
                 ATTRS ({SCENE_ATTRS}) TILE_EXTENTS (86400000, 64, 64) {PIXEL_GEOREF}"
            ),
            "",
            "42601",
        ),
        // A BANDS list must name one entry per file band.
        case(
            "geo_bands",
            pixel_array("geo_bands", "0..191", SCENE_ATTRS, PIXEL_GEOREF),
            ", BANDS (b1, b2)",
            "22023",
        ),
    ]
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn open_time_refusals_commit_no_row() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    for case in cases() {
        create(&server, &case.ddl).await;
        assert_refused(&server, case.name, &path, case.extra, case.code).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupt_file_is_a_bad_copy_file_format() {
    let server = TestServer::start().await;
    create(
        &server,
        &pixel_array("geo_corrupt", "0..191", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    // The encoder writes the tiles, then the directory, so a cut file
    // keeps its header and loses the directory it points to.
    let bytes = scene_tiff();
    let cut = write_tiff(&bytes[..bytes.len() / 2]);
    let path = cut.path().to_string_lossy().into_owned();
    assert_refused(&server, "geo_corrupt", &path, "", "22P04").await;
}
