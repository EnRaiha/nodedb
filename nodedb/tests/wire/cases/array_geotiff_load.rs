// SPDX-License-Identifier: BUSL-1.1

//! `COPY <array> FROM '<path>.tif'`: what a GeoTIFF load writes.
//!
//! The scene holds 49152 pixels, so its cells span several 4 MiB load
//! chunks. The all-zero block is nodata under `GDAL_NODATA` and loads no
//! cell. `NODATA` replaces the file value, `BANDS` picks the bands, and
//! `TIME` fills a TIMESTAMP dim.

use crate::harness::TestServer;

use super::array_geotiff::fixture::{
    HEIGHT, KEPT_PIXELS, NODATA_COLS, NODATA_ROWS, WIDTH, in_nodata_block, sample, scene_file,
};
use super::array_geotiff::support::{
    PIXEL_GEOREF, SCENE_ATTRS, aggregate, copy_geotiff, create, pixel_array, pixel_cells,
};

/// The sum of band `k` over every pixel `keep` accepts.
fn band_sum(k: u32, keep: impl Fn(u32, u32) -> bool) -> f64 {
    let mut sum = 0u64;
    for r in 0..HEIGHT {
        for c in 0..WIDTH {
            if keep(r, c) {
                sum += u64::from(sample(k, r, c));
            }
        }
    }
    sum as f64
}

async fn load_scene(server: &TestServer, name: &str, path: &str) {
    create(
        server,
        &pixel_array(name, "0..191", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    let loaded = copy_geotiff(server, name, path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY {name}: {e:?}"));
    assert_eq!(loaded, KEPT_PIXELS, "the tag is COPY 49052");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_multi_chunk_load_keeps_every_pixel_but_nodata() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    load_scene(&server, "geo_load", &path).await;

    assert_eq!(
        aggregate(&server, "geo_load", "b1", "count", "").await,
        KEPT_PIXELS as f64
    );
    let want = band_sum(3, |r, c| !in_nodata_block(r, c));
    assert_eq!(aggregate(&server, "geo_load", "b3", "sum", "").await, want);

    // A ring of one pixel around the nodata block holds cells, the block none.
    let (r0, r1) = (NODATA_ROWS.start - 1, NODATA_ROWS.end);
    let (c0, c1) = (NODATA_COLS.start - 1, NODATA_COLS.end);
    let cells = pixel_cells(&server, "geo_load", (r0, r1), (c0, c1)).await;
    assert_eq!(
        cells.len(),
        12 * 12 - 100,
        "the ring loads, the block does not"
    );
    assert!(cells.keys().all(|&(r, c)| !in_nodata_block(r, c)));
    let corner = cells.get(&(r0, c0)).expect("ring corner");
    assert_eq!(corner, &sample(1, r0, c0).to_string());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bands_and_nodata_replace_the_file_defaults() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    create(
        &server,
        &pixel_array("geo_b1", "0..191", "b1 UINT16", PIXEL_GEOREF),
    )
    .await;
    // NODATA 1000 replaces GDAL_NODATA 0, so the zero block loads and every
    // b1 of 1000 is skipped. Bands 2 and 3 load nowhere.
    let loaded = copy_geotiff(
        &server,
        "geo_b1",
        &path,
        ", BANDS (b1, SKIP, SKIP), NODATA 1000",
    )
    .await
    .unwrap_or_else(|e| panic!("COPY geo_b1: {e:?}"));
    let skipped = (0..HEIGHT)
        .flat_map(|r| (0..WIDTH).map(move |c| (r, c)))
        .filter(|&(r, c)| sample(1, r, c) == 1000)
        .count() as u64;
    assert!(skipped > 0, "some b1 samples are 1000");
    assert_eq!(loaded, u64::from(WIDTH * HEIGHT) - skipped);

    let zero = pixel_cells(&server, "geo_b1", (100, 100), (120, 120)).await;
    assert_eq!(zero.get(&(100, 120)).map(String::as_str), Some("0"));
    let origin = pixel_cells(&server, "geo_b1", (0, 0), (0, 0)).await;
    assert!(origin.is_empty(), "b1(0, 0) is 1000, the new nodata");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_nodata_band_in_a_not_null_attr_names_its_pixel() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    create(
        &server,
        &pixel_array(
            "geo_strict",
            "0..191",
            "b1 UINT16 NOT NULL, b2 UINT16, b3 UINT16",
            PIXEL_GEOREF,
        ),
    )
    .await;
    // Pixel (0, 0) holds 1000, 2000, 3000: only b1 is nodata, so it loads.
    let err = copy_geotiff(&server, "geo_strict", &path, ", NODATA 1000")
        .await
        .expect_err("b1 is NOT NULL");
    assert_eq!(err.code, "23502", "{err:?}");
    assert!(err.message.contains("row 1:"), "{err:?}");
    assert!(err.message.contains("(pixel row 0, col 0)"), "{err:?}");
    assert!(err.message.ends_with("(0 rows committed)"), "{err:?}");
    assert_eq!(
        aggregate(&server, "geo_strict", "b2", "count", "").await,
        0.0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn time_fills_the_timestamp_dim() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    create(
        &server,
        &format!(
            "CREATE ARRAY geo_time \
             DIMS (t TIMESTAMP_MS [1735689600000..1798761600000], \
                   row INT64 [0..191], col INT64 [0..255]) \
             ATTRS ({SCENE_ATTRS}) TILE_EXTENTS (86400000, 64, 64) {PIXEL_GEOREF}"
        ),
    )
    .await;
    let loaded = copy_geotiff(&server, "geo_time", &path, ", TIME '2025-06-01T00:00:00Z'")
        .await
        .unwrap_or_else(|e| panic!("COPY geo_time: {e:?}"));
    assert_eq!(loaded, KEPT_PIXELS);
    let rows = server
        .query_named_rows("SELECT * FROM ARRAY_AGG('geo_time', 'b1', 'count', 't')")
        .await
        .expect("count by t");
    assert_eq!(rows.len(), 1, "one scene instant: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_load_inside_a_block_rolls_back_whole() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    create(
        &server,
        &pixel_array("geo_txn", "0..191", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    server.exec("BEGIN").await.expect("BEGIN");
    let loaded = copy_geotiff(&server, "geo_txn", &path, "")
        .await
        .unwrap_or_else(|e| panic!("COPY in a block: {e:?}"));
    assert_eq!(loaded, KEPT_PIXELS);
    server.exec("ROLLBACK").await.expect("ROLLBACK");
    assert_eq!(
        aggregate(&server, "geo_txn", "b1", "count", "").await,
        0.0,
        "ROLLBACK discards every staged chunk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loaded_scene_survives_restart() {
    let server = TestServer::start().await;
    let (_file, path) = scene_file();
    load_scene(&server, "geo_restart", &path).await;

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        aggregate(&server, "geo_restart", "b1", "count", "").await,
        KEPT_PIXELS as f64
    );
    let want = band_sum(2, |r, c| !in_nodata_block(r, c));
    assert_eq!(
        aggregate(&server, "geo_restart", "b2", "sum", "").await,
        want
    );
}
