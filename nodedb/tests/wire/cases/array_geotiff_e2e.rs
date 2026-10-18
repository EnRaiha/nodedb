// SPDX-License-Identifier: BUSL-1.1

//! End to end: a GeoTIFF loads into a pixel-mode and a world-mode array,
//! and `ARRAY_AGG` over a WGS 84 polygon answers what an independent
//! computation answers.
//!
//! The answer is computed in the test: each pixel centre through the file
//! geotransform, a Snyder UTM inverse to WGS 84, an even-odd
//! point-in-polygon test, nodata excluded. Every pixel centre lies at
//! least 0.5 m from the polygon edges, so a millimetre difference between
//! two projection series cannot move a pixel across an edge.

use crate::harness::TestServer;

use super::array_geotiff::fixture::{
    HEIGHT, KEPT_PIXELS, WIDTH, in_nodata_block, pixel_centre, sample, scene_file,
};
use super::array_geotiff::support::{
    PIXEL_GEOREF, SCENE_ATTRS, aggregate, copy_geotiff, create, pixel_array, world_array,
};
use super::array_geotiff::utm::{edge_distance_m, inside, utm47n_to_lonlat};

/// An oblique quadrilateral inside the scene footprint, `(lon, lat)`,
/// counter-clockwise. Its edges run between pixel-centre rows, so every
/// centre keeps a margin.
const ZONE: [(f64, f64); 4] = [
    (99.913_085_804, 6.323_341_113),
    (99.919_310_793, 6.291_911_559),
    (99.956_934_156, 6.287_137_216),
    (99.962_236_586, 6.313_413_463),
];

/// The least distance, in metres, from any pixel centre to a zone edge.
const MARGIN_M: f64 = 0.5;

/// Relative tolerance of a mean. The engine can merge partial means in
/// any order, so the last bits can differ.
const MEAN_TOLERANCE: f64 = 1e-9;

fn zone_ewkt() -> String {
    let ring: Vec<String> = ZONE
        .iter()
        .chain(ZONE.first())
        .map(|(lon, lat)| format!("{lon} {lat}"))
        .collect();
    format!("SRID=4326;POLYGON(({}))", ring.join(", "))
}

/// The `ARRAY_AGG` tail of a zone query: no group, no slice, the zone,
/// and the GEOREF axes.
fn zone_tail() -> String {
    format!(", NULL, NULL, '{}', NULL", zone_ewkt())
}

/// The independent answer for band `k`: the count and the sum of the
/// kept pixels whose centre lies inside the zone.
struct Expected {
    count: u64,
    sum: u64,
    nodata_inside: u64,
}

fn expected(k: u32) -> Expected {
    let mut answer = Expected {
        count: 0,
        sum: 0,
        nodata_inside: 0,
    };
    for r in 0..HEIGHT {
        for c in 0..WIDTH {
            let (easting, northing) = pixel_centre(r, c);
            let lonlat = utm47n_to_lonlat(easting, northing);
            let margin = edge_distance_m(lonlat, &ZONE);
            assert!(
                margin >= MARGIN_M,
                "pixel ({r}, {c}) centre is {margin} m from a zone edge"
            );
            if !inside(lonlat, &ZONE) {
                continue;
            }
            if in_nodata_block(r, c) {
                answer.nodata_inside += 1;
                continue;
            }
            answer.count += 1;
            answer.sum += u64::from(sample(k, r, c));
        }
    }
    answer
}

/// The zone answers of `array` for `b2`: count, sum, mean.
async fn zone_answers(server: &TestServer, array: &str) -> [f64; 3] {
    let tail = zone_tail();
    [
        aggregate(server, array, "b2", "count", &tail).await,
        aggregate(server, array, "b2", "sum", &tail).await,
        aggregate(server, array, "b2", "mean", &tail).await,
    ]
}

fn assert_answers(array: &str, got: [f64; 3], want: &Expected) {
    let [count, sum, mean] = got;
    assert_eq!(count, want.count as f64, "{array} count");
    assert_eq!(sum, want.sum as f64, "{array} sum");
    let want_mean = want.sum as f64 / want.count as f64;
    assert!(
        (mean - want_mean).abs() <= MEAN_TOLERANCE * want_mean,
        "{array} mean {mean} != {want_mean}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zone_aggregates_match_an_independent_projection() {
    let want = expected(2);
    assert!(
        want.count > 10_000,
        "the zone covers a large part of the scene"
    );
    assert_eq!(
        want.nodata_inside, 100,
        "the nodata block lies inside the zone, so the exclusion is tested"
    );

    let server = TestServer::start().await;
    create(
        &server,
        &pixel_array("scene_px", "0..191", SCENE_ATTRS, PIXEL_GEOREF),
    )
    .await;
    create(&server, &world_array("scene_w")).await;
    let (_file, path) = scene_file();
    for array in ["scene_px", "scene_w"] {
        let loaded = copy_geotiff(&server, array, &path, "")
            .await
            .unwrap_or_else(|e| panic!("COPY {array}: {e:?}"));
        assert_eq!(loaded, KEPT_PIXELS, "{array}: the tag is COPY 49052");
    }

    let pixel = zone_answers(&server, "scene_px").await;
    let world = zone_answers(&server, "scene_w").await;
    assert_answers("scene_px", pixel, &want);
    assert_answers("scene_w", world, &want);
    assert_eq!(pixel[..2], world[..2], "both arrays count and sum the same");
    assert!(
        (pixel[2] - world[2]).abs() <= MEAN_TOLERANCE * pixel[2],
        "both arrays average the same: {} vs {}",
        pixel[2],
        world[2]
    );
}
