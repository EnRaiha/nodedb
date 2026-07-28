// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `CREATE SPATIAL INDEX` DDL acceptance and option
//! validation.
//!
//! The handler reads whitespace-split tokens by fixed position and scans the
//! rest for `USING` / `PRECISION`, so the statement shapes it was not written
//! against are misread rather than rejected: the forms shown in `docs/` fail
//! on the position check, an unknown `USING` value falls through to the R-tree
//! default, and an out-of-range or unparsed `PRECISION` is clamped or zeroed
//! without a word to the client. These tests assert that every accepted
//! statement means what it says and every unsupported one is an error.

mod common;

use common::pgwire_harness::TestServer;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_accepts_documented_fields_form() {
    // `CREATE SPATIAL INDEX ON <collection> FIELDS <field>` is the form shown
    // in `docs/spatial.md` and `docs/bitemporal.md`.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_fields \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE SPATIAL INDEX ON si_fields FIELDS location")
        .await
        .expect("documented FIELDS form must be accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_accepts_documented_unnamed_paren_form() {
    // `CREATE SPATIAL INDEX ON <collection>(<field>) USING RTREE` is the form
    // shown in `docs/query-language.md` — no index name before `ON`.
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_paren \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE SPATIAL INDEX ON si_paren(location) USING RTREE")
        .await
        .expect("documented unnamed collection(field) form must be accepted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_rejects_unknown_using_type() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_using \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();

    // Anything that is not RTREE or GEOHASH currently falls through to RTREE,
    // so a user asking for an index type that does not exist gets a different
    // one and is never told.
    server
        .expect_error(
            "CREATE SPATIAL INDEX idx_si_using ON si_using(location) USING QUADTREE",
            "quadtree",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_rejects_out_of_range_precision() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_prec \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();

    // Geohash precision tops out at 12. A larger value is silently clamped,
    // so the index is built at a resolution the statement did not request.
    server
        .expect_error(
            "CREATE SPATIAL INDEX idx_si_prec ON si_prec(location) USING GEOHASH PRECISION 99",
            "precision",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_rejects_non_numeric_precision() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_prectext \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();

    // A value that does not parse must be an error, not a silent fall back to
    // the zero default.
    server
        .expect_error(
            "CREATE SPATIAL INDEX idx_si_prectext ON si_prectext(location) \
             USING GEOHASH PRECISION high",
            "precision",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_spatial_index_rejects_unrecognized_trailing_tokens() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION si_tail \
             COLUMNS (id TEXT, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .unwrap();

    // Tokens past the recognized option keywords are dropped on the floor, so
    // a mistyped or unsupported clause reads as a successful CREATE.
    server
        .expect_error(
            "CREATE SPATIAL INDEX idx_si_tail ON si_tail(location) WITH (index = 'rtree')",
            "unrecognized option 'WITH'",
        )
        .await;
}
