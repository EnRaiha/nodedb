// SPDX-License-Identifier: BUSL-1.1

//! A float aggregate that overflows or meets NaN reaches the client as
//! PostgreSQL renders it: `Infinity`, `-Infinity` or `NaN`, never NULL.
//!
//! `1e308 + 1e308` overflows `f64` to `Infinity`. A SUM over a NaN row is
//! NaN. Each engine runs the same checks.

use crate::harness::TestServer;

/// The text of the one cell `SELECT SUM(f) FROM collection` returns.
/// `None` for SQL NULL.
async fn sum_text(srv: &TestServer, collection: &str) -> Option<String> {
    let msgs = srv
        .client
        .simple_query(&format!("SELECT SUM(f) FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("{collection}: SUM must run: {e:?}"));
    let cells: Vec<Option<String>> = msgs
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row.get(0).map(str::to_owned)),
            _ => None,
        })
        .collect();
    assert_eq!(
        cells.len(),
        1,
        "{collection}: one aggregate row, got {cells:?}"
    );
    cells.into_iter().next().flatten()
}

/// Insert `(id, f)` rows into `collection`. `ts` is the row index, so a
/// collection keyed by time gets distinct keys.
async fn insert_floats(srv: &TestServer, collection: &str, values: &[&str]) {
    for (i, f) in values.iter().enumerate() {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, ts, f) VALUES ('r{i}', {i}, {f})"
        ))
        .await
        .unwrap_or_else(|e| panic!("{collection}: insert {f}: {e}"));
    }
}

/// Run the overflow and NaN checks against collections made by
/// `create(name)`.
async fn check_engine(srv: &TestServer, create: impl Fn(&str) -> String) {
    srv.exec(&create("overflow")).await.unwrap();
    insert_floats(srv, "overflow", &["1e308", "1e308"]).await;
    assert_eq!(
        sum_text(srv, "overflow").await.as_deref(),
        Some("Infinity"),
        "SUM past f64::MAX must render Infinity"
    );

    srv.exec(&create("negative_overflow")).await.unwrap();
    insert_floats(srv, "negative_overflow", &["-1e308", "-1e308"]).await;
    assert_eq!(
        sum_text(srv, "negative_overflow").await.as_deref(),
        Some("-Infinity"),
        "SUM past -f64::MAX must render -Infinity"
    );

    srv.exec(&create("nan")).await.unwrap();
    insert_floats(srv, "nan", &["1.5", "'NaN'::float8"]).await;
    assert_eq!(
        sum_text(srv, "nan").await.as_deref(),
        Some("NaN"),
        "SUM over a NaN row must render NaN"
    );
}

#[tokio::test]
async fn document_schemaless_non_finite_sum_renders_postgres_text() {
    let srv = TestServer::start().await;
    check_engine(&srv, |name| {
        format!("CREATE COLLECTION {name} WITH (engine='document_schemaless')")
    })
    .await;
}

#[tokio::test]
async fn columnar_non_finite_sum_renders_postgres_text() {
    let srv = TestServer::start().await;
    check_engine(&srv, |name| {
        format!(
            "CREATE COLLECTION {name} \
             COLUMNS (id TEXT, ts BIGINT, f FLOAT) \
             WITH (engine='columnar')"
        )
    })
    .await;
}
