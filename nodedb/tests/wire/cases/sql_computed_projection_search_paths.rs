// SPDX-License-Identifier: BUSL-1.1

//! Computed SELECT-list columns over the access paths that evaluate no
//! expression: a spatial predicate scan and a secondary-index fetch.
//!
//! Each path returns stored rows. The computed columns, a sort on a computed
//! alias, DISTINCT, and LIMIT apply to those rows as they do on a full scan.

use crate::harness::TestServer;

/// Three towns, at 0 km, about 73 km, and about 109 km from Temerloh.
async fn seed_towns(srv: &TestServer, collection: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {collection} (location GEOMETRY SPATIAL_INDEX, name VARCHAR) \
         WITH (engine='spatial')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (name, lon, lat) in [
        ("Temerloh", 102.42, 3.45),
        ("Kuantan", 103.33, 3.82),
        ("Raub", 101.86, 3.79),
    ] {
        srv.exec(&format!(
            "INSERT INTO {collection} (name, location) VALUES ('{name}', ST_Point({lon}, {lat}))"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {name}: {e}"));
    }
}

fn number(cell: &str) -> f64 {
    cell.parse()
        .unwrap_or_else(|e| panic!("cell '{cell}' is not a number: {e}"))
}

#[tokio::test]
async fn spatial_predicate_scan_orders_by_a_computed_alias() {
    let srv = TestServer::start().await;
    seed_towns(&srv, "swhere").await;

    let rows = srv
        .query_rows(
            "SELECT name, ROUND(ST_Distance(location, ST_Point(102.42, 3.45)) / 1000) AS km \
             FROM swhere WHERE ST_DWithin(location, ST_Point(102.42, 3.45), 150000) \
             ORDER BY km",
        )
        .await
        .unwrap_or_else(|e| panic!("spatial scan with a computed alias: {e}"));
    let names: Vec<&str> = rows.iter().map(|row| row[0].as_str()).collect();
    assert_eq!(
        names,
        ["Temerloh", "Raub", "Kuantan"],
        "rows sort by the computed distance: {rows:?}"
    );
    assert_eq!(number(&rows[0][1]), 0.0, "Temerloh is the centre: {rows:?}");
    let raub = number(&rows[1][1]);
    let kuantan = number(&rows[2][1]);
    assert!((60.0..90.0).contains(&raub), "Raub distance: {rows:?}");
    assert!(
        (95.0..125.0).contains(&kuantan),
        "Kuantan distance: {rows:?}"
    );
}

#[tokio::test]
async fn spatial_predicate_scan_evaluates_unaliased_computed_columns() {
    let srv = TestServer::start().await;
    seed_towns(&srv, "swhere_plain").await;

    let rows = srv
        .query_rows(
            "SELECT name, ST_AsText(location), upper(name) FROM swhere_plain \
             WHERE ST_DWithin(location, ST_Point(102.42, 3.45), 1000)",
        )
        .await
        .unwrap_or_else(|e| panic!("spatial scan with computed columns: {e}"));
    assert_eq!(
        rows,
        vec![vec![
            "Temerloh".to_string(),
            "POINT(102.42 3.45)".to_string(),
            "TEMERLOH".to_string(),
        ]],
        "the matched row carries every computed column"
    );

    let rows = srv
        .query_rows(
            "SELECT name AS town FROM swhere_plain \
             WHERE ST_DWithin(location, ST_Point(102.42, 3.45), 150000) ORDER BY town LIMIT 2",
        )
        .await
        .unwrap_or_else(|e| panic!("spatial scan with a renamed column: {e}"));
    assert_eq!(
        rows,
        vec![vec!["Kuantan".to_string()], vec!["Raub".to_string()]],
        "a renamed column sorts and limits like a stored one"
    );
}

/// Create `collection` with an index on `city`, then three rows.
async fn seed_indexed(srv: &TestServer, collection: &str, ddl: &str) {
    srv.exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    srv.exec(&format!("CREATE INDEX ON {collection} (city)"))
        .await
        .unwrap_or_else(|e| panic!("index {collection}: {e}"));
    for (id, name, city, qty) in [
        ("a", "ali", "temerloh", 7),
        ("b", "abu", "temerloh", 3),
        ("c", "siti", "raub", 9),
    ] {
        srv.exec(&format!(
            "INSERT INTO {collection} (id, name, city, qty) \
             VALUES ('{id}', '{name}', '{city}', {qty})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {id} into {collection}: {e}"));
    }
}

/// Computed columns, a computed sort, DISTINCT, and LIMIT over an
/// equality on the indexed `city`.
async fn assert_indexed_fetch_computes(srv: &TestServer, collection: &str) {
    let rows = srv
        .query_rows(&format!(
            "SELECT name, upper(city), qty * 2, CASE WHEN qty > 5 THEN 'big' ELSE 'small' END \
             FROM {collection} WHERE city = 'temerloh' ORDER BY name"
        ))
        .await
        .unwrap_or_else(|e| panic!("{collection}: indexed fetch with computed columns: {e}"));
    assert_eq!(
        rows,
        vec![
            vec!["abu", "TEMERLOH", "6", "small"],
            vec!["ali", "TEMERLOH", "14", "big"],
        ]
        .into_iter()
        .map(|row| row.into_iter().map(str::to_string).collect::<Vec<_>>())
        .collect::<Vec<_>>(),
        "{collection}: each fetched row carries its computed columns"
    );

    let rows = srv
        .query_rows(&format!(
            "SELECT DISTINCT upper(city) FROM {collection} WHERE city = 'temerloh'"
        ))
        .await
        .unwrap_or_else(|e| panic!("{collection}: DISTINCT over a computed column: {e}"));
    assert_eq!(
        rows,
        vec![vec!["TEMERLOH".to_string()]],
        "{collection}: DISTINCT compares the computed values"
    );

    let rows = srv
        .query_rows(&format!(
            "SELECT name, qty * 10 AS big_qty FROM {collection} \
             WHERE city = 'temerloh' ORDER BY big_qty DESC LIMIT 1"
        ))
        .await
        .unwrap_or_else(|e| panic!("{collection}: sort on a computed alias: {e}"));
    assert_eq!(
        rows,
        vec![vec!["ali".to_string(), "70".to_string()]],
        "{collection}: the sort reads the computed alias, then LIMIT cuts"
    );
}

#[tokio::test]
async fn schemaless_indexed_fetch_evaluates_computed_columns() {
    let srv = TestServer::start().await;
    seed_indexed(&srv, "ixdoc", "CREATE COLLECTION ixdoc").await;
    assert_indexed_fetch_computes(&srv, "ixdoc").await;
}

#[tokio::test]
async fn strict_indexed_fetch_evaluates_computed_columns() {
    let srv = TestServer::start().await;
    seed_indexed(
        &srv,
        "ixstrict",
        "CREATE COLLECTION ixstrict (id TEXT PRIMARY KEY, name TEXT, city TEXT, qty INT) \
         WITH (engine='document_strict')",
    )
    .await;
    assert_indexed_fetch_computes(&srv, "ixstrict").await;
}
