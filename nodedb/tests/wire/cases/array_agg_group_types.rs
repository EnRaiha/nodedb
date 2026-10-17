// SPDX-License-Identifier: BUSL-1.1

//! The `group` column of a grouped `ARRAY_AGG` takes its group dim's type.
//!
//! For each dim type, the aggregate announces the OID `ARRAY_SLICE`
//! announces for that dim, and each `group` value reads as the slice's
//! text for the same coordinate: a `TIMESTAMP_MS` dim reads as a
//! timestamp, never as raw milliseconds.

use std::collections::BTreeSet;

use crate::harness::TestServer;

const INT8: u32 = 20;
const TEXT: u32 = 25;
const FLOAT8: u32 = 701;
const TIMESTAMP: u32 = 1114;

/// One fixture: an array whose dim `g` is the group dim, and its cells.
struct Fixture {
    name: &'static str,
    /// The `g` dim declaration.
    dim: &'static str,
    /// The `g` tile extent.
    extent: &'static str,
    /// The `g` coordinate literal of each cell.
    coords: &'static [&'static str],
    /// The OID both reads announce for `g`.
    oid: u32,
}

async fn create(srv: &TestServer, f: &Fixture) {
    srv.exec(&format!(
        "CREATE ARRAY {} DIMS ({}, x INT64 [0..3]) ATTRS (v FLOAT64) TILE_EXTENTS ({}, 1)",
        f.name, f.dim, f.extent
    ))
    .await
    .unwrap_or_else(|e| panic!("CREATE ARRAY {}: {e}", f.name));
    let cells: Vec<String> = f
        .coords
        .iter()
        .enumerate()
        .map(|(i, g)| format!("COORDS ({g}, {}) VALUES ({}.0)", i % 4, i + 1))
        .collect();
    srv.exec(&format!(
        "INSERT INTO ARRAY {} {}",
        f.name,
        cells.join(", ")
    ))
    .await
    .unwrap_or_else(|e| panic!("INSERT INTO ARRAY {}: {e}", f.name));
}

/// The OID `sql` announces for `column`.
async fn described_oid(srv: &TestServer, sql: &str, column: &str) -> u32 {
    let stmt = srv
        .client
        .prepare(sql)
        .await
        .unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
    stmt.columns()
        .iter()
        .find(|c| c.name() == column)
        .unwrap_or_else(|| panic!("{sql}: no '{column}' column"))
        .type_()
        .oid()
}

/// The text of `column` in every row of `sql`.
async fn texts(srv: &TestServer, sql: &str, column: &str) -> BTreeSet<String> {
    srv.query_named_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|row| {
            row.get(column)
                .cloned()
                .unwrap_or_else(|| panic!("{sql}: no '{column}' column in {row:?}"))
        })
        .collect()
}

async fn check(f: Fixture) {
    let srv = TestServer::start().await;
    create(&srv, &f).await;
    let slice = format!("SELECT * FROM ARRAY_SLICE('{}', '{{x: [0, 3]}}')", f.name);
    let agg = format!("SELECT * FROM ARRAY_AGG('{}', 'v', 'sum', 'g')", f.name);

    assert_eq!(described_oid(&srv, &slice, "g").await, f.oid, "slice dim");
    assert_eq!(described_oid(&srv, &agg, "group").await, f.oid, "agg group");

    let slice_values = texts(&srv, &slice, "g").await;
    let groups = texts(&srv, &agg, "group").await;
    assert_eq!(
        groups, slice_values,
        "{}: each group reads as the slice reads its dim",
        f.name
    );
}

#[tokio::test]
async fn a_timestamp_group_reads_as_a_timestamp() {
    check(Fixture {
        name: "agg_ts",
        dim: "g TIMESTAMP_MS [1735689600000..1798761600000]",
        extent: "86400000",
        coords: &["1768435200000", "1768435200000", "1768521600000"],
        oid: TIMESTAMP,
    })
    .await;

    let srv = TestServer::start().await;
    create(
        &srv,
        &Fixture {
            name: "agg_ts_raw",
            dim: "g TIMESTAMP_MS [1735689600000..1798761600000]",
            extent: "86400000",
            coords: &["1768435200000"],
            oid: TIMESTAMP,
        },
    )
    .await;
    let groups = texts(
        &srv,
        "SELECT * FROM ARRAY_AGG('agg_ts_raw', 'v', 'sum', 'g')",
        "group",
    )
    .await;
    let group = groups.iter().next().expect("one group");
    assert!(
        group.starts_with("2026-01-15"),
        "a timestamp group reads as a timestamp, got {group:?}"
    );
}

#[tokio::test]
async fn a_float_group_reads_as_float8() {
    check(Fixture {
        name: "agg_f64",
        dim: "g FLOAT64 [99..105]",
        extent: "1",
        coords: &["102.5", "102.5", "103.25"],
        oid: FLOAT8,
    })
    .await;
}

#[tokio::test]
async fn an_int_group_reads_as_int8() {
    check(Fixture {
        name: "agg_i64",
        dim: "g INT64 [0..9]",
        extent: "1",
        coords: &["3", "3", "7"],
        oid: INT8,
    })
    .await;
}

#[tokio::test]
async fn a_string_group_reads_as_text() {
    check(Fixture {
        name: "agg_str",
        dim: "g STRING",
        extent: "1",
        coords: &["'chr1'", "'chr1'", "'chrX'"],
        oid: TEXT,
    })
    .await;
}

/// A temporal aggregate types `group` and keeps its summary row flag.
#[tokio::test]
async fn a_temporal_group_keeps_its_type_and_summary_flag() {
    let srv = TestServer::start().await;
    create(
        &srv,
        &Fixture {
            name: "agg_ts_asof",
            dim: "g TIMESTAMP_MS [1735689600000..1798761600000]",
            extent: "86400000",
            coords: &["1768435200000", "1768521600000"],
            oid: TIMESTAMP,
        },
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time after epoch")
        .as_millis() as i64;
    let sql =
        format!("SELECT * FROM ARRAY_AGG('agg_ts_asof', 'v', 'sum', 'g') AS OF SYSTEM TIME {now}");
    assert_eq!(described_oid(&srv, &sql, "group").await, TIMESTAMP);
    let rows = srv
        .query_named_rows(&sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let flags: Vec<&String> = rows
        .iter()
        .filter_map(|row| row.get("truncated_before_horizon"))
        .filter(|flag| !flag.is_empty())
        .collect();
    assert_eq!(flags.len(), 1, "one summary row: {rows:?}");
    let groups: Vec<&String> = rows
        .iter()
        .filter_map(|row| row.get("group"))
        .filter(|g| !g.is_empty())
        .collect();
    assert_eq!(groups.len(), 2, "two groups: {rows:?}");
    assert!(
        groups.iter().all(|g| g.starts_with("2026-01-1")),
        "timestamp groups: {groups:?}"
    );
}
