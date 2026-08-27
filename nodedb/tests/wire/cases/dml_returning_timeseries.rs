// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the timeseries engine.
//!
//! A stored timeseries point is `Vec<ColumnValue>` in schema order, built after
//! the time key is normalized, tags and fields are split, and the schema is
//! resolved — every ingest format is rewritten into line protocol before a
//! point exists, so the row handed back is read from storage rather than echoed
//! from the request.
//!
//! The read-back goes through the SAME projection a `SELECT` scan uses. That is
//! load-bearing rather than tidy: a float field the row omitted is stored as
//! `NaN` and rendered as SQL NULL on read, so a projection written over the
//! ingest-side values would have returned the text "NaN" where `SELECT` returns
//! empty.

mod common;

use common::insert_returning_engines;
use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Number of RESULT SETS in a simple-query response: one `CommandComplete` per
/// result set, which is what a driver counts for the statement.
fn result_set_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
        .count()
}

/// Every row in `collection` with its FULL column set, rendered as sorted
/// `name=value` pairs.
///
/// Captured into the agreement assertions rather than kept as a throwaway
/// probe. When two read paths disagree, the projected columns alone cannot
/// distinguish the two failures that look identical from outside:
///
/// - the column resolved under the name asked for and its VALUE is NULL, or
/// - the stored row calls that column something else entirely, so the
///   projection found nothing to render.
///
/// The second is what a schema inferred rather than declared produces — the
/// inferrer names the time column `timestamp`, so a collection declaring
/// `ts TIMESTAMP TIME_KEY` reads back empty under `ts` while the value sits
/// intact under another name. Only the full column set tells those apart, and
/// this family of bug has now cost two debugging rounds without it.
async fn full_rows(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_named_rows(&format!("SELECT * FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("SELECT * FROM {collection}: {e}"))
        .into_iter()
        .map(|row| {
            let mut cells: Vec<String> = row.iter().map(|(k, v)| format!("{k}={v}")).collect();
            cells.sort();
            format!("{{{}}}", cells.join(", "))
        })
        .collect()
}

async fn create_ts(server: &TestServer, name: &str, columns: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} ({columns}) WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// `RETURNING *` returns the stored point, with the time key under its declared
/// name and normalized to the value the engine actually stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_ingest_returning_star_returns_the_stored_point() {
    let server = TestServer::start().await;
    create_ts(
        &server,
        "ts_ret_star",
        "ts TIMESTAMP TIME_KEY, host TEXT, v FLOAT",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO ts_ret_star (ts, host, v) VALUES (1000, 'h1', 2.5) RETURNING *",
        )
        .await
        .expect("timeseries INSERT RETURNING must return the stored point");

    assert_eq!(returned.len(), 1, "one ingested point: {returned:?}");
    assert_eq!(returned[0].get("host").map(String::as_str), Some("h1"));
    assert_eq!(returned[0].get("v").map(String::as_str), Some("2.5"));
    assert!(
        returned[0].contains_key("ts"),
        "the declared TIME_KEY must appear under its own name: {returned:?}"
    );
}

/// Named columns and aliases project exactly what was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_ingest_returning_named_columns() {
    let server = TestServer::start().await;
    create_ts(&server, "ts_ret_named", "ts TIMESTAMP TIME_KEY, v FLOAT").await;

    let returned = server
        .query_named_rows(
            "INSERT INTO ts_ret_named (ts, v) VALUES (1000, 7.5) RETURNING v AS reading",
        )
        .await
        .expect("named RETURNING must succeed");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(
        returned[0].get("reading").map(String::as_str),
        Some("7.5"),
        "the alias must name the column: {returned:?}"
    );
    assert!(
        !returned[0].contains_key("v"),
        "an aliased column must not also appear under its source name: {returned:?}"
    );
}

/// An ingest is batch-shaped — one payload, many points — so a multi-row insert
/// returns one row per point, in submission order, in ONE result set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_multi_row_ingest_returns_one_result_set_in_order() {
    let server = TestServer::start().await;
    create_ts(&server, "ts_ret_multi", "ts TIMESTAMP TIME_KEY, v FLOAT").await;

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO ts_ret_multi (ts, v) VALUES (1000, 1.0), (2000, 2.0), (3000, 3.0) \
             RETURNING v",
        )
        .await
        .expect("multi-row timeseries ingest with RETURNING must succeed");

    assert_eq!(
        result_set_count(&msgs),
        1,
        "one statement is one result set, however many points it ingested"
    );

    let returned: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(row.get(0).unwrap_or("").to_string()),
            _ => None,
        })
        .collect();

    // Compared against a `SELECT` of the same column rather than against a
    // hand-written literal. A literal here encodes one path's rendering as if
    // it were the contract, and whichever path disagreed would look wrong even
    // when it was right. Comparing the two makes a genuine divergence — a
    // whole-number float rendering as "1" on one side and "1.0" on the other —
    // fail loudly instead of being absorbed by an expectation chosen to match.
    let selected: Vec<String> = server
        .query_rows("SELECT v FROM ts_ret_multi ORDER BY v")
        .await
        .expect("read the same points back")
        .into_iter()
        .map(|r| r.join("|"))
        .collect();
    assert_eq!(
        returned, selected,
        "RETURNING and SELECT must render the same stored values: \
         returned={returned:?} selected={selected:?}"
    );
    assert_eq!(
        returned.len(),
        3,
        "one row per point, in submission order: {returned:?}"
    );
}

/// A float field the statement omitted is stored as `NaN` and read back as SQL
/// NULL. Both sides must agree on that, which is the rule a hand-written
/// projection over the ingest values would have gotten wrong by printing "NaN".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_omitted_float_field_is_null_on_both_sides() {
    let server = TestServer::start().await;
    create_ts(
        &server,
        "ts_ret_nan",
        "ts TIMESTAMP TIME_KEY, v FLOAT, other FLOAT",
    )
    .await;

    let returned = server
        .query_named_rows("INSERT INTO ts_ret_nan (ts, v) VALUES (1000, 1.5) RETURNING v, other")
        .await
        .expect("ingest with an omitted column must succeed");
    assert_eq!(returned.len(), 1, "one point: {returned:?}");
    assert_eq!(
        returned[0].get("other").map(String::as_str).unwrap_or(""),
        "",
        "an omitted float must come back as NULL, never as the text NaN: {returned:?}"
    );

    let selected = server
        .query_named_rows("SELECT v, other FROM ts_ret_nan")
        .await
        .expect("read back");
    assert_eq!(
        selected[0].get("other").map(String::as_str).unwrap_or(""),
        "",
        "SELECT must render the same stored NaN as NULL: {selected:?}"
    );
}

/// Whatever the ingest path materializes, `RETURNING` reports it — the returned
/// row is compared against a `SELECT`, before AND after a restart.
///
/// The restart matters: it moves the read from the live memtable to the durable
/// path, and both must render the same stored point. Comparing only within one
/// process would let the two projections drift exactly where a user would
/// notice it least.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_ingest_returning_agrees_with_select_across_a_restart() {
    let server = TestServer::start().await;
    create_ts(
        &server,
        "ts_ret_agree",
        "ts TIMESTAMP TIME_KEY, host TEXT, v FLOAT",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO ts_ret_agree (ts, host, v) VALUES (1000, 'h1', 4.5) \
             RETURNING ts, host, v",
        )
        .await
        .expect("timeseries INSERT RETURNING must return the stored point");
    assert_eq!(returned.len(), 1, "one ingested point: {returned:?}");

    let selected = server
        .query_named_rows("SELECT ts, host, v FROM ts_ret_agree")
        .await
        .expect("read the point back");
    assert_eq!(selected.len(), 1, "one stored point: {selected:?}");

    // The stored row's own column set, before the restart. Carried into every
    // message below so a failure shows what the row IS, not only what the
    // projection managed to pull out of it.
    let shape_before = full_rows(&server, "ts_ret_agree").await;

    for column in ["ts", "host", "v"] {
        assert_eq!(
            returned[0].get(column),
            selected[0].get(column),
            "RETURNING and SELECT must agree on {column}: \
             returned={returned:?} selected={selected:?}\n\
             stored column set before restart: {shape_before:?}"
        );
    }
    // Non-empty on both sides, so the agreement above is not two empty rows
    // agreeing with each other.
    assert_eq!(returned[0].get("host").map(String::as_str), Some("h1"));
    assert_eq!(returned[0].get("v").map(String::as_str), Some("4.5"));

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    let after = server
        .query_named_rows("SELECT ts, host, v FROM ts_ret_agree")
        .await
        .expect("read the point back after restart");
    let shape_after = full_rows(&server, "ts_ret_agree").await;
    assert_eq!(
        after.len(),
        1,
        "the point must have survived: {after:?}\n\
         stored column set after restart: {shape_after:?}"
    );
    for column in ["ts", "host", "v"] {
        assert_eq!(
            returned[0].get(column),
            after[0].get(column),
            "the row a write handed back must survive a restart unchanged on {column}: \
             returned={returned:?} after={after:?}\n\
             stored column set BEFORE restart: {shape_before:?}\n\
             stored column set AFTER  restart: {shape_after:?}"
        );
    }
    // The restart must not change the row's SHAPE either. A column set that
    // differs across the boundary means the collection was rebuilt from an
    // inferred schema rather than its declared one, which is a distinct defect
    // from a value being lost — and it would otherwise only ever surface as a
    // confusing NULL in the per-column assertions above.
    assert_eq!(
        shape_before, shape_after,
        "the stored row's column set must survive a restart unchanged"
    );
}

/// Every engine the shared list calls refused still refuses, and every engine
/// it calls supported still hands back its stored row. Timeseries moved from
/// the first list to the second in this change, so both halves assert it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_refused_and_supported_engine_lists_both_still_hold() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_refused_engines_still_refuse(&server, "ts_ret_refused").await;
    insert_returning_engines::assert_supported_engines_return_their_row(&server, "ts_ret_ok").await;
}
