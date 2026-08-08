// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the columnar engine and its spatial peer.
//!
//! A columnar row is `Vec<Value>` in schema order rather than a stored document
//! body, so the row a write hands back is assembled by the same builder the
//! columnar WHERE evaluator and the row-level-security write gate use. What
//! comes back is the STORED post-image: the merged row after an `ON CONFLICT DO
//! UPDATE`, never the values the caller submitted.
//!
//! Spatial is not a separate write path — an `engine='spatial'` insert compiles
//! to the same `ColumnarOp::Insert` — so it is covered here rather than in a
//! file of its own, and tested to keep that shared routing honest.

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

/// Rows of `sql`, each row's columns joined by `|`.
async fn rows(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect()
}

async fn create_columnar(server: &TestServer, name: &str, columns: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} ({columns}) WITH (engine='columnar')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// `RETURNING *` on a columnar row returns the stored row — every declared
/// column, including the ones the statement never mentioned.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_returning_star_returns_the_stored_row() {
    let server = TestServer::start().await;
    create_columnar(
        &server,
        "col_ret_star",
        "id TEXT PRIMARY KEY, n INT, note TEXT",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO col_ret_star (id, n, note) VALUES ('c1', 7, 'hello') RETURNING *",
        )
        .await
        .expect("columnar INSERT RETURNING must return the stored row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("c1"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("7"));
    assert_eq!(returned[0].get("note").map(String::as_str), Some("hello"));
}

/// Named columns and aliases project exactly what was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_returning_named_columns() {
    let server = TestServer::start().await;
    create_columnar(&server, "col_ret_named", "id TEXT PRIMARY KEY, n INT").await;

    let returned = server
        .query_named_rows(
            "INSERT INTO col_ret_named (id, n) VALUES ('c1', 42) RETURNING n AS count, id",
        )
        .await
        .expect("named RETURNING must succeed");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(
        returned[0].get("count").map(String::as_str),
        Some("42"),
        "the alias must name the column: {returned:?}"
    );
    assert_eq!(returned[0].get("id").map(String::as_str), Some("c1"));
    assert!(
        !returned[0].contains_key("n"),
        "an aliased column must not also appear under its source name: {returned:?}"
    );
}

/// The spatial engine compiles to the same insert op, so the clause rides the
/// same path. If spatial ever grew its own write op, this is the test that
/// notices before a user does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_insert_returning_returns_the_stored_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION sp_ret (id TEXT PRIMARY KEY, location GEOMETRY, name TEXT) \
             WITH (engine='spatial')",
        )
        .await
        .expect("create spatial collection");

    let returned = server
        .query_named_rows(
            "INSERT INTO sp_ret (id, location, name) VALUES \
             ('s1', '{\"type\":\"Point\",\"coordinates\":[-122.4,37.8]}', 'SF') \
             RETURNING id, name",
        )
        .await
        .expect("spatial INSERT RETURNING must return the stored row");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("s1"));
    assert_eq!(returned[0].get("name").map(String::as_str), Some("SF"));
}

/// A multi-row columnar insert returns one row per entry, in order, in ONE
/// result set.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_multi_row_insert_returns_one_result_set_in_order() {
    let server = TestServer::start().await;
    create_columnar(&server, "col_ret_multi", "id TEXT PRIMARY KEY, n INT").await;

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO col_ret_multi (id, n) VALUES ('r1', 1), ('r2', 2), ('r3', 3) \
             RETURNING id, n",
        )
        .await
        .expect("multi-row columnar INSERT RETURNING must succeed");

    assert_eq!(
        result_set_count(&msgs),
        1,
        "one statement is one result set, however many tasks it planned to"
    );

    let returned: Vec<String> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_string())
                    .collect::<Vec<_>>()
                    .join("|"),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(
        returned,
        vec!["r1|1".to_string(), "r2|2".to_string(), "r3|3".to_string()],
        "one row per entry, in insert order"
    );

    assert_eq!(
        rows(&server, "SELECT id FROM col_ret_multi ORDER BY id").await,
        vec!["r1".to_string(), "r2".to_string(), "r3".to_string()],
        "every entry must also have landed"
    );
}

/// `ON CONFLICT DO UPDATE ... RETURNING` returns the MERGED row. The merge is
/// resolved in the Data Plane against the stored row, so echoing the submitted
/// values would report a row that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_on_conflict_do_update_returning_shows_the_merged_post_image() {
    let server = TestServer::start().await;
    create_columnar(
        &server,
        "col_ret_conflict",
        "id TEXT PRIMARY KEY, n INT, note TEXT",
    )
    .await;
    server
        .exec("INSERT INTO col_ret_conflict (id, n, note) VALUES ('c1', 10, 'original')")
        .await
        .expect("seed");

    let returned = server
        .query_named_rows(
            "INSERT INTO col_ret_conflict (id, n) VALUES ('c1', 99) \
             ON CONFLICT (id) DO UPDATE SET n = 11 RETURNING id, n, note",
        )
        .await
        .expect("ON CONFLICT DO UPDATE RETURNING must return the merged row");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(
        returned[0].get("n").map(String::as_str),
        Some("11"),
        "the conflict branch's assignment decides the value, not the submitted 99: {returned:?}"
    );
    assert_eq!(
        returned[0].get("note").map(String::as_str),
        Some("original"),
        "a column the statement never mentioned must keep its stored value: {returned:?}"
    );
}

/// `ON CONFLICT DO NOTHING` that hits a conflict writes nothing, so its
/// `RETURNING` ships an EMPTY row set rather than a row that was never stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_on_conflict_do_nothing_returning_ships_no_row() {
    let server = TestServer::start().await;
    create_columnar(&server, "col_ret_nothing", "id TEXT PRIMARY KEY, n INT").await;
    server
        .exec("INSERT INTO col_ret_nothing (id, n) VALUES ('c1', 1)")
        .await
        .expect("seed");

    let returned = rows(
        &server,
        "INSERT INTO col_ret_nothing (id, n) VALUES ('c1', 2) \
         ON CONFLICT DO NOTHING RETURNING id, n",
    )
    .await;
    assert!(
        returned.is_empty(),
        "nothing was written, so nothing may be returned: {returned:?}"
    );

    assert_eq!(
        rows(&server, "SELECT id, n FROM col_ret_nothing").await,
        vec!["c1|1".to_string()],
        "the conflicting write must have been skipped, not applied"
    );
}

/// Whatever the write path materializes, `RETURNING` reports it — the returned
/// row is compared against a `SELECT` of the same key rather than against a
/// hand-written expectation.
///
/// This is the increment's design rule stated as a test: a row handed back by a
/// write must equal the row a read of that key produces. Asserting specific
/// values instead would pin one path's behaviour and let the other drift, which
/// is how two row shapers silently disagreed on other engines.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_insert_returning_agrees_with_a_select_of_the_same_key() {
    let server = TestServer::start().await;
    create_columnar(
        &server,
        "col_ret_agree",
        "id TEXT PRIMARY KEY, n INT, status TEXT DEFAULT 'pending'",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO col_ret_agree (id, n) VALUES ('c1', 1) RETURNING id, n, status",
        )
        .await
        .expect("columnar INSERT RETURNING must return the stored row");
    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");

    let selected = server
        .query_named_rows("SELECT id, n, status FROM col_ret_agree WHERE id = 'c1'")
        .await
        .expect("read the same key back");
    assert_eq!(selected.len(), 1, "one stored row: {selected:?}");

    for column in ["id", "n", "status"] {
        assert_eq!(
            returned[0].get(column),
            selected[0].get(column),
            "RETURNING and SELECT must agree on {column}: \
             returned={returned:?} selected={selected:?}"
        );
    }
    // Every column is non-empty on both sides, so the agreement above is not
    // two empty rows agreeing with each other — including `status`, which the
    // statement omitted and the DEFAULT filled in.
    assert_eq!(returned[0].get("id").map(String::as_str), Some("c1"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("1"));
    assert_eq!(
        returned[0].get("status").map(String::as_str),
        Some("pending"),
        "a value the engine materializes must appear in the returned row: {returned:?}"
    );
}

/// The engines that cannot yet carry the clause still refuse it, by name —
/// timeseries and vector-primary today, read from the one shared list so that
/// giving either of them the slot updates this assertion automatically instead
/// of leaving a claim here that is no longer true.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_an_unsupported_engine_is_still_refused() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_refused_engines_still_refuse(&server, "col_ret_refused").await;
}
