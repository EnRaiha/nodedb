// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the key-value engine.
//!
//! A KV row is `{key, value…}`, and the row a write hands back is built by the
//! same helper the KV scan paths use — so `RETURNING *` and `SELECT *` on the
//! same key agree, field for field. What comes back is the STORED post-image:
//! the merged row after an `ON CONFLICT DO UPDATE`, never the values the caller
//! submitted.

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

async fn create_kv(server: &TestServer, name: &str, columns: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} ({columns}) WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
}

/// `RETURNING *` on a multi-column KV row returns the stored row, `key`
/// included — the same shape a `SELECT` on that key produces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_star_on_a_multi_column_row() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_ret_star",
        "key TEXT PRIMARY KEY, n INT, note TEXT",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_ret_star (key, n, note) VALUES ('k1', 7, 'hello') RETURNING *",
        )
        .await
        .expect("KV INSERT RETURNING * must return the inserted row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("key").map(String::as_str), Some("k1"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("7"));
    assert_eq!(returned[0].get("note").map(String::as_str), Some("hello"));

    // The projection agrees with a read of the same key.
    assert_eq!(
        rows(&server, "SELECT key, n, note FROM kv_ret_star").await,
        vec!["k1|7|hello".to_string()],
        "RETURNING must report the row a SELECT sees"
    );
}

/// Named columns and aliases project exactly what was asked for.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_named_columns() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_ret_named",
        "key TEXT PRIMARY KEY, n INT, note TEXT",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_ret_named (key, n, note) VALUES ('k1', 7, 'hello') \
             RETURNING n AS count, key",
        )
        .await
        .expect("KV INSERT RETURNING named columns must return the inserted row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("count").map(String::as_str), Some("7"));
    assert_eq!(returned[0].get("key").map(String::as_str), Some("k1"));
    assert!(
        !returned[0].contains_key("note"),
        "a column outside the projection must not be shipped: {returned:?}"
    );
}

/// A single-column KV collection stores one opaque scalar with no field map.
/// `RETURNING *` must still answer coherently, and with the same `{key, value}`
/// shape a `SELECT` on that collection produces — inventing a second shape here
/// would make the two disagree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_star_on_an_opaque_scalar_row() {
    let server = TestServer::start().await;
    create_kv(&server, "kv_ret_opaque", "key TEXT PRIMARY KEY, value TEXT").await;

    let returned = server
        .query_named_rows("INSERT INTO kv_ret_opaque (key, value) VALUES ('k1', 'v1') RETURNING *")
        .await
        .expect("KV INSERT RETURNING * must answer on an opaque-scalar row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("key").map(String::as_str), Some("k1"));
    assert_eq!(returned[0].get("value").map(String::as_str), Some("v1"));

    let read_back = server
        .query_named_rows("SELECT key, value FROM kv_ret_opaque")
        .await
        .expect("read back");
    assert_eq!(
        read_back[0].get("value").map(String::as_str),
        returned[0].get("value").map(String::as_str),
        "RETURNING and SELECT must agree on the opaque value"
    );
}

/// Whatever the write path materializes, `RETURNING` reports it — the returned
/// row is compared against a `SELECT` of the same key rather than against a
/// hand-written expectation.
///
/// This is the increment's design rule stated as a test: a row handed back by a
/// write must equal the row a read of that key produces. Asserting a specific
/// value instead would pin one path's behaviour and let the other drift, which
/// is exactly how the two silently disagreed on the opaque-scalar form.
///
/// `status` is deliberately omitted from the statement and carries a DEFAULT:
/// it is materialized at write time, so both sides read it back from the same
/// stored bytes. If a future change made either side synthesize the default on
/// read instead, only one would, and this test would catch the split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_returning_agrees_with_a_select_of_the_same_key() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_ret_agree",
        "key TEXT PRIMARY KEY, n INT, status TEXT DEFAULT 'pending'",
    )
    .await;

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_ret_agree (key, n) VALUES ('k1', 1) RETURNING key, n, status",
        )
        .await
        .expect("KV INSERT RETURNING must return the stored row");
    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");

    let selected = server
        .query_named_rows("SELECT key, n, status FROM kv_ret_agree WHERE key = 'k1'")
        .await
        .expect("read the same key back");
    assert_eq!(selected.len(), 1, "one stored row: {selected:?}");

    for column in ["key", "n", "status"] {
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
    assert_eq!(returned[0].get("key").map(String::as_str), Some("k1"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("1"));
    assert_eq!(
        returned[0].get("status").map(String::as_str),
        Some("pending")
    );
    assert_eq!(
        selected[0].get("status").map(String::as_str),
        Some("pending")
    );
}

/// A multi-row KV insert returns one row per entry, in order, in ONE result
/// set — the tasks fold together exactly as the document engine's do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_multi_row_insert_returns_one_result_set_in_order() {
    let server = TestServer::start().await;
    create_kv(&server, "kv_ret_multi", "key TEXT PRIMARY KEY, n INT").await;

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO kv_ret_multi (key, n) VALUES ('r1', 1), ('r2', 2), ('r3', 3) \
             RETURNING key, n",
        )
        .await
        .expect("multi-row KV INSERT RETURNING must succeed");

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
        rows(&server, "SELECT key FROM kv_ret_multi ORDER BY key").await,
        vec!["r1".to_string(), "r2".to_string(), "r3".to_string()],
        "every entry must also have landed"
    );
}

/// `ON CONFLICT DO UPDATE ... RETURNING` returns the MERGED row: the caller's
/// submitted values are only part of what the row ends up holding, so echoing
/// them would report a row that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_on_conflict_do_update_returning_shows_the_merged_post_image() {
    let server = TestServer::start().await;
    create_kv(
        &server,
        "kv_ret_conflict",
        "key TEXT PRIMARY KEY, n INT, note TEXT",
    )
    .await;
    server
        .exec("INSERT INTO kv_ret_conflict (key, n, note) VALUES ('c1', 10, 'original')")
        .await
        .expect("seed");

    let returned = server
        .query_named_rows(
            "INSERT INTO kv_ret_conflict (key, n) VALUES ('c1', 99) \
             ON CONFLICT (key) DO UPDATE SET n = n + 1 RETURNING key, n, note",
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
async fn kv_on_conflict_do_nothing_returning_ships_no_row() {
    let server = TestServer::start().await;
    create_kv(&server, "kv_ret_nothing", "key TEXT PRIMARY KEY, n INT").await;
    server
        .exec("INSERT INTO kv_ret_nothing (key, n) VALUES ('c1', 1)")
        .await
        .expect("seed");

    let returned = rows(
        &server,
        "INSERT INTO kv_ret_nothing (key, n) VALUES ('c1', 2) \
         ON CONFLICT (key) DO NOTHING RETURNING key, n",
    )
    .await;
    assert!(
        returned.is_empty(),
        "nothing was written, so nothing may be returned: {returned:?}"
    );

    assert_eq!(
        rows(&server, "SELECT key, n FROM kv_ret_nothing").await,
        vec!["c1|1".to_string()],
        "the conflicting write must have been skipped, not applied"
    );
}

/// The engines that cannot yet carry the clause still refuse it, by name —
/// taken from the one shared list rather than naming an engine here, because a
/// hard-coded engine name in this file is exactly what went stale when columnar
/// gained support.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_an_unsupported_engine_is_still_refused() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_refused_engines_still_refuse(&server, "kv_ret_refused").await;
}

/// The columnar engine DOES carry the clause now, so the statement this file
/// once used as its refusal example returns its stored row instead.
///
/// Kept deliberately beside the refusal above, in the same shape the key-value
/// pair has in the document file: the two claims are about the same statement
/// on different engines, and reading them together is what stops one from
/// quietly contradicting the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_a_columnar_collection_returns_its_row() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION kv_ret_col (id TEXT PRIMARY KEY, v FLOAT) WITH (engine='columnar')",
        )
        .await
        .expect("create columnar collection");

    assert_eq!(
        rows(
            &server,
            "INSERT INTO kv_ret_col (id, v) VALUES ('c1', 1.5) RETURNING id, v"
        )
        .await,
        vec!["c1|1.5".to_string()],
    );
}
