// SPDX-License-Identifier: BUSL-1.1

//! `INSERT ... RETURNING` on the document engines.
//!
//! The clause returns the STORED post-image — the row as it landed, with
//! generated columns evaluated, defaults applied and an `ON CONFLICT` merge
//! resolved — never the values the caller submitted. That distinction is the
//! whole point: an echo of the request would report what was asked for rather
//! than what exists, and it would bypass the read gate every other `RETURNING`
//! passes through.

mod common;

use common::insert_returning_engines;
use common::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Number of RESULT SETS in a simple-query response: PostgreSQL emits exactly
/// one `CommandComplete` per result set, so counting them counts the results a
/// driver would see for the statement.
fn result_set_count(msgs: &[SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, SimpleQueryMessage::CommandComplete(_)))
        .count()
}

/// Rows of `sql`, each row's columns joined by `|`, run as the superuser.
async fn rows(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect()
}

/// `RETURNING *` on a schemaless collection returns the row that landed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_star_on_a_schemaless_collection() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_less (id TEXT PRIMARY KEY, name TEXT)")
        .await
        .expect("create collection");

    let returned = server
        .query_named_rows("INSERT INTO ins_ret_less (id, name) VALUES ('a', 'alpha') RETURNING *")
        .await
        .expect("INSERT RETURNING * must return the inserted row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("a"));
    assert_eq!(returned[0].get("name").map(String::as_str), Some("alpha"));

    assert_eq!(
        rows(&server, "SELECT id, name FROM ins_ret_less").await,
        vec!["a|alpha".to_string()],
        "the write must still land"
    );
}

/// Named columns and aliases project exactly what was asked for, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_named_columns_on_a_strict_collection() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ins_ret_named (id TEXT PRIMARY KEY, name TEXT, note TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create collection");

    let returned = server
        .query_named_rows(
            "INSERT INTO ins_ret_named (id, name, note) VALUES ('s1', 'alpha', 'n') \
             RETURNING name AS label, id",
        )
        .await
        .expect("INSERT RETURNING named columns must return the inserted row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("label").map(String::as_str), Some("alpha"));
    assert_eq!(returned[0].get("id").map(String::as_str), Some("s1"));
    assert!(
        !returned[0].contains_key("note"),
        "a column outside the projection must not be shipped: {returned:?}"
    );
}

/// `RETURNING *` on a strict collection decodes the Binary Tuple the row was
/// stored as. Decoding it as MessagePack would succeed and yield a document
/// with none of the row's real columns.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_star_on_a_strict_collection() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ins_ret_strict (id TEXT PRIMARY KEY, name TEXT, qty INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create collection");

    let returned = server
        .query_named_rows(
            "INSERT INTO ins_ret_strict (id, name, qty) VALUES ('s1', 'alpha', 7) RETURNING *",
        )
        .await
        .expect("INSERT RETURNING * must return the inserted row");

    assert_eq!(returned.len(), 1, "one inserted row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("s1"));
    assert_eq!(returned[0].get("name").map(String::as_str), Some("alpha"));
    assert_eq!(returned[0].get("qty").map(String::as_str), Some("7"));
}

/// A column the statement never mentioned still comes back with its stored
/// value: the row is read from storage, not reconstructed from the request.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_shows_a_default_the_statement_never_supplied() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ins_ret_default (\
                 id TEXT PRIMARY KEY, name TEXT, status TEXT DEFAULT 'pending') \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create collection");

    let returned = server
        .query_named_rows(
            "INSERT INTO ins_ret_default (id, name) VALUES ('d1', 'alpha') RETURNING id, status",
        )
        .await
        .expect("INSERT RETURNING must return the stored row");

    assert_eq!(
        returned[0].get("status").map(String::as_str),
        Some("pending"),
        "the default the storage layer applied must appear: {returned:?}"
    );
}

/// A generated column is computed on the way to disk, so it can only appear in
/// the returned row if that row is the stored image.
///
/// Declared `document_strict` because that is the only engine that carries
/// generated columns: the expression is lifted into a `StrictSchema` at CREATE
/// time and reaches the write path from there. A schemaless collection accepts
/// the same DDL text and stores no expression, so the column would simply be
/// absent — and the projection would faithfully report the absence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_shows_a_generated_column() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION ins_ret_gen (\
                 id TEXT PRIMARY KEY, price FLOAT, tax_rate FLOAT, \
                 total FLOAT GENERATED ALWAYS AS (price * (1 + tax_rate))) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create collection");

    let returned = server
        .query_named_rows(
            "INSERT INTO ins_ret_gen (id, price, tax_rate) VALUES ('g1', 100.0, 0.1) \
             RETURNING id, total",
        )
        .await
        .expect("INSERT RETURNING must return the stored row");

    let total = returned[0]
        .get("total")
        .unwrap_or_else(|| panic!("generated column must be present: {returned:?}"));
    let total: f64 = total
        .parse()
        .unwrap_or_else(|e| panic!("generated column must be numeric ({total}): {e}"));
    assert!(
        (total - 110.0).abs() < 1e-6,
        "the generated column must carry its COMPUTED value, not the submitted \
         (absent) one: {returned:?}"
    );
}

/// A multi-row insert returns one row per inserted row, in insert order, as ONE
/// result set.
///
/// The single result set is the load-bearing half. A multi-row document insert
/// plans one `PointInsert` task per row, and answering each task with its own
/// RowDescription/DataRow/CommandComplete sequence hands an extended-query
/// client several results for one statement — which drivers that expect exactly
/// one either mis-read or reject. Counting `CommandComplete` counts exactly
/// what such a driver would see.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_returns_one_result_set_with_every_row_in_order() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_multi (id TEXT PRIMARY KEY, n INT)")
        .await
        .expect("create collection");

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO ins_ret_multi (id, n) VALUES ('r1', 1), ('r2', 2), ('r3', 3) \
             RETURNING id, n",
        )
        .await
        .expect("multi-row INSERT RETURNING must succeed");

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
        "one row per inserted row, in insert order"
    );

    assert_eq!(
        rows(&server, "SELECT id FROM ins_ret_multi ORDER BY id").await,
        vec!["r1".to_string(), "r2".to_string(), "r3".to_string()],
        "every row must also have landed"
    );
}

/// `RETURNING *` across a multi-row insert keeps every column, in a stable
/// order, in ONE result set.
///
/// `RETURNING *` derives its column list per task, from that task's own stored
/// row, and the tasks are folded into one result set. The fold takes the UNION
/// of those column lists: keeping only the first task's columns would drop a
/// later row's extra field from the response entirely, because the encoder
/// reads each row strictly through the final column list and never sees a key
/// missing from it. The per-task union rule is pinned directly in
/// `response_shape::types`; this pins the end-to-end wire shape.
///
/// Column ORDER is asserted without assuming which order the star expands to:
/// what must hold is that both rows use the same column at each position, so a
/// positional client never sees columns reshuffle between rows of one result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_row_insert_returning_star_keeps_every_column_in_one_result_set() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_star (id TEXT PRIMARY KEY, name TEXT, note TEXT)")
        .await
        .expect("create collection");

    let msgs = server
        .client
        .simple_query(
            "INSERT INTO ins_ret_star (id, name, note) \
             VALUES ('s1', 'alpha', 'x'), ('s2', 'beta', 'y') RETURNING *",
        )
        .await
        .expect("multi-row INSERT RETURNING * must succeed");

    assert_eq!(
        result_set_count(&msgs),
        1,
        "one statement is one result set"
    );

    let cells: Vec<Vec<String>> = msgs
        .iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or("").to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect();
    assert_eq!(cells.len(), 2, "one row per inserted row");

    // No column was dropped: every value of every row survives the fold.
    for (row, expected) in cells
        .iter()
        .zip([["s1", "alpha", "x"], ["s2", "beta", "y"]])
    {
        assert_eq!(row.len(), 3, "all three columns must be present: {row:?}");
        for value in expected {
            assert!(
                row.iter().any(|cell| cell == value),
                "column value {value:?} was dropped from the response: {row:?}"
            );
        }
    }

    // Both rows use the same column at each position.
    for (position, (first, second)) in cells[0].iter().zip(cells[1].iter()).enumerate() {
        let pair = (first.as_str(), second.as_str());
        assert!(
            matches!(pair, ("s1", "s2") | ("alpha", "beta") | ("x", "y")),
            "position {position} holds different columns in the two rows: {pair:?}"
        );
    }
}

/// Inside an explicit transaction the write is staged or buffered until COMMIT,
/// so it has no stored row to project — the statement is refused, naming the
/// limitation, rather than succeeding with no rows.
///
/// Success-with-no-rows is the exact silence this clause exists to remove: a
/// caller asked for rows and would be handed a command tag with no indication
/// the request had been dropped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_transaction_insert_returning_is_refused_not_silently_dropped() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_txn (id TEXT PRIMARY KEY, n INT)")
        .await
        .expect("create collection");

    for sql in [
        "INSERT INTO ins_ret_txn (id, n) VALUES ('t1', 1) RETURNING id",
        "UPSERT INTO ins_ret_txn (id, n) VALUES ('t2', 2) RETURNING id",
    ] {
        server.exec("BEGIN").await.expect("begin");
        let message = server
            .exec(sql)
            .await
            .expect_err("an in-transaction RETURNING write must be refused");
        assert!(
            message.contains("RETURNING") && message.contains("transaction"),
            "the refusal must name the clause and the limitation; sql = {sql}, got: {message}"
        );
        server
            .client
            .simple_query("ROLLBACK")
            .await
            .expect("rollback");
    }

    // Autocommit is unaffected: the same statement outside a transaction
    // answers with its row.
    assert_eq!(
        rows(
            &server,
            "INSERT INTO ins_ret_txn (id, n) VALUES ('t3', 3) RETURNING id"
        )
        .await,
        vec!["t3".to_string()],
        "the refusal must be scoped to explicit transactions"
    );
}

/// `ON CONFLICT DO UPDATE ... RETURNING` returns the MERGED row. The submitted
/// body is only part of what the row ends up holding, so echoing it back would
/// report a row that does not exist.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn on_conflict_do_update_returning_shows_the_merged_post_image() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_conflict (id TEXT PRIMARY KEY, n INT, note TEXT)")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO ins_ret_conflict (id, n, note) VALUES ('c1', 10, 'original')")
        .await
        .expect("seed");

    let returned = server
        .query_named_rows(
            "INSERT INTO ins_ret_conflict (id, n) VALUES ('c1', 99) \
             ON CONFLICT (id) DO UPDATE SET n = n + 1 RETURNING id, n, note",
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

/// The `UPSERT INTO` form returns the STORED row, not the submitted one.
///
/// This path rebuilds the statement from its own parse before planning, and it
/// used to answer `RETURNING` from that parse — reporting the caller's own
/// values, merged with nothing, gated by nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn upsert_returning_shows_the_stored_row_not_the_submitted_one() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_upsert (id TEXT PRIMARY KEY, n INT, note TEXT)")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO ins_ret_upsert (id, n, note) VALUES ('u1', 1, 'kept')")
        .await
        .expect("seed");

    let returned = server
        .query_named_rows(
            "UPSERT INTO ins_ret_upsert (id, n) VALUES ('u1', 2) RETURNING id, n, note",
        )
        .await
        .expect("UPSERT RETURNING must return the merged stored row");

    assert_eq!(returned.len(), 1, "one row: {returned:?}");
    assert_eq!(returned[0].get("n").map(String::as_str), Some("2"));
    assert_eq!(
        returned[0].get("note").map(String::as_str),
        Some("kept"),
        "the merged row keeps the column the upsert never mentioned — a body \
         echoed from the request would have no `note` at all: {returned:?}"
    );
}

/// `INSERT ... RETURNING` into an engine with no `returning` slot is refused,
/// naming the engine. Dropping the clause silently answered a statement that
/// asked for rows with a bare command tag.
///
/// The engines still without the slot are asserted as a GROUP, from the one
/// shared list, so the refusal message stays accurate as each one gains
/// support. Naming a specific engine here is what went stale twice: the
/// assertion kept passing in isolation and only failed once the whole suite
/// ran. Adding an engine now means moving one row in that list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_an_unsupported_engine_is_refused_by_name() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_refused_engines_still_refuse(&server, "ins_ret_refused").await;
}

/// The other half of that split: every engine the list calls supported must
/// actually hand back its stored row.
///
/// Pinned beside the refusal so the two cannot drift into claiming the same
/// thing — an engine dropped from the refusal list without gaining the clause
/// fails here rather than passing both.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_every_supported_engine_returns_its_row() {
    let server = TestServer::start().await;
    insert_returning_engines::assert_supported_engines_return_their_row(&server, "ins_ret_ok")
        .await;
}

/// The key-value engine DOES carry the clause now, so the same statement that
/// used to be refused returns its stored row. Pinned here beside the refusal so
/// the two cannot drift into claiming the same thing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_returning_into_a_key_value_collection_returns_its_row() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION ins_ret_kv (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .expect("create collection");

    assert_eq!(
        rows(
            &server,
            "INSERT INTO ins_ret_kv (key, n) VALUES ('k1', 1) RETURNING key, n"
        )
        .await,
        vec!["k1|1".to_string()],
    );
}
