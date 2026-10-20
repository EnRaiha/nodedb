// SPDX-License-Identifier: BUSL-1.1

//! Predicate `DELETE` and `UPDATE`, `TRUNCATE` and facet counts on a
//! bitemporal collection.
//!
//! A bitemporal collection stores its rows only as versions. A predicate
//! write matches each row's current version. A `DELETE` or `TRUNCATE` closes
//! that version with a tombstone and an `UPDATE` writes a new one, so
//! `AS OF SYSTEM TIME` before the statement still returns the rows as they
//! were. Facet counts read the current versions.

use tokio_postgres::SimpleQueryMessage;

use crate::harness::TestServer;

/// Create bitemporal `name` holding `a` (`x`), `b` (`y`) and `c` (`x`).
async fn seed(srv: &TestServer, name: &str) {
    srv.exec(&format!("CREATE COLLECTION {name} WITH (bitemporal=true)"))
        .await
        .unwrap();
    for (id, kind) in [("a", "x"), ("b", "y"), ("c", "x")] {
        srv.exec(&format!(
            "INSERT INTO {name} {{ id: '{id}', kind: '{kind}' }}"
        ))
        .await
        .unwrap();
    }
}

/// The wall-clock millisecond strictly between the writes before and after
/// it.
async fn instant_between_writes() -> i64 {
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before epoch")
        .as_millis();
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    i64::try_from(now).expect("millisecond clock fits i64")
}

/// The `CommandComplete` row count of the single statement `sql`.
async fn command_count(srv: &TestServer, sql: &str) -> u64 {
    let counts: Vec<u64> = srv
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e:?}"))
        .into_iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::CommandComplete(n) => Some(n),
            _ => None,
        })
        .collect();
    assert_eq!(
        counts.len(),
        1,
        "{sql} answers one command tag, got {counts:?}"
    );
    counts[0]
}

/// `(id, kind)` of every row `sql` returns, sorted by id.
async fn id_kind_rows(srv: &TestServer, sql: &str) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = srv
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"))
        .into_iter()
        .map(|row| (row[0].clone(), row[1].clone()))
        .collect();
    rows.sort();
    rows
}

fn rows(expected: &[(&str, &str)]) -> Vec<(String, String)> {
    expected
        .iter()
        .map(|(id, kind)| (id.to_string(), kind.to_string()))
        .collect()
}

/// The current rows of `name` and the rows `AS OF SYSTEM TIME` `before`.
async fn current_and_as_of(
    srv: &TestServer,
    name: &str,
    before: i64,
) -> (Vec<(String, String)>, Vec<(String, String)>) {
    let current = id_kind_rows(srv, &format!("SELECT id, kind FROM {name} ORDER BY id")).await;
    let as_of = id_kind_rows(
        srv,
        &format!("SELECT id, kind FROM {name} AS OF SYSTEM TIME {before}"),
    )
    .await;
    (current, as_of)
}

/// Autocommit: the predicate `DELETE` removes both matches and the
/// predicate `UPDATE` rewrites its match. History before each stays
/// readable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_predicate_writes_match_current_versions() {
    let srv = TestServer::start().await;
    seed(&srv, "bt_pw_auto").await;

    let before_delete = instant_between_writes().await;
    assert_eq!(
        command_count(&srv, "DELETE FROM bt_pw_auto WHERE kind = 'x'").await,
        2,
        "DELETE matches the current versions of a and c"
    );
    let (current, as_of) = current_and_as_of(&srv, "bt_pw_auto", before_delete).await;
    assert_eq!(current, rows(&[("b", "y")]), "the deleted rows are gone");
    assert_eq!(
        as_of,
        rows(&[("a", "x"), ("b", "y"), ("c", "x")]),
        "AS OF before the DELETE returns the deleted rows"
    );

    let before_update = instant_between_writes().await;
    assert_eq!(
        command_count(&srv, "UPDATE bt_pw_auto SET kind = 'z' WHERE kind = 'y'").await,
        1,
        "UPDATE matches the current version of b"
    );
    let (current, as_of) = current_and_as_of(&srv, "bt_pw_auto", before_update).await;
    assert_eq!(current, rows(&[("b", "z")]), "the update is current");
    assert_eq!(
        as_of,
        rows(&[("b", "y")]),
        "AS OF before the UPDATE returns the old value"
    );
}

/// The same writes inside `BEGIN ... COMMIT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_predicate_writes_match_current_versions() {
    let srv = TestServer::start().await;
    seed(&srv, "bt_pw_txn").await;

    let before_delete = instant_between_writes().await;
    srv.exec("BEGIN").await.unwrap();
    assert_eq!(
        command_count(&srv, "DELETE FROM bt_pw_txn WHERE kind = 'x'").await,
        2,
        "DELETE matches the current versions of a and c"
    );
    srv.exec("COMMIT").await.unwrap();
    let (current, as_of) = current_and_as_of(&srv, "bt_pw_txn", before_delete).await;
    assert_eq!(current, rows(&[("b", "y")]), "the deleted rows are gone");
    assert_eq!(
        as_of,
        rows(&[("a", "x"), ("b", "y"), ("c", "x")]),
        "AS OF before the DELETE returns the deleted rows"
    );

    let before_update = instant_between_writes().await;
    srv.exec("BEGIN").await.unwrap();
    assert_eq!(
        command_count(&srv, "UPDATE bt_pw_txn SET kind = 'z' WHERE kind = 'y'").await,
        1,
        "UPDATE matches the current version of b"
    );
    srv.exec("COMMIT").await.unwrap();
    let (current, as_of) = current_and_as_of(&srv, "bt_pw_txn", before_update).await;
    assert_eq!(current, rows(&[("b", "z")]), "the update is current");
    assert_eq!(
        as_of,
        rows(&[("b", "y")]),
        "AS OF before the UPDATE returns the old value"
    );
}

/// Autocommit `TRUNCATE` closes every row. `AS OF SYSTEM TIME` before it
/// still returns them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn autocommit_truncate_closes_every_current_version() {
    let srv = TestServer::start().await;
    seed(&srv, "bt_tr_auto").await;

    let before = instant_between_writes().await;
    srv.exec("TRUNCATE bt_tr_auto").await.unwrap();
    let (current, as_of) = current_and_as_of(&srv, "bt_tr_auto", before).await;
    assert!(
        current.is_empty(),
        "TRUNCATE leaves no current row, got {current:?}"
    );
    assert_eq!(
        as_of,
        rows(&[("a", "x"), ("b", "y"), ("c", "x")]),
        "AS OF before the TRUNCATE returns every row"
    );
}

/// The same `TRUNCATE` inside `BEGIN ... COMMIT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transaction_truncate_closes_every_current_version() {
    let srv = TestServer::start().await;
    seed(&srv, "bt_tr_txn").await;

    let before = instant_between_writes().await;
    srv.exec("BEGIN").await.unwrap();
    srv.exec("TRUNCATE bt_tr_txn").await.unwrap();
    srv.exec("COMMIT").await.unwrap();
    let (current, as_of) = current_and_as_of(&srv, "bt_tr_txn", before).await;
    assert!(
        current.is_empty(),
        "TRUNCATE leaves no current row, got {current:?}"
    );
    assert_eq!(
        as_of,
        rows(&[("a", "x"), ("b", "y"), ("c", "x")]),
        "AS OF before the TRUNCATE returns every row"
    );
}

/// Facet counts read each row's current version: the rows a `DELETE`
/// closed no longer count.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facet_counts_read_current_versions() {
    let srv = TestServer::start().await;
    seed(&srv, "bt_facet").await;
    srv.exec("INSERT INTO bt_facet { id: 'd', kind: 'y' }")
        .await
        .unwrap();
    assert_eq!(
        command_count(&srv, "DELETE FROM bt_facet WHERE id = 'a'").await,
        1
    );

    let text = srv
        .query_text_joined("SELECT FACET_COUNTS(collection => 'bt_facet', fields => ['kind'])")
        .await
        .unwrap()
        .join("");
    let facets: serde_json::Value =
        serde_json::from_str(&text).unwrap_or_else(|e| panic!("facet JSON {text}: {e}"));
    let mut counts: Vec<(String, u64)> = facets["kind"]
        .as_array()
        .unwrap_or_else(|| panic!("a kind facet, got {facets}"))
        .iter()
        .map(|entry| {
            (
                entry["value"].as_str().unwrap_or_default().to_string(),
                entry["count"].as_u64().unwrap_or_default(),
            )
        })
        .collect();
    counts.sort();
    assert_eq!(
        counts,
        vec![("x".to_string(), 1), ("y".to_string(), 2)],
        "the deleted row a no longer counts"
    );
}
