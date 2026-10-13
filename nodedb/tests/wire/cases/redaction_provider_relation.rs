// SPDX-License-Identifier: BUSL-1.1

//! A redaction policy on a continuous aggregate masks its column in a
//! SELECT, as a policy on a collection does. A copy of the aggregate's rows
//! into a collection is refused, since copied values never pass the masking
//! hook.

use std::time::Duration;

use crate::harness::TestServer;

const PASSWORD: &str = "redact-provider-secret-1";

/// A memtable budget of one byte flushes every ingest to a partition, and a
/// flush is what an `OnFlush` aggregate folds.
const FLUSH_EVERY_INGEST: usize = 1;

async fn seed(srv: &TestServer, src: &str, view: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {src} (ts BIGINT TIME_KEY, dept TEXT, salary FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {src}: {e}"));
    srv.exec(&format!(
        "CREATE CONTINUOUS AGGREGATE {view} ON {src} BUCKET '1m' \
         AGGREGATE SUM(salary) AS total GROUP BY dept"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {view}: {e}"));
    for (ts, dept, salary) in [(1_000, "eng", 100.0), (2_000, "ops", 50.0)] {
        srv.exec(&format!(
            "INSERT INTO {src} (ts, dept, salary) VALUES ({ts}, '{dept}', {salary})"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert row at {ts}: {e}"));
    }
    // The flush that feeds the aggregate runs on the core after the insert
    // answers: wait until both buckets read back.
    let sql = format!("SELECT dept FROM {view}");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        let rows = srv
            .query_rows(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        if rows.len() >= 2 {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{view} never served both buckets: {rows:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn create_reader(srv: &TestServer, user: &str) {
    srv.exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    srv.exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Rows of `sql` run as `user`, or the SQLSTATE and message of the refusal.
async fn rows_as(
    srv: &TestServer,
    user: &str,
    sql: &str,
) -> Result<Vec<Vec<String>>, (String, String)> {
    let (client, handle) = srv
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    match result {
        Ok(messages) => Ok(messages
            .iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|i| row.get(i).unwrap_or("NULL").to_string())
                        .collect(),
                ),
                _ => None,
            })
            .collect()),
        Err(e) => Err(e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_redacted_continuous_aggregate_column_is_masked_in_select() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "rpr_src", "rpr_view").await;
    let user = "rpr_reader";
    create_reader(&srv, user).await;
    srv.exec("CREATE REDACTION POLICY rpr_mask ON rpr_view FOR ROLE readwrite (total MASK '***')")
        .await
        .unwrap_or_else(|e| panic!("create redaction policy: {e}"));

    let rows = rows_as(&srv, user, "SELECT dept, total FROM rpr_view ORDER BY dept")
        .await
        .unwrap_or_else(|(code, message)| panic!("SELECT as {user}: {code} {message}"));
    assert_eq!(
        rows,
        vec![
            vec!["eng".to_string(), "***".to_string()],
            vec!["ops".to_string(), "***".to_string()],
        ],
        "the masked column must read masked and the other column unchanged"
    );

    // The owner, whose roles carry no rule, reads the stored totals.
    let stored = srv
        .query_rows("SELECT dept, total FROM rpr_view ORDER BY dept")
        .await
        .expect("SELECT as owner");
    assert_eq!(stored.len(), 2, "{stored:?}");
    assert_ne!(stored[0][1], "***", "{stored:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_copy_of_a_redacted_continuous_aggregate_is_refused() {
    let srv = TestServer::start_with_timeseries_memtable_budget(FLUSH_EVERY_INGEST).await;
    seed(&srv, "rpc_src", "rpc_view").await;
    srv.exec("CREATE COLLECTION rpc_copy")
        .await
        .expect("create copy target");
    let user = "rpc_reader";
    create_reader(&srv, user).await;
    srv.exec("CREATE REDACTION POLICY rpc_mask ON rpc_view FOR ROLE readwrite (total MASK '***')")
        .await
        .unwrap_or_else(|e| panic!("create redaction policy: {e}"));

    match rows_as(
        &srv,
        user,
        "INSERT INTO rpc_copy (id, total) SELECT dept, total FROM rpc_view",
    )
    .await
    {
        Err((_, message)) => assert!(message.contains("rpc_view"), "{message}"),
        Ok(rows) => panic!("the copy of a redacted aggregate was admitted: {rows:?}"),
    }
}
