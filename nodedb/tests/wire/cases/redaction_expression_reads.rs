// SPDX-License-Identifier: BUSL-1.1

//! A read of a redacted column outside a plain projection is refused with
//! `42501`, naming the column, on every engine.
//!
//! A plain projection is masked in the result rows. An expression, a
//! predicate, a sort, or a group key reads the stored value before any mask
//! applies, so `upper(ssn)` would return the clear value and `WHERE ssn = ...`
//! would confirm it by filtering.

use crate::harness::TestServer;

const PASSWORD: &str = "redact-expr-secret-1";

/// The stored value the policy protects.
const SSN: &str = "123-45-6789";

/// Statements that read `ssn` outside a plain projection, over `{c}`.
const REFUSED: [&str; 6] = [
    "SELECT name, upper(ssn) FROM {c}",
    "SELECT name, length(ssn) + 1 FROM {c}",
    "SELECT name, CASE WHEN ssn = '123-45-6789' THEN 1 ELSE 0 END FROM {c}",
    "SELECT name FROM {c} WHERE ssn = '123-45-6789'",
    "SELECT name FROM {c} ORDER BY ssn",
    "SELECT ssn, COUNT(*) FROM {c} GROUP BY ssn",
];

async fn create_reader(srv: &TestServer, user: &str) {
    srv.exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    srv.exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .unwrap_or_else(|e| panic!("grant readwrite to {user}: {e}"));
}

/// Rows of `sql` on `client`, or the SQLSTATE and message of the refusal.
async fn rows_on(
    client: &tokio_postgres::Client,
    sql: &str,
) -> Result<Vec<Vec<String>>, (String, String)> {
    match client.simple_query(sql).await {
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

/// Assert `sql` is refused with `42501` naming `column`.
async fn assert_refused(client: &tokio_postgres::Client, sql: &str, column: &str) {
    match rows_on(client, sql).await {
        Ok(rows) => panic!("{sql}: a read of redacted '{column}' must be refused, got {rows:?}"),
        Err((code, message)) => {
            assert_eq!(
                code, "42501",
                "{sql}: expected 42501, got {code}: {message}"
            );
            assert!(
                message.contains(column),
                "{sql}: the refusal must name '{column}': {message}"
            );
        }
    }
}

/// Create `collection` with `ddl`, insert one row with `insert`, put `ssn`
/// under a mask for `readwrite`, and check every statement as a reader.
async fn assert_expression_reads_refused(
    srv: &TestServer,
    collection: &str,
    ddl: &str,
    insert: &str,
) {
    srv.exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    srv.exec(insert)
        .await
        .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    let user = format!("{collection}_reader");
    create_reader(srv, &user).await;
    srv.exec(&format!(
        "CREATE REDACTION POLICY {collection}_mask ON {collection} FOR ROLE readwrite \
         (ssn MASK '***')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create redaction policy on {collection}: {e}"));

    let (client, handle) = srv
        .connect_as(&user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));

    let masked = rows_on(&client, &format!("SELECT name, ssn FROM {collection}"))
        .await
        .unwrap_or_else(|(code, message)| panic!("{collection}: plain SELECT: {code} {message}"));
    assert_eq!(
        masked,
        vec![vec!["ali".to_string(), "***".to_string()]],
        "{collection}: a plain projection reads masked"
    );

    let clear = rows_on(
        &client,
        &format!("SELECT name, upper(name) FROM {collection} WHERE name = 'ali' ORDER BY name"),
    )
    .await
    .unwrap_or_else(|(code, message)| {
        panic!("{collection}: reads of an unruled column: {code} {message}")
    });
    assert_eq!(
        clear,
        vec![vec!["ali".to_string(), "ALI".to_string()]],
        "{collection}: expressions over an unruled column still run"
    );

    for template in REFUSED {
        assert_refused(&client, &template.replace("{c}", collection), "ssn").await;
    }
    drop(client);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_document_refuses_reads_of_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_expression_reads_refused(
        &srv,
        "rx_doc",
        "CREATE COLLECTION rx_doc",
        &format!("INSERT INTO rx_doc (id, name, ssn) VALUES ('a', 'ali', '{SSN}')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_document_refuses_reads_of_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_expression_reads_refused(
        &srv,
        "rx_strict",
        "CREATE COLLECTION rx_strict (id TEXT PRIMARY KEY, name TEXT, ssn TEXT) \
         WITH (engine='document_strict')",
        &format!("INSERT INTO rx_strict (id, name, ssn) VALUES ('a', 'ali', '{SSN}')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_refuses_reads_of_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_expression_reads_refused(
        &srv,
        "rx_col",
        "CREATE COLLECTION rx_col (id TEXT, name TEXT, ssn TEXT) WITH (engine='columnar')",
        &format!("INSERT INTO rx_col (id, name, ssn) VALUES ('a', 'ali', '{SSN}')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_refuses_reads_of_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_expression_reads_refused(
        &srv,
        "rx_kv",
        "CREATE COLLECTION rx_kv (k TEXT PRIMARY KEY, name TEXT, ssn TEXT) WITH (engine='kv')",
        &format!("INSERT INTO rx_kv (k, name, ssn) VALUES ('a', 'ali', '{SSN}')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_refuses_reads_of_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_expression_reads_refused(
        &srv,
        "rx_ts",
        "CREATE COLLECTION rx_ts (ts BIGINT TIME_KEY, name TEXT, ssn TEXT) \
         WITH (engine='timeseries')",
        &format!("INSERT INTO rx_ts (ts, name, ssn) VALUES (1000, 'ali', '{SSN}')"),
    )
    .await;
}

/// A geometry under a mask: a geometry function and a spatial predicate both
/// read the stored geometry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spatial_refuses_reads_of_a_redacted_geometry() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE COLLECTION rx_spatial (location GEOMETRY SPATIAL_INDEX, name VARCHAR) \
         WITH (engine='spatial')",
    )
    .await
    .unwrap_or_else(|e| panic!("create rx_spatial: {e}"));
    srv.exec("INSERT INTO rx_spatial (name, location) VALUES ('Temerloh', ST_Point(102.42, 3.45))")
        .await
        .unwrap_or_else(|e| panic!("insert into rx_spatial: {e}"));
    let user = "rx_spatial_reader";
    create_reader(&srv, user).await;
    srv.exec(
        "CREATE REDACTION POLICY rx_spatial_mask ON rx_spatial FOR ROLE readwrite \
         (location MASK '***')",
    )
    .await
    .unwrap_or_else(|e| panic!("create redaction policy on rx_spatial: {e}"));

    let (client, handle) = srv
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let names = rows_on(&client, "SELECT name FROM rx_spatial")
        .await
        .unwrap_or_else(|(code, message)| panic!("plain SELECT: {code} {message}"));
    assert_eq!(names, vec![vec!["Temerloh".to_string()]]);
    for sql in [
        "SELECT name, ST_AsText(location) FROM rx_spatial",
        "SELECT name FROM rx_spatial WHERE ST_DWithin(location, ST_Point(102.42, 3.45), 1000)",
    ] {
        assert_refused(&client, sql, "location").await;
    }
    drop(client);
    handle.abort();
}

/// A `WHERE` beside a similarity search rides on the search as its filter.
/// Over one row `a` (name `ali`) ranked by `order_by`, check as a reader:
/// - a plain projection of `ssn` reads masked,
/// - a filter on `name` still runs,
/// - a filter on `ssn` is refused.
async fn assert_search_filter_refused(
    srv: &TestServer,
    collection: &str,
    ddl: &str,
    insert: &str,
    order_by: &str,
) {
    srv.exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    srv.exec(insert)
        .await
        .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    let user = format!("{collection}_reader");
    create_reader(srv, &user).await;
    srv.exec(&format!(
        "CREATE REDACTION POLICY {collection}_mask ON {collection} FOR ROLE readwrite \
         (ssn MASK '***')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create redaction policy on {collection}: {e}"));

    let (client, handle) = srv
        .connect_as(&user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let masked = rows_on(
        &client,
        &format!("SELECT id, ssn FROM {collection} ORDER BY {order_by} LIMIT 1"),
    )
    .await
    .unwrap_or_else(|(code, message)| panic!("{collection}: plain projection: {code} {message}"));
    assert_eq!(
        masked,
        vec![vec!["a".to_string(), "***".to_string()]],
        "{collection}: a projected redacted column reads masked"
    );
    let names = rows_on(
        &client,
        &format!("SELECT id FROM {collection} WHERE name = 'ali' ORDER BY {order_by} LIMIT 1"),
    )
    .await
    .unwrap_or_else(|(code, message)| {
        panic!("{collection}: filter on an unruled column: {code} {message}")
    });
    assert_eq!(names, vec![vec!["a".to_string()]]);
    assert_refused(
        &client,
        &format!("SELECT id FROM {collection} WHERE ssn = '{SSN}' ORDER BY {order_by} LIMIT 1"),
        "ssn",
    )
    .await;
    drop(client);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sparse_search_refuses_a_filter_on_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_search_filter_refused(
        &srv,
        "rx_sparse",
        "CREATE TABLE rx_sparse \
         (id TEXT PRIMARY KEY, terms SPARSEVECTOR, name TEXT, ssn TEXT)",
        &format!(
            "INSERT INTO rx_sparse (id, terms, name, ssn) \
             VALUES ('a', '{{1: 1.0}}', 'ali', '{SSN}')"
        ),
        "sparse_score(terms, '{1: 1.0}') DESC",
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_search_refuses_a_filter_on_a_redacted_column() {
    let srv = TestServer::start().await;
    assert_search_filter_refused(
        &srv,
        "rx_vec",
        "CREATE COLLECTION rx_vec \
         FIELDS (id TEXT, embedding VECTOR(2), name TEXT, ssn TEXT) \
         WITH (engine='document_schemaless', m=8, ef_construction=50)",
        &format!(
            "INSERT INTO rx_vec (id, embedding, name, ssn) \
             VALUES ('a', ARRAY[0.0, 0.0], 'ali', '{SSN}')"
        ),
        "vector_distance(embedding, ARRAY[0.0, 0.0])",
    )
    .await;
}

/// A statement planned and cached before the policy exists is checked again
/// once the policy is created, on the same session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_policy_created_mid_session_refuses_a_cached_predicate() {
    let srv = TestServer::start().await;
    srv.exec("CREATE COLLECTION rx_cache")
        .await
        .unwrap_or_else(|e| panic!("create rx_cache: {e}"));
    srv.exec(&format!(
        "INSERT INTO rx_cache (id, name, ssn) VALUES ('a', 'ali', '{SSN}')"
    ))
    .await
    .unwrap_or_else(|e| panic!("insert into rx_cache: {e}"));
    let user = "rx_cache_reader";
    create_reader(&srv, user).await;

    let (client, handle) = srv
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let sql = format!("SELECT name FROM rx_cache WHERE ssn = '{SSN}'");
    for _ in 0..2 {
        let rows = rows_on(&client, &sql)
            .await
            .unwrap_or_else(|(code, message)| panic!("before the policy: {code} {message}"));
        assert_eq!(rows, vec![vec!["ali".to_string()]]);
    }

    srv.exec(
        "CREATE REDACTION POLICY rx_cache_mask ON rx_cache FOR ROLE readwrite (ssn MASK '***')",
    )
    .await
    .unwrap_or_else(|e| panic!("create redaction policy on rx_cache: {e}"));
    assert_refused(&client, &sql, "ssn").await;

    srv.exec("DROP REDACTION POLICY ON rx_cache FOR ROLE readwrite")
        .await
        .unwrap_or_else(|e| panic!("drop redaction policy on rx_cache: {e}"));
    let rows = rows_on(&client, &sql)
        .await
        .unwrap_or_else(|(code, message)| panic!("after the drop: {code} {message}"));
    assert_eq!(rows, vec![vec!["ali".to_string()]]);
    drop(client);
    handle.abort();
}

/// A lookup by a redacted key column tells the caller whether the key
/// exists: a hit returns a row, a miss returns none. Both are refused, and a
/// delete by key is refused too, since its affected count tells the same.
/// A plain read still returns the key masked.
async fn assert_key_lookups_refused(
    srv: &TestServer,
    collection: &str,
    key: &str,
    ddl: &str,
    insert: &str,
) {
    srv.exec(ddl)
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    srv.exec(insert)
        .await
        .unwrap_or_else(|e| panic!("insert into {collection}: {e}"));
    let user = format!("{collection}_reader");
    create_reader(srv, &user).await;
    srv.exec(&format!(
        "CREATE REDACTION POLICY {collection}_mask ON {collection} FOR ROLE readwrite \
         ({key} MASK '***')"
    ))
    .await
    .unwrap_or_else(|e| panic!("create redaction policy on {collection}: {e}"));

    let (client, handle) = srv
        .connect_as(&user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let masked = rows_on(&client, &format!("SELECT {key}, name FROM {collection}"))
        .await
        .unwrap_or_else(|(code, message)| panic!("{collection}: plain SELECT: {code} {message}"));
    assert_eq!(
        masked,
        vec![vec!["***".to_string(), "ali".to_string()]],
        "{collection}: the key reads masked"
    );
    for value in [SSN, "000-00-0000"] {
        assert_refused(
            &client,
            &format!("SELECT name FROM {collection} WHERE {key} = '{value}'"),
            key,
        )
        .await;
    }
    assert_refused(
        &client,
        &format!("DELETE FROM {collection} WHERE {key} = '{SSN}'"),
        key,
    )
    .await;
    drop(client);
    handle.abort();

    let stored = srv
        .query_rows(&format!("SELECT name FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("{collection}: read as owner: {e}"));
    assert_eq!(
        stored,
        vec![vec!["ali".to_string()]],
        "{collection}: the refused delete removed nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_document_refuses_a_lookup_by_a_redacted_key() {
    let srv = TestServer::start().await;
    assert_key_lookups_refused(
        &srv,
        "rx_key_doc",
        "id",
        "CREATE COLLECTION rx_key_doc (id TEXT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
        &format!("INSERT INTO rx_key_doc (id, name) VALUES ('{SSN}', 'ali')"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_refuses_a_lookup_by_a_redacted_key() {
    let srv = TestServer::start().await;
    assert_key_lookups_refused(
        &srv,
        "rx_key_kv",
        "k",
        "CREATE COLLECTION rx_key_kv (k TEXT PRIMARY KEY, name TEXT) WITH (engine='kv')",
        &format!("INSERT INTO rx_key_kv (k, name) VALUES ('{SSN}', 'ali')"),
    )
    .await;
}
