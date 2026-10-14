// SPDX-License-Identifier: BUSL-1.1

//! Column redaction reaches rows the coordinator gathers.
//!
//! A join's build side, a post-processed body, a nested join's input and a
//! derived join side reach the client as rows the coordinator gathered from
//! another plan. A role with
//! a rule on `ssn` reads `ssn` through each of them and gets the mask. An
//! aggregate over a join reading `ssn` is refused. The owner reads the stored
//! values.

use nodedb_test_support::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

const PASSWORD: &str = "redacted-secret-42";
const ROLE: &str = "gr_auditor";
const USER: &str = "gr_redacted";
const MASK: &str = "***";
const SSNS: [&str; 2] = ["111-11-1111", "222-22-2222"];

/// The join of the two collections, reading the ruled column.
const JOIN: &str = "SELECT o.id, u.ssn FROM gr_orders o JOIN gr_users u ON o.user_id = u.id";

/// An ORDER BY over a derived table: the body is gathered, then
/// post-processed.
const POST_PROCESSED: &str =
    "SELECT t.ssn FROM (SELECT id, ssn FROM gr_users) t ORDER BY t.id LIMIT 2";

/// A three-way join. The inner join reaches the outer one as an
/// `Exchange{Broadcast}` input, which the coordinator gathers and embeds.
const BROADCAST_JOIN: &str = "SELECT o.id, v.ssn FROM gr_orders o \
                              JOIN gr_users u ON o.user_id = u.id \
                              JOIN gr_users v ON v.id = u.id";

/// A derived table as a join side. The coordinator gathers its body and
/// embeds the rows as the join input, qualified by the derived alias.
const DERIVED_JOIN: &str = "SELECT o.id, t.ssn FROM gr_orders o \
                            JOIN (SELECT id, ssn FROM gr_users) t ON o.user_id = t.id";

/// An aggregate over the join reading the ruled column.
const AGGREGATE_OVER_JOIN: &str = "SELECT u.name, MIN(u.ssn) AS s FROM gr_orders o \
                                   JOIN gr_users u ON o.user_id = u.id GROUP BY u.name";

/// Every shape that returns the ruled column, and the rows it returns.
const MASKED_SHAPES: [(&str, usize); 4] = [
    (JOIN, 3),
    (POST_PROCESSED, 2),
    (BROADCAST_JOIN, 3),
    (DERIVED_JOIN, 3),
];

async fn seed(srv: &TestServer) {
    for collection in ["gr_orders", "gr_users"] {
        srv.exec(&format!("CREATE COLLECTION {collection}"))
            .await
            .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    }
    for row in [
        "INSERT INTO gr_users { id: 'u1', name: 'ann', ssn: '111-11-1111' }",
        "INSERT INTO gr_users { id: 'u2', name: 'bob', ssn: '222-22-2222' }",
        "INSERT INTO gr_orders { id: 'o1', user_id: 'u1', amount: 5 }",
        "INSERT INTO gr_orders { id: 'o2', user_id: 'u1', amount: 7 }",
        "INSERT INTO gr_orders { id: 'o3', user_id: 'u2', amount: 1 }",
    ] {
        srv.exec(row)
            .await
            .unwrap_or_else(|e| panic!("seed '{row}': {e}"));
    }
}

/// A user holding only `ROLE`, granted Read on both collections, with a
/// rule masking `ssn` for that role.
async fn redacted_reader(srv: &TestServer) -> tokio_postgres::Client {
    for sql in [
        format!("CREATE ROLE {ROLE}"),
        format!("CREATE USER {USER} WITH PASSWORD '{PASSWORD}' ROLE {ROLE}"),
        format!("GRANT READ ON gr_orders TO {USER}"),
        format!("GRANT READ ON gr_users TO {USER}"),
        format!(
            "CREATE REDACTION POLICY gr_mask_ssn ON gr_users FOR ROLE {ROLE} (ssn MASK '{MASK}')"
        ),
    ] {
        srv.exec(&sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let (client, _connection) = srv
        .connect_as(USER, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {USER}: {e}"));
    client
}

/// Every cell of every row, as text.
fn rows_of(messages: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    messages
        .iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(
                (0..row.columns().len())
                    .map(|i| row.get(i).unwrap_or_default().to_string())
                    .collect(),
            ),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_ruled_column_is_masked_through_every_gathered_shape() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    let client = redacted_reader(&srv).await;
    for (sql, expected) in MASKED_SHAPES {
        let messages = client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        let rows = rows_of(&messages);
        assert_eq!(rows.len(), expected, "{sql}: {rows:?}");
        for row in &rows {
            assert!(
                row.iter().any(|cell| cell == MASK),
                "{sql}: the ruled column must be masked: {row:?}"
            );
            assert!(
                !row.iter().any(|cell| SSNS.contains(&cell.as_str())),
                "{sql}: a stored ssn must not reach the role: {row:?}"
            );
        }
    }
}

#[tokio::test]
async fn an_aggregate_over_a_join_reading_a_ruled_column_is_refused() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    let client = redacted_reader(&srv).await;
    let refused = client.simple_query(AGGREGATE_OVER_JOIN).await;
    assert!(
        refused.is_err(),
        "an aggregate over a redacted column is computed over the stored values: {refused:?}"
    );
}

#[tokio::test]
async fn the_owner_reads_the_stored_values_through_every_shape() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    // The policy binds the reader's role. The owner holds another role, so
    // it reads the stored values.
    let _client = redacted_reader(&srv).await;
    for (sql, expected) in MASKED_SHAPES {
        let rows = srv
            .query_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(rows.len(), expected, "{sql}: {rows:?}");
        for row in &rows {
            assert!(
                row.iter().any(|cell| SSNS.contains(&cell.as_str())),
                "{sql}: the owner reads the stored ssn: {row:?}"
            );
            assert!(!row.iter().any(|cell| cell == MASK), "{sql}: {row:?}");
        }
    }
    srv.query_rows(AGGREGATE_OVER_JOIN)
        .await
        .unwrap_or_else(|e| panic!("the owner aggregates: {e}"));
}
