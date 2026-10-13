// SPDX-License-Identifier: BUSL-1.1

//! Rows the coordinator gathers keep the authorization of the plan they
//! came from.
//!
//! The coordinator replaces a join's build side, a post-processed body, an
//! aggregate's input or a nested join's input with a scan of its gathered
//! rows, then authorizes the task again. A user whose role reads nothing,
//! granted Read on exactly the two collections a query reads, runs every
//! such shape. The same user without the grant on one of them is refused.

use nodedb_test_support::pgwire_harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

const ORDERS: &str = "gr_orders";
const USERS: &str = "gr_users";
const PASSWORD: &str = "gathered-secret-42";

/// The join of the two collections.
const JOIN: &str = "SELECT o.id, u.name FROM gr_orders o JOIN gr_users u ON o.user_id = u.id";

/// An aggregate over the join: the join's rows are its gathered input.
const AGGREGATE_OVER_JOIN: &str = "SELECT u.name, COUNT(*) AS n FROM gr_orders o \
                                   JOIN gr_users u ON o.user_id = u.id GROUP BY u.name";

/// An ORDER BY over a derived table: the body is gathered, then
/// post-processed.
const POST_PROCESSED: &str =
    "SELECT t.id FROM (SELECT id, amount FROM gr_orders) t ORDER BY t.amount DESC LIMIT 2";

/// A three-way join. The converter plans the inner join as the outer
/// join's input and wraps it in `Exchange{Broadcast}`, because the inner
/// join reads sharded collections. The coordinator gathers it and embeds
/// its rows into the outer join. A derived table cannot be a JOIN target,
/// so this is the SQL shape that reaches the broadcast join input.
const BROADCAST_JOIN: &str = "SELECT o.id, v.name FROM gr_orders o \
                              JOIN gr_users u ON o.user_id = u.id \
                              JOIN gr_users v ON v.id = u.id";

/// Every shape and the rows it returns.
const SHAPES: [(&str, usize); 4] = [
    (JOIN, 3),
    (AGGREGATE_OVER_JOIN, 2),
    (POST_PROCESSED, 2),
    (BROADCAST_JOIN, 3),
];

async fn seed(srv: &TestServer) {
    for collection in [ORDERS, USERS] {
        srv.exec(&format!("CREATE COLLECTION {collection}"))
            .await
            .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    }
    for row in [
        "INSERT INTO gr_users { id: 'u1', name: 'ann' }",
        "INSERT INTO gr_users { id: 'u2', name: 'bob' }",
        "INSERT INTO gr_orders { id: 'o1', user_id: 'u1', amount: 5 }",
        "INSERT INTO gr_orders { id: 'o2', user_id: 'u1', amount: 7 }",
        "INSERT INTO gr_orders { id: 'o3', user_id: 'u2', amount: 1 }",
    ] {
        srv.exec(row)
            .await
            .unwrap_or_else(|e| panic!("seed '{row}': {e}"));
    }
}

/// A user whose role reads nothing, granted Read on `granted` only.
async fn reader(srv: &TestServer, user: &str, granted: &[&str]) -> tokio_postgres::Client {
    srv.exec(&format!(
        "CREATE USER {user} WITH PASSWORD '{PASSWORD}' ROLE monitor"
    ))
    .await
    .unwrap_or_else(|e| panic!("create {user}: {e}"));
    for collection in granted {
        srv.exec(&format!("GRANT READ ON {collection} TO {user}"))
            .await
            .unwrap_or_else(|e| panic!("grant {collection} to {user}: {e}"));
    }
    let (client, _connection) = srv
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    client
}

fn row_count(messages: &[SimpleQueryMessage]) -> usize {
    messages
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count()
}

#[tokio::test]
async fn gathered_shapes_run_with_grants_on_their_collections() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    let client = reader(&srv, "gr_granted", &[ORDERS, USERS]).await;
    for (sql, rows) in SHAPES {
        let messages = client
            .simple_query(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e:?}"));
        assert_eq!(row_count(&messages), rows, "{sql}");
    }
}

#[tokio::test]
async fn gathered_shapes_are_refused_without_a_grant_on_either_collection() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    for (user, granted) in [("gr_orders_only", ORDERS), ("gr_users_only", USERS)] {
        let client = reader(&srv, user, &[granted]).await;
        for (sql, _) in SHAPES {
            // The post-processed body reads only the orders collection.
            if sql == POST_PROCESSED && granted == ORDERS {
                continue;
            }
            let denied = client
                .simple_query(sql)
                .await
                .expect_err("a read of an ungranted collection is refused");
            assert_eq!(
                denied.as_db_error().map(|e| e.code().code()),
                Some("42501"),
                "{user}: {sql}: {denied:?}"
            );
        }
    }
}

#[tokio::test]
async fn a_post_processed_body_runs_with_the_grant_it_reads() {
    let srv = TestServer::start().await;
    seed(&srv).await;
    let client = reader(&srv, "gr_body_reader", &[ORDERS]).await;
    let messages = client
        .simple_query(POST_PROCESSED)
        .await
        .unwrap_or_else(|e| panic!("{POST_PROCESSED}: {e:?}"));
    assert_eq!(row_count(&messages), 2);
}
