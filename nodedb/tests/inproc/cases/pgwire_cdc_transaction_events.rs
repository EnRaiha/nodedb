// SPDX-License-Identifier: BUSL-1.1

//! A committed transaction publishes its rows on the Control-Plane change
//! stream, once each, at the commit's position. A rolled-back transaction
//! publishes nothing.
//!
//! Single node: the commit applies through this node's own feed, the `Local`
//! partition. An autocommit write after each transaction is the sentinel: the
//! next event must be its, so nothing of the transaction arrived in between.

use std::collections::BTreeSet;
use std::time::Duration;

use nodedb::control::change_stream::{ChangeOperation, SequencedChangeEvent, Subscription};
use nodedb_test_support::pgwire_harness::TestServer;

/// The next event the subscription receives, or a test error.
async fn next_event(sub: &mut Subscription, what: &str) -> SequencedChangeEvent {
    match tokio::time::timeout(Duration::from_secs(5), sub.recv_sequenced()).await {
        Ok(Ok(event)) => event,
        Ok(Err(e)) => panic!("change stream closed while awaiting {what}: {e}"),
        Err(_) => panic!("no change event for {what}"),
    }
}

#[tokio::test]
async fn committed_transaction_publishes_each_row_once_at_the_commit() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION txn_cdc (id TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION txn_cdc");

    let mut sub = server
        .shared
        .change_stream
        .subscribe(Some("txn_cdc".into()), None);

    server.exec("BEGIN").await.expect("BEGIN");
    server
        .exec("INSERT INTO txn_cdc (id, status) VALUES ('o1', 'new')")
        .await
        .expect("INSERT o1 in transaction");
    server
        .exec("INSERT INTO txn_cdc (id, status) VALUES ('o2', 'new')")
        .await
        .expect("INSERT o2 in transaction");
    server
        .exec("UPDATE txn_cdc SET status = 'shipped' WHERE id = 'o1'")
        .await
        .expect("UPDATE in transaction");
    server.exec("COMMIT").await.expect("COMMIT");

    server
        .exec("INSERT INTO txn_cdc (id, status) VALUES ('s1', 'sentinel')")
        .await
        .expect("sentinel INSERT");

    let first = next_event(&mut sub, "the first committed row").await;
    let second = next_event(&mut sub, "the second committed row").await;
    let rows: BTreeSet<String> = [&first, &second]
        .iter()
        .map(|event| event.document_id.as_str().to_owned())
        .collect();
    assert_eq!(
        rows,
        BTreeSet::from(["o1".to_owned(), "o2".to_owned()]),
        "the commit publishes each row it wrote once"
    );
    assert_eq!(
        (first.position().epoch, first.position().index),
        (second.position().epoch, second.position().index),
        "every row of the commit publishes at the commit's position"
    );
    assert!(
        first.position().sequence < second.position().sequence,
        "the commit's events are ordered within its position"
    );

    let sentinel = next_event(&mut sub, "the sentinel").await;
    assert_eq!(
        sentinel.document_id.as_str(),
        "s1",
        "the commit published an event more than once"
    );
    assert_eq!(sentinel.operation, ChangeOperation::Insert);
    assert!(
        sentinel.position() > second.position(),
        "the write after the commit publishes above it"
    );

    server.graceful_shutdown().await;
}

#[tokio::test]
async fn rolled_back_transaction_publishes_nothing() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION txn_cdc_rb (id TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION txn_cdc_rb");

    let mut sub = server
        .shared
        .change_stream
        .subscribe(Some("txn_cdc_rb".into()), None);

    server.exec("BEGIN").await.expect("BEGIN");
    server
        .exec("INSERT INTO txn_cdc_rb (id, status) VALUES ('r1', 'new')")
        .await
        .expect("INSERT in transaction");
    server.exec("ROLLBACK").await.expect("ROLLBACK");

    server
        .exec("INSERT INTO txn_cdc_rb (id, status) VALUES ('s1', 'sentinel')")
        .await
        .expect("sentinel INSERT");

    let event = next_event(&mut sub, "the sentinel").await;
    assert_eq!(
        event.document_id.as_str(),
        "s1",
        "a rolled-back transaction published a change event"
    );

    server.graceful_shutdown().await;
}

/// Several writes to one row in one transaction publish one event with the
/// row's net kind:
/// - insert, then update: `Insert`;
/// - update of a committed row: `Update`;
/// - update, then delete: `Delete`;
/// - insert, then delete: nothing.
#[tokio::test]
async fn transaction_writes_publish_their_net_kind() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION txn_net (id TEXT PRIMARY KEY, status TEXT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("CREATE COLLECTION txn_net");

    let mut sub = server
        .shared
        .change_stream
        .subscribe(Some("txn_net".into()), None);

    for id in ["u1", "d1"] {
        server
            .exec(&format!(
                "INSERT INTO txn_net (id, status) VALUES ('{id}', 'new')"
            ))
            .await
            .expect("seed INSERT");
        let seed = next_event(&mut sub, "a seed row").await;
        assert_eq!(seed.document_id.as_str(), id);
    }

    server.exec("BEGIN").await.expect("BEGIN");
    for sql in [
        "INSERT INTO txn_net (id, status) VALUES ('n1', 'new')",
        "UPDATE txn_net SET status = 'changed' WHERE id = 'n1'",
        "UPDATE txn_net SET status = 'changed' WHERE id = 'u1'",
        "UPDATE txn_net SET status = 'changed' WHERE id = 'd1'",
        "DELETE FROM txn_net WHERE id = 'd1'",
        "INSERT INTO txn_net (id, status) VALUES ('x1', 'new')",
        "DELETE FROM txn_net WHERE id = 'x1'",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    server.exec("COMMIT").await.expect("COMMIT");
    server
        .exec("INSERT INTO txn_net (id, status) VALUES ('s1', 'sentinel')")
        .await
        .expect("sentinel INSERT");

    let mut published = BTreeSet::new();
    loop {
        let event = next_event(&mut sub, "the committed rows and the sentinel").await;
        if event.document_id.as_str() == "s1" {
            break;
        }
        assert!(
            published.insert((
                event.document_id.as_str().to_owned(),
                event.operation.as_str()
            )),
            "a row published more than once: {}",
            event.document_id.as_str()
        );
    }
    assert_eq!(
        published,
        BTreeSet::from([
            ("n1".to_owned(), "INSERT"),
            ("u1".to_owned(), "UPDATE"),
            ("d1".to_owned(), "DELETE"),
        ]),
        "each row publishes its net kind, and the inserted-then-deleted row nothing"
    );

    server.graceful_shutdown().await;
}
