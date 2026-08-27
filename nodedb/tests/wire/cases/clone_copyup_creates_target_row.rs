// SPDX-License-Identifier: BUSL-1.1

//! UPDATE on a source-only row in a clone must copy the source row into
//! target storage (copy-up) so the clone reads the new value while the
//! source keeps its own.

use crate::harness::TestServer;

/// After an UPDATE on a row that originally exists only in source, the row must
/// be readable from the clone with the new value — proving the copy-up completed.
#[tokio::test]
async fn update_source_only_row_creates_copyup_in_target() {
    let server = TestServer::start().await;
    let client = &*server.client;

    // Source: one row.
    client
        .simple_query("CREATE DATABASE src_cup")
        .await
        .expect("CREATE DATABASE src_cup");
    client
        .simple_query("USE DATABASE src_cup")
        .await
        .expect("USE src_cup");
    client
        .simple_query(
            "CREATE COLLECTION products (key STRING PRIMARY KEY, price STRING) WITH (engine='kv')",
        )
        .await
        .expect("CREATE COLLECTION products");
    client
        .simple_query("INSERT INTO products (key, price) VALUES ('p1', '10')")
        .await
        .expect("INSERT p1");

    // Clone at LATEST — p1 is only in source at this point.
    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE clone_cup FROM src_cup LATEST")
        .await
        .expect("CLONE DATABASE");

    // Switch to clone and UPDATE the source-only row.
    client
        .simple_query("USE DATABASE clone_cup")
        .await
        .expect("USE clone_cup");
    client
        .simple_query("UPDATE products SET price = '99' WHERE key = 'p1'")
        .await
        .expect("UPDATE p1 in clone");

    // Clone must see the updated price.
    let msgs = client
        .simple_query("SELECT price FROM products WHERE key = 'p1'")
        .await
        .expect("SELECT price from clone");

    let mut found_price: Option<String> = None;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            found_price = row.get(0).map(|s| s.to_string());
        }
    }
    assert_eq!(
        found_price.as_deref(),
        Some("99"),
        "clone must see updated price '99' after copy-up; got {found_price:?}"
    );

    // Source is unaffected: the copy-up must not mutate source storage.
    client
        .simple_query("USE DATABASE src_cup")
        .await
        .expect("USE src_cup");
    let msgs = client
        .simple_query("SELECT price FROM products WHERE key = 'p1'")
        .await
        .expect("SELECT price from source");

    let mut src_price: Option<String> = None;
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            src_price = row.get(0).map(|s| s.to_string());
        }
    }
    assert_eq!(
        src_price.as_deref(),
        Some("10"),
        "source must still have original price '10'; got {src_price:?}"
    );
}
