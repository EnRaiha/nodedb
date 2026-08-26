// SPDX-License-Identifier: BUSL-1.1

//! A write in a shadowed clone must leave exactly one visible row per key.
//!
//! The clone read path concatenates the target's rows with the source's, so
//! every write that supersedes a source row has to record a suppression entry
//! for it — a tombstone for a delete or an insert over the same key, a copy-up
//! mapping for an update. Without one the superseded source row is merged back
//! in and the key comes back twice.
//!
//! Each test asserts the ROW COUNT. Merge order puts target rows first, so a
//! first-column assertion alone reports the right value while hiding the
//! duplicate behind it.

use crate::harness::TestServer;

fn first_value(msgs: &[tokio_postgres::SimpleQueryMessage]) -> Option<String> {
    for msg in msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row.get(0).map(|s| s.to_owned());
        }
    }
    None
}

fn row_count(msgs: &[tokio_postgres::SimpleQueryMessage]) -> usize {
    msgs.iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count()
}

/// Document engine: UPDATE of a source-only row copies it up; the source copy
/// must not also be returned.
#[tokio::test]
async fn document_update_in_clone_returns_one_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cws_upd_src")
        .await
        .expect("CREATE DATABASE cws_upd_src");
    client
        .simple_query("USE DATABASE cws_upd_src")
        .await
        .expect("USE cws_upd_src");
    client
        .simple_query(
            "CREATE COLLECTION notes (id STRING PRIMARY KEY, body STRING) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    client
        .simple_query("INSERT INTO notes (id, body) VALUES ('n1', 'original')")
        .await
        .expect("INSERT n1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cws_upd_clone FROM cws_upd_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("USE DATABASE cws_upd_clone")
        .await
        .expect("USE cws_upd_clone");
    client
        .simple_query("UPDATE notes SET body = 'updated' WHERE id = 'n1'")
        .await
        .expect("UPDATE in clone");

    let msgs = client
        .simple_query("SELECT body FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT in clone after update");
    assert_eq!(
        row_count(&msgs),
        1,
        "clone must return one row after UPDATE; the superseded source row must be suppressed"
    );
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("updated"),
        "clone must see the updated value"
    );

    // The source is untouched.
    client
        .simple_query("USE DATABASE cws_upd_src")
        .await
        .expect("USE cws_upd_src");
    let msgs = client
        .simple_query("SELECT body FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT in source");
    assert_eq!(
        row_count(&msgs),
        1,
        "source must still hold exactly one row"
    );
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("original"),
        "source must keep its original value"
    );
}

/// Document engine: DELETE of a source-only row tombstones it; the clone must
/// return nothing.
#[tokio::test]
async fn document_delete_in_clone_returns_no_rows() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cws_del_src")
        .await
        .expect("CREATE DATABASE cws_del_src");
    client
        .simple_query("USE DATABASE cws_del_src")
        .await
        .expect("USE cws_del_src");
    client
        .simple_query(
            "CREATE COLLECTION notes (id STRING PRIMARY KEY, body STRING) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    client
        .simple_query("INSERT INTO notes (id, body) VALUES ('n1', 'doomed')")
        .await
        .expect("INSERT n1");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cws_del_clone FROM cws_del_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("USE DATABASE cws_del_clone")
        .await
        .expect("USE cws_del_clone");
    client
        .simple_query("DELETE FROM notes WHERE id = 'n1'")
        .await
        .expect("DELETE in clone");

    let msgs = client
        .simple_query("SELECT body FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT in clone after delete");
    assert_eq!(
        row_count(&msgs),
        0,
        "clone must return no rows after DELETE of a source-only row"
    );

    client
        .simple_query("USE DATABASE cws_del_src")
        .await
        .expect("USE cws_del_src");
    let msgs = client
        .simple_query("SELECT body FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT in source after clone delete");
    assert_eq!(
        row_count(&msgs),
        1,
        "source must still hold the row after a clone DELETE"
    );
}

/// Document engine: INSERT into the clone of a key the source also holds.
/// The clone's own row wins and the source row must be suppressed.
#[tokio::test]
async fn document_insert_of_source_key_returns_one_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cws_ins_src")
        .await
        .expect("CREATE DATABASE cws_ins_src");
    client
        .simple_query("USE DATABASE cws_ins_src")
        .await
        .expect("USE cws_ins_src");
    client
        .simple_query(
            "CREATE COLLECTION notes (id STRING PRIMARY KEY, body STRING) \
             WITH (engine='document_schemaless')",
        )
        .await
        .expect("CREATE COLLECTION notes");
    client
        .simple_query("INSERT INTO notes (id, body) VALUES ('n1', 'from-source')")
        .await
        .expect("INSERT n1 in source");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cws_ins_clone FROM cws_ins_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("USE DATABASE cws_ins_clone")
        .await
        .expect("USE cws_ins_clone");
    client
        .simple_query("INSERT INTO notes (id, body) VALUES ('n1', 'from-clone')")
        .await
        .expect("INSERT n1 in clone");

    let msgs = client
        .simple_query("SELECT body FROM notes WHERE id = 'n1'")
        .await
        .expect("SELECT in clone after insert");
    assert_eq!(
        row_count(&msgs),
        1,
        "clone must return one row for a key it inserted over a source row"
    );
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("from-clone"),
        "the clone's own row must win"
    );
}

/// KV engine: INSERT into the clone of a key the source also holds.
#[tokio::test]
async fn kv_insert_of_source_key_returns_one_row() {
    let server = TestServer::start().await;
    let client = &*server.client;

    client
        .simple_query("CREATE DATABASE cws_kv_src")
        .await
        .expect("CREATE DATABASE cws_kv_src");
    client
        .simple_query("USE DATABASE cws_kv_src")
        .await
        .expect("USE cws_kv_src");
    client
        .simple_query("CREATE COLLECTION items (k STRING PRIMARY KEY, v STRING) WITH (engine='kv')")
        .await
        .expect("CREATE COLLECTION items");
    client
        .simple_query("INSERT INTO items (k, v) VALUES ('k1', 'from-source')")
        .await
        .expect("INSERT k1 in source");

    client
        .simple_query("USE DATABASE default")
        .await
        .expect("USE default");
    client
        .simple_query("CLONE DATABASE cws_kv_clone FROM cws_kv_src")
        .await
        .expect("CLONE DATABASE");

    client
        .simple_query("USE DATABASE cws_kv_clone")
        .await
        .expect("USE cws_kv_clone");
    client
        .simple_query("INSERT INTO items (k, v) VALUES ('k1', 'from-clone')")
        .await
        .expect("INSERT k1 in clone");

    let msgs = client
        .simple_query("SELECT v FROM items WHERE k = 'k1'")
        .await
        .expect("SELECT in clone after insert");
    assert_eq!(
        row_count(&msgs),
        1,
        "clone must return one row for a key it inserted over a source row"
    );
    assert_eq!(
        first_value(&msgs).as_deref(),
        Some("from-clone"),
        "the clone's own row must win"
    );
}
