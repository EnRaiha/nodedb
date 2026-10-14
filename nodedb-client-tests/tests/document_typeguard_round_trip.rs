// SPDX-License-Identifier: BUSL-1.1

//! A native document write is held to the collection's typeguards and
//! generated columns, as a SQL write is.
//!
//! A native put reaches the Data Plane without the SQL planner. The typeguard
//! hook on the Data Plane judges its body all the same, and a body that
//! supplies a generated column is refused before dispatch.

use nodedb_client::native::pool::PoolConfig;
use nodedb_client::{Document, NativeClient, NodeDb, Value};
use nodedb_test_support::pgwire_harness::TestServer;

fn native(server: &TestServer) -> NativeClient {
    NativeClient::new(PoolConfig::new(
        format!("127.0.0.1:{}", server.native_port),
        nodedb_types::protocol::AuthMethod::Trust {
            username: "nodedb".into(),
        },
    ))
}

fn document(id: &str, fields: &[(&str, &str)]) -> Document {
    let mut doc = Document::new(id);
    for (name, value) in fields {
        doc.set(*name, Value::String((*value).into()));
    }
    doc
}

#[tokio::test]
async fn native_put_enforces_typeguards() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION tg_native").await.unwrap();
    server
        .exec(
            "CREATE TYPEGUARD ON tg_native (\
                 email STRING REQUIRED, \
                 slug STRING VALUE LOWER(title)\
             )",
        )
        .await
        .unwrap();
    let native = native(&server);

    native
        .document_put("tg_native", document("n1", &[("title", "Hi")]))
        .await
        .expect_err("a put without the required email must be refused");

    native
        .document_put(
            "tg_native",
            document("n2", &[("title", "Native Row"), ("email", "a@b.c")]),
        )
        .await
        .expect("a valid put lands");
    let read = native
        .document_get("tg_native", "n2")
        .await
        .expect("document_get")
        .expect("the written document exists");
    assert_eq!(
        read.get("slug"),
        Some(&Value::String("native row".into())),
        "the guard VALUE must inject: {read:?}"
    );
}

#[tokio::test]
async fn native_put_supplying_a_generated_column_is_refused() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION gen_native (\
                 id TEXT PRIMARY KEY, price TEXT, \
                 label TEXT GENERATED ALWAYS AS (LOWER(price))) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    let native = native(&server);

    native
        .document_put(
            "gen_native",
            document("g1", &[("price", "TEN"), ("label", "mine")]),
        )
        .await
        .expect_err("a put that supplies a generated column must be refused");
}
