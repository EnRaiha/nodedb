// SPDX-License-Identifier: BUSL-1.1

//! A `document_strict` `JSON` or `ARRAY` column advertises `json` (OID 114)
//! and its cells travel as JSON text. The simple protocol, the extended
//! protocol's Describe, and its row encoder agree on that one type.

use std::error::Error;

use tokio_postgres::types::{FromSql, Type};

use crate::harness::TestServer;

/// The JSON text a `doc JSON` cell renders.
const DOC_JSON: &str = r#"{"a":[1,2]}"#;
/// The JSON text a `tags ARRAY` cell renders.
const TAGS_JSON: &str = r#"["x","y"]"#;

/// The raw text of a `json` cell, read as the bytes the server sent.
struct JsonText(String);

impl<'a> FromSql<'a> for JsonText {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(Self(std::str::from_utf8(raw)?.to_owned()))
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSON
    }
}

/// Create the strict collection and store one row with a JSON and an
/// ARRAY cell.
async fn seed(server: &TestServer) {
    server
        .exec(
            "CREATE COLLECTION strict_structured \
             (id TEXT PRIMARY KEY, doc JSON, tags ARRAY) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create strict_structured");
    server
        .exec(&format!(
            "INSERT INTO strict_structured (id, doc, tags) \
             VALUES ('r1', '{DOC_JSON}', ARRAY['x', 'y'])"
        ))
        .await
        .expect("insert into strict_structured");
}

/// The simple protocol renders each structured cell as its JSON text, not
/// as `\x` hex.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_structured_columns_render_json_text_over_simple_query() {
    let server = TestServer::start().await;
    seed(&server).await;

    let rows = server
        .query_rows("SELECT doc, tags FROM strict_structured WHERE id = 'r1'")
        .await
        .expect("simple-query SELECT of structured columns");
    assert_eq!(
        rows,
        vec![vec![DOC_JSON.to_string(), TAGS_JSON.to_string()]],
        "a JSON and an ARRAY cell render their JSON text"
    );
}

/// Describe advertises `json` for both columns, and the extended protocol
/// sends each cell as its JSON text.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_structured_columns_advertise_json_over_extended_query() {
    let server = TestServer::start().await;
    seed(&server).await;

    let stmt = server
        .client
        .prepare("SELECT doc, tags FROM strict_structured WHERE id = $1")
        .await
        .expect("prepare SELECT of structured columns");
    let described: Vec<&Type> = stmt.columns().iter().map(|c| c.type_()).collect();
    assert_eq!(
        described,
        vec![&Type::JSON, &Type::JSON],
        "Describe advertises json for a JSON and an ARRAY column"
    );

    let rows = server
        .client
        .query(&stmt, &[&"r1"])
        .await
        .expect("extended-query SELECT of structured columns");
    assert_eq!(rows.len(), 1, "one stored row");
    assert_eq!(
        rows[0].columns()[0].type_(),
        &Type::JSON,
        "the row's doc field is json"
    );
    assert_eq!(rows[0].get::<_, JsonText>("doc").0, DOC_JSON);
    assert_eq!(rows[0].get::<_, JsonText>("tags").0, TAGS_JSON);
}
