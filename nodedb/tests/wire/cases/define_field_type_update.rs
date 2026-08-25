// SPDX-License-Identifier: BUSL-1.1

//! Regression coverage: DEFINE FIELD on an existing field must update the type
//! in both `fields` (the schema-structure list) and `field_defs` (the behavior
//! list). Previously only `field_defs` was updated — `fields` kept the old type.

use crate::harness::TestServer;

/// Re-defining a field with a new TYPE must update the type visible in
/// DESCRIBE output, which reads from `coll.fields`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn define_field_changes_type_updates_simple_fields_list() {
    let server = TestServer::start().await;

    server
        .exec(
            "CREATE COLLECTION df_type_coll (\
                id TEXT PRIMARY KEY) WITH (engine='document_strict')",
        )
        .await
        .unwrap();

    // First definition: field 'score' typed as 'any'.
    server
        .exec("DEFINE FIELD score ON df_type_coll TYPE any")
        .await
        .unwrap();

    // Re-define 'score' with a more specific type.
    server
        .exec("DEFINE FIELD score ON df_type_coll TYPE float")
        .await
        .unwrap();

    // DESCRIBE returns (field, type, nullable) rows. Parse the type column for
    // the 'score' row. DESCRIBE reads from coll.fields, so this directly
    // validates that the simple fields list was updated.
    let msgs = server
        .client
        .simple_query("DESCRIBE df_type_coll")
        .await
        .unwrap();

    let mut score_type: Option<String> = None;
    for msg in &msgs {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            let col0 = row.get(0).unwrap_or("");
            if col0 == "score" {
                score_type = Some(row.get(1).unwrap_or("").to_string());
            }
        }
    }

    assert!(
        score_type.is_some(),
        "field 'score' should appear in DESCRIBE output after DEFINE FIELD"
    );

    let ft = score_type.unwrap();
    assert!(
        ft.to_lowercase().contains("float"),
        "field type in coll.fields should be 'float' after re-definition, got: {ft:?}"
    );
}
