// SPDX-License-Identifier: BUSL-1.1

//! The DSL insert `INSERT INTO c { ... }` writes a numeric-array field no
//! vector index covers as a vector insert beside the document. Inside
//! BEGIN..COMMIT the vector insert joins the transaction: COMMIT keeps it
//! with the document, and ROLLBACK drops both.

use crate::harness::TestServer;

async fn nearest(server: &TestServer) -> Vec<String> {
    server
        .query_text(
            "SELECT id FROM dsl_vectors \
             ORDER BY vector_distance(emb, ARRAY[1.0, 0.0]) LIMIT 5",
        )
        .await
        .unwrap_or_else(|e| panic!("vector search: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dsl_vector_insert_commits_and_rolls_back_with_its_block() {
    let server = TestServer::start().await;
    server.exec("CREATE COLLECTION dsl_vectors").await.unwrap();

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO dsl_vectors { id: 'gone', emb: [1.0, 0.0] }")
        .await
        .unwrap();
    server.exec("ROLLBACK").await.unwrap();
    assert!(
        nearest(&server).await.is_empty(),
        "a rolled-back vector insert leaves no vector"
    );
    assert!(
        server
            .query_text("SELECT id FROM dsl_vectors")
            .await
            .unwrap()
            .is_empty(),
        "a rolled-back document insert leaves no row"
    );

    server.exec("BEGIN").await.unwrap();
    server
        .exec("INSERT INTO dsl_vectors { id: 'kept', emb: [1.0, 0.0] }")
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();
    assert_eq!(nearest(&server).await, vec!["kept".to_string()]);
}
