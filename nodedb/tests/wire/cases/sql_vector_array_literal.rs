// SPDX-License-Identifier: BUSL-1.1

//! `ARRAY[...]` literals written to a declared `VECTOR(dim)` column.
//!
//! A fractional literal resolves to an exact decimal in the planner. Each
//! element must reach the engine as a float, so every engine stores the
//! vector the client wrote. An element that is not a number is refused with
//! SQLSTATE `42804`, naming the column.

use crate::harness::TestServer;

/// The numbers of a rendered vector cell: `{0.1,0.2}` or `[0.1,0.2]`.
fn vector_numbers(cell: &str) -> Vec<f64> {
    cell.trim_matches(|c| matches!(c, '[' | ']' | '{' | '}'))
        .split(',')
        .map(|n| {
            n.trim()
                .parse::<f64>()
                .unwrap_or_else(|e| panic!("vector element {n:?} of {cell:?}: {e}"))
        })
        .collect()
}

/// Read the `embedding` of row `id` and compare it to `expected` within
/// `f32` precision.
async fn assert_embedding(server: &TestServer, collection: &str, id: &str, expected: &[f64]) {
    let rows = server
        .query_text(&format!(
            "SELECT embedding FROM {collection} WHERE id = '{id}'"
        ))
        .await
        .unwrap_or_else(|e| panic!("SELECT {collection} {id}: {e}"));
    assert_eq!(
        rows.len(),
        1,
        "{collection} {id}: expected one row, got {rows:?}"
    );
    let got = vector_numbers(&rows[0]);
    assert_eq!(
        got.len(),
        expected.len(),
        "{collection} {id}: element count of {:?}",
        rows[0]
    );
    for (g, e) in got.iter().zip(expected) {
        assert!(
            (g - e).abs() < 1e-6,
            "{collection} {id}: stored {:?}, expected {expected:?}",
            rows[0]
        );
    }
}

async fn create(server: &TestServer, name: &str, engine: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, embedding VECTOR(3)) \
             WITH (engine = '{engine}')"
        ))
        .await
        .unwrap_or_else(|e| panic!("CREATE {name}: {e}"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_vector_accepts_a_fractional_array() {
    let server = TestServer::start().await;
    create(&server, "vec_strict_frac", "document_strict").await;

    server
        .exec("INSERT INTO vec_strict_frac (id, embedding) VALUES ('a1', ARRAY[0.1, 0.2, 0.3])")
        .await
        .expect("fractional ARRAY into VECTOR(3)");
    assert_embedding(&server, "vec_strict_frac", "a1", &[0.1, 0.2, 0.3]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_vector_accepts_a_mixed_integer_and_fractional_array() {
    let server = TestServer::start().await;
    create(&server, "vec_strict_mixed", "document_strict").await;

    server
        .exec("INSERT INTO vec_strict_mixed (id, embedding) VALUES ('m1', ARRAY[1, 0.5, -2])")
        .await
        .expect("mixed ARRAY into VECTOR(3)");
    assert_embedding(&server, "vec_strict_mixed", "m1", &[1.0, 0.5, -2.0]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_vector_update_accepts_a_fractional_array() {
    let server = TestServer::start().await;
    create(&server, "vec_strict_upd", "document_strict").await;

    server
        .exec("INSERT INTO vec_strict_upd (id, embedding) VALUES ('u1', ARRAY[1, 2, 3])")
        .await
        .expect("integer ARRAY into VECTOR(3)");
    server
        .exec("UPDATE vec_strict_upd SET embedding = ARRAY[0.25, 0.5, 0.75] WHERE id = 'u1'")
        .await
        .expect("SET a fractional ARRAY on VECTOR(3)");
    assert_embedding(&server, "vec_strict_upd", "u1", &[0.25, 0.5, 0.75]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_vector_refuses_a_non_numeric_element() {
    let server = TestServer::start().await;
    create(&server, "vec_strict_bad", "document_strict").await;

    let err = server
        .exec("INSERT INTO vec_strict_bad (id, embedding) VALUES ('b1', ARRAY[0.1, 'abc', 0.3])")
        .await
        .expect_err("a text element is not a vector element");
    assert!(err.contains("42804"), "expected SQLSTATE 42804: {err}");
    assert!(
        err.contains("embedding"),
        "the error names the column: {err}"
    );

    let err = server
        .exec("INSERT INTO vec_strict_bad (id, embedding) VALUES ('b2', ARRAY[0.1, '0.2', 0.3])")
        .await
        .expect_err("numeric text is still text");
    assert!(err.contains("42804"), "expected SQLSTATE 42804: {err}");

    let rows = server
        .query_text("SELECT id FROM vec_strict_bad")
        .await
        .expect("SELECT vec_strict_bad");
    assert!(rows.is_empty(), "a refused row is never stored: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_vector_refuses_a_fractional_array_of_the_wrong_length() {
    let server = TestServer::start().await;
    create(&server, "vec_strict_len", "document_strict").await;

    let err = server
        .exec("INSERT INTO vec_strict_len (id, embedding) VALUES ('l1', ARRAY[0.1, 0.2])")
        .await
        .expect_err("two elements for VECTOR(3)");
    assert!(err.contains("22000"), "expected SQLSTATE 22000: {err}");
    assert!(
        err.contains("got 2 elements"),
        "the error counts the elements: {err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_vector_stores_a_fractional_array_as_numbers() {
    let server = TestServer::start().await;
    create(&server, "vec_loose_frac", "document_schemaless").await;

    server
        .exec("INSERT INTO vec_loose_frac (id, embedding) VALUES ('s1', ARRAY[0.1, 0.2, 0.3])")
        .await
        .expect("fractional ARRAY into a schemaless VECTOR(3)");
    assert_embedding(&server, "vec_loose_frac", "s1", &[0.1, 0.2, 0.3]).await;

    let rows = server
        .query_text("SELECT embedding FROM vec_loose_frac WHERE id = 's1'")
        .await
        .expect("SELECT vec_loose_frac");
    assert!(
        !rows[0].contains('"'),
        "the elements are stored as numbers, not text: {:?}",
        rows[0]
    );

    let err = server
        .exec("INSERT INTO vec_loose_frac (id, embedding) VALUES ('s2', ARRAY[0.1, 'abc', 0.3])")
        .await
        .expect_err("a text element is not a vector element");
    assert!(err.contains("42804"), "expected SQLSTATE 42804: {err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_vector_accepts_a_fractional_array() {
    let server = TestServer::start().await;
    create(&server, "vec_col_frac", "columnar").await;

    server
        .exec("INSERT INTO vec_col_frac (id, embedding) VALUES ('c1', ARRAY[0.1, 0.2, 0.3])")
        .await
        .expect("fractional ARRAY into a columnar VECTOR(3)");
    assert_embedding(&server, "vec_col_frac", "c1", &[0.1, 0.2, 0.3]).await;
}
