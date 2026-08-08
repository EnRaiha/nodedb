// SPDX-License-Identifier: BUSL-1.1

//! The object-literal write forms refuse what they cannot carry.
//!
//! `INSERT INTO c { … }` and `UPSERT INTO c { … }` are rewritten to standard
//! SQL by reconstructing the statement from the parsed fields. Nothing written
//! after the literal survives that reconstruction, so a trailing `RETURNING` or
//! `ON CONFLICT` has nowhere to go.
//!
//! Such a statement used to succeed with the clause quietly removed — a write
//! that applied, an empty result set, and no indication that half of what the
//! author wrote had been discarded. These tests pin that it now fails instead,
//! naming the clause, and that the write does not apply: a refusal that still
//! wrote the row would be the same failure wearing an error message.
//!
//! The limit itself is deliberate. Carrying the clause is not a matter of
//! appending text: the downstream `(cols) VALUES (…)` scanner locates the value
//! list by searching backwards for `)`, which `RETURNING upper(x)` or
//! `ON CONFLICT (id)` would capture, and the INSERT handler rebuilds its SQL
//! from the parsed fields a second time. Supporting trailing clauses means
//! rebuilding that pipeline; until then the honest answer is to say so. The
//! `(cols) VALUES (…)` form takes these clauses today and is what the error
//! points authors at.

mod common;

use common::pgwire_harness::TestServer;

/// Assert `sql` is refused with a message naming `expected`, and that it wrote
/// nothing.
async fn assert_refused_and_unwritten(
    server: &TestServer,
    collection: &str,
    sql: &str,
    expected: &str,
) {
    match server.exec(sql).await {
        Ok(()) => panic!("`{sql}` must be refused, but it succeeded"),
        Err(message) => assert!(
            message.contains(expected),
            "the refusal must name what it could not account for; sql = {sql}, got: {message}"
        ),
    }
    assert!(
        server
            .query_rows(&format!("SELECT id FROM {collection}"))
            .await
            .unwrap_or_else(|e| panic!("read back {collection}: {e}"))
            .is_empty(),
        "a refused statement must not have written its row: {sql}"
    );
}

/// Every trailing clause the brace form cannot carry, on every form that
/// reaches the rewrite: single object, array batch, and the UPSERT keyword.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clause_after_the_object_literal_is_refused_and_nothing_is_written() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_trail")
        .await
        .expect("create collection");

    for (expected, sql) in [
        (
            "RETURNING",
            "INSERT INTO objlit_trail { id: 't1', owner: 'alice' } RETURNING *",
        ),
        (
            "RETURNING",
            "UPSERT INTO objlit_trail { id: 't2', owner: 'alice' } RETURNING *",
        ),
        (
            "ON CONFLICT",
            "INSERT INTO objlit_trail { id: 't3', owner: 'alice' } ON CONFLICT (id) DO NOTHING",
        ),
        (
            "RETURNING",
            "INSERT INTO objlit_trail [{ id: 't4', owner: 'alice' }] RETURNING *",
        ),
    ] {
        assert_refused_and_unwritten(&server, "objlit_trail", sql, expected).await;
    }
}

/// Trailing text that is not even a clause is refused the same way, rather than
/// being treated as an elaborate no-op.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn trailing_garbage_after_the_object_literal_does_not_vanish() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_garbage")
        .await
        .expect("create collection");

    assert_refused_and_unwritten(
        &server,
        "objlit_garbage",
        "INSERT INTO objlit_garbage { id: 'g1', owner: 'alice' } WHAT IS THIS",
        "WHAT IS THIS",
    )
    .await;
}

/// A `}` inside a quoted value is part of the value, not the end of the
/// literal, so tightening the trailing-input check must not start rejecting
/// statements that were always valid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_brace_inside_a_quoted_value_is_still_accepted() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_brace")
        .await
        .expect("create collection");

    server
        .exec("INSERT INTO objlit_brace { id: 'b1', note: '} not the end' }")
        .await
        .expect("a brace inside a string belongs to the value");
    server
        .exec("INSERT INTO objlit_brace [{ id: 'b2', note: ']x[' }]")
        .await
        .expect("a bracket inside a string belongs to the value");

    assert_eq!(
        server
            .query_rows("SELECT id FROM objlit_brace ORDER BY id")
            .await
            .expect("read back objlit_brace"),
        vec![vec!["b1".to_string()], vec!["b2".to_string()]],
    );
}

/// A statement terminator is not a clause, and the clean forms still work — the
/// tightening must not turn ordinary object-literal writes into errors.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_clean_forms_and_a_trailing_semicolon_still_apply() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_clean")
        .await
        .expect("create collection");

    for sql in [
        "INSERT INTO objlit_clean { id: 'c1', n: 1 }",
        "INSERT INTO objlit_clean { id: 'c2', n: 2 };",
        "UPSERT INTO objlit_clean { id: 'c2', n: 3 }",
        "INSERT INTO objlit_clean [{ id: 'c3', n: 4 }, { id: 'c4', n: 5 }]",
        "INSERT INTO objlit_clean [{ id: 'c5', n: 6 }];",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must apply: {e}"));
    }

    assert_eq!(
        server
            .query_rows("SELECT id FROM objlit_clean ORDER BY id")
            .await
            .expect("read back objlit_clean")
            .len(),
        5,
        "every clean form must have written exactly one row"
    );
}
