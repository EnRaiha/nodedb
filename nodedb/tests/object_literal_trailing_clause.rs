// SPDX-License-Identifier: BUSL-1.1

//! The object-literal write forms refuse what they cannot carry.
//!
//! `INSERT INTO c { … }` and `UPSERT INTO c { … }` are rewritten to standard
//! SQL by reconstructing the statement from the parsed fields. Nothing written
//! after the literal survives that reconstruction, so a trailing `ON CONFLICT`
//! — or trailing text that is not a clause at all — has nowhere to go.
//!
//! Such a statement used to succeed with the clause quietly removed — a write
//! that applied, an empty result set, and no indication that half of what the
//! author wrote had been discarded. These tests pin that it now fails instead,
//! naming the clause, and that the write does not apply: a refusal that still
//! wrote the row would be the same failure wearing an error message.
//!
//! The limit itself is deliberate. Carrying a clause is not a matter of
//! appending text: the INSERT handler rebuilds its SQL from the parsed fields a
//! second time, and the downstream `(cols) VALUES (…)` scanner locates the value
//! list by searching backwards for `)`, which `ON CONFLICT (id)` would capture.
//! Supporting trailing clauses means rebuilding that pipeline; until then the
//! honest answer is to say so.
//!
//! `RETURNING` is the exception, and deliberately so: it is split off the text
//! before the rewrite and re-attached to the rebuilt statement, so it survives
//! the reconstruction rather than being leftover input. Every write form
//! therefore answers it with the stored rows — pinned below, because "the clause
//! is carried" and "the clause is quietly dropped" both look like success from
//! the outside and only the returned rows tell them apart.

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

/// A trailing clause the rewrite cannot carry is refused on every form that
/// reaches it: single object, array batch, and the UPSERT keyword.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clause_after_the_object_literal_is_refused_and_nothing_is_written() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_trail")
        .await
        .expect("create collection");

    for (expected, sql) in [
        (
            "ON CONFLICT",
            "INSERT INTO objlit_trail { id: 't3', owner: 'alice' } ON CONFLICT (id) DO NOTHING",
        ),
        (
            "ON CONFLICT",
            "UPSERT INTO objlit_trail { id: 't5', owner: 'alice' } ON CONFLICT (id) DO NOTHING",
        ),
        (
            "ON CONFLICT",
            "INSERT INTO objlit_trail [{ id: 't6', owner: 'alice' }] ON CONFLICT (id) DO NOTHING",
        ),
    ] {
        assert_refused_and_unwritten(&server, "objlit_trail", sql, expected).await;
    }
}

/// `RETURNING` after an object literal is CARRIED, not dropped: every form
/// answers with the stored row.
///
/// The rows are the assertion, not the absence of an error. A statement that
/// applied its write and quietly discarded the clause also "succeeds" — it just
/// hands back nothing — and that silent drop is the original defect this whole
/// file exists to prevent. Asserting the row content is the only thing that
/// tells the two apart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_after_an_object_literal_is_carried_and_answers_with_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION objlit_ret_ok")
        .await
        .expect("create collection");

    for (id, sql) in [
        (
            "r1",
            "INSERT INTO objlit_ret_ok { id: 'r1', owner: 'alice' } RETURNING id",
        ),
        (
            "r2",
            "UPSERT INTO objlit_ret_ok { id: 'r2', owner: 'alice' } RETURNING id",
        ),
        (
            "r3",
            "INSERT INTO objlit_ret_ok [{ id: 'r3', owner: 'alice' }] RETURNING id",
        ),
    ] {
        let rows = server
            .query_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("`{sql}` must return its row: {e}"));
        assert_eq!(
            rows,
            vec![vec![id.to_string()]],
            "`{sql}` must answer with the stored row, not an empty success"
        );
    }

    assert_eq!(
        server
            .query_rows("SELECT id FROM objlit_ret_ok ORDER BY id")
            .await
            .expect("read back objlit_ret_ok"),
        vec![
            vec!["r1".to_string()],
            vec!["r2".to_string()],
            vec!["r3".to_string()]
        ],
        "every returning write must also have applied"
    );
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
