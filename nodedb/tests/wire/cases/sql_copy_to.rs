// SPDX-License-Identifier: BUSL-1.1

//! Integration tests for `COPY <collection> TO '<path>'` and
//! `COPY (SELECT ...) TO '<path>'` bulk export.
//!
//! Spins up a full NodeDB server via the pgwire harness and exercises
//! NDJSON, JSON array, and CSV export paths, path-traversal rejection,
//! missing-collection rejection, and full round-trips through COPY FROM.

mod common;

use common::pgwire_harness::TestServer;

// ── helpers ─────────────────────────────────────────────────────────────────

/// Create a named temp file path that will be written by COPY TO.
/// Returns the path string; the file does not exist yet.
fn temp_output_path(suffix: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let path = dir.path().join(format!("output{suffix}"));
    (dir, path.to_string_lossy().into_owned())
}

/// Count rows in a collection via SELECT COUNT(*).
async fn count_rows(srv: &TestServer, collection: &str) -> i64 {
    let rows = srv
        .query_text(&format!("SELECT COUNT(*) FROM {collection}"))
        .await
        .expect("SELECT COUNT(*)");
    rows.first()
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0)
}

/// Seed a collection with N rows and return (collection_name, row_count).
async fn seed_collection(srv: &TestServer, name: &str) {
    srv.exec(&format!(
        "CREATE COLLECTION {name} (id INT, name TEXT, score FLOAT)"
    ))
    .await
    .expect("CREATE COLLECTION");
    srv.exec(&format!(
        "INSERT INTO {name} (id, name, score) VALUES (1, 'alice', 9.5)"
    ))
    .await
    .expect("INSERT 1");
    srv.exec(&format!(
        "INSERT INTO {name} (id, name, score) VALUES (2, 'bob', 8.1)"
    ))
    .await
    .expect("INSERT 2");
    srv.exec(&format!(
        "INSERT INTO {name} (id, name, score) VALUES (3, 'carol', 7.7)"
    ))
    .await
    .expect("INSERT 3");
}

// ── test 1: NDJSON export ────────────────────────────────────────────────────

#[tokio::test]
async fn copy_to_ndjson_basic() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_ndjson").await;

    let (_dir, out_path) = temp_output_path(".ndjson");
    srv.exec(&format!("COPY copy_to_ndjson TO '{out_path}'"))
        .await
        .expect("COPY TO NDJSON");

    let content = std::fs::read_to_string(&out_path).expect("read output file");
    // Each line must be a valid JSON object.
    let mut count = 0usize;
    for line in content.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_str(line)
            .unwrap_or_else(|e| panic!("invalid NDJSON line: {line}: {e}"));
        assert!(v.is_object(), "expected JSON object per line");
        count += 1;
    }
    assert_eq!(count, 3, "expected 3 rows in NDJSON output");
}

// ── test 2: JSON array export ────────────────────────────────────────────────

#[tokio::test]
async fn copy_to_json_array_basic() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_json").await;

    let (_dir, out_path) = temp_output_path(".json");
    srv.exec(&format!("COPY copy_to_json TO '{out_path}'"))
        .await
        .expect("COPY TO JSON");

    let content = std::fs::read_to_string(&out_path).expect("read output file");
    let arr: serde_json::Value =
        serde_json::from_str(&content).unwrap_or_else(|e| panic!("invalid JSON: {e}"));
    assert!(arr.is_array(), "expected JSON array output");
    assert_eq!(arr.as_array().unwrap().len(), 3);
}

// ── test 3: CSV with header ──────────────────────────────────────────────────

#[tokio::test]
async fn copy_to_csv_with_header() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_csv_hdr").await;

    let (_dir, out_path) = temp_output_path(".csv");
    srv.exec(&format!(
        "COPY copy_to_csv_hdr TO '{out_path}' WITH (FORMAT csv, HEADER true)"
    ))
    .await
    .expect("COPY TO CSV with header");

    let content = std::fs::read_to_string(&out_path).expect("read output file");
    let lines: Vec<&str> = content.lines().collect();
    // First line is the header.
    assert!(!lines.is_empty(), "expected at least a header line");
    // Should have header + 3 data rows.
    assert_eq!(lines.len(), 4, "expected header + 3 rows");
}

// ── test 4: CSV without header ───────────────────────────────────────────────

#[tokio::test]
async fn copy_to_csv_no_header() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_csv_nohdr").await;

    let (_dir, out_path) = temp_output_path(".csv");
    srv.exec(&format!(
        "COPY copy_to_csv_nohdr TO '{out_path}' WITH (FORMAT csv, HEADER false)"
    ))
    .await
    .expect("COPY TO CSV without header");

    let content = std::fs::read_to_string(&out_path).expect("read output file");
    let lines: Vec<&str> = content.lines().collect();
    // No header: should have exactly 3 rows.
    assert_eq!(lines.len(), 3, "expected 3 data rows, no header");
}

// ── test 5: COPY (SELECT ...) TO query-form ──────────────────────────────────

#[tokio::test]
async fn copy_to_query_form() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_query").await;

    let (_dir, out_path) = temp_output_path(".ndjson");
    // Export only rows where id < 3.
    srv.exec(&format!(
        "COPY (SELECT * FROM copy_to_query WHERE id < 3) TO '{out_path}'"
    ))
    .await
    .expect("COPY query-form TO NDJSON");

    let content = std::fs::read_to_string(&out_path).expect("read output file");
    let count = content.lines().filter(|l| !l.trim().is_empty()).count();
    assert_eq!(count, 2, "expected 2 rows matching id < 3");
}

// ── test 6: path traversal rejection ────────────────────────────────────────

#[tokio::test]
async fn copy_to_path_traversal_rejected() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_traversal").await;

    let result = srv
        .exec("COPY copy_to_traversal TO '/tmp/../etc/shadow.ndjson'")
        .await;
    assert!(result.is_err(), "expected error on path traversal");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("..") || msg.contains("traversal") || msg.contains("not permitted"),
        "expected path traversal error, got: {msg}"
    );
}

// ── test 7: missing collection rejection ─────────────────────────────────────

#[tokio::test]
async fn copy_to_missing_collection() {
    let srv = TestServer::start().await;

    let (_dir, out_path) = temp_output_path(".ndjson");
    let result = srv
        .exec(&format!("COPY no_such_collection_xyz TO '{out_path}'"))
        .await;
    assert!(
        result.is_err(),
        "expected error for non-existent collection"
    );
    let msg = result.unwrap_err();
    assert!(
        msg.contains("does not exist") || msg.contains("not found") || msg.contains("no_such"),
        "expected missing-collection error, got: {msg}"
    );
}

// ── test 8: round-trip COPY TO + COPY FROM ───────────────────────────────────

#[tokio::test]
async fn copy_to_from_roundtrip_ndjson() {
    let srv = TestServer::start().await;

    // Source collection.
    seed_collection(&srv, "copy_rt_src").await;

    // Export to temp file.
    let (_dir, out_path) = temp_output_path(".ndjson");
    srv.exec(&format!("COPY copy_rt_src TO '{out_path}'"))
        .await
        .expect("COPY TO NDJSON");

    // Import into a new collection.
    srv.exec("CREATE COLLECTION copy_rt_dst (id INT, name TEXT, score FLOAT)")
        .await
        .expect("CREATE COLLECTION dst");
    srv.exec(&format!("COPY copy_rt_dst FROM '{out_path}'"))
        .await
        .expect("COPY FROM NDJSON");

    // Verify row count matches.
    assert_eq!(count_rows(&srv, "copy_rt_dst").await, 3);
}

// ── test 9: round-trip via JSON array ────────────────────────────────────────

#[tokio::test]
async fn copy_to_from_roundtrip_json_array() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_json_rt_src").await;

    let (_dir, out_path) = temp_output_path(".json");
    srv.exec(&format!("COPY copy_json_rt_src TO '{out_path}'"))
        .await
        .expect("COPY TO JSON array");

    srv.exec("CREATE COLLECTION copy_json_rt_dst (id INT, name TEXT, score FLOAT)")
        .await
        .expect("CREATE COLLECTION dst");
    srv.exec(&format!("COPY copy_json_rt_dst FROM '{out_path}'"))
        .await
        .expect("COPY FROM JSON array");

    assert_eq!(count_rows(&srv, "copy_json_rt_dst").await, 3);
}

// ── test 10: COPY tag format matches PostgreSQL ───────────────────────────────

#[tokio::test]
async fn copy_to_returns_copy_tag() {
    let srv = TestServer::start().await;

    seed_collection(&srv, "copy_to_tag").await;

    let (_dir, out_path) = temp_output_path(".ndjson");
    // Execute via simple_query and check the command tag.
    let msgs = srv
        .client
        .simple_query(&format!("COPY copy_to_tag TO '{out_path}'"))
        .await
        .expect("COPY TO");

    // Find the CommandComplete message with tag "COPY N".
    let has_copy_tag = msgs.iter().any(|m| {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = m {
            // n is the row count reported
            *n == 3
        } else {
            false
        }
    });
    // The tag should be COPY 3 (3 rows exported).
    assert!(has_copy_tag, "expected COPY 3 command tag, got: {msgs:?}");
}
