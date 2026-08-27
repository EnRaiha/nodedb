// SPDX-License-Identifier: BUSL-1.1

//! Filesystem path-jail rules for SQL surface area.
//!
//! `BACKUP TENANT`, `RESTORE TENANT` (+ `DRY RUN`), and `EXPORT AUDIT
//! LOG` reject any quoted-path argument with a SQLSTATE error naming
//! the wire-streaming alternative (`COPY ... TO STDOUT`, `... FROM
//! STDIN`, `SELECT FROM system.audit_log`). Rejection happens before
//! any `std::fs` call, so caller-named paths never reach the
//! filesystem and bytes from caller-named files never reach a
//! deserializer.
//!
//! `COPY <coll> FROM '<path>'` is a real feature, but its path jail
//! still blocks (a) non-absolute paths and (b) any path containing a
//! `..` component. Absolute, non-traversal paths are accepted under
//! the server UID's filesystem permissions.

use crate::harness::TestServer;

async fn err_of(server: &TestServer, sql: &str) -> String {
    match server.client.simple_query(sql).await {
        Ok(_) => panic!("expected error, got success for: {sql}"),
        Err(e) => {
            let detail = if let Some(db) = e.as_db_error() {
                format!("{}: {}", db.code().code(), db.message())
            } else {
                format!("{e:?}")
            };
            detail.to_lowercase()
        }
    }
}

/// A rejection is "structural" if it refuses the path *form* itself,
/// not the path *contents*. A leaked `os error N` string proves the
/// server tried `std::fs` on the caller's bytes — same bug class.
fn assert_structural_rejection(msg: &str, advertise: &[&str]) {
    assert!(
        !msg.contains("os error"),
        "rejection routed through std::fs (leaked io error): {msg}"
    );
    assert!(
        !msg.contains("no such file") && !msg.contains("permission denied ("),
        "rejection routed through std::fs: {msg}"
    );
    assert!(
        advertise.iter().any(|kw| msg.contains(kw)),
        "rejection should advertise the wire alternative ({advertise:?}), got: {msg}"
    );
}

// ────────────────────────────────────────────────────────────────────
// BACKUP TENANT — path form is gone.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn backup_rejects_path_form_relative() {
    let server = TestServer::start().await;
    let probe = std::env::temp_dir().join("nodedb_pjail_backup_rel_probe.bak");
    let _ = std::fs::remove_file(&probe);

    let msg = err_of(
        &server,
        "BACKUP TENANT 1 TO '../../../../tmp/nodedb_pjail_backup_rel_probe.bak'",
    )
    .await;
    assert_structural_rejection(&msg, &["stdout", "copy", "wire"]);
    assert!(
        !probe.exists(),
        "path-form was processed instead of rejected ({})",
        probe.display()
    );
}

#[tokio::test]
async fn backup_rejects_path_form_absolute_system_dir() {
    let server = TestServer::start().await;
    let msg = err_of(
        &server,
        "BACKUP TENANT 1 TO '/etc/sudoers.d/nodedb_pjail_probe'",
    )
    .await;
    assert_structural_rejection(&msg, &["stdout", "copy", "wire"]);
}

#[tokio::test]
async fn backup_rejects_path_form_proc() {
    let server = TestServer::start().await;
    let msg = err_of(&server, "BACKUP TENANT 1 TO '/proc/self/mem'").await;
    assert_structural_rejection(&msg, &["stdout", "copy", "wire"]);
}

#[tokio::test]
async fn backup_rejects_path_form_tempdir() {
    // Even a path the server *could* legitimately write to must be
    // rejected — the spec is "path form is not part of the surface",
    // not "some paths are allowed and some aren't".
    let server = TestServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("benign.bak");

    let sql = format!("BACKUP TENANT 1 TO '{}'", target.display());
    let msg = err_of(&server, &sql).await;
    assert_structural_rejection(&msg, &["stdout", "copy", "wire"]);
    assert!(
        !target.exists(),
        "server wrote to caller-named path despite path form being removed"
    );
}

// ────────────────────────────────────────────────────────────────────
// RESTORE TENANT — path form is gone (eager + DRY RUN).
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn restore_rejects_path_form_relative_traversal() {
    let server = TestServer::start().await;
    let msg = err_of(&server, "RESTORE TENANT 1 FROM '../../../../etc/passwd'").await;
    assert_structural_rejection(&msg, &["stdin", "copy", "wire"]);
    // Specifically: rejection precedes any read of the named file,
    // so deserializer context derived from /etc/passwd bytes must
    // never appear in the error returned to the client.
    assert!(
        !msg.contains("msgpack")
            && !msg.contains("deserialization")
            && !msg.contains("invalid type")
            && !msg.contains("missing field")
            && !msg.contains("invalid marker"),
        "deserializer ran against caller-named path: {msg}"
    );
}

#[tokio::test]
async fn restore_rejects_path_form_absolute_system_file() {
    let server = TestServer::start().await;
    let msg = err_of(&server, "RESTORE TENANT 1 FROM '/etc/passwd'").await;
    assert_structural_rejection(&msg, &["stdin", "copy", "wire"]);
    assert!(
        !msg.contains("msgpack") && !msg.contains("deserialization"),
        "deserializer ran against /etc/passwd: {msg}"
    );
}

#[tokio::test]
async fn restore_rejects_path_form_proc_self_environ() {
    let server = TestServer::start().await;
    let msg = err_of(&server, "RESTORE TENANT 1 FROM '/proc/self/environ'").await;
    assert_structural_rejection(&msg, &["stdin", "copy", "wire"]);
    // Env-var bytes must never reach the deserializer and surface in
    // the error message back to the client.
    assert!(
        !msg.contains("path=")
            && !msg.contains("home=")
            && !msg.contains("shell=")
            && !msg.contains("user="),
        "env contents leaked into error: {msg}"
    );
}

#[tokio::test]
async fn restore_dry_run_rejects_path_form() {
    let server = TestServer::start().await;
    let msg = err_of(
        &server,
        "RESTORE TENANT 1 FROM '../../../../etc/passwd' DRY RUN",
    )
    .await;
    assert_structural_rejection(&msg, &["stdin", "copy", "wire"]);
}

#[tokio::test]
async fn restore_rejects_path_form_tempdir() {
    // Same rationale as the backup tempdir case: there is no
    // "permitted path"; the path form is gone.
    let server = TestServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("benign.bak");
    std::fs::write(&target, b"any bytes at all").unwrap();

    let sql = format!("RESTORE TENANT 1 FROM '{}'", target.display());
    let msg = err_of(&server, &sql).await;
    assert_structural_rejection(&msg, &["stdin", "copy", "wire"]);
}

// ────────────────────────────────────────────────────────────────────
// COPY <coll> FROM '<path>' — Postgres-standard answer is STDIN.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn copy_from_rejects_path_form_relative() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pjc (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')")
        .await
        .ok();

    let msg = err_of(&server, "COPY pjc FROM '../../../../etc/passwd'").await;
    assert_structural_rejection(&msg, &["stdin", "copy"]);
}

#[tokio::test]
async fn copy_from_rejects_path_form_absolute_system_file() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pjc2 (id TEXT PRIMARY KEY, content TEXT) WITH (engine='document_strict')")
        .await
        .ok();

    let msg = err_of(&server, "COPY pjc2 FROM '/etc/passwd'").await;
    assert_structural_rejection(&msg, &["stdin", "copy"]);
}

// `COPY <coll> FROM '<absolute readable path>'` is a real feature —
// see `control/server/pgwire/ddl/collection/copy_from/`. The path
// jail is "no relative paths, no `..` traversal", not "no paths at
// all". An absolute, non-traversal path that resolves to a readable
// file under the server's UID is accepted; it's the operator's job
// to grant or deny that UID access to the path.

// ────────────────────────────────────────────────────────────────────
// EXPORT AUDIT LOG TO '<path>' — same removal.
// ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn export_audit_log_rejects_path_form_relative() {
    let server = TestServer::start().await;
    let probe = std::env::temp_dir().join("nodedb_pjail_audit_rel_probe.log");
    let _ = std::fs::remove_file(&probe);

    let msg = err_of(
        &server,
        "EXPORT AUDIT LOG TO '../../../../tmp/nodedb_pjail_audit_rel_probe.log'",
    )
    .await;
    assert_structural_rejection(&msg, &["stdout", "select", "query"]);
    assert!(
        !probe.exists(),
        "EXPORT bypassed the rejection: {} was written",
        probe.display()
    );
}

#[tokio::test]
async fn export_audit_log_rejects_path_form_absolute_system_file() {
    let server = TestServer::start().await;
    let msg = err_of(
        &server,
        "EXPORT AUDIT LOG TO '/etc/cron.d/nodedb_pjail_probe'",
    )
    .await;
    assert_structural_rejection(&msg, &["stdout", "select", "query"]);
}

#[tokio::test]
async fn export_audit_log_rejects_path_form_tempdir() {
    let server = TestServer::start().await;
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("audit.log");
    let sql = format!("EXPORT AUDIT LOG TO '{}'", target.display());
    let msg = err_of(&server, &sql).await;
    assert_structural_rejection(&msg, &["stdout", "select", "query"]);
    assert!(
        !target.exists(),
        "server wrote to caller-named path despite path form being removed"
    );
}
