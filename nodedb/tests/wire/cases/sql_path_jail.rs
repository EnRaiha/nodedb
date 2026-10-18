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
//! `..` component. A server-path `COPY FROM` or `COPY TO` needs
//! superuser (`42501`), and `COPY FROM` reads regular files only
//! (`58030` for a FIFO, device, or directory).

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

// `COPY <coll> FROM '<absolute readable path>'` is a real feature. The
// path jail refuses relative paths and `..` traversal. Beyond the jail,
// a server path needs superuser, and it must open as a regular file.
// The tests below lock both rules, for COPY FROM and COPY TO.

const COPY_PASSWORD: &str = "pjail-copy-secret-3";

/// Bound on a COPY against a FIFO or device: a hang fails the test.
const COPY_DEADLINE: std::time::Duration = std::time::Duration::from_secs(20);

/// Create `user` with the default ReadWrite role and run `sql` as that
/// user. `Err((sqlstate, message))` on refusal.
async fn run_as_writer(server: &TestServer, user: &str, sql: &str) -> Result<(), (String, String)> {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{COPY_PASSWORD}'"))
        .await
        .unwrap_or_else(|e| panic!("create user {user}: {e}"));
    let (client, handle) = server
        .connect_as(user, COPY_PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    result.map(|_| ()).map_err(|e| {
        e.as_db_error().map_or_else(
            || (String::new(), e.to_string()),
            |db| (db.code().code().to_string(), db.message().to_string()),
        )
    })
}

/// Run `sql` as the superuser under [`COPY_DEADLINE`]; the lower-cased
/// `sqlstate: message` of its refusal.
async fn superuser_refusal(server: &TestServer, sql: &str) -> String {
    tokio::time::timeout(COPY_DEADLINE, err_of(server, sql))
        .await
        .unwrap_or_else(|_| panic!("COPY hung on a non-regular file: {sql}"))
}

#[tokio::test]
async fn copy_from_server_path_requires_superuser() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION pj_su (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')",
        )
        .await
        .expect("create collection");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("rows.ndjson");
    std::fs::write(&path, "{\"id\":\"a\",\"v\":\"x\"}\n").expect("write rows");

    let refusal = run_as_writer(
        &server,
        "pj_writer_from",
        &format!("COPY pj_su FROM '{}'", path.display()),
    )
    .await
    .expect_err("a non-superuser server-path COPY FROM must be refused");
    assert_eq!(refusal.0, "42501", "{refusal:?}");
    assert!(refusal.1.contains("superuser"), "{refusal:?}");
    let rows = server
        .query_text("SELECT id FROM pj_su")
        .await
        .expect("scan");
    assert!(rows.is_empty(), "a refused COPY wrote rows: {rows:?}");

    // The superuser runs the same statement.
    server
        .exec(&format!("COPY pj_su FROM '{}'", path.display()))
        .await
        .expect("superuser COPY FROM");
}

#[tokio::test]
async fn copy_to_server_path_requires_superuser() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pj_su_to (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')")
        .await
        .expect("create collection");
    server
        .exec("INSERT INTO pj_su_to (id, v) VALUES ('a', 'x')")
        .await
        .expect("seed");
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("out.ndjson");

    let refusal = run_as_writer(
        &server,
        "pj_writer_to",
        &format!("COPY pj_su_to TO '{}'", path.display()),
    )
    .await
    .expect_err("a non-superuser server-path COPY TO must be refused");
    assert_eq!(refusal.0, "42501", "{refusal:?}");
    assert!(!path.exists(), "a refused COPY TO wrote {}", path.display());
}

#[tokio::test]
async fn copy_from_a_device_is_refused() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pj_dev (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')")
        .await
        .expect("create collection");
    let msg = superuser_refusal(&server, "COPY pj_dev FROM '/dev/zero' WITH (FORMAT csv)").await;
    assert!(msg.starts_with("58030"), "{msg}");
    assert!(msg.contains("not a regular file"), "{msg}");
}

#[tokio::test]
async fn copy_from_a_symlink_to_a_fifo_or_a_directory_is_refused() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION pj_fifo (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')")
        .await
        .expect("create collection");
    let dir = tempfile::tempdir().expect("tempdir");
    let fifo = dir.path().join("pipe");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo failed: {status}");
    let link = dir.path().join("rows.csv");
    std::os::unix::fs::symlink(&fifo, &link).expect("symlink to fifo");

    let msg = superuser_refusal(&server, &format!("COPY pj_fifo FROM '{}'", link.display())).await;
    assert!(msg.starts_with("58030"), "{msg}");
    assert!(msg.contains("not a regular file"), "{msg}");

    let subdir = dir.path().join("dir.csv");
    std::fs::create_dir(&subdir).expect("create dir");
    let msg = superuser_refusal(
        &server,
        &format!("COPY pj_fifo FROM '{}'", subdir.display()),
    )
    .await;
    assert!(msg.starts_with("58030"), "{msg}");
    assert!(msg.contains("not a regular file"), "{msg}");
}

#[tokio::test]
async fn geotiff_over_stdin_names_the_server_path_form() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE ARRAY pj_scene DIMS (y INT64 [0..7], x INT64 [0..7]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (4, 4)",
        )
        .await
        .expect("create array");
    let msg = superuser_refusal(&server, "COPY pj_scene FROM STDIN WITH (FORMAT geotiff)").await;
    assert!(msg.starts_with("0a000"), "{msg}");
    assert!(msg.contains("from '<path>.tif'"), "{msg}");
}

#[tokio::test]
async fn array_copy_from_a_device_is_refused() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE ARRAY pj_arr DIMS (y INT64 [0..7], x INT64 [0..7]) \
             ATTRS (b1 FLOAT64) TILE_EXTENTS (4, 4)",
        )
        .await
        .expect("create array");
    let msg = superuser_refusal(&server, "COPY pj_arr FROM '/dev/zero' WITH (FORMAT csv)").await;
    assert!(msg.starts_with("58030"), "{msg}");
    assert!(msg.contains("not a regular file"), "{msg}");
}

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
