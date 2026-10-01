// SPDX-License-Identifier: BUSL-1.1

//! `BACKUP DATABASE <db> TO '<uri>'` and
//! `RESTORE DATABASE <db> FROM '<uri>' [FORCE] [DRY RUN]` over pgwire.
//!
//! The source server writes a database backup to a `file://` URI inside its
//! backup root. The file moves to a fresh server's backup root, and that
//! server restores it, creating the database. A DRY RUN leaves the fresh
//! server without the database. A URI outside the backup root, an unknown
//! scheme and an S3 URI with no key each fail with SQLSTATE 22023.

use crate::harness::TestServer;

const DATABASE: &str = "ubd_shop";
const OBJECT: &str = "nightly/ubd_shop.ndbb";

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn count(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT COUNT(*) FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("count {collection}: {e}"))
}

/// A source server holding `DATABASE`, its backup written, and the backup
/// copied into the backup root of a fresh target server.
async fn backed_up_into_fresh_target() -> TestServer {
    let source = TestServer::start().await;
    exec(&source, &format!("CREATE DATABASE {DATABASE}")).await;
    exec(&source, &format!("USE DATABASE {DATABASE}")).await;
    for sql in [
        "CREATE COLLECTION ubd_orders (id TEXT PRIMARY KEY, total INT) \
         WITH (engine='document_strict')",
        "INSERT INTO ubd_orders (id, total) VALUES ('o1', 10)",
        "INSERT INTO ubd_orders (id, total) VALUES ('o2', 20)",
        "CREATE COLLECTION ubd_kv (key STRING PRIMARY KEY, value STRING) WITH (engine='kv')",
        "INSERT INTO ubd_kv (key, value) VALUES ('k1', 'one')",
    ] {
        exec(&source, sql).await;
    }
    exec(&source, "USE DATABASE default").await;
    exec(
        &source,
        &format!(
            "BACKUP DATABASE {DATABASE} TO '{}'",
            source.backup_uri(OBJECT)
        ),
    )
    .await;

    let target = TestServer::start().await;
    let from = source.backup_root().join(OBJECT);
    let to = target.backup_root().join(OBJECT);
    std::fs::create_dir_all(to.parent().expect("object directory")).expect("create directory");
    std::fs::copy(&from, &to).expect("copy the backup object");
    target
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_database_backup_restores_into_a_fresh_server() {
    let target = backed_up_into_fresh_target().await;
    exec(
        &target,
        &format!(
            "RESTORE DATABASE {DATABASE} FROM '{}'",
            target.backup_uri(OBJECT)
        ),
    )
    .await;
    exec(&target, &format!("USE DATABASE {DATABASE}")).await;
    assert_eq!(count(&target, "ubd_orders").await, vec!["2"]);
    assert_eq!(count(&target, "ubd_kv").await, vec!["1"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dry_run_writes_nothing() {
    let target = backed_up_into_fresh_target().await;
    exec(
        &target,
        &format!(
            "RESTORE DATABASE {DATABASE} FROM '{}' DRY RUN",
            target.backup_uri(OBJECT)
        ),
    )
    .await;
    let databases = target
        .query_text("SHOW DATABASES")
        .await
        .expect("SHOW DATABASES");
    assert!(
        !databases.iter().any(|name| name == DATABASE),
        "a dry run creates no database: {databases:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bad_uri_is_a_typed_error() {
    let server = TestServer::start().await;
    exec(&server, "CREATE DATABASE ubd_bad").await;
    for sql in [
        "BACKUP DATABASE ubd_bad TO 'file:///etc/ubd_escape.ndbb'".to_string(),
        format!(
            "BACKUP DATABASE ubd_bad TO 'file://{}/../escape.ndbb'",
            server.backup_root().display()
        ),
        "BACKUP DATABASE ubd_bad TO 'ftp://host/x.ndbb'".to_string(),
        "RESTORE DATABASE ubd_bad FROM 's3://only-a-bucket'".to_string(),
    ] {
        let err = server
            .exec(&sql)
            .await
            .expect_err(&format!("{sql} must be refused"));
        assert!(err.contains("22023"), "{sql}: expected 22023, got {err}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_restores_only_under_its_own_name() {
    let target = backed_up_into_fresh_target().await;
    let err = target
        .exec(&format!(
            "RESTORE DATABASE ubd_other FROM '{}'",
            target.backup_uri(OBJECT)
        ))
        .await
        .expect_err("another name must be refused");
    assert!(
        err.contains(DATABASE),
        "names the backed-up database: {err}"
    );
}
