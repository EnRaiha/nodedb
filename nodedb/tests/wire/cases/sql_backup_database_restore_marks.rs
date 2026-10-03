// SPDX-License-Identifier: BUSL-1.1

//! Compiled only with `--features failpoints`.
//!
//! A RESTORE's writes record their marks on the same side of a database
//! backup's cut as the writes themselves.
//!
//! - A RESTORE that ran before a backup's cut is in that backup, and its marks
//!   are below the cut: a later RESTORE of that backup needs no FORCE.
//! - A RESTORE into the same database while a backup is parked after its cut
//!   is out of that backup, and its marks are above the cut: a later RESTORE
//!   of that backup is refused as stale without FORCE.
//!
//! The fail gate `backup::database::after_cut` parks a backup after its cut
//! captured every tenant. The gate is open while its release file exists.

#[cfg(feature = "failpoints")]
use std::time::Duration;

#[cfg(feature = "failpoints")]
use crate::harness::TestServer;

#[cfg(feature = "failpoints")]
const DATABASE: &str = "rmarks";

/// How long the parked backup must stay unfinished.
#[cfg(feature = "failpoints")]
const PARKED_FOR: Duration = Duration::from_millis(1500);

#[cfg(feature = "failpoints")]
async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

#[cfg(feature = "failpoints")]
fn backup_sql(server: &TestServer, object: &str) -> String {
    format!(
        "BACKUP DATABASE {DATABASE} TO '{}'",
        server.backup_uri(object)
    )
}

#[cfg(feature = "failpoints")]
fn restore_sql(server: &TestServer, object: &str, force: bool) -> String {
    format!(
        "RESTORE DATABASE {DATABASE} FROM '{}'{}",
        server.backup_uri(object),
        if force { " FORCE" } else { "" }
    )
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_marks_on_its_own_side_of_a_database_cut() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-cut");
    std::fs::write(&release, b"").expect("open the gate");
    let server = TestServer::start_with_failpoints(&format!(
        "backup::database::after_cut=wait_file({})",
        release.display()
    ))
    .await;
    for sql in [
        format!("CREATE DATABASE {DATABASE}"),
        format!("USE DATABASE {DATABASE}"),
        "CREATE COLLECTION rm_docs (id TEXT PRIMARY KEY, v TEXT) \
         WITH (engine='document_strict')"
            .to_string(),
        "INSERT INTO rm_docs (id, v) VALUES ('a1', 'x')".to_string(),
    ] {
        exec(&server, &sql).await;
    }

    // A RESTORE before the cut: in the backup, marked below its cut.
    exec(&server, &backup_sql(&server, "marks/first.ndbb")).await;
    exec(&server, &restore_sql(&server, "marks/first.ndbb", true)).await;
    exec(&server, &backup_sql(&server, "marks/after.ndbb")).await;
    exec(&server, &restore_sql(&server, "marks/after.ndbb", false)).await;

    // A RESTORE while a backup is parked after its cut: out of the backup,
    // marked above its cut.
    std::fs::remove_file(&release).expect("close the gate");
    let (backup_client, backup_handle) = server
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the backup client");
    let parked_sql = backup_sql(&server, "marks/parked.ndbb");
    let backup = tokio::spawn(async move {
        backup_client
            .simple_query(&parked_sql)
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !backup.is_finished(),
        "the backup was not parked after its cut"
    );
    exec(&server, &restore_sql(&server, "marks/first.ndbb", true)).await;
    std::fs::write(&release, b"").expect("release the backup");
    backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("backup: {e}"));

    let refused = server
        .exec(&restore_sql(&server, "marks/parked.ndbb", false))
        .await
        .expect_err("a RESTORE after the cut marks above it");
    assert!(
        refused.contains("restore refused"),
        "the refusal is the staleness gate: {refused}"
    );
    exec(&server, &restore_sql(&server, "marks/parked.ndbb", true)).await;

    backup_handle.abort();
}
