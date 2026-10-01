// SPDX-License-Identifier: BUSL-1.1

//! Compiled only with `--features failpoints`.
//!
//! `BACKUP DATABASE` takes one consistent cut for every tenant of the
//! database and records it in its manifest. Every row in the backup committed
//! at or below that cut.
//!
//! The fail gate `backup::database::after_cut` parks the backup after its cut
//! captured every tenant. While it is parked, tenant B writes a row. The
//! released backup never holds that row: the capture ended before the write
//! began. Tenant A's and tenant B's earlier rows are in the backup, and the
//! manifest's cut is older than the late write.

#[cfg(feature = "failpoints")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "failpoints")]
use nodedb_types::backup_envelope::{
    DEFAULT_MAX_TOTAL_BYTES, DatabaseBackupManifest, SECTION_ORIGIN_DATABASE_MANIFEST,
    parse_encrypted,
};

#[cfg(feature = "failpoints")]
use crate::harness::{TEST_BACKUP_KEK, TestServer};

#[cfg(feature = "failpoints")]
const DATABASE: &str = "cutdb";

#[cfg(feature = "failpoints")]
const OBJECT: &str = "cut/cutdb.ndbb";

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
fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .expect("clock after the epoch")
}

#[cfg(feature = "failpoints")]
async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    let mut ids = server
        .query_text(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"));
    ids.sort();
    ids
}

#[cfg(feature = "failpoints")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn no_row_in_a_database_backup_is_newer_than_its_cut() {
    let gate_dir = tempfile::tempdir().expect("gate tempdir");
    let release = gate_dir.path().join("release-cut");
    let source = TestServer::start_with_failpoints(&format!(
        "backup::database::after_cut=wait_file({})",
        release.display()
    ))
    .await;
    for sql in [
        format!("CREATE DATABASE {DATABASE}"),
        format!("USE DATABASE {DATABASE}"),
        "CREATE TENANT cut_b ID 2".to_string(),
        "CREATE COLLECTION cut_a (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
            .to_string(),
        "INSERT INTO cut_a (id, v) VALUES ('a1', 'x')".to_string(),
        "SET TENANT = 2".to_string(),
        "CREATE COLLECTION cut_b (id TEXT PRIMARY KEY, v TEXT) WITH (engine='document_strict')"
            .to_string(),
        "INSERT INTO cut_b (id, v) VALUES ('b1', 'x')".to_string(),
    ] {
        exec(&source, &sql).await;
    }

    // Park the backup after its cut.
    let (backup_client, backup_handle) = source
        .connect_as("nodedb", "nodedb")
        .await
        .expect("connect the backup client");
    let backup_sql = format!(
        "BACKUP DATABASE {DATABASE} TO '{}'",
        source.backup_uri(OBJECT)
    );
    let backup = tokio::spawn(async move {
        backup_client
            .simple_query(&backup_sql)
            .await
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    });
    tokio::time::sleep(PARKED_FOR).await;
    assert!(
        !backup.is_finished(),
        "the backup was not parked after its cut"
    );

    // Tenant B writes while the backup is parked.
    let before_late_write = now_ns();
    exec(&source, "INSERT INTO cut_b (id, v) VALUES ('b_late', 'x')").await;
    std::fs::write(&release, b"").expect("release the backup");
    backup
        .await
        .expect("backup task")
        .unwrap_or_else(|e| panic!("backup: {e}"));

    let bytes = std::fs::read(source.backup_root().join(OBJECT)).expect("read the backup");
    let env = parse_encrypted(&bytes, DEFAULT_MAX_TOTAL_BYTES, &TEST_BACKUP_KEK).expect("parse");
    let manifest_section = env
        .sections
        .iter()
        .find(|s| s.origin_node_id == SECTION_ORIGIN_DATABASE_MANIFEST)
        .expect("a manifest section");
    let manifest: DatabaseBackupManifest =
        zerompk::from_msgpack(&manifest_section.body).expect("decode the manifest");
    assert_eq!(
        manifest.tenants,
        vec![1, 2],
        "both tenants are in the backup"
    );

    // Restore into a fresh server and read every tenant's rows.
    let target = TestServer::start().await;
    exec(&target, "CREATE TENANT cut_b ID 2").await;
    let to = target.backup_root().join(OBJECT);
    std::fs::create_dir_all(to.parent().expect("object directory")).expect("create directory");
    std::fs::write(&to, &bytes).expect("copy the backup");
    exec(
        &target,
        &format!(
            "RESTORE DATABASE {DATABASE} FROM '{}'",
            target.backup_uri(OBJECT)
        ),
    )
    .await;
    exec(&target, &format!("USE DATABASE {DATABASE}")).await;
    assert_eq!(
        ids(&target, "cut_a").await,
        vec!["a1"],
        "tenant A's pre-cut row"
    );
    exec(&target, "SET TENANT = 2").await;
    assert_eq!(
        ids(&target, "cut_b").await,
        vec!["b1"],
        "tenant B's pre-cut row, and never the row written after the cut"
    );
    assert!(
        manifest.cut_hlc < before_late_write,
        "the recorded cut {} is older than the late write, which began at {}",
        manifest.cut_hlc,
        before_late_write
    );

    backup_handle.abort();
}
