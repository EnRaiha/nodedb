// SPDX-License-Identifier: BUSL-1.1

//! Offline point-in-time restore of a single node, end to end: a base
//! snapshot, archived WAL, `nodedb restore` to a time between two writes,
//! and a boot over the restored directory.

mod crash_harness;

use std::io::Write as _;
use std::os::unix::fs::OpenOptionsExt as _;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crash_harness::CrashHarness;
use nodedb::control::security::catalog::SystemCatalog;
use nodedb::storage::cold::{ColdStorage, ColdStorageConfig};
use nodedb::storage::metadata_timeline::{ROOT_TIMELINE, fetch_branch};
use nodedb::storage::raft_log_archive::fetch_all_frontiers;
use nodedb::wal::archiver::load_or_mint_incarnation;

/// Node id of a single-node server. Single-node Calvin is on by default, and
/// the server archives its WAL and base snapshots under this id.
const NODE_ID: u64 = nodedb::control::cluster::SINGLE_NODE_CALVIN_NODE_ID;

/// Bytes of one large row. Five of them roll the 1 MiB WAL segment.
const LARGE_ROW_BYTES: usize = 300 * 1024;

/// Stores shared by the source node and the restore, outside both data
/// directories.
struct Stores {
    root: tempfile::TempDir,
}

impl Stores {
    fn new() -> Self {
        let stores = Self {
            root: tempfile::tempdir().expect("stores tempdir"),
        };
        // The WAL key file must be a regular file only its owner can read.
        let mut key = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(stores.key())
            .expect("create the WAL key file");
        key.write_all(&[0x42; 32]).expect("write the WAL key");
        stores
    }

    fn key(&self) -> PathBuf {
        self.root.path().join("wal.key")
    }

    fn cold(&self) -> PathBuf {
        self.root.path().join("cold")
    }

    fn snapshots(&self) -> PathBuf {
        self.root.path().join("snapshots")
    }

    /// Write a server config for `data_dir` and return its path.
    fn config(&self, name: &str, data_dir: &Path, pitr: bool) -> PathBuf {
        let path = self.root.path().join(name);
        let text = format!(
            "[server]\ndata_dir = {data:?}\n\n\
             [checkpoint]\nwal_segment_target_mb = 1\n\n\
             [encryption]\nkey_path = {key:?}\n\n\
             [cold_storage]\nlocal_dir = {cold:?}\n\n\
             [snapshot_storage]\nlocal_dir = {snaps:?}\n\n\
             [pitr]\nenabled = {pitr}\n",
            data = data_dir.display().to_string(),
            key = self.key().display().to_string(),
            cold = self.cold().display().to_string(),
            snaps = self.snapshots().display().to_string(),
        );
        std::fs::write(&path, text).expect("write the config file");
        path
    }

    fn cold_storage(&self) -> ColdStorage {
        ColdStorage::new(ColdStorageConfig {
            local_dir: Some(self.cold()),
            ..Default::default()
        })
        .expect("open cold storage")
    }
}

/// Every file named `name` under `dir`, recursively.
fn find_files(dir: &Path, name: &str) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(dir) = pending.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|n| n == name) {
                found.push(path);
            }
        }
    }
    found
}

/// Block until the PITR task has written a base snapshot manifest.
fn wait_for_base(stores: &Stores, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while find_files(&stores.snapshots(), "manifest.msgpack").is_empty() {
        assert!(
            Instant::now() < deadline,
            "no base snapshot appeared under {} within {timeout:?}",
            stores.snapshots().display()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

fn now_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_micros() as u64
}

fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_nanos() as u64
}

/// Block until some node life's metadata log archive frontier reaches
/// `hlc_ns`: every metadata entry committed by then is archived.
async fn wait_for_metadata_frontier(stores: &Stores, hlc_ns: u64, timeout: Duration) {
    let cold = stores.cold_storage();
    let key =
        nodedb_wal::crypto::WalEncryptionKey::from_file(&stores.key()).expect("read the WAL key");
    let deadline = Instant::now() + timeout;
    loop {
        // The source never restored, so it writes the root timeline.
        let reached = fetch_all_frontiers(&cold.object_store(), cold.prefix(), 0, &key)
            .await
            .expect("read the metadata log archive frontiers")
            .iter()
            .any(|frontier| frontier.stamped_through_ns >= hlc_ns);
        if reached {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the metadata log archive did not reach {hlc_ns}ns within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Upload every WAL segment on disk, the active one included, as the
/// archiver uploads a sealed one.
async fn archive_all_segments(stores: &Stores, data_dir: &Path) {
    let cold = stores.cold_storage();
    let incarnation = load_or_mint_incarnation(data_dir).expect("read the node incarnation");
    let segments =
        nodedb_wal::segment::discover_segments(&data_dir.join("wal")).expect("list WAL segments");
    assert!(!segments.is_empty(), "the node wrote no WAL segment");
    for segment in segments {
        cold.upload_wal_segment(
            &segment.path,
            NODE_ID,
            incarnation.as_str(),
            segment.first_lsn,
            &[],
        )
        .await
        .expect("archive a WAL segment");
    }
}

/// Run `nodedb restore` with `args`. Returns (success, stdout, stderr).
fn run_restore(args: &[&str]) -> (bool, String, String) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_nodedb"))
        .arg("restore")
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("run nodedb restore");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

async fn ids(h: &CrashHarness) -> Vec<String> {
    let mut ids = h.query_col("SELECT id FROM pitr_rows", "id").await;
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread")]
async fn restore_to_a_time_replays_exactly_to_it_and_refuses_a_gap() {
    let stores = Stores::new();
    let mut source = CrashHarness::new();
    let source_dir = source.data_dir().to_path_buf();

    // Rows A exist before the base: the base holds them.
    let plain = stores.config("source.toml", &source_dir, false);
    source.set_env("NODEDB_CONFIG", &plain.display().to_string());
    source.spawn();
    source.wait_ready_extended();
    source
        .exec(
            "CREATE COLLECTION pitr_rows (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await;
    for id in ["a1", "a2"] {
        source
            .exec(&format!(
                "INSERT INTO pitr_rows (id, v) VALUES ('{id}', 'a')"
            ))
            .await;
    }
    source.kill_9();

    // PITR on: the first base is taken at boot, with no base in the store.
    let pitr = stores.config("source-pitr.toml", &source_dir, true);
    source.set_env("NODEDB_CONFIG", &pitr.display().to_string());
    source.spawn();
    source.wait_ready_extended();
    wait_for_base(&stores, Duration::from_secs(60));

    for id in ["b1", "b2"] {
        source
            .exec(&format!(
                "INSERT INTO pitr_rows (id, v) VALUES ('{id}', 'b')"
            ))
            .await;
    }
    // A collection created after the base and before T, with a row: the
    // base's catalog lacks it, and WAL replay of its strict row needs it.
    source
        .exec(
            "CREATE COLLECTION pitr_mid (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await;
    source
        .exec("INSERT INTO pitr_mid (id, v) VALUES ('m1', 'm')")
        .await;
    // Every B write and the mid DDL are acknowledged, so each committed
    // before T. The late DDL and every C write start after T.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let target_us = now_micros();
    tokio::time::sleep(Duration::from_millis(50)).await;
    source
        .exec(
            "CREATE COLLECTION pitr_late (id TEXT PRIMARY KEY, v TEXT) \
             WITH (engine='document_strict')",
        )
        .await;
    let late_created_ns = now_nanos();
    let large = "c".repeat(LARGE_ROW_BYTES);
    for i in 0..5 {
        source
            .exec(&format!(
                "INSERT INTO pitr_rows (id, v) VALUES ('c{i}', '{large}')"
            ))
            .await;
    }
    // The metadata log archive holds the late DDL, so the restore drops it
    // by its stamp, not by its absence.
    wait_for_metadata_frontier(&stores, late_created_ns, Duration::from_secs(90)).await;
    source.kill_9();
    archive_all_segments(&stores, &source_dir).await;
    let target = target_us.to_string();

    // Restore to T into a fresh directory, then boot it.
    let mut restored = CrashHarness::new();
    let restored_dir = restored.data_dir().to_path_buf();
    let restored_config = stores.config("restored.toml", &restored_dir, false);
    let config_arg = restored_config.display().to_string();
    let (ok, stdout, stderr) = run_restore(&[
        "--config",
        &config_arg,
        "--target-time",
        &target,
        "--dry-run",
    ]);
    assert!(ok, "dry run failed:\n{stdout}\n{stderr}");
    assert!(stdout.contains("dry run"), "{stdout}");
    assert!(
        std::fs::read_dir(&restored_dir)
            .expect("list")
            .next()
            .is_none(),
        "a dry run writes nothing"
    );
    let last_segment = stdout
        .lines()
        .filter_map(|line| line.split_whitespace().find(|w| w.ends_with(".seg")))
        .next_back()
        .expect("the plan lists the WAL segments it restores")
        .to_string();

    let (ok, stdout, stderr) = run_restore(&["--config", &config_arg, "--target-time", &target]);
    assert!(ok, "restore failed:\n{stdout}\n{stderr}");

    // The restored node writes a new metadata timeline, which branches off
    // the source's root timeline.
    let timeline = SystemCatalog::open(&restored_dir.join("system.redb"))
        .expect("open the restored catalog")
        .load_metadata_timeline()
        .expect("read the restored timeline");
    assert_ne!(timeline, ROOT_TIMELINE, "a restore starts a new timeline");
    let wal_key =
        nodedb_wal::crypto::WalEncryptionKey::from_file(&stores.key()).expect("read the WAL key");
    let cold = stores.cold_storage();
    let branch = fetch_branch(&cold.object_store(), cold.prefix(), timeline, &wal_key)
        .await
        .expect("read the timeline descriptor")
        .expect("the restore records its timeline");
    assert_eq!(branch.parent, ROOT_TIMELINE);

    restored.set_env("NODEDB_CONFIG", &config_arg);
    restored.spawn();
    restored.wait_ready_extended();
    assert_eq!(
        ids(&restored).await,
        ["a1", "a2", "b1", "b2"],
        "the restore holds every row committed by T and none after it"
    );
    assert_eq!(
        restored.query_col("SELECT id FROM pitr_mid", "id").await,
        ["m1"],
        "a collection created after the base and before T exists, with its row"
    );
    let (client, connection) =
        tokio_postgres::connect(&restored.pgwire_conn_str(), tokio_postgres::NoTls)
            .await
            .expect("connect to the restored node");
    let connection = tokio::spawn(connection);
    let late = client.simple_query("SELECT id FROM pitr_late").await;
    drop(client);
    let _ = connection.await;
    let Err(err) = late else {
        panic!("a collection created after T does not exist, yet a query of it succeeded");
    };
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| err.to_string());
    let lowered = message.to_lowercase();
    assert!(
        message.contains("pitr_late")
            && (lowered.contains("not found") || lowered.contains("does not exist")),
        "expected a collection-not-found error naming pitr_late, got: {message:?}"
    );
    restored.kill_9();

    // Without the segment holding the target, the archive has a gap.
    let incarnation = load_or_mint_incarnation(&source_dir).expect("read the node incarnation");
    let first_lsn =
        nodedb_wal::segment::parse_segment_filename(&last_segment).expect("a segment file name");
    let cold = stores.cold_storage();
    let archived = cold
        .archived_wal_segments(NODE_ID, incarnation.as_str(), 0)
        .await
        .expect("list the archive");
    let markers = archived
        .get(&first_lsn)
        .map(|seg| seg.crc32c.clone())
        .expect("the target segment is archived");
    assert!(
        archived.keys().any(|&first| first > first_lsn),
        "a segment after the target's must exist for the deleted one to be a gap"
    );
    cold.delete_archived_wal_segment(NODE_ID, incarnation.as_str(), first_lsn, &markers)
        .await
        .expect("delete the archived target segment");

    let gap_dir = tempfile::tempdir().expect("gap tempdir");
    let gap_config = stores.config("gap.toml", gap_dir.path(), false);
    let (ok, stdout, stderr) = run_restore(&[
        "--config",
        &gap_config.display().to_string(),
        "--target-time",
        &target,
    ]);
    assert!(!ok, "a restore across a gap must fail:\n{stdout}");
    assert!(
        stderr.contains("archived WAL is missing LSNs") && stderr.contains("..="),
        "the error names the missing range: {stderr}"
    );
    assert!(
        std::fs::read_dir(gap_dir.path())
            .expect("list")
            .next()
            .is_none(),
        "a refused restore writes nothing"
    );
}
