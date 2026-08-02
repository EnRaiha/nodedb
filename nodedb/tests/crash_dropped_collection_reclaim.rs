// SPDX-License-Identifier: BUSL-1.1

//! Pins whether `DROP COLLECTION` actually unlinks a vector/spatial
//! collection's index checkpoint files, or only appears to.
//!
//! The write path shards checkpoints per Data Plane core and publishes each
//! core's files as a generation:
//! `{data_dir}/{vector,spatial}-ckpt/core-{core_id}/gen-{n}/...`, with a
//! sibling `MANIFEST` naming the live generation. The reclaim helpers invoked
//! by `DROP COLLECTION` have to walk that same shape — every core, then that
//! core's live generation. A reclaim that reads the wrong level finds only
//! directory entries, filters them out on the `.ckpt` extension check, and
//! returns success having deleted nothing: the dropped collection's index then
//! reloads at every subsequent boot.
//!
//! This test forces the server down a single core (the harness always sets
//! `NODEDB_DATA_PLANE_CORES=1`), then proves whether the dropped collection's
//! checkpoint file is still anywhere under the engine's checkpoint root after
//! `DROP COLLECTION` and a restart.
//!
//! A SHORT checkpoint interval is used here — the opposite of most crash
//! tests in this file family, which push the interval out to make a
//! checkpoint impossible. Here a checkpoint must actually run before the
//! drop, or there would be no on-disk file for the reclaim path to either
//! delete or leak, and the test would prove nothing.

mod crash_harness;

use std::time::{Duration, Instant};

use crash_harness::CrashHarness;

const CHECKPOINT_WAIT: Duration = Duration::from_secs(20);

/// Server env for a harness that must actually produce a checkpoint file
/// within the test's runtime. A 1-second interval guarantees at least one
/// flush fires well inside the `CHECKPOINT_WAIT` poll budget below.
fn checkpoint_every_second() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "1")
}

/// Recursively collect every file path under `root` (root need not exist —
/// an absent directory just yields an empty list, matching a collection
/// whose engine never flushed anything).
fn walk_files(root: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}

/// Poll `root` (recursively) until at least one `*.ckpt` file exists,
/// bounded by `CHECKPOINT_WAIT`. Panics with a message that names the
/// premise being relied on, since a timeout here means the test can prove
/// nothing about reclaim — there would be no file on disk either way.
fn wait_for_any_ckpt_file(root: &std::path::Path, engine: &str) -> std::path::PathBuf {
    let deadline = Instant::now() + CHECKPOINT_WAIT;
    loop {
        if let Some(f) = walk_files(root)
            .into_iter()
            .find(|p| p.extension().and_then(|e| e.to_str()) == Some("ckpt"))
        {
            return f;
        }
        assert!(
            Instant::now() < deadline,
            "no {engine} checkpoint (*.ckpt) file appeared under {} within {CHECKPOINT_WAIT:?} \
             even with a 1-second checkpoint interval; the test's premise — that a real \
             checkpoint file exists on disk before the drop — never held, so this run cannot \
             say anything about whether DROP COLLECTION reclaims it",
            root.display()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// True if any file anywhere under `root` has `needle` in its file name.
/// Filenames encode the collection name directly (`vector_checkpoint_filename`
/// / `spatial_checkpoint_prefix`), so a substring match on a distinctive
/// collection name is unambiguous without needing to know the exact
/// database/tenant id the collection was created under.
fn any_file_name_contains(root: &std::path::Path, needle: &str) -> Option<std::path::PathBuf> {
    walk_files(root).into_iter().find(|p| {
        p.file_name()
            .and_then(|n| n.to_str())
            .is_some_and(|n| n.contains(needle))
    })
}

/// Connect fresh and assert `sql` fails with a "collection not found" style
/// error — the direct proof that the collection itself did not resurrect,
/// independent of whatever its index checkpoint files still say on disk.
async fn assert_collection_missing(h: &CrashHarness, sql: &str, collection: &str) {
    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .expect("connect to post-restart server");
    let conn_handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = client.simple_query(sql).await;
    drop(client);
    let _ = conn_handle.await;
    let err = result.err().unwrap_or_else(|| {
        panic!(
            "'{sql}' succeeded after restart; the dropped collection '{collection}' \
             resurrected instead of staying gone"
        )
    });
    let message = err
        .as_db_error()
        .map(|db| db.message().to_string())
        .unwrap_or_else(|| err.to_string());
    // Accept either phrasing the server uses for an absent collection; the assertion
    // that matters is that the query FAILED naming this collection, not the wording.
    let lowered = message.to_lowercase();
    assert!(
        message.contains(collection)
            && (lowered.contains("not found") || lowered.contains("does not exist")),
        "expected a 'collection not found' style error naming '{collection}' after restart, \
         got: {message:?}"
    );
}

/// The claim under test, for the vector engine: a checkpoint file for the
/// dropped collection must be gone after `DROP COLLECTION` + restart, and
/// the collection itself must not be queryable.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_vector_collection_does_not_resurrect_after_restart() {
    const COLLECTION: &str = "crashdropvec";

    let mut h = checkpoint_every_second();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(&format!("CREATE COLLECTION {COLLECTION} TYPE document"))
        .await;
    h.exec(&format!(
        "CREATE VECTOR INDEX idx_{COLLECTION} ON {COLLECTION} (embedding) METRIC cosine DIM 4"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, embedding) VALUES ('r1', ARRAY[1.0,0.0,0.0,0.0])"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, embedding) VALUES ('r2', ARRAY[0.0,1.0,0.0,0.0])"
    ))
    .await;

    let ckpt_root = h.data_dir().join("vector-ckpt");
    let first_ckpt = wait_for_any_ckpt_file(&ckpt_root, "vector");
    assert!(
        any_file_name_contains(&ckpt_root, COLLECTION).is_some(),
        "a vector checkpoint file exists ({}), but none of them name '{COLLECTION}' — test \
         setup is not exercising the collection under test",
        first_ckpt.display()
    );

    h.exec(&format!("DROP COLLECTION {COLLECTION} PURGE")).await;

    // The harness offers no graceful-shutdown restart, only a real `kill -9`
    // followed by reopening the same data directory — so that is what proves
    // the on-disk state, not an in-process pretend-restart. This is also the
    // stronger form of the claim under test either way: a hard kill gives the
    // reclaim path every chance to have already run (it executes synchronously
    // as part of the DROP, before the kill), so a leaked file here cannot be
    // blamed on an interrupted graceful shutdown.
    h.kill_9();
    h.reopen();

    assert_collection_missing(&h, &format!("SELECT id FROM {COLLECTION}"), COLLECTION).await;

    let leaked = any_file_name_contains(&ckpt_root, COLLECTION);
    assert!(
        leaked.is_none(),
        "vector checkpoint file for dropped collection '{COLLECTION}' is still on disk after \
         DROP COLLECTION + restart: {leaked:?} (all files under {}: {:?})",
        ckpt_root.display(),
        walk_files(&ckpt_root)
    );
}

/// Same claim, spatial engine: mirrors
/// `crash_recovery::spatial_index_survives_kill_9`'s DDL/insert shape.
#[tokio::test(flavor = "multi_thread")]
async fn dropped_spatial_collection_does_not_resurrect_after_restart() {
    const COLLECTION: &str = "crashdropspatial";

    let mut h = checkpoint_every_second();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(&format!(
        "CREATE COLLECTION {COLLECTION} (id TEXT, location GEOMETRY SPATIAL_INDEX, name TEXT) \
         WITH (engine='spatial')"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, location, name) \
         VALUES ('p1', ST_Point(-73.9857, 40.7580), 'Times Square')"
    ))
    .await;
    h.exec(&format!(
        "INSERT INTO {COLLECTION} (id, location, name) \
         VALUES ('p2', ST_Point(2.3522, 48.8566), 'Paris')"
    ))
    .await;

    let ckpt_root = h.data_dir().join("spatial-ckpt");
    let first_ckpt = wait_for_any_ckpt_file(&ckpt_root, "spatial");
    assert!(
        any_file_name_contains(&ckpt_root, COLLECTION).is_some(),
        "a spatial checkpoint file exists ({}), but none of them name '{COLLECTION}' — test \
         setup is not exercising the collection under test",
        first_ckpt.display()
    );

    h.exec(&format!("DROP COLLECTION {COLLECTION} PURGE")).await;

    // Same rationale as the vector test above: the harness has no clean
    // restart, and a hard kill only strengthens the claim since the
    // synchronous reclaim inside DROP already had its chance to run.
    h.kill_9();
    h.reopen();

    assert_collection_missing(&h, &format!("SELECT id FROM {COLLECTION}"), COLLECTION).await;

    let leaked = any_file_name_contains(&ckpt_root, COLLECTION);
    assert!(
        leaked.is_none(),
        "spatial checkpoint file for dropped collection '{COLLECTION}' is still on disk after \
         DROP COLLECTION + restart: {leaked:?} (all files under {}: {:?})",
        ckpt_root.display(),
        walk_files(&ckpt_root)
    );
}
