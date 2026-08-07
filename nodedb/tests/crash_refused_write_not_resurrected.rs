// SPDX-License-Identifier: BUSL-1.1

//! A write the server REFUSED must not come back after `kill -9`.
//!
//! The write funnel appends a write's redo record before the Data Plane has
//! decided whether to accept it, so every refusal that is decided inside the
//! engine leaves a record in the WAL describing a write that never happened.
//! Replay is indifferent to why the record is there, so without an explicit
//! cancellation it re-applies the refused write and recovery silently undoes
//! the refusal.
//!
//! This file pins the acknowledged case against the REAL server binary: the
//! client is told the write was rejected, the process is killed outright, and
//! the same data directory is reopened. `kill -9` rather than a graceful
//! shutdown is deliberate — a clean shutdown could flush state that hides a
//! WAL-level defect; only a hard kill leaves recovery with nothing but the WAL,
//! which is where the resurrection happens.
//!
//! The refusal used here is a unique-constraint violation, on the key-value
//! engine. Constraint rather than row-level security because it needs no second
//! identity, and key-value because its rows live only in an in-memory hash
//! table: WAL replay is their sole recovery path, so a replayed record wins
//! outright rather than being masked by a durable store. The row-level-security
//! half of this invariant, on both the document and key-value engines, is
//! covered in `restart_refused_write_not_resurrected.rs`.

mod crash_harness;

use std::time::Duration;

use crash_harness::CrashHarness;

/// Checkpoints run on a periodic timer, and one landing between the refused
/// statement and the kill would flush engine state to disk independently of the
/// WAL — recovery would then come up correct for a reason that has nothing to
/// do with the abort record. Pushing the interval out to an hour makes that
/// physically impossible inside a test that finishes in seconds, so a pass can
/// only mean replay itself excluded the refused write.
fn harness() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
}

/// Run `sql` as the superuser and require the server to refuse it.
///
/// A statement that is ACCEPTED here fails the test immediately: the whole
/// point is what happens to a refused write's WAL record, so a test that
/// silently proceeded on an accepted write would assert nothing.
///
/// The message is read off the attached `DbError`; the `tokio_postgres::Error`
/// wrapper's own `Display` is the fixed string "db error", which would make
/// every refusal indistinguishable from every other failure.
async fn refuse(h: &CrashHarness, sql: &str) -> String {
    let (client, connection) = tokio_postgres::connect(&h.pgwire_conn_str(), tokio_postgres::NoTls)
        .await
        .unwrap_or_else(|e| panic!("connect: {e}"));
    let handle = tokio::spawn(async move {
        let _ = connection.await;
    });
    let result = client.simple_query(sql).await;
    drop(client);
    handle.abort();
    match result {
        Ok(_) => panic!("the server accepted a statement it must refuse: {sql}"),
        Err(e) => e
            .as_db_error()
            .map(|db| format!("{}: {}", db.code().code(), db.message()))
            .unwrap_or_else(|| e.to_string()),
    }
}

/// Every `id|owner|note` row of `collection`, read back as the superuser.
async fn stored(h: &CrashHarness, collection: &str) -> Vec<String> {
    let ids = h
        .query_col(&format!("SELECT id FROM {collection} ORDER BY id"), "id")
        .await;
    let owners = h
        .query_col(
            &format!("SELECT owner FROM {collection} ORDER BY id"),
            "owner",
        )
        .await;
    let notes = h
        .query_col(
            &format!("SELECT note FROM {collection} ORDER BY id"),
            "note",
        )
        .await;
    ids.into_iter()
        .zip(owners)
        .zip(notes)
        .map(|((id, owner), note)| format!("{id}|{owner}|{note}"))
        .collect()
}

/// A unique-constraint violation is decided in the storage engine, after the
/// funnel already appended the insert's redo record. The refused INSERT carries
/// a different `note`, so a replay that re-applies its absolute-overwrite
/// post-image clobbers the row that legitimately exists.
#[tokio::test(flavor = "multi_thread")]
async fn constraint_refused_insert_is_not_resurrected_by_replay() {
    let mut h = harness();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    h.exec(
        "CREATE COLLECTION refused_pk (id TEXT PRIMARY KEY, owner TEXT, note TEXT) \
         WITH (engine='kv')",
    )
    .await;
    h.exec("INSERT INTO refused_pk (id, owner, note) VALUES ('dup', 'nodedb', 'original')")
        .await;

    let before = stored(&h, "refused_pk").await;
    assert_eq!(before, vec!["dup|nodedb|original".to_string()]);

    let message = refuse(
        &h,
        "INSERT INTO refused_pk (id, owner, note) VALUES ('dup', 'nodedb', 'resurrected')",
    )
    .await;
    assert!(
        message.contains("23505") || message.to_lowercase().contains("duplicate"),
        "the insert must be refused as a duplicate-key/unique violation, not by some \
         unrelated failure that would make this test pass for the wrong reason: {message}"
    );

    assert_eq!(
        stored(&h, "refused_pk").await,
        before,
        "a refused insert must leave the existing row untouched even before any crash"
    );

    h.kill_9();
    h.reopen();

    assert_eq!(
        stored(&h, "refused_pk").await,
        before,
        "an insert the server refused on a unique-constraint violation was replayed \
         after a restart and overwrote the existing row"
    );
}
