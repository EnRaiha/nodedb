// SPDX-License-Identifier: BUSL-1.1

//! BACKUP and RESTORE carry arrays: the catalog row and every cell version.
//!
//! The backup records one verification record per array: one row per cell
//! version, tombstones included. A restore into a fresh server recreates the
//! array and its cells, and a repeated restore over the restored array
//! verifies again.

use nodedb_types::backup_envelope::{
    CollectionVerification, DEFAULT_MAX_TOTAL_BYTES, SECTION_ORIGIN_ARRAY_CATALOG,
    SECTION_ORIGIN_VERIFICATION, VerifiedPart, parse_encrypted,
};

use super::backup_support::{drain_backup, push_restore};
use crate::harness::{TEST_BACKUP_KEK, TestServer};

const TENANT: u64 = 1;

async fn pause() {
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
}

/// A backup of an array with four cell versions: two inserts, an overwrite
/// after a flush, and a delete.
async fn source_backup() -> Vec<u8> {
    let source = TestServer::start().await;
    source
        .exec("CREATE ARRAY ra DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16)")
        .await
        .expect("create the array");
    source
        .exec("INSERT INTO ARRAY ra COORDS (1) VALUES (10), COORDS (2) VALUES (20)")
        .await
        .expect("insert the first cells");
    source
        .exec("SELECT ARRAY_FLUSH('ra')")
        .await
        .expect("flush");
    pause().await;
    source
        .exec("INSERT INTO ARRAY ra COORDS (2) VALUES (21)")
        .await
        .expect("overwrite a cell");
    pause().await;
    source
        .exec("DELETE FROM ARRAY ra WHERE COORDS IN ((1))")
        .await
        .expect("delete a cell");
    drain_backup(&source.client, TENANT)
        .await
        .expect("take the backup")
}

fn sections_of(envelope: &[u8], origin: u64) -> Vec<Vec<u8>> {
    parse_encrypted(envelope, DEFAULT_MAX_TOTAL_BYTES, &TEST_BACKUP_KEK)
        .expect("parse the envelope")
        .sections
        .into_iter()
        .filter(|s| s.origin_node_id == origin)
        .map(|s| s.body)
        .collect()
}

async fn live_cells(server: &TestServer) -> Vec<String> {
    let rows = server
        .query_named_rows("SELECT * FROM ARRAY_SLICE('ra', '{x: [0, 15]}', ['v'], 100)")
        .await
        .expect("slice the restored array");
    rows.iter()
        .map(|row| {
            let mut cells: Vec<String> = row.values().cloned().collect();
            cells.sort();
            cells.join(",")
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_backup_carries_the_array_row_and_every_version() {
    let backup = source_backup().await;
    assert_eq!(
        sections_of(&backup, SECTION_ORIGIN_ARRAY_CATALOG).len(),
        1,
        "one array catalog section"
    );
    let verification = sections_of(&backup, SECTION_ORIGIN_VERIFICATION);
    let records: Vec<CollectionVerification> =
        zerompk::from_msgpack(&verification[0]).expect("decode the verification section");
    let array = records
        .iter()
        .find(|r| r.collection == "ra" && r.part == VerifiedPart::Array)
        .unwrap_or_else(|| panic!("no array record: {records:?}"));
    assert_eq!(array.tally.count, 4, "three puts and one tombstone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_recreates_the_array_and_its_live_cells() {
    let backup = source_backup().await;

    let target = TestServer::start().await;
    for attempt in ["first", "second"] {
        push_restore(&target.client, TENANT, backup.clone())
            .await
            .unwrap_or_else(|e| panic!("{attempt} restore: {e}"));
    }
    let cells = live_cells(&target).await;
    assert_eq!(
        cells.len(),
        1,
        "only the overwritten cell is live: {cells:?}"
    );
    assert!(
        cells[0].contains("21"),
        "the newest value is live: {cells:?}"
    );
}

/// A destination array of the same name and another schema refuses the
/// restore before it writes anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_refuses_an_array_of_another_schema() {
    let backup = source_backup().await;

    let target = TestServer::start().await;
    target
        .exec("CREATE ARRAY ra DIMS (x INT64 [0..31]) ATTRS (v INT64) TILE_EXTENTS (32)")
        .await
        .expect("create a different array");
    let err = push_restore(&target.client, TENANT, backup)
        .await
        .expect_err("another schema refuses the restore");
    assert!(err.contains("another schema"), "{err}");
}
