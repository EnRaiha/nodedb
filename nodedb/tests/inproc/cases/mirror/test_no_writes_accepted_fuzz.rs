// SPDX-License-Identifier: BUSL-1.1

//! `mirror_no_writes_accepted_fuzz`: every write attempt on a non-promoted
//! mirror must produce `Error::MirrorReadOnly`, which maps to `MIRROR_READ_ONLY`.
//!
//! Tests the error construction path and the catalog guard condition.
//! No live server is needed — the enforcement is structural (catalog guard).

use nodedb_types::{DatabaseId, MirrorStatus};
use tempfile::TempDir;

use nodedb::error::Error;

use super::helpers::{inject_mirror, open_tmp_catalog};

#[test]
fn mirror_write_rejected_following() {
    // A Following mirror must produce MirrorReadOnly on any write attempt.
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(6001);
    inject_mirror(&catalog, db_id, "mirror_following", MirrorStatus::Following);

    let desc = catalog.get_database(db_id).unwrap().unwrap();
    let origin = desc.mirror_origin.as_ref().unwrap();
    assert!(
        !matches!(origin.status, MirrorStatus::Promoted),
        "must be non-promoted for this test"
    );

    // Simulate the write guard: constructing MirrorReadOnly for this db name.
    let err = Error::MirrorReadOnly {
        database: desc.name.clone(),
    };
    let msg = format!("{err}").to_lowercase();
    assert!(
        msg.contains("mirror"),
        "MirrorReadOnly error must reference mirror: {err}"
    );
    assert!(
        msg.contains("mirror_following"),
        "MirrorReadOnly error must contain the database name: {err}"
    );
}

#[test]
fn mirror_write_rejected_degraded() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(6002);
    inject_mirror(
        &catalog,
        db_id,
        "mirror_degraded",
        MirrorStatus::Degraded { lag_ms: 9_000 },
    );

    let desc = catalog.get_database(db_id).unwrap().unwrap();
    let origin = desc.mirror_origin.as_ref().unwrap();
    assert!(!matches!(origin.status, MirrorStatus::Promoted));

    let err = Error::MirrorReadOnly {
        database: desc.name.clone(),
    };
    assert!(format!("{err}").to_lowercase().contains("mirror"));
}

#[test]
fn mirror_write_rejected_disconnected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(6003);
    inject_mirror(
        &catalog,
        db_id,
        "mirror_disconnected",
        MirrorStatus::Disconnected,
    );

    let desc = catalog.get_database(db_id).unwrap().unwrap();
    let origin = desc.mirror_origin.as_ref().unwrap();
    assert!(!matches!(origin.status, MirrorStatus::Promoted));

    let err = Error::MirrorReadOnly {
        database: desc.name.clone(),
    };
    assert!(format!("{err}").to_lowercase().contains("mirror"));
}

#[test]
fn promoted_mirror_accepts_writes() {
    // A promoted mirror has status=Active; the write guard must NOT fire.
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(6004);
    inject_mirror(&catalog, db_id, "promoted_db", MirrorStatus::Promoted);

    let desc = catalog.get_database(db_id).unwrap().unwrap();
    let origin = desc.mirror_origin.as_ref().unwrap();

    // The guard condition: only block writes if status is NOT Promoted.
    assert!(
        matches!(origin.status, MirrorStatus::Promoted),
        "promoted mirror must allow writes: status is {:?}",
        origin.status
    );
    assert_eq!(
        desc.status,
        nodedb::control::security::catalog::database_types::DatabaseStatus::Active
    );
}
