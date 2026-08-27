// SPDX-License-Identifier: BUSL-1.1

//! `mirror_eventual_read_serves_local`: read with `ReadConsistency::Eventual`
//! must serve locally even when the mirror lag is far beyond any reasonable
//! bounded-staleness window (>60 s simulated).
//!
//! Tests the catalog-level consistency gate directly.

use nodedb_types::{DatabaseId, Lsn, MirrorMode, MirrorOrigin, MirrorStatus};
use tempfile::TempDir;

use nodedb::control::server::pgwire::ddl::database::{
    MirrorReadOutcome, check_mirror_read_consistency,
};
use nodedb::types::ReadConsistency;

use super::helpers::{TEST_SOURCE_CLUSTER, inject_lag_record_for_id, now_ms, open_tmp_catalog};

#[test]
fn mirror_eventual_read_serves_local() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(2001);

    // Write a lag record that is 120 seconds stale — far beyond any
    // practical BoundedStaleness window.
    inject_lag_record_for_id(&catalog, db_id, 120_000, 1);

    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        mode: MirrorMode::Async,
        last_applied: Lsn::new(1),
        status: MirrorStatus::Degraded { lag_ms: 120_000 },
    };

    // Eventual always serves locally regardless of lag.
    match check_mirror_read_consistency(
        &catalog,
        db_id,
        &origin,
        ReadConsistency::Eventual,
        now_ms(),
    ) {
        MirrorReadOutcome::ServeLocally => {}
        MirrorReadOutcome::Reject { message, .. } => {
            panic!("Eventual read must always serve locally, got reject: {message}");
        }
    }
}

#[test]
fn mirror_eventual_serves_even_when_disconnected() {
    let dir = TempDir::new().unwrap();
    let catalog = open_tmp_catalog(&dir);
    let db_id = DatabaseId::new(2002);

    let origin = MirrorOrigin {
        source_cluster: TEST_SOURCE_CLUSTER.to_string(),
        source_database: DatabaseId::new(0),
        mode: MirrorMode::Async,
        last_applied: Lsn::new(0),
        status: MirrorStatus::Disconnected,
    };

    match check_mirror_read_consistency(
        &catalog,
        db_id,
        &origin,
        ReadConsistency::Eventual,
        now_ms(),
    ) {
        MirrorReadOutcome::ServeLocally => {}
        MirrorReadOutcome::Reject { message, .. } => {
            panic!("Eventual read on Disconnected mirror must still serve locally: {message}");
        }
    }
}
