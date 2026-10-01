// SPDX-License-Identifier: BUSL-1.1

//! A restore over an existing array of the same schema applies the backup's
//! catalog settings and keeps the array's incarnation.

use nodedb::control::backup::{backup_tenant, restore_tenant};
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::{DatabaseId, TenantId};

const TENANT: u64 = 1;

fn create(audit_retain_ms: u64) -> String {
    format!(
        "CREATE ARRAY kept DIMS (x INT64 [0..15]) ATTRS (v INT64) TILE_EXTENTS (16) \
         WITH (audit_retain_ms = {audit_retain_ms}, minimum_audit_retain_ms = 0)"
    )
}

#[tokio::test]
async fn a_restore_applies_the_backup_settings_to_a_kept_array() {
    let source = TestServer::start().await;
    source
        .exec(&create(7_000))
        .await
        .expect("create the source array");
    source
        .exec("INSERT INTO ARRAY kept COORDS (3) VALUES (30)")
        .await
        .expect("insert a cell");
    let envelope = backup_tenant(&source.shared, TENANT)
        .await
        .expect("take the backup");

    let target = TestServer::start().await;
    target
        .exec(&create(1_000))
        .await
        .expect("create the target array");
    let entry_of = |srv: &TestServer| {
        srv.shared
            .array_catalog
            .read()
            .expect("array catalog")
            .lookup_by_name_in_database(TenantId::new(TENANT), DatabaseId::DEFAULT, "kept")
            .expect("the array exists")
    };
    let before = entry_of(&target);

    restore_tenant(&target.shared, TENANT, &envelope, false, false)
        .await
        .expect("restore over the kept array");

    let after = entry_of(&target);
    assert_eq!(
        after.audit_retain_ms,
        Some(7_000),
        "the backup's retention applies"
    );
    assert_eq!(
        after.incarnation, before.incarnation,
        "the array keeps its incarnation"
    );
    let registry = target.shared.bitemporal_retention_registry.snapshot();
    let retention = registry
        .iter()
        .find(|entry| entry.collection == "kept")
        .expect("the retention registry holds the array");
    assert_eq!(retention.retention.audit_retain_ms, 7_000);

    let cells = target
        .query_text("SELECT * FROM ARRAY_SLICE('kept', '{x: [0, 15]}', ['v'], 10)")
        .await
        .expect("slice the restored array");
    assert_eq!(cells.len(), 1, "the restored cell: {cells:?}");
}
