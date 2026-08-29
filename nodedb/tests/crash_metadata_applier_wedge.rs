// SPDX-License-Identifier: BUSL-1.1

//! Pins a real captured failure: after `kill -9` + reopen, the metadata
//! applier can wedge permanently when a replayed `PutCollection`'s version
//! matches the local one but its payload differs — `/healthz` stays green
//! but every query then fails with a descriptor-lease timeout. Reproduced
//! by a burst of three DDL statements right after boot, before any DML.

mod crash_harness;

use crash_harness::CrashHarness;

const COLLECTION: &str = "crash_wedge_ts";
const USER_PASSWORD: &str = "crash-wedge-secret-1";

/// An incidental checkpoint between the DDL burst and the kill could compact
/// log entries, masking the exact replay shape under test.
fn no_incidental_checkpoint() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
}

/// After `kill -9` + reopen, a query against the collection created before
/// the crash must succeed. A wedged applier hangs it: `/healthz` reports
/// ready, but the descriptor lease can never be granted past the stuck
/// watermark.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_burst_after_boot_does_not_wedge_metadata_applier_on_replay() {
    let mut h = no_incidental_checkpoint();
    h.spawn();
    h.wait_ready();

    // Three catalog-affecting DDL statements back to back, before any DML.
    // Only the first touches `crash_wedge_ts`'s descriptor.
    h.exec(
        "CREATE COLLECTION crash_wedge_ts \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')",
    )
    .await;
    h.exec(&format!(
        "CREATE USER crash_wedge_user PASSWORD '{USER_PASSWORD}'"
    ))
    .await;
    h.exec("GRANT ROLE readwrite TO crash_wedge_user").await;

    // Pre-crash sanity: confirms the burst landed, before attributing any
    // post-restart failure to replay.
    let live = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {COLLECTION}"), 0)
        .await;
    assert_eq!(
        live,
        vec!["0".to_string()],
        "SELECT COUNT(*) FROM {COLLECTION} must succeed BEFORE the crash (test-setup sanity): \
         got {live:?}"
    );

    h.kill_9();
    h.reopen();

    // A wedged applier hangs until the descriptor-lease wait times out, so
    // this reads as a lease timeout unless the faultbox check below runs first.
    let count = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {COLLECTION}"), 0)
        .await;
    assert_eq!(
        count,
        vec!["0".to_string()],
        "SELECT COUNT(*) FROM {COLLECTION} must succeed after kill -9 + reopen; a failure or \
         hang here means the metadata Raft applier wedged on replay and every query now fails \
         with a descriptor-lease timeout while /healthz still reports ready (got {count:?})"
    );

    // Confirm the capture site caught the root cause, not just the symptom.
    // Grouping key isn't exposed on `Report`, so match the JSON domain payload.
    let reports = crash_harness::diagnostics::faultbox_reports(h.data_dir());
    let descriptor_anomaly_wedges: Vec<String> = reports
        .iter()
        .filter(|g| {
            g.first.domain_kind.as_deref() == Some("nodedb.metadata_apply_wedged")
                && g.first.domain.get("entry_kind").and_then(|v| v.as_str()) == Some("DdlPrepared")
                && g.first
                    .domain
                    .get("error_class")
                    .and_then(|v| v.as_str())
                    .is_some_and(|class| class.contains(COLLECTION))
        })
        .map(faultbox::reader::Group::summary)
        .collect();
    assert!(
        descriptor_anomaly_wedges.is_empty(),
        "metadata applier wedged on a replayed DdlPrepared entry with a descriptor-version \
         anomaly for '{COLLECTION}' — an already-applied entry was rejected on replay and the \
         apply watermark never advanced past it, so every later query fails with a \
         descriptor-lease timeout even though /healthz reports ready: {descriptor_anomaly_wedges:?} \
         (all faultbox reports: {:?})",
        reports
            .iter()
            .map(faultbox::reader::Group::summary)
            .collect::<Vec<_>>(),
    );
}

const RECREATED_COLLECTION: &str = "crash_wedge_recreated";

/// `DROP COLLECTION` (soft delete, no `PURGE`) leaves the inactive row in
/// place — `CREATE COLLECTION` under the same name is legal against an
/// inactive row (only an *active* row blocks it), and re-derives the next
/// descriptor version from that same inactive row. If the DROP never
/// stamped a fresh `descriptor_version` / `modification_hlc` on the way
/// down, the CREATE that follows it, and any replay of the sequence after a
/// crash, has nothing but the original CREATE's ordering metadata to derive
/// from — the exact scenario this test pins.
#[tokio::test(flavor = "multi_thread")]
async fn create_drop_recreate_does_not_wedge_metadata_applier_on_replay() {
    let mut h = no_incidental_checkpoint();
    h.spawn();
    h.wait_ready();

    // CREATE, soft DROP, then CREATE the same name again — all before any
    // DML, so replay must reconstruct the descriptor lineage from DDL alone.
    h.exec(&format!(
        "CREATE COLLECTION {RECREATED_COLLECTION} \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await;
    h.exec(&format!("DROP COLLECTION {RECREATED_COLLECTION}"))
        .await;
    h.exec(&format!(
        "CREATE COLLECTION {RECREATED_COLLECTION} \
         COLUMNS (id TEXT, ts BIGINT TIME_KEY, metric TEXT, value FLOAT) \
         WITH (engine='timeseries')"
    ))
    .await;

    // Pre-crash sanity: confirms the recreate landed, before attributing any
    // post-restart failure to replay.
    let live = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {RECREATED_COLLECTION}"), 0)
        .await;
    assert_eq!(
        live,
        vec!["0".to_string()],
        "SELECT COUNT(*) FROM {RECREATED_COLLECTION} must succeed BEFORE the crash \
         (test-setup sanity): got {live:?}"
    );

    h.kill_9();
    h.reopen();

    // A wedged applier hangs until the descriptor-lease wait times out, so
    // this reads as a lease timeout unless the faultbox check below runs first.
    let count = h
        .query_col_idx(&format!("SELECT COUNT(*) FROM {RECREATED_COLLECTION}"), 0)
        .await;
    assert_eq!(
        count,
        vec!["0".to_string()],
        "SELECT COUNT(*) FROM {RECREATED_COLLECTION} must succeed after kill -9 + reopen; a \
         failure or hang here means the metadata Raft applier wedged on replay of the \
         CREATE/DROP/CREATE sequence and every later query now fails with a descriptor-lease \
         timeout while /healthz still reports ready (got {count:?})"
    );

    // Confirm the capture site caught the root cause, not just the symptom.
    let reports = crash_harness::diagnostics::faultbox_reports(h.data_dir());
    let descriptor_anomaly_wedges: Vec<String> = reports
        .iter()
        .filter(|g| {
            g.first.domain_kind.as_deref() == Some("nodedb.metadata_apply_wedged")
                && g.first.domain.get("entry_kind").and_then(|v| v.as_str()) == Some("DdlPrepared")
                && g.first
                    .domain
                    .get("error_class")
                    .and_then(|v| v.as_str())
                    .is_some_and(|class| class.contains(RECREATED_COLLECTION))
        })
        .map(faultbox::reader::Group::summary)
        .collect();
    assert!(
        descriptor_anomaly_wedges.is_empty(),
        "metadata applier wedged on a replayed DdlPrepared entry with a descriptor-version \
         anomaly for '{RECREATED_COLLECTION}' after a CREATE/DROP/CREATE sequence — the soft \
         DROP left the row's descriptor_version and modification_hlc unstamped, so the \
         recreate's ordering metadata collided with the original CREATE's on replay and the \
         apply watermark never advanced past it: {descriptor_anomaly_wedges:?} (all faultbox \
         reports: {:?})",
        reports
            .iter()
            .map(faultbox::reader::Group::summary)
            .collect::<Vec<_>>(),
    );
}
