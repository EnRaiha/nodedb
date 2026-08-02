// SPDX-License-Identifier: BUSL-1.1

//! Pins a real captured failure: after `kill -9` + reopen, the metadata Raft
//! applier can wedge permanently on replay. A faultbox report from a real run
//! showed the exact shape —
//!
//!   entry_kind = DdlPrepared, raft_index = 3, last_applied_watermark = 2,
//!   error: "descriptor version anomaly for 'crash_ilp_ts': replicated
//!           version 1 is inconsistent with local prior 1 (expected 1 or
//!           prior+1)"
//!
//! `descriptor_validate::validate()` (`src/control/catalog_entry/descriptor_validate.rs`)
//! rejects a replayed `PutCollection` whose carried version equals the
//! locally persisted version but whose payload differs, and
//! `metadata_applier/dispatch.rs` refuses to advance the apply watermark past
//! an entry it could not durably apply — so every later committed entry
//! never applies either, `/healthz` stays green, and every subsequent query
//! fails with a descriptor-lease timeout.
//!
//! `crash_recovery.rs` and `crash_recovery_analytics.rs` also create
//! collections (including a timeseries collection with the exact same column
//! shape as the one in the original report), `kill -9`, reopen, and query —
//! and never hit this. The only structural difference in the sequence that
//! produced the original report is that it issued THREE separate
//! catalog-affecting DDL statements back to back, immediately after boot and
//! before any DML (`CREATE COLLECTION`, `CREATE USER`, `GRANT ROLE`), each
//! going through its own propose/apply round trip against the metadata Raft
//! group. No passing crash test issues more than one such DDL statement
//! before a collection's first write. `CREATE USER` and `GRANT ROLE`
//! themselves produce `PutUser` / `PutRole` catalog entries, which
//! `descriptor_stamp::stamp()` explicitly passes through unstamped (they
//! carry no `descriptor_version` at all — see the "Variants without
//! descriptor fields" list in that module) and `validate()` defaults to
//! `Apply` for; they cannot themselves conflict with `crash_ilp_ts`'s
//! version. This test reproduces the sequence with the fewest steps that
//! still matches every structural feature of the original report — a
//! multi-statement DDL burst on a freshly booted node — while dropping ILP
//! and the native protocol handshake, neither of which writes to the catalog
//! at all and so cannot participate in a descriptor-version conflict.

mod crash_harness;

use std::time::Duration;

use crash_harness::CrashHarness;

const COLLECTION: &str = "crash_wedge_ts";
const USER_PASSWORD: &str = "crash-wedge-secret-1";

/// Same rationale as the other crash tests that tune this: an incidental
/// checkpoint landing between the DDL burst and the kill could compact or
/// skip log entries in a way that changes what boot-time replay actually
/// re-delivers, masking the exact replay shape the original report captured.
fn no_incidental_checkpoint() -> CrashHarness {
    CrashHarness::new().with_env("NODEDB_CHECKPOINT_INTERVAL_SECS", "3600")
}

/// After `kill -9` + reopen, a query against the collection created before
/// the crash must succeed. If the metadata applier wedged on a replayed
/// `DdlPrepared` entry, this hangs the applier permanently: `/healthz`
/// reports ready (the applier failure does not gate readiness) but every
/// query that needs a descriptor lease times out, because the lease can
/// never be granted past the stuck watermark.
#[tokio::test(flavor = "multi_thread")]
async fn ddl_burst_after_boot_does_not_wedge_metadata_applier_on_replay() {
    let mut h = no_incidental_checkpoint();
    h.spawn();
    h.wait_ready(Duration::from_secs(20));

    // Three catalog-affecting DDL statements, back to back, before any DML —
    // the structural feature every passing crash test lacks. Only the first
    // touches `crash_wedge_ts`'s descriptor; the other two are unrelated
    // descriptors (`PutUser`, `PutRole`) included because the original
    // report's sequence had them and dropping them is not yet proven safe.
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

    // Live sanity BEFORE the crash: the collection is queryable pre-restart,
    // so any post-restart failure is attributable to replay, not to the DDL
    // burst itself never having landed.
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

    // A wedged applier never fails this query outright — it hangs until the
    // descriptor-lease wait times out, so the panic from a stuck `exec` reads
    // as an unrelated lease timeout unless the faultbox assertion below has
    // already named the real cause.
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

    // Prove the capture site actually caught the root cause rather than only
    // the downstream symptom above. A `DdlPrepared` / descriptor-anomaly
    // report for `crash_wedge_ts` means replay rejected an already-applied
    // entry and the applier's watermark is stuck behind it — the exact shape
    // of the original captured failure. The grouping key format
    // (`entry=<kind>;cause=<class>`) is not exposed on `Report` directly, so
    // match on the JSON domain payload instead — that carries `entry_kind`
    // and `error_class` as recorded by `diag::metadata_apply_wedged`.
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
