// SPDX-License-Identifier: BUSL-1.1

//! `verify_and_repair` recovery contract: an orphan row from a
//! parent-replicated DDL type must not brick startup.
//!
//! The current `CatalogSanityCheck` pass is fail-fast on any
//! `OrphanRow` divergence — `verify_redb_integrity` reports it,
//! `verify_and_repair` passes it through unchanged into
//! `VerifyReport.integrity_violations`, and `is_acceptable()`
//! returns `false`. The `StartupSequencer` then transitions to
//! `Failed` and the server refuses to come up. The operator's only
//! recovery today is to wipe `system.redb`, which loses every user,
//! role, and collection registration on disk.
//!
//! Every primary row for the eight parent-replicated DDL types
//! carries the owner identity in-band — `StoredCollection.owner`,
//! `StoredFunction.owner`, etc. — so the missing `StoredOwner` row is
//! always reconstructible from the surviving primary. The
//! alternative (quarantine the primary, log a warning, continue
//! startup) is also acceptable; both satisfy the spec asserted here:
//! after `verify_and_repair` returns, `report.is_acceptable()` must
//! be `true` and the catalog must be in a consistent state — every
//! surviving primary row has its matching owner row, OR the primary
//! was quarantined out of the live tables.
//!
//! These tests assert the spec, not the implementation choice
//! between auto-repair and quarantine. Both pass.

mod catalog_integrity_helpers;

use std::sync::Arc;

use catalog_integrity_helpers::*;
use nodedb::bridge::dispatch::Dispatcher;
use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;
use nodedb::control::cluster::verify_and_repair;
use nodedb::control::security::credential::store::CredentialStore;
use nodedb::control::state::SharedState;
use nodedb::wal::WalManager;

/// Build a `SharedState` whose `credentials.catalog()` returns a
/// real on-disk catalog the test can plant orphans in. Mirrors
/// `catalog_recovery_check.rs::make_shared`.
fn make_shared() -> (tempfile::TempDir, Arc<SharedState>, Arc<CredentialStore>) {
    let dir = tempfile::tempdir().unwrap();
    let wal_path = dir.path().join("test.wal");
    let catalog_path = dir.path().join("system.redb");

    let wal = Arc::new(WalManager::open_for_testing(&wal_path).unwrap());
    let (dispatcher, _data_sides) = Dispatcher::new(1, 64);
    let credentials = Arc::new(CredentialStore::open(&catalog_path).unwrap());
    put_admin_user(credentials.catalog());
    let shared =
        SharedState::new_with_credentials(dispatcher, wal, Arc::clone(&credentials)).unwrap();
    (dir, shared, credentials)
}

/// After repair, the catalog must be in a consistent state: every
/// surviving primary row of `object_type` has its matching owner row
/// (auto-repair path), OR the primary row is gone (quarantine path).
/// A surviving primary with no matching owner is the exact state the
/// bug puts redb into and must not persist after repair.
fn assert_consistent_after_repair(
    catalog: &nodedb::control::security::catalog::SystemCatalog,
    object_type: &str,
    object_name: &str,
) {
    let owner_present = owner_row_present(catalog, object_type, object_name);
    let violations = verify_redb_integrity(catalog);
    let primary_still_orphaned = violations.iter().any(|v| {
        matches!(
            &v.kind,
            nodedb::control::cluster::recovery_check::divergence::DivergenceKind::OrphanRow {
                kind,
                expected_parent_kind: "owner",
                ..
            } if *kind == object_type
        )
    });
    assert!(
        !primary_still_orphaned,
        "verify_and_repair must not leave an {object_type}('{object_name}') \
         primary row orphaned. Either reconstruct the owner row from the \
         primary's in-band `owner` field, or quarantine the primary. \
         Owner present after repair: {owner_present}, raw violations: {violations:?}"
    );
}

async fn run_repair(shared: &SharedState) -> nodedb::control::cluster::VerifyReport {
    verify_and_repair(shared)
        .await
        .expect("verify_and_repair should return Ok even on dirty catalog")
}

// ── 1. Collection — the single-DDL orphan wedge ──────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_collection() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog
        .put_collection(
            nodedb_types::DatabaseId::DEFAULT,
            &make_collection("orphan_repro"),
        )
        .unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "a single CREATE COLLECTION that left only the primary \
         row in redb must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "collection", "orphan_repro");
}

// ── 2. Function ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_function() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog.put_function(&make_function("orphan_fn")).unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan function row from a CREATE FUNCTION on single-node must \
         not brick the next restart. VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "function", "orphan_fn");
}

// ── 3. Procedure ─────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_procedure() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog
        .put_procedure(&make_procedure("orphan_proc"))
        .unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan procedure row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "procedure", "orphan_proc");
}

// ── 4. Trigger (needs a parent collection with its own owner row) ───────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_trigger() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    // Plant the parent collection via the *correct* apply path so the
    // dangling-reference check (Check 4: trigger.collection →
    // collection) does not also fire. This test is about the trigger
    // orphan only.
    apply_to(
        &CatalogEntry::PutCollection(Box::new(make_collection("orders"))),
        catalog,
    );
    catalog
        .put_trigger(&make_trigger("orphan_trg", "orders"))
        .unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan trigger row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "trigger", "orphan_trg");
}

// ── 5. Materialized view (needs a parent collection) ────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_materialized_view() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    // Source collection must exist & be owned, so Check 6 (mv.source
    // → collection) doesn't fire.
    apply_to(
        &CatalogEntry::PutCollection(Box::new(make_collection("mv_src"))),
        catalog,
    );
    catalog
        .put_materialized_view(&make_mv_sourced("orphan_mv", "mv_src"))
        .unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan materialized_view row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "materialized_view", "orphan_mv");
}

// ── 6. Sequence ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_sequence() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog.put_sequence(&make_sequence("orphan_seq")).unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan sequence row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "sequence", "orphan_seq");
}

// ── 7. Schedule ──────────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_schedule() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog.put_schedule(&make_schedule("orphan_sch")).unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan schedule row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "schedule", "orphan_sch");
}

// ── 8. Change stream ─────────────────────────────────────────────────────────

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_and_repair_recovers_orphan_change_stream() {
    let (_dir, shared, credentials) = make_shared();
    let catalog = credentials.catalog();
    catalog
        .put_change_stream(&make_stream("orphan_cs"))
        .unwrap();

    let report = run_repair(&shared).await;
    assert!(
        report.is_acceptable(),
        "an orphan change_stream row must not brick the next restart. \
         VerifyReport: {report}"
    );
    assert_consistent_after_repair(catalog, "change_stream", "orphan_cs");
}
