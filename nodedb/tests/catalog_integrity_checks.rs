// SPDX-License-Identifier: BUSL-1.1

//! Cross-table referential integrity checks 6–8 (dangling-reference
//! divergences) plus the end-to-end clean-integrity test that applies
//! every parent-replicated `Put<T>` variant and asserts zero violations.

mod catalog_integrity_helpers;

use catalog_integrity_helpers::*;
use nodedb::control::catalog_entry::CatalogEntry;
use nodedb::control::catalog_entry::apply::apply_to;
use nodedb::control::cluster::recovery_check::divergence::DivergenceKind;
use nodedb::control::cluster::recovery_check::integrity::verify_redb_integrity;

// ── Check 6: materialized_view.source → collection ────────────────────────

#[test]
fn check_6_flags_dangling_materialized_view_source() {
    let (_dir, catalog) = make_catalog();
    catalog
        .put_materialized_view(&make_mv_sourced("mv_ghost", "ghost"))
        .unwrap();

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.iter().any(|v| matches!(
            &v.kind,
            DivergenceKind::DanglingReference {
                from_kind: "materialized_view",
                to_kind: "collection",
                ..
            }
        )),
        "Check 6 must flag dangling MV source: {violations:?}"
    );
}

// ── Check 7: change_stream.collection → collection (wildcard exempt) ──────

#[test]
fn check_7_flags_dangling_change_stream_collection() {
    let (_dir, catalog) = make_catalog();
    let mut stream = make_stream("cs_ghost");
    stream.collection = "ghost".into();
    catalog.put_change_stream(&stream).unwrap();

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.iter().any(|v| matches!(
            &v.kind,
            DivergenceKind::DanglingReference {
                from_kind: "change_stream",
                to_kind: "collection",
                ..
            }
        )),
        "Check 7 must flag dangling change-stream collection: {violations:?}"
    );
}

#[test]
fn check_7_ignores_wildcard_change_stream() {
    let (_dir, catalog) = make_catalog();
    catalog.put_change_stream(&make_stream("cs_star")).unwrap();

    let violations = verify_redb_integrity(&catalog);
    assert!(
        !violations.iter().any(|v| matches!(
            &v.kind,
            DivergenceKind::DanglingReference {
                from_kind: "change_stream",
                to_kind: "collection",
                ..
            }
        )),
        "Check 7 must skip wildcard (`*`) streams: {violations:?}"
    );
}

// ── Check 8: schedule.target_collection → collection (None exempt) ────────

#[test]
fn check_8_flags_dangling_schedule_target_collection() {
    let (_dir, catalog) = make_catalog();
    let mut sch = make_schedule("sch_ghost");
    sch.target_collection = Some("ghost".into());
    catalog.put_schedule(&sch).unwrap();

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.iter().any(|v| matches!(
            &v.kind,
            DivergenceKind::DanglingReference {
                from_kind: "schedule",
                to_kind: "collection",
                ..
            }
        )),
        "Check 8 must flag dangling schedule target_collection: {violations:?}"
    );
}

#[test]
fn check_8_ignores_schedule_without_target() {
    let (_dir, catalog) = make_catalog();
    // Cross-collection / opaque job: target_collection is None. Must
    // NOT trigger Check 8 — those schedules run on `_system`.
    catalog.put_schedule(&make_schedule("sch_cross")).unwrap();

    let violations = verify_redb_integrity(&catalog);
    assert!(
        !violations.iter().any(|v| matches!(
            &v.kind,
            DivergenceKind::DanglingReference {
                from_kind: "schedule",
                to_kind: "collection",
                ..
            }
        )),
        "Check 8 must skip schedules with target_collection=None: {violations:?}"
    );
}

// ── end-to-end: applying every parent-replicated Put<T> leaves no ─────────
//    integrity violations. ────────────────────────────────────────────────

#[test]
fn apply_all_put_entries_produces_clean_redb_integrity() {
    let (_dir, catalog) = make_catalog();

    apply_to(
        &CatalogEntry::PutCollection(Box::new(make_collection("orders"))),
        &catalog,
    )
    .expect("apply put_collection");
    apply_to(
        &CatalogEntry::PutFunction(Box::new(make_function("f1"))),
        &catalog,
    )
    .expect("apply put_function");
    apply_to(
        &CatalogEntry::PutProcedure(Box::new(make_procedure("p1"))),
        &catalog,
    )
    .expect("apply put_procedure");
    apply_to(
        &CatalogEntry::PutTrigger(Box::new(make_trigger("t1", "orders"))),
        &catalog,
    )
    .expect("apply put_trigger");
    // MV source must match an existing collection or Check 6 will flag it.
    apply_to(
        &CatalogEntry::PutMaterializedView(Box::new(make_mv_sourced("mv1", "orders"))),
        &catalog,
    )
    .expect("apply put_materialized_view");
    apply_to(
        &CatalogEntry::PutSequence(Box::new(make_sequence("s1"))),
        &catalog,
    )
    .expect("apply put_sequence");
    apply_to(
        &CatalogEntry::PutSchedule(Box::new(make_schedule("sch1"))),
        &catalog,
    )
    .expect("apply put_schedule");
    apply_to(
        &CatalogEntry::PutChangeStream(Box::new(make_stream("cs1"))),
        &catalog,
    )
    .expect("apply put_change_stream");

    let violations = verify_redb_integrity(&catalog);
    assert!(
        violations.is_empty(),
        "expected zero integrity violations after applying all \
         parent-replicated Put entries, got: {:?}",
        violations
    );
}
