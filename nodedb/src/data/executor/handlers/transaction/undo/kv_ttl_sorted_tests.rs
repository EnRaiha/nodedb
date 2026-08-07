// SPDX-License-Identifier: BUSL-1.1

//! Unit tests for `Expire`/`Persist`/`RegisterSortedIndex`/`DropSortedIndex`
//! at the COMMIT-replay level (`execute_tx_sub_plan` -> `execute_tx_kv`).
//!
//! `plan_requires_txn_buffering`
//! (`control/server/shared/write_admission/predicate/txn_buffering.rs`)
//! classifies all four `true` (buffered), so a client statement issued
//! inside `BEGIN ... COMMIT` replays through this exact call at COMMIT.
//! Pre-fix, `execute_tx_kv`'s reject arm returned
//! `ErrorCode::Internal { detail: "KV DDL / TTL operations are not
//! permitted inside a TransactionBatch" }` for all four, so every
//! `.expect(...)` below on the sub-plan's result is the assertion that used
//! to fail with that error.
//!
//! Two of the four have no SQL surface that reaches this path today, which
//! is why these tests drive `execute_tx_sub_plan` directly rather than a
//! `sql_transactions_*.rs` pgwire integration test:
//!
//! - `Expire`/`Persist` have NO pgwire SQL surface at all (no `EXPIRE(...)`
//!   / `PERSIST(...)` SQL function is wired in
//!   `control/server/shared/ddl/neutral/router/string_engine_ops.rs`). The
//!   RESP protocol's `EXPIRE`/`PERSIST` commands exist but RESP has no
//!   `MULTI`/`EXEC` transaction support, so they can never reach a `BEGIN`
//!   block. Only the native binary protocol threads a `txn_id` through
//!   (`control/server/native/dispatch/direct_ops.rs`) for these ops.
//! - `RegisterSortedIndex`/`DropSortedIndex` DO have pgwire SQL syntax
//!   (`CREATE SORTED INDEX` / `DROP SORTED INDEX`,
//!   `control/server/shared/ddl/neutral/kv_sorted_index.rs`), but that
//!   handler dispatches immediately via `dispatch_to_data_plane` without
//!   ever consulting the connection's transaction state -- a separate,
//!   pre-existing gap where SQL sorted-index DDL is never staged at all, so
//!   it cannot replay at COMMIT via that surface regardless of this fix.

use super::UndoEntry;
use crate::bridge::envelope::PhysicalPlan;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::tests::make_core_with_dir;
use crate::engine::kv::current_ms;
use nodedb_physical::physical_plan::KvOp;

const DB: u64 = 0;
const TID: u64 = 1;

fn put_kv(core: &mut CoreLoop, collection: &str, key: &[u8], value: &[u8], ttl_ms: u64) {
    core.kv_engine.put(crate::engine::kv::KvPutParams {
        database_id: DB,
        tenant_id: TID,
        collection,
        key,
        value,
        ttl_ms,
        now_ms: current_ms(),
        surrogate: nodedb_types::Surrogate::ZERO,
    });
}

fn ttl_ms(core: &CoreLoop, collection: &str, key: &[u8]) -> Option<i64> {
    core.kv_engine
        .get_ttl_ms(DB, TID, collection, key, current_ms())
}

// ── Expire ───────────────────────────────────────────────────────────────

#[test]
fn kv_expire_in_tx_commit_replay_sets_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    put_kv(&mut core, "cache", b"k", b"v", 0);
    assert_eq!(
        ttl_ms(&core, "cache", b"k"),
        Some(-1),
        "key must start persistent (no TTL)"
    );

    let plan = PhysicalPlan::Kv(KvOp::Expire {
        collection: "cache".to_string(),
        key: b"k".to_vec(),
        ttl_ms: 5_000,
        rls_write_check: Vec::new(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("EXPIRE sub-plan must succeed at COMMIT replay");

    let remaining = ttl_ms(&core, "cache", b"k").expect("key must still exist after EXPIRE");
    assert!(
        remaining > 0 && remaining <= 5_000,
        "TTL must be set by the COMMIT replay, got {remaining}"
    );
    assert_eq!(undo_log.len(), 1, "EXPIRE must push exactly one undo entry");
    assert!(
        matches!(
            undo_log[0],
            UndoEntry::KvTtl {
                prior_expiry: None,
                ..
            }
        ),
        "prior state (no TTL) must be captured for rollback"
    );
}

#[test]
fn kv_expire_in_tx_rollback_reverts_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    put_kv(&mut core, "cache", b"k", b"v", 0);

    let plan = PhysicalPlan::Kv(KvOp::Expire {
        collection: "cache".to_string(),
        key: b"k".to_vec(),
        ttl_ms: 5_000,
        rls_write_check: Vec::new(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("EXPIRE sub-plan must succeed");
    assert!(ttl_ms(&core, "cache", b"k").unwrap() > 0);

    // A sibling sub-plan fails later in the same COMMIT: reverse the batch.
    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");

    assert_eq!(
        ttl_ms(&core, "cache", b"k"),
        Some(-1),
        "rollback must revert the key to its pre-EXPIRE persistent state"
    );
}

// ── Persist ──────────────────────────────────────────────────────────────

#[test]
fn kv_persist_in_tx_commit_replay_clears_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    put_kv(&mut core, "cache", b"k", b"v", 60_000);
    assert!(
        ttl_ms(&core, "cache", b"k").unwrap() > 0,
        "key must start with a TTL"
    );

    let plan = PhysicalPlan::Kv(KvOp::Persist {
        collection: "cache".to_string(),
        key: b"k".to_vec(),
        rls_write_check: Vec::new(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("PERSIST sub-plan must succeed at COMMIT replay");

    assert_eq!(
        ttl_ms(&core, "cache", b"k"),
        Some(-1),
        "TTL must be cleared by the COMMIT replay"
    );
    assert_eq!(undo_log.len(), 1);
    assert!(
        matches!(
            undo_log[0],
            UndoEntry::KvTtl {
                prior_expiry: Some(_),
                ..
            }
        ),
        "prior TTL instant must be captured for rollback"
    );
}

#[test]
fn kv_persist_in_tx_rollback_restores_ttl() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());

    put_kv(&mut core, "cache", b"k", b"v", 60_000);
    let before = ttl_ms(&core, "cache", b"k").unwrap();

    let plan = PhysicalPlan::Kv(KvOp::Persist {
        collection: "cache".to_string(),
        key: b"k".to_vec(),
        rls_write_check: Vec::new(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("PERSIST sub-plan must succeed");
    assert_eq!(ttl_ms(&core, "cache", b"k"), Some(-1));

    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");

    let after = ttl_ms(&core, "cache", b"k").expect("key must still exist");
    assert!(
        after > 0 && after <= before,
        "rollback must restore a TTL close to the pre-PERSIST value \
         (before={before}, after={after})"
    );
}

// ── RegisterSortedIndex ──────────────────────────────────────────────────

fn seed_players(core: &mut CoreLoop) {
    for (key, score) in [("p1", 10i64), ("p2", 30), ("p3", 20)] {
        let value = nodedb_types::json_to_msgpack(&serde_json::json!({
            "player_id": key,
            "score": score,
        }))
        .unwrap();
        put_kv(core, "players", key.as_bytes(), &value, 0);
    }
}

fn register_plan() -> PhysicalPlan {
    PhysicalPlan::Kv(KvOp::RegisterSortedIndex {
        collection: "players".to_string(),
        index_name: "lb".to_string(),
        sort_columns: vec![("score".to_string(), "DESC".to_string())],
        key_column: "player_id".to_string(),
        window_type: "none".to_string(),
        window_timestamp_column: String::new(),
        window_start_ms: 0,
        window_end_ms: 0,
    })
}

#[test]
fn kv_register_sorted_index_in_tx_commit_replay_is_queryable() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    seed_players(&mut core);

    let plan = register_plan();
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("RegisterSortedIndex sub-plan must succeed at COMMIT replay");

    assert_eq!(undo_log.len(), 1);
    assert!(matches!(
        undo_log[0],
        UndoEntry::SortedIndexDdl {
            prior_def: None,
            ..
        }
    ));

    let top = core
        .kv_engine
        .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
        .expect("index must be queryable immediately after COMMIT replay");
    let ranked_keys: Vec<Vec<u8>> = top.into_iter().map(|(_, pk)| pk).collect();
    assert_eq!(
        ranked_keys,
        vec![b"p2".to_vec(), b"p3".to_vec(), b"p1".to_vec()],
        "DESC top-3 must rank by score: p2(30) > p3(20) > p1(10)"
    );
}

#[test]
fn kv_register_sorted_index_in_tx_rollback_removes_index() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    seed_players(&mut core);

    let plan = register_plan();
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("RegisterSortedIndex sub-plan must succeed");
    assert!(
        core.kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .is_some()
    );

    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");

    assert!(
        core.kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .is_none(),
        "rollback must remove the index a fresh RegisterSortedIndex created"
    );
}

// ── DropSortedIndex ──────────────────────────────────────────────────────

/// Register `lb` live (outside a transaction), exactly as
/// `execute_kv_register_sorted_index` would -- the def this seeds is what
/// the `DropSortedIndex` undo entry must capture and restore.
fn seed_live_index(core: &mut CoreLoop) {
    seed_players(core);
    let def = crate::data::executor::handlers::kv::sorted_index_compute::build_sorted_index_def(
        crate::data::executor::handlers::kv::sorted_index_compute::BuildSortedIndexDefParams {
            collection: "players",
            index_name: "lb",
            sort_columns: &[("score".to_string(), "DESC".to_string())],
            key_column: "player_id",
            window_type: "",
            window_timestamp_column: "",
            window_start_ms: 0,
            window_end_ms: 0,
        },
    )
    .expect("build sorted index def");
    core.kv_engine
        .register_sorted_index(DB, TID, "players", def);
}

#[test]
fn kv_drop_sorted_index_in_tx_commit_replay_removes_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    seed_live_index(&mut core);
    assert!(
        core.kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .is_some()
    );

    let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: "lb".to_string(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("DropSortedIndex sub-plan must succeed at COMMIT replay");

    assert!(
        core.kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .is_none(),
        "index must be gone after COMMIT replay"
    );
    assert_eq!(undo_log.len(), 1);
    assert!(matches!(
        undo_log[0],
        UndoEntry::SortedIndexDdl {
            prior_def: Some(_),
            ..
        }
    ));
}

#[test]
fn kv_drop_sorted_index_in_tx_rollback_restores_it() {
    let dir = tempfile::tempdir().unwrap();
    let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
    seed_live_index(&mut core);

    let plan = PhysicalPlan::Kv(KvOp::DropSortedIndex {
        index_name: "lb".to_string(),
    });
    let mut undo_log = Vec::new();
    let mut crdt_deltas = Vec::new();
    core.execute_tx_sub_plan(TID, &plan, &mut undo_log, &mut crdt_deltas, &[])
        .expect("DropSortedIndex sub-plan must succeed");
    assert!(
        core.kv_engine
            .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
            .is_none()
    );

    core.rollback_undo_log(DB, TID, undo_log)
        .expect("rollback must succeed");

    let top = core
        .kv_engine
        .sorted_index_top_k(DB, TID, "lb", 3, current_ms())
        .expect("rollback must restore the dropped index, rebuilt from live KV data");
    let ranked_keys: Vec<Vec<u8>> = top.into_iter().map(|(_, pk)| pk).collect();
    assert_eq!(
        ranked_keys,
        vec![b"p2".to_vec(), b"p3".to_vec(), b"p1".to_vec()],
        "restored index must rank identically to the original"
    );
}
