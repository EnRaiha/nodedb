// SPDX-License-Identifier: Apache-2.0

use loro::LoroValue;

use super::core::CrdtState;

#[test]
fn upsert_and_check_existence() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "user-1",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    assert!(state.row_exists("users", "user-1"));
    assert!(!state.row_exists("users", "user-2"));
}

#[test]
fn delete_row() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "user-1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();

    assert!(state.row_exists("users", "user-1"));
    state.delete("users", "user-1").unwrap();
    assert!(!state.row_exists("users", "user-1"));
}

#[test]
fn row_ids_listing() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert("users", "a", &[("x", LoroValue::I64(1))])
        .unwrap();
    state
        .upsert("users", "b", &[("x", LoroValue::I64(2))])
        .unwrap();

    let mut ids = state.row_ids("users");
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
}

#[test]
fn field_value_uniqueness_check() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("alice@example.com".into()))],
        )
        .unwrap();

    assert!(state.field_value_exists(
        "users",
        "email",
        &LoroValue::String("alice@example.com".into()),
        None,
    ));
    assert!(!state.field_value_exists(
        "users",
        "email",
        &LoroValue::String("bob@example.com".into()),
        None,
    ));
}

#[test]
fn field_value_exists_self_exclusion() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("a@example.com".into()))],
        )
        .unwrap();

    let value = LoroValue::String("a@example.com".into());

    // Re-validating the very row that holds the value, while excluding it,
    // must NOT report a collision — a committed row cannot conflict with its
    // own just-written version.
    assert!(!state.field_value_exists("users", "email", &value, Some("u1")));

    // A DIFFERENT row carrying the same value (excluding only itself) still
    // collides with the existing u1.
    assert!(state.field_value_exists("users", "email", &value, Some("u2")));

    // With no exclusion the value is plainly present.
    assert!(state.field_value_exists("users", "email", &value, None));
}

#[test]
fn field_value_exists_live_self_exclusion() {
    let state = CrdtState::new(1).unwrap();
    // Live rows (no `_ts_valid_until`) — the live probe treats them as active.
    state
        .upsert(
            "users",
            "u1",
            &[("email", LoroValue::String("a@example.com".into()))],
        )
        .unwrap();

    let value = LoroValue::String("a@example.com".into());

    // Excluding the holder row → no self-collision.
    assert!(!state.field_value_exists_live("users", "email", &value, Some("u1")));

    // A distinct row with the same value → collision against the live u1.
    assert!(state.field_value_exists_live("users", "email", &value, Some("u2")));

    assert!(state.field_value_exists_live("users", "email", &value, None));
}

#[test]
fn compact_history_preserves_state() {
    let mut state = CrdtState::new(1).unwrap();
    // Create some state with history.
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice".into()))],
        )
        .unwrap();
    state
        .upsert("users", "u2", &[("name", LoroValue::String("Bob".into()))])
        .unwrap();
    // Update to create more history.
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice Updated".into()))],
        )
        .unwrap();

    // Compact.
    state.compact_history().unwrap();

    // State should be preserved after compaction.
    assert!(state.row_exists("users", "u1"));
    assert!(state.row_exists("users", "u2"));

    // New operations should still work.
    state
        .upsert(
            "users",
            "u3",
            &[("name", LoroValue::String("Carol".into()))],
        )
        .unwrap();
    assert!(state.row_exists("users", "u3"));
}

#[test]
fn estimated_memory_grows_with_data() {
    let state = CrdtState::new(1).unwrap();
    let before = state.estimated_memory_bytes();

    for i in 0..100 {
        state
            .upsert(
                "items",
                &format!("item-{i}"),
                &[("value", LoroValue::I64(i))],
            )
            .unwrap();
    }

    let after = state.estimated_memory_bytes();
    assert!(
        after > before,
        "memory should grow: before={before}, after={after}"
    );
}

#[test]
fn snapshot_roundtrip() {
    let state1 = CrdtState::new(1).unwrap();
    state1
        .upsert("users", "u1", &[("name", LoroValue::String("Bob".into()))])
        .unwrap();

    let snapshot = state1.export_snapshot().unwrap();

    let state2 = CrdtState::new(2).unwrap();
    state2.import(&snapshot).unwrap();

    assert!(state2.row_exists("users", "u1"));
}
