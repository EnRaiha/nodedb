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
fn restore_to_version_produces_forward_delta() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert("docs", "doc1", &[("body", LoroValue::String("v1".into()))])
        .unwrap();
    let vv1 = state.oplog_version_vector();
    state
        .upsert("docs", "doc1", &[("body", LoroValue::String("v2".into()))])
        .unwrap();

    // Snapshot the pre-restore state: a forward delta exported from `vv_before`
    // carries only the ops after it, so it is meaningful solely to a peer that
    // already holds everything up to that point. Replay works the same way —
    // it imports every delta in order from empty.
    let pre_restore = state.export_snapshot().unwrap();

    let delta = state.restore_to_version("docs", "doc1", &vv1).unwrap();
    assert!(
        !delta.is_empty(),
        "restoring to a genuinely earlier version must produce a non-empty forward delta"
    );

    // The live document carries the restored value.
    assert_eq!(
        state.read_field("docs", "doc1", "body"),
        Some(LoroValue::String("v1".into()))
    );

    // A peer caught up to the pre-restore state converges on the restored value
    // once it imports the delta.
    let peer = CrdtState::new(2).unwrap();
    peer.import(&pre_restore).unwrap();
    peer.import(&delta).unwrap();
    assert_eq!(
        peer.read_field("docs", "doc1", "body"),
        Some(LoroValue::String("v1".into()))
    );
}

#[test]
fn restore_to_current_version_is_empty() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert("docs", "doc1", &[("body", LoroValue::String("v1".into()))])
        .unwrap();
    let current = state.oplog_version_vector();

    let delta = state.restore_to_version("docs", "doc1", &current).unwrap();
    assert!(
        delta.is_empty(),
        "restoring to the version a document is already at must return a genuinely empty delta, \
         not a header-only export"
    );
}

/// Attach a nested `blocks` `LoroMovableList` directly onto an existing row,
/// mirroring how `list_ops.rs` stores a Notion-style block list as a
/// container-valued key inside the row `LoroMap`. Returns the single block's
/// id so callers can assert it survived.
fn attach_nested_block_list(state: &CrdtState, collection: &str, row_id: &str) {
    let coll = state.doc().get_map(collection);
    let row = match coll.get(row_id) {
        Some(loro::ValueOrContainer::Container(loro::Container::Map(m))) => m,
        _ => panic!("expected row '{row_id}' to already exist as a map"),
    };
    let blocks = row
        .insert_container("blocks", loro::LoroMovableList::new())
        .unwrap();
    let blk0 = blocks.insert_container(0, loro::LoroMap::new()).unwrap();
    blk0.insert("id", LoroValue::String("blk-0".into()))
        .unwrap();
}

#[test]
fn upsert_preserves_nested_movable_list_across_disjoint_scalar_upsert() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "pages",
            "doc-1",
            &[("title", LoroValue::String("Draft".into()))],
        )
        .unwrap();

    attach_nested_block_list(&state, "pages", "doc-1");

    // A later upsert with a completely disjoint scalar field set must not
    // destroy the nested "blocks" container.
    state
        .upsert(
            "pages",
            "doc-1",
            &[("status", LoroValue::String("published".into()))],
        )
        .unwrap();

    let len = crate::list_ops::list_length(state.doc(), "pages", "doc-1", "blocks").unwrap();
    assert_eq!(len, 1, "nested block list must survive an unrelated upsert");

    let val = crate::list_ops::list_get(state.doc(), "pages", "doc-1", "blocks", 0)
        .unwrap()
        .unwrap();
    if let LoroValue::Map(map) = val {
        assert_eq!(map.get("id"), Some(&LoroValue::String("blk-0".into())));
    } else {
        panic!("expected block map with fields intact, got {val:?}");
    }
}

#[test]
fn upsert_still_replaces_absent_scalar_fields() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "users",
            "u1",
            &[
                ("name", LoroValue::String("Alice".into())),
                ("email", LoroValue::String("alice@example.com".into())),
            ],
        )
        .unwrap();

    // Second upsert omits "email" — the row must end up WITHOUT it. This
    // pins that reusing the row's LoroMap (instead of destroying and
    // recreating it) did not turn `upsert` into a merge.
    state
        .upsert(
            "users",
            "u1",
            &[("name", LoroValue::String("Alice Updated".into()))],
        )
        .unwrap();

    assert_eq!(
        state.read_field("users", "u1", "name"),
        Some(LoroValue::String("Alice Updated".into()))
    );
    assert_eq!(
        state.read_field("users", "u1", "email"),
        None,
        "a field absent from the latest upsert must be gone, not merged"
    );
}

#[test]
fn restore_to_version_preserves_nested_movable_list_as_live_container() {
    let state = CrdtState::new(1).unwrap();
    state
        .upsert(
            "pages",
            "doc-1",
            &[("title", LoroValue::String("v1".into()))],
        )
        .unwrap();

    attach_nested_block_list(&state, "pages", "doc-1");
    let vv_with_blocks = state.oplog_version_vector();

    // Move the row forward — Fix 1 keeps "blocks" alive across this upsert.
    state
        .upsert(
            "pages",
            "doc-1",
            &[("title", LoroValue::String("v2".into()))],
        )
        .unwrap();

    let delta = state
        .restore_to_version("pages", "doc-1", &vv_with_blocks)
        .unwrap();
    assert!(
        !delta.is_empty(),
        "restoring to a genuinely earlier version must produce a forward delta"
    );

    assert_eq!(
        state.read_field("pages", "doc-1", "title"),
        Some(LoroValue::String("v1".into()))
    );

    // The restored block list must be a live, queryable CRDT container —
    // not a flattened/dangling value produced by routing the historical
    // container through the scalar `insert` path.
    let len = crate::list_ops::list_length(state.doc(), "pages", "doc-1", "blocks").unwrap();
    assert_eq!(len, 1);
    let val = crate::list_ops::list_get(state.doc(), "pages", "doc-1", "blocks", 0)
        .unwrap()
        .unwrap();
    if let LoroValue::Map(map) = val {
        assert_eq!(map.get("id"), Some(&LoroValue::String("blk-0".into())));
    } else {
        panic!("expected block map with container identity preserved, got {val:?}");
    }
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
