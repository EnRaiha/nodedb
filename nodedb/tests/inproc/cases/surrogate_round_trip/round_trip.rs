// SPDX-License-Identifier: BUSL-1.1

use super::harness::open_core;
use super::inserts::{
    insert_array, insert_columnar, insert_document, insert_fts, insert_graph, insert_kv,
    insert_spatial, insert_vector,
};
use super::intersection::assert_bitmap_intersection;
use super::uniqueness::assert_surrogate_uniqueness;

// ── Test ───────────────────────────────────────────────────────────────────

/// Main assertion: insert into eight engine paths, check uniqueness, check
/// the three-way Vector ∩ Graph ∩ Array bitmap intersection via real query
/// execution.
#[test]
fn surrogate_round_trip_all_engines() {
    let (mut core, mut tx, mut rx, _dir) = open_core();

    insert_vector(&mut core, &mut tx, &mut rx);
    insert_document(&mut core, &mut tx, &mut rx);
    insert_graph(&mut core, &mut tx, &mut rx);
    insert_kv(&mut core, &mut tx, &mut rx);
    insert_columnar(&mut core, &mut tx, &mut rx);
    insert_array(&mut core, &mut tx, &mut rx);
    insert_fts(&mut core, &mut tx, &mut rx);
    insert_spatial(&mut core, &mut tx, &mut rx);

    assert_surrogate_uniqueness();
    assert_bitmap_intersection(&mut core, &mut tx, &mut rx);
}
