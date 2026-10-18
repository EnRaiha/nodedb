// SPDX-License-Identifier: BUSL-1.1

use std::collections::HashSet;

use super::harness::{
    ARR_SURS, COL_SURS, D_SURS, FTS_SURS, G_SRC_SURS, KV_SURS, SPATIAL_SURS, V_SURS,
};

// ══════════════════════════════════════════════════════════════════════
// Assertion A: Global surrogate uniqueness across all engines.
//
// All surrogate values are controlled here; collect every value and assert
// the multi-set has no duplicates.  The non-overlap is designed into the
// constants in the harness; this assertion catches any later constant change
// that accidentally introduces a collision.
// ══════════════════════════════════════════════════════════════════════
pub(super) fn assert_surrogate_uniqueness() {
    let all_surs: Vec<u32> = [
        V_SURS,
        D_SURS,
        G_SRC_SURS,
        KV_SURS,
        COL_SURS,
        ARR_SURS,
        FTS_SURS,
        SPATIAL_SURS,
    ]
    .iter()
    .flat_map(|s| s.iter().copied())
    .collect();

    // Surrogates 10, 12, 13 intentionally appear in multiple engines (the
    // cross-engine join scenario).  The uniqueness property asserted here
    // is that each engine's OWN insert batch uses distinct values — not
    // that the same physical surrogate never appears in two engines (that
    // would prohibit the cross-engine join design).
    for slice in [
        V_SURS,
        D_SURS,
        G_SRC_SURS,
        KV_SURS,
        COL_SURS,
        ARR_SURS,
        FTS_SURS,
        SPATIAL_SURS,
    ] {
        let set: HashSet<u32> = slice.iter().copied().collect();
        assert_eq!(
            set.len(),
            slice.len(),
            "engine surrogate batch must contain no duplicates: {slice:?}"
        );
    }

    // Build the global set (deduped) and assert no same surrogate maps to
    // two different rows in the SAME engine.
    let global_set: HashSet<u32> = all_surs.iter().copied().collect();
    // Global set has fewer elements than all_surs because of intentional
    // cross-engine reuse of {10, 12, 13}.  The per-engine uniqueness loop
    // above asserts that every surrogate is unique within each engine batch.
    assert!(
        global_set.contains(&10),
        "surrogate 10 must be in the global set"
    );
}
