// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base scan result so an
//! in-transaction SCAN observes the transaction's own uncommitted point
//! writes (read-your-own-writes for scans).
//!
//! The base scan only reads durable rows. This step layers the per-transaction
//! overlay on top: a staged tombstone hides its base row, a staged put replaces
//! the base body (and is re-checked against the scan predicate, since an update
//! may have moved the row out of the result), and a staged put for a surrogate
//! absent from the base set is appended when it satisfies the predicate. The
//! `seen` set keeps additions from duplicating rows already present.
//!
//! Current-version only: temporal (`AS OF` / valid-at) scans never call this,
//! because staged bodies represent the current version alone. Staged put bodies
//! are encoded identically to base-stored bodies (same canonicalization /
//! Binary Tuple form), so the caller's decode + scan predicate + projection
//! apply to them unchanged.

use std::collections::HashSet;

use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::{DatabaseId, TenantId, TxnId};

impl CoreLoop {
    /// Merge the overlay for `txn_id` into `rows` (base scan `(hex_row_key,
    /// body)` pairs). `matches` is the SAME predicate the base scan applied,
    /// evaluated on a stored body (Binary Tuple for strict, MessagePack for
    /// schemaless). No-op when the transaction has no overlay entries.
    pub(in crate::data::executor) fn merge_overlay_into_scan(
        &self,
        txn_id: TxnId,
        coll_key: &(DatabaseId, TenantId, String),
        rows: &mut Vec<(String, Vec<u8>)>,
        matches: &dyn Fn(&[u8]) -> bool,
    ) {
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        // Surrogates already represented in the base result. Additions consult
        // this to avoid re-adding a row that base already carries (or that the
        // retain pass has just superseded in place).
        let mut seen: HashSet<u32> = rows
            .iter()
            .filter_map(|(k, _)| u32::from_str_radix(k, 16).ok())
            .collect();

        // Base-minus-superseded: a single in-place pass. Drop tombstoned rows,
        // replace put-superseded bodies and re-check the predicate, keep the
        // rest untouched.
        rows.retain_mut(|(row_key, body)| {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                return true;
            };
            match overlay.get(coll_key, surrogate) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(staged_body)) => {
                    *body = staged_body.clone();
                    matches(body)
                }
                None => true,
            }
        });

        // Overlay additions: staged puts for surrogates the base scan did not
        // return. A tombstone for a surrogate absent from base hides nothing.
        for (surrogate, staged) in overlay.iter_for_collection(coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            match staged {
                Staged::Put(body) => {
                    if matches(body) {
                        rows.push((surrogate_to_doc_id(Surrogate::new(surrogate)), body.clone()));
                        seen.insert(surrogate);
                    }
                }
                Staged::Tombstone => {}
            }
        }
    }
}
