// SPDX-License-Identifier: BUSL-1.1

//! BALANCED constraint check at a transaction's commit boundary.
//!
//! The entries checked here are the signed contributions every write in the
//! transaction handed to
//! [`settle_balanced_entries`](crate::data::executor::core_loop::CoreLoop::settle_balanced_entries)
//! as it ran — an insert's post-image added, a delete's pre-image subtracted,
//! an update's both. They are NOT re-derived from the undo log, and that is the
//! point:
//!
//! * the undo log records a delete as an entry to be REVERSED, not as an amount
//!   the transaction removed, so a transaction that deleted one leg of a
//!   balanced journal contributed nothing and passed;
//! * `old_value: None` is not "this was an insert" — a `PointPut` onto an
//!   absent row inside a rolled-back savepoint carries the same shape;
//! * re-reading each row from the store to recover its body repeats a read for
//!   bytes the write itself already held and decoded.

use std::collections::HashMap;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::enforcement::balanced::{self, BalancedEntry};

impl CoreLoop {
    /// Check BALANCED constraints across everything this transaction wrote.
    ///
    /// `entries` is the transaction's accumulated `(collection, entry)` set,
    /// grouped here so each collection is judged against its own definition.
    pub(in crate::data::executor::handlers::transaction) fn check_balanced_constraints(
        &self,
        database_id: u64,
        tid: u64,
        entries: Vec<(String, BalancedEntry)>,
    ) -> crate::Result<()> {
        let mut by_collection: HashMap<String, Vec<BalancedEntry>> = HashMap::new();
        for (collection, entry) in entries {
            by_collection.entry(collection).or_default().push(entry);
        }

        for (collection, collection_entries) in &by_collection {
            // A collection with entries but no definition can only happen if
            // the constraint was dropped mid-transaction; there is then no rule
            // left to judge those entries against.
            let Some(def) = self.balanced_def(database_id, tid, collection) else {
                continue;
            };
            balanced::check_balanced(collection, &def, collection_entries)?;
        }

        Ok(())
    }
}
