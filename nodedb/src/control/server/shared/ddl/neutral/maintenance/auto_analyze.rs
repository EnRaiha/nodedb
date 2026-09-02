// SPDX-License-Identifier: BUSL-1.1

//! Auto-ANALYZE trigger: tracks DML counts per collection and triggers
//! automatic ANALYZE when a threshold is exceeded.
//!
//! Wired into SharedState and called from the write dispatch path after
//! each successful DML operation. When the threshold is exceeded,
//! `record_and_maybe_analyze` runs `handle_analyze` on a background
//! blocking thread to refresh column statistics. The write path never
//! waits for the collection scan.
//!
//! Threshold: 10% of the last ANALYZE row_count, floored at
//! `[tuning.maintenance] auto_analyze_min_mutations` (1000 by default).
//! The counter is in-memory only — reset on restart (conservative).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use nodedb_types::DatabaseId;

use crate::control::maintenance::{MaintenanceOutcome, with_budget};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;

use super::super::super::result::DdlError;
use super::support::ddl_err;

/// Duration estimate handed to the maintenance budget pre-screen.
///
/// The lease records the real elapsed time on drop, so this value only
/// decides whether a run starts against the remaining per-minute cap.
const ANALYZE_ESTIMATED_SECS: f64 = 1.0;

/// Per-collection DML counter for auto-ANALYZE triggering.
///
/// Stored on `SharedState`. Called after successful writes to track
/// mutation volume per collection. `record_and_maybe_analyze` reads it on
/// each write to decide whether to re-ANALYZE.
pub struct DmlCounter {
    /// `(database_id, tenant_id, collection)` → mutation count since last
    /// ANALYZE. The database scopes the key, so two databases holding a
    /// same-named collection count apart.
    /// Uses Mutex + HashMap (not RwLock) because `record_dml` always
    /// needs write access to insert new entries via the entry API.
    counts: Mutex<HashMap<(u64, u64, String), u64>>,
    /// Keys whose background ANALYZE is running. The counter only clears on
    /// completion, so this set stops every write past the threshold from
    /// starting another full scan of the same collection.
    in_flight: Mutex<HashSet<(u64, u64, String)>>,
}

impl DmlCounter {
    pub fn new() -> Self {
        Self {
            counts: Mutex::new(HashMap::new()),
            in_flight: Mutex::new(HashSet::new()),
        }
    }

    /// Increment the DML count for a collection.
    ///
    /// Called after each successful INSERT/UPDATE/DELETE dispatch.
    /// Uses the entry API to atomically insert-or-increment (no TOCTOU).
    pub fn record_dml(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        *map.entry((database_id, tenant_id, collection.to_string()))
            .or_insert(0) += 1;
    }

    /// Check if a collection has exceeded the auto-ANALYZE threshold.
    ///
    /// Returns `true` if the DML count since last ANALYZE reaches
    /// `max(last_row_count * 0.10, min_mutations)`. `min_mutations` comes
    /// from `[tuning.maintenance] auto_analyze_min_mutations`.
    pub fn should_analyze(
        &self,
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        last_row_count: u64,
        min_mutations: u64,
    ) -> bool {
        let threshold = (last_row_count / 10).max(min_mutations);
        let map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&(database_id, tenant_id, collection.to_string()))
            .copied()
            .unwrap_or(0)
            >= threshold
    }

    /// Reset the DML count for a collection (called after ANALYZE completes).
    pub fn reset(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&(database_id, tenant_id, collection.to_string()));
    }

    /// Claim the background-ANALYZE slot for a collection.
    ///
    /// Returns `true` when the caller owns the slot. Returns `false` when a
    /// run is already active, and the caller skips this round.
    pub fn try_begin_analyze(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let mut set = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        set.insert((database_id, tenant_id, collection.to_string()))
    }

    /// Release the background-ANALYZE slot for a collection.
    pub fn end_analyze(&self, database_id: u64, tenant_id: u64, collection: &str) {
        let mut set = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        set.remove(&(database_id, tenant_id, collection.to_string()));
    }

    /// Report whether a background ANALYZE holds the slot for a collection.
    pub fn analyze_in_flight(&self, database_id: u64, tenant_id: u64, collection: &str) -> bool {
        let set = self.in_flight.lock().unwrap_or_else(|p| p.into_inner());
        set.contains(&(database_id, tenant_id, collection.to_string()))
    }
}

impl Default for DmlCounter {
    fn default() -> Self {
        Self::new()
    }
}

/// Owner of the counter that an [`InFlightGuard`] releases against.
///
/// The guard moves into a background task, so it owns its handle rather
/// than borrowing the counter.
pub(crate) trait DmlCounterHandle: Send + 'static {
    fn counter(&self) -> &DmlCounter;
}

impl DmlCounterHandle for Arc<SharedState> {
    fn counter(&self) -> &DmlCounter {
        &self.dml_counter
    }
}

/// Releases the background-ANALYZE claim when the run ends.
///
/// Drop covers the success path, the error path, and a panic, so a single
/// failed run never wedges a collection out of all later ANALYZE.
struct InFlightGuard<H: DmlCounterHandle> {
    owner: H,
    database_id: u64,
    tenant_id: u64,
    collection: String,
}

impl<H: DmlCounterHandle> Drop for InFlightGuard<H> {
    fn drop(&mut self) {
        self.owner
            .counter()
            .end_analyze(self.database_id, self.tenant_id, &self.collection);
    }
}

/// Record one DML mutation and start a background ANALYZE when the
/// collection has drifted past its threshold.
///
/// Called from the Control Plane write dispatch path after each successful
/// write. The scan runs on a blocking thread under the database's
/// maintenance budget, so the client's write returns immediately.
pub fn record_and_maybe_analyze(
    state: &Arc<SharedState>,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) {
    let db = database_id.as_u64();
    // `handle_analyze` resets the counter under the identity's tenant, so the
    // trigger keys on the same tenant or the reset misses the entry.
    let tenant_id = identity.tenant_id.as_u64();
    let counter = &state.dml_counter;
    counter.record_dml(db, tenant_id, collection);

    // The threshold never drops below the floor, so the floor screens the
    // write path out before it touches the catalog. Reading stored statistics
    // on every INSERT puts a redb read transaction in the hot path.
    let min_mutations = state.tuning.maintenance.auto_analyze_min_mutations;
    if !counter.should_analyze(db, tenant_id, collection, 0, min_mutations)
        || counter.analyze_in_flight(db, tenant_id, collection)
    {
        return;
    }

    let last_row_count = last_analyzed_row_count(state, db, tenant_id, collection);
    if !counter.should_analyze(db, tenant_id, collection, last_row_count, min_mutations) {
        return;
    }
    if !counter.try_begin_analyze(db, tenant_id, collection) {
        return;
    }

    let guard = InFlightGuard {
        owner: Arc::clone(state),
        database_id: db,
        tenant_id,
        collection: collection.to_string(),
    };
    let identity = identity.clone();
    let collection = collection.to_string();
    tokio::task::spawn_blocking(move || {
        run_budgeted_analyze(&guard.owner, &identity, database_id, &collection);
        // `guard` drops here and releases the slot.
    });
}

/// Row count recorded by the last ANALYZE, or `0` when none ran.
///
/// Every column of one collection carries the same count, so the first row
/// answers for the set. `0` drives `should_analyze` to its configured floor,
/// which is the wanted behavior for a collection with no statistics.
fn last_analyzed_row_count(
    state: &SharedState,
    database_id: u64,
    tenant_id: u64,
    collection: &str,
) -> u64 {
    match state
        .credentials
        .catalog()
        .load_column_stats(database_id, tenant_id, collection)
    {
        Ok(rows) => rows.first().map_or(0, |row| row.row_count),
        Err(error) => {
            tracing::warn!(
                %collection,
                %error,
                "auto-ANALYZE cannot read column statistics; assuming none exist"
            );
            0
        }
    }
}

/// Run ANALYZE under the database's maintenance budget and log the outcome.
///
/// Runs on a `spawn_blocking` thread, so the budget lease stays inside one
/// synchronous scope and never crosses an await.
fn run_budgeted_analyze(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) {
    let outcome = with_budget(
        &state.maintenance_budget,
        database_id,
        ANALYZE_ESTIMATED_SECS,
        || blocking_analyze(state, identity, database_id, collection),
    );
    match outcome {
        MaintenanceOutcome::Deferred => tracing::debug!(
            %collection,
            "auto-ANALYZE deferred: the database is over its maintenance budget"
        ),
        MaintenanceOutcome::Ran(Ok(())) => tracing::debug!(%collection, "auto-ANALYZE ran"),
        MaintenanceOutcome::Ran(Err(error)) => tracing::warn!(
            %collection,
            sqlstate = %error.sqlstate,
            message = %error.message,
            "auto-ANALYZE failed; column statistics stay stale until the next attempt"
        ),
    }
}

/// Drive the async `handle_analyze` to completion from a blocking thread.
fn blocking_analyze(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    collection: &str,
) -> Result<(), DdlError> {
    let handle = tokio::runtime::Handle::try_current().map_err(|error| {
        ddl_err(
            "XX000",
            format!("auto-ANALYZE needs a Tokio runtime: {error}"),
        )
    })?;
    // `handle_analyze` reads the collection name off the second whitespace
    // token and lowercases it, so the bare name is what it expects.
    let sql = format!("ANALYZE {collection}");
    handle.block_on(super::analyze::handle_analyze(
        state,
        identity,
        &sql,
        database_id,
    ))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shipped default of `[tuning.maintenance]
    /// auto_analyze_min_mutations`, which these cases pin explicitly.
    const FLOOR: u64 = 1000;

    #[test]
    fn basic_counting() {
        let counter = DmlCounter::new();
        counter.record_dml(4, 1, "users");
        counter.record_dml(4, 1, "users");
        counter.record_dml(4, 1, "users");
        assert!(!counter.should_analyze(4, 1, "users", 0, FLOOR));
    }

    #[test]
    fn threshold_exceeded() {
        let counter = DmlCounter::new();
        for _ in 0..1001 {
            counter.record_dml(4, 1, "users");
        }
        assert!(counter.should_analyze(4, 1, "users", 0, FLOOR));
    }

    #[test]
    fn percentage_threshold() {
        let counter = DmlCounter::new();
        for _ in 0..10_001 {
            counter.record_dml(4, 1, "big_table");
        }
        assert!(counter.should_analyze(4, 1, "big_table", 100_000, FLOOR));
    }

    #[test]
    fn configured_floor_lowers_the_trigger_point() {
        let counter = DmlCounter::new();
        for _ in 0..20 {
            counter.record_dml(4, 1, "users");
        }
        assert!(!counter.should_analyze(4, 1, "users", 0, FLOOR));
        assert!(
            counter.should_analyze(4, 1, "users", 0, 20),
            "a lowered floor triggers on the same count"
        );
    }

    #[test]
    fn configured_floor_never_beats_the_percentage() {
        let counter = DmlCounter::new();
        for _ in 0..100 {
            counter.record_dml(4, 1, "big_table");
        }
        assert!(
            !counter.should_analyze(4, 1, "big_table", 100_000, 20),
            "10% of 100_000 rows outranks a floor of 20"
        );
    }

    #[test]
    fn reset_clears() {
        let counter = DmlCounter::new();
        for _ in 0..2000 {
            counter.record_dml(4, 1, "users");
        }
        assert!(counter.should_analyze(4, 1, "users", 0, FLOOR));
        counter.reset(4, 1, "users");
        assert!(!counter.should_analyze(4, 1, "users", 0, FLOOR));
    }

    #[test]
    fn two_databases_count_apart() {
        let counter = DmlCounter::new();
        for _ in 0..1001 {
            counter.record_dml(4, 1, "users");
        }
        assert!(counter.should_analyze(4, 1, "users", 0, FLOOR));
        assert!(
            !counter.should_analyze(5, 1, "users", 0, FLOOR),
            "the key is scoped by database, so the sibling collection is untouched"
        );

        counter.reset(4, 1, "users");
        for _ in 0..1001 {
            counter.record_dml(5, 1, "users");
        }
        assert!(counter.should_analyze(5, 1, "users", 0, FLOOR));
        assert!(!counter.should_analyze(4, 1, "users", 0, FLOOR));
    }

    impl DmlCounterHandle for Arc<DmlCounter> {
        fn counter(&self) -> &DmlCounter {
            self
        }
    }

    fn guard_for(counter: &Arc<DmlCounter>) -> InFlightGuard<Arc<DmlCounter>> {
        InFlightGuard {
            owner: Arc::clone(counter),
            database_id: 4,
            tenant_id: 1,
            collection: "users".to_string(),
        }
    }

    #[test]
    fn second_trigger_skips_while_in_flight() {
        let counter = DmlCounter::new();
        assert!(counter.try_begin_analyze(4, 1, "users"));
        assert!(
            !counter.try_begin_analyze(4, 1, "users"),
            "a run already holds the slot, so the second trigger skips"
        );
        assert!(counter.analyze_in_flight(4, 1, "users"));
    }

    #[test]
    fn in_flight_slot_is_per_collection() {
        let counter = DmlCounter::new();
        assert!(counter.try_begin_analyze(4, 1, "users"));
        assert!(counter.try_begin_analyze(4, 1, "orders"));
        assert!(counter.try_begin_analyze(5, 1, "users"));
    }

    #[test]
    fn guard_releases_after_completion() {
        let counter = Arc::new(DmlCounter::new());
        assert!(counter.try_begin_analyze(4, 1, "users"));
        {
            let _guard = guard_for(&counter);
        }
        assert!(!counter.analyze_in_flight(4, 1, "users"));
        assert!(
            counter.try_begin_analyze(4, 1, "users"),
            "the next trigger claims the released slot"
        );
    }

    #[test]
    fn guard_releases_after_error() {
        let counter = Arc::new(DmlCounter::new());
        assert!(counter.try_begin_analyze(4, 1, "users"));

        fn failing_run(_guard: InFlightGuard<Arc<DmlCounter>>) -> Result<(), &'static str> {
            Err("ANALYZE scan failed")
        }
        assert!(failing_run(guard_for(&counter)).is_err());

        assert!(!counter.analyze_in_flight(4, 1, "users"));
        assert!(counter.try_begin_analyze(4, 1, "users"));
    }

    #[test]
    fn guard_releases_after_panic() {
        let counter = Arc::new(DmlCounter::new());
        assert!(counter.try_begin_analyze(4, 1, "users"));

        let guard_counter = Arc::clone(&counter);
        let panicked = std::panic::catch_unwind(move || {
            let _guard = guard_for(&guard_counter);
            panic!("ANALYZE scan panicked");
        });
        assert!(panicked.is_err());
        assert!(!counter.analyze_in_flight(4, 1, "users"));
    }
}
