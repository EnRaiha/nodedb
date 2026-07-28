// SPDX-License-Identifier: BUSL-1.1

//! Build a minimal, unfiltered, full-collection scan `PhysicalPlan` for a
//! catalog-resolved collection.
//!
//! Shared by the cross-node join paths that need a complete by-name scan of a
//! user collection on the coordinator:
//!
//! - `resolve::exchange::gather_join_build_side` — gathers a broadcast-join
//!   build side across all vShards and inlines it as a `ProviderScan`.
//! - `resolve::shuffle` — encodes each side's scan as `plan_bytes` for the
//!   distributed shuffle producers.
//!
//! Both need the SAME "scan everything for this engine, no filter, no
//! projection" plan, so it lives here once. The match over `CollectionType` is
//! EXHAUSTIVE — every catalog-creatable engine is handled and there is no
//! name-scan fallback for an "unsupported engine". The Array engine is
//! intentionally absent: it is not a `CollectionType` variant (Array uses its
//! own `CREATE ARRAY` DDL and never appears as a `StoredCollection`), so it
//! cannot reach this path.

use nodedb_physical::physical_plan::{ColumnarOp, DocumentOp, KvOp, PhysicalPlan, TimeseriesOp};
use nodedb_types::{CollectionType, ColumnarProfile, DocumentMode, SystemTimeScope};

use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId};

/// The build/scan side of a join must be COMPLETE — every row is needed for
/// correct match output, so the scan is unbounded (no row cap). A fixed cap
/// would silently drop join matches for larger collections. This is
/// allocation-safe: the scan path sizes its buffer as `with_capacity(limit
/// .min(256))` and bounds output with `take(limit)`, and `fetch_limit` uses
/// `saturating_mul`, so `usize::MAX` returns all rows without pre-allocating or
/// overflowing.
const COMPLETE_SCAN: usize = usize::MAX;

/// Build a full-collection scan plan for `collection`, or `Ok(None)` when the
/// catalog has no record for it on this node.
///
/// `Ok(None)` is a graceful "fall back to a by-name scan on the executing
/// node" signal for callers that have a name-scan fallback — it is never an
/// error. Callers that REQUIRE a scan plan (the shuffle producer cannot scan
/// by name across nodes) treat `None` as a typed error themselves.
pub fn full_scan_plan_for_collection(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
) -> crate::Result<Option<PhysicalPlan>> {
    let catalog = state.credentials.catalog();
    let stored = match catalog.get_collection(database_id, tenant_id.as_u64(), collection)? {
        Some(s) => s,
        None => return Ok(None),
    };

    let plan = match &stored.collection_type {
        CollectionType::Document(DocumentMode::Schemaless)
        | CollectionType::Document(DocumentMode::Strict(_)) => {
            PhysicalPlan::Document(DocumentOp::Scan {
                collection: collection.into(),
                limit: COMPLETE_SCAN,
                offset: 0,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                distinct: false,
                projection: Vec::new(),
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
            })
        }
        CollectionType::KeyValue(_) => PhysicalPlan::Kv(KvOp::Scan {
            collection: collection.into(),
            cursor: Vec::new(),
            count: COMPLETE_SCAN,
            filters: Vec::new(),
            sort_keys: Vec::new(),
            match_pattern: None,
            surrogate_ceiling: None,
        }),
        CollectionType::Columnar(ColumnarProfile::Plain)
        | CollectionType::Columnar(ColumnarProfile::Spatial { .. }) => {
            PhysicalPlan::Columnar(ColumnarOp::Scan {
                collection: collection.into(),
                projection: Vec::new(),
                limit: COMPLETE_SCAN,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
                prefilter: None,
                computed_columns: Vec::new(),
            })
        }
        CollectionType::Columnar(ColumnarProfile::Timeseries { .. }) => {
            PhysicalPlan::Timeseries(TimeseriesOp::Scan {
                collection: collection.into(),
                // (0, i64::MAX) = no time filter — scan all rows.
                time_range: (0, i64::MAX),
                projection: Vec::new(),
                limit: COMPLETE_SCAN,
                filters: Vec::new(),
                sort_keys: Vec::new(),
                bucket_interval_ms: 0,
                group_by: Vec::new(),
                aggregates: Vec::new(),
                gap_fill: String::new(),
                computed_columns: Vec::new(),
                rls_filters: Vec::new(),
                system_time: SystemTimeScope::Current,
                valid_at_ms: None,
            })
        }
    };

    Ok(Some(plan))
}
