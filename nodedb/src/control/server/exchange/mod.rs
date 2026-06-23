// SPDX-License-Identifier: BUSL-1.1

//! Coordinator-mediated data-movement engine.
//!
//! `gather` provides the single fan-out/gather primitive over all Data-Plane
//! cores.  `resolve` provides the per-request plan resolver that materializes
//! catalog providers and resolves `Exchange` nodes before dispatch.
//! `streamable` provides the shared streaming-eligibility predicate used by the
//! lazy query sinks (pgwire fast path, native protocol, HTTP-NDJSON).

pub mod all_cores;
pub mod full_scan;
pub mod gather;
pub mod resolve;
pub mod streamable;

pub use all_cores::{NodeLevelResult, execute_plan_all_local_cores};
pub use gather::{GatherOutcome, finalize_aggregate, gather_all_cores};
pub use resolve::{Resolved, resolve_and_materialize, resolve_exchange_in_plan};
