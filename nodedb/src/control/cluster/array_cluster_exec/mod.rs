// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane executor for `PhysicalPlan::ClusterArray` variants.
//!
//! Split by concern:
//! - [`dispatch`]: `NexarArrayDispatch` — bridges `ShardRpcDispatch` ↔
//!   `NexarTransport`, with a local Data Plane short-circuit fast path.
//! - [`executor`]: `ClusterArrayExecutor` — wraps the dispatch and routing
//!   table, constructs the appropriate `ArrayCoordinator`, and converts
//!   results into raw response bytes.

mod dispatch;
mod executor;

pub use dispatch::NexarArrayDispatch;
pub use executor::ClusterArrayExecutor;
