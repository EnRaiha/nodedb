// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane executor for `PhysicalPlan::ClusterArray` variants.
//!
//! Split by concern:
//! - [`dispatch`]: `NexarArrayDispatch` — bridges `ShardRpcDispatch` ↔
//!   `NexarTransport`, with a local Data Plane short-circuit fast path.
//! - [`executor`]: `ClusterArrayExecutor` — wraps the dispatch and routing
//!   table, constructs the appropriate `ArrayCoordinator`, and converts
//!   results into raw response bytes.
//! - [`elementwise`]: the `Elementwise` fan-out of `ClusterArrayExecutor`.
//! - [`maint`]: the `Flush` / `Compact` fan-out of `ClusterArrayExecutor`.
//! - [`surrogate`]: the cross-shard surrogate bitmap of an array slice.

mod dispatch;
mod elementwise;
mod executor;
mod maint;
mod surrogate;

pub use dispatch::NexarArrayDispatch;
pub use executor::ClusterArrayExecutor;
