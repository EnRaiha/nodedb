// SPDX-License-Identifier: BUSL-1.1

//! Plan resolution: materialize catalog providers and resolve Exchange nodes.
//!
//! `resolve_and_materialize` is the single shared entry point called by both
//! the pgwire dispatch path and the native dispatch path before handing a plan
//! to the gateway or SPSC bridge.  It performs two passes in order:
//!
//! 1. **Catalog materialization** (`materialize`): walk the plan tree; for every
//!    `QueryOp::ProviderScan { provider: Some(name), rows: [] }`, call
//!    `catalog::catalog_rows` (async, identity-scoped) and replace `rows`
//!    with the encoded result.  This happens per-request, post-cache, so
//!    identity-scoped catalog rows never enter the plan cache.
//!
//! 2. **Exchange resolution** (`exchange`):
//!    - `Gather{as_aggregate}` at the plan root → fan child to all vShards,
//!      merge, and return `Resolved::Gathered`.
//!    - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!      gather child to coordinator, encode as a merged msgpack array, and
//!      embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!      self-contained and returned as `Resolved::Plan`.
//!    - `Shuffle{keys, num_parts}` at the plan root wrapping a `HashJoin` →
//!      `shuffle`: allocate a shuffle id, fan producers to each side's owner
//!      nodes, then consumers to the part-owners, and merge the joined rows
//!      into a `Resolved::Gathered` response (real cross-node grace hash join).
//!    - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

pub mod exchange;
mod materialize;
mod shuffle;

pub use exchange::{Resolved, resolve_and_materialize, resolve_exchange_in_plan};
