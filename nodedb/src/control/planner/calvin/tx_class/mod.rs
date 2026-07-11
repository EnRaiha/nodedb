// SPDX-License-Identifier: BUSL-1.1

//! `TxClass` construction for Calvin dispatch.
//!
//! Builds the replicated transaction descriptor (`TxClass`) from a physical
//! task slice: the per-engine write set (`EngineKeySet` — document / vector
//! surrogates, KV raw keys, graph-edge identity + routing homes) plus the
//! msgpack-encoded plans. Four builders, split by shape (static vs
//! dependent-read) and participant floor (strict multi-vshard vs the
//! single-vshard opt-in):
//!
//! - [`build_static_tx_class`] / [`build_single_vshard_tx_class`] — every
//!   write key is known upfront.
//! - [`build_dependent_tx_class`] / [`build_single_vshard_dependent_tx_class`]
//!   — the OLLP collection's write set comes from reconnaissance-predicted
//!   surrogates; all other tasks use static extraction.

pub mod dependent_builder;
pub mod shared;
pub mod static_builder;

pub use dependent_builder::{build_dependent_tx_class, build_single_vshard_dependent_tx_class};
pub(crate) use shared::collection_name_from_plan;
pub use static_builder::{build_single_vshard_tx_class, build_static_tx_class};
