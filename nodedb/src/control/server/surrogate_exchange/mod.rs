// SPDX-License-Identifier: BUSL-1.1

//! Routed-surrogate-exchange (F1b): the leader-side cluster hook that
//! assign-or-returns the authoritative surrogate for a `(collection, pk)`
//! endpoint key, and the coordinator-side helper that routes the request to the
//! home vShard's leader (or assigns locally when this node IS the leader).

pub mod hook;
pub mod resolve;

pub use hook::RegistryAssignRemoteSurrogate;
pub use resolve::{assign_surrogate_routed, lookup_surrogate_routed};
