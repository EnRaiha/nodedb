// SPDX-License-Identifier: Apache-2.0

//! Multi-agent CRDT constraint resolution lifecycles.
//!
//! Inline `validator/tests.rs` covers each policy mechanism in isolation
//! (one agent, one violation, one resolution). `crdt_sql_paradox.rs`
//! covers single-offline-agent strict-mode → DLQ scenarios. The lifecycle
//! glue between them — multiple offline agents producing conflicting
//! deltas, deferred-then-resolved FK chains, and exhaustion paths back
//! to the DLQ — has no end-to-end coverage. This module fills that gap.
//!
//! Tests treat the validator as the system-under-test and the deferred-queue
//! lifecycle as the integration concern. They do NOT exercise Raft replication
//! or the multi-Raft layer — that is `nodedb-cluster`'s responsibility.

#[path = "constraint_resolution/common.rs"]
mod common;

#[path = "constraint_resolution/cascade_defer.rs"]
mod cascade_defer;

#[path = "constraint_resolution/fk_chain.rs"]
mod fk_chain;

#[path = "constraint_resolution/unique_rename.rs"]
mod unique_rename;
