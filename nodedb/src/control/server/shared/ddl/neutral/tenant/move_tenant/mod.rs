// SPDX-License-Identifier: BUSL-1.1

//! `MOVE TENANT <name> FROM <source_db> TO <target_db>` — offline v1.
//!
//! Executes a four-phase tenant re-parenting sequence, each phase durable
//! through WAL + Raft. A [`MoveTenantJournalEntry`] redb record tracks the in-
//! flight state so crash recovery can resume or compensate.
//!
//! ## Phases
//!
//! 1. **Pre-flight** — verify every collection the tenant has data in exists
//!    in the target with a compatible schema. No state mutation.
//! 2. **Drain** — revoke the tenant's sessions, then drain every source
//!    collection through the replicated descriptor-lease drain: every node
//!    refuses new plans on it, and the phase waits until no node holds a lease
//!    on it (bounded timeout). The cutover's catalog entry ends the drain.
//! 3. **Snapshot** — capture every live collection of the source database
//!    from every vShard of the cluster, after a consistent cut.
//! 4. **Cutover** — re-issue the captured rows into the target database as
//!    replicated writes, then one Raft proposal moves every catalog row. Each
//!    node that applies it reclaims the source-keyed storage on every core.
//! 5. **Resume** — drain is released; writes accepted on target.
//!
//! ## Online MOVE TENANT is a separate initiative
//!
//! The dual-write + cutover variant (no drain window) is explicitly declared
//! as a separate follow-up initiative and is out of scope here.
//!
//! ## Compensating actions
//!
//! | Phase failure           | Compensation                                        |
//! |-------------------------|-----------------------------------------------------|
//! | Pre-flight              | Nothing; no state changed.                          |
//! | Drain timeout           | Release drain; resume source writes; return error.  |
//! | Snapshot failure        | Same as drain + delete partial snapshot.            |
//! | Cutover failure         | Release drain; source catalog and rows intact.      |
//! | Already moved (retry)   | Return `MOVE_TENANT_ALREADY_AT_TARGET`.             |

pub mod arrays;
pub mod cutover;
pub mod drain;
pub mod entry;
pub mod journal;
pub mod preflight;
pub mod recovery;
pub mod snapshot;

pub use crate::control::security::catalog::{MovePhase, MoveTenantJournalEntry};
pub use entry::handle_move_tenant;
