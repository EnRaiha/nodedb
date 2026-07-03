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
//! 2. **Drain** — revoke the tenant's active sessions on the source via the
//!    [`SessionInvalidationBus`]; reject new writes; wait for in-flight ops to
//!    complete (bounded timeout).
//! 3. **Snapshot** — call the tenant backup orchestrator to write the tenant's
//!    data to an in-cluster temporary area.
//! 4. **Cutover** — a single Raft proposal that atomically performs DROP TENANT
//!    from the source database, RESTORE TENANT into the target database, and
//!    updates the tenant→database catalog mapping.
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
//! | Cutover failure         | Release drain; source intact (single-proposal fail).|
//! | Already moved (retry)   | Return `MOVE_TENANT_ALREADY_AT_TARGET`.             |

pub mod cutover;
pub mod drain;
pub mod entry;
pub mod journal;
pub mod preflight;
pub mod recovery;
pub mod snapshot;

pub use entry::handle_move_tenant;
pub use journal::{MovePhase, MoveTenantJournalEntry};
