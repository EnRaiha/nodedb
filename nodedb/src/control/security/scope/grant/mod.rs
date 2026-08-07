// SPDX-License-Identifier: BUSL-1.1

//! Scope grant management: GRANT/REVOKE/RENEW SCOPE TO/FROM ORG/USER/TEAM.
//!
//! Layout mirrors the collection-permission module:
//! - [`types`] — `ScopeGrant`, `ScopeStatus`, `ScopeGrantParams`.
//! - [`store`] — `ScopeGrantStore`: boot replay plus the read path.
//! - [`replication`] — helpers behind `CatalogEntry::{PutScopeGrant,
//!   DeleteScopeGrant}`: `prepare_grant`, `prepare_renew`, `propose_grant`,
//!   `propose_revoke`, `install_replicated_grant`, `install_replicated_revoke`.

pub mod replication;
pub mod store;
pub mod types;

pub use replication::RenewOutcome;
pub use store::ScopeGrantStore;
pub use types::{ScopeGrant, ScopeGrantParams, ScopeStatus};
