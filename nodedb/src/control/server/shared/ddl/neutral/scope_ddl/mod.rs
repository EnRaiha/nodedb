// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral scope management DDL commands.
//!
//! ```sql
//! DEFINE SCOPE 'profile:read' AS READ ON user_profiles, READ ON user_settings
//! DEFINE SCOPE 'customer' AS INCLUDE 'profile:read', INCLUDE 'orders:write'
//! DROP SCOPE 'profile:read'
//! GRANT SCOPE 'pro:all' TO ORG 'acme'
//! GRANT SCOPE 'profile:read' TO USER 'user_42'
//! GRANT SCOPE 'ops:all' TO USER 'user_42' WHEN BETWEEN '09:00' AND '17:00' ON WEEKDAYS
//! GRANT SCOPE 'ops:all' TO USER 'user_42' REQUIRE MFA REQUIRE IP IN ('10.0.0.0/8')
//! REVOKE SCOPE 'pro:all' FROM ORG 'acme'
//! RENEW SCOPE 'pro:all' FOR ORG 'acme' EXTEND BY 30d
//! SHOW SCOPES
//! SHOW SCOPE 'profile:read'
//! SHOW SCOPE GRANTS
//! ```
//!
//! Ported from the pgwire `ddl::scope_ddl` handlers. The superuser gate,
//! `scope_defs` / `scope_grants` catalog mutations, and `audit_record` side
//! effects are preserved verbatim; only the result construction changed from
//! pgwire `Response` / `QueryResponse` / `Tag` to the protocol-neutral
//! [`super::super::result::DdlResult`] over
//! [`crate::control::server::response_shape::types::ShapedRows`].

mod define;
mod grant;
mod show;
mod support;

pub use self::define::{define_scope, drop_scope};
pub use self::grant::{grant_scope, renew_scope, revoke_scope, show_scope_grants};
pub use self::show::show_scopes;
