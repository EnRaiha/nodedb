// SPDX-License-Identifier: BUSL-1.1

//! Column redaction inputs for the Redis-wire surface.
//!
//! RESP commands never reach the named-projection shaping core: each handler
//! decodes its own Data-Plane payload and writes the answer straight to the
//! socket. Every command that hands back a stored field value therefore
//! applies the shared masking hook itself, on the inputs resolved here.
//!
//! A redaction policy is keyed on `(tenant, collection, role)`. RESP supplies
//! all three: the tenant and roles come from the session's authenticated
//! identity, and the collection is the one the session selected — the same
//! collection every dispatch in this module targets.

use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::state::SharedState;
use crate::types::DatabaseId;

use super::session::RespSession;

/// Resolve this command's redaction inputs, once.
///
/// `None` only when the session has not authenticated. That is not a hole:
/// `gateway_dispatch::authorize_resp_task` — the single seam every RESP
/// command reaches the Data Plane through — refuses a session with no identity
/// before any row is read, so a `None` here can never accompany a delivered
/// row. Every authenticated command resolves a real context.
///
/// `DatabaseId::DEFAULT` matches the database RESP pins every dispatch to (the
/// protocol carries no database selector), so the policy snapshot is resolved
/// in the same database the rows are read from.
pub(super) fn resp_redaction(state: &SharedState, session: &RespSession) -> Option<QueryRedaction> {
    let identity = session.identity.as_ref()?;
    let scope = RequestAuthScope::for_database(identity, state.auth_stores(), DatabaseId::DEFAULT);
    Some(QueryRedaction::for_collections(
        session.tenant_id,
        scope.auth(),
        vec![(String::new(), session.collection.clone())],
    ))
}

#[cfg(test)]
mod tests {
    use crate::control::security::identity::{
        AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
    };
    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };
    use crate::types::TenantId;

    use super::*;

    /// A policy is keyed on the role NAME the resolved `AuthContext` carries,
    /// which for a DB identity is its built-in role rendered as a string —
    /// `Role::ReadWrite` is `"readwrite"`.
    const RESP_ROLE: &str = "readwrite";

    fn resp_identity() -> AuthenticatedIdentity {
        AuthenticatedIdentity::new_regular(
            7,
            "resp-user",
            TenantId::new(1),
            AuthMethod::Trust,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::All,
        )
    }

    fn store_with_mask(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: 1,
            collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    /// An unauthenticated session resolves no context — and cannot deliver a
    /// row either, because dispatch refuses it first.
    #[test]
    fn unauthenticated_session_resolves_no_redaction() {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().expect("create test directory");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        assert!(resp_redaction(&state, &RespSession::default()).is_none());
    }

    /// An authenticated session resolves a context keyed on the SELECTed
    /// collection and the identity's own roles.
    #[test]
    fn authenticated_session_resolves_the_selected_collection() {
        use crate::bridge::dispatch::Dispatcher;
        use crate::wal::WalManager;

        let dir = tempfile::tempdir().expect("create test directory");
        let wal = std::sync::Arc::new(
            WalManager::open_for_testing(&dir.path().join("test.wal")).expect("open test WAL"),
        );
        let (dispatcher, _sides) = Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("construct shared state");

        let session = RespSession {
            collection: "users".into(),
            identity: Some(resp_identity()),
            ..RespSession::default()
        };
        let redaction = resp_redaction(&state, &session).expect("authenticated session");

        let store = store_with_mask("users", RESP_ROLE, "email");
        assert!(redaction.field_has_rule(&store, "email"));
        assert!(!redaction.field_has_rule(&store, "name"));
        // Keyed on the SELECTed collection, not on any other the tenant holds.
        assert!(!redaction.field_has_rule(&store_with_mask("orders", RESP_ROLE, "email"), "email"));
    }
}
