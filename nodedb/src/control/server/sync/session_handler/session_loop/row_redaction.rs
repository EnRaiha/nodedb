// SPDX-License-Identifier: BUSL-1.1

//! Column redaction for the CRDT row pushes a session delivers.
//!
//! A `RowPush` carries a row post-image — the stored MessagePack map — straight
//! to a device, which persists it in its local replica. Like the shape
//! snapshot beside it, that payload never reaches the SELECT-path shaping core
//! where a query's rows are redacted, so the subscriber's rules are applied to
//! the stored bytes here through the one shared hook.
//!
//! The roles a policy is keyed on belong to the session, not to the delta, so
//! they are resolved once when the drain starts. The rules themselves are
//! keyed per collection, so the resolved [`QueryRedaction`] is rebuilt only
//! when consecutive deltas cross a collection boundary — a drain of one
//! collection's deltas resolves exactly once.

use crate::control::security::redaction::RedactionStore;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::response_shape::redaction::{
    QueryRedaction, redact_document_row_bytes,
};
use crate::control::server::sync::session::SyncSession;
use crate::control::state::SharedState;
use crate::types::TenantId;

/// The session-scoped redaction inputs for its outbound row pushes.
pub(super) struct RowPushRedaction {
    tenant_id: TenantId,
    roles: Vec<String>,
    /// The last collection resolved, with its inputs.
    resolved: Option<(String, QueryRedaction)>,
}

impl RowPushRedaction {
    /// Resolve the subscriber's roles for this session.
    ///
    /// Returns `None` when the session has no established identity: there is
    /// then no principal to evaluate a policy against, and the caller must
    /// deliver nothing rather than deliver unredacted.
    pub(super) fn for_session(shared: &SharedState, session: &SyncSession) -> Option<Self> {
        let identity = session.identity.as_ref()?;
        let scope =
            RequestAuthScope::for_database(identity, shared.auth_stores(), session.database_id());
        Some(Self {
            tenant_id: identity.tenant_id,
            roles: scope.auth().roles.clone(),
            resolved: None,
        })
    }

    /// Redact one row post-image in place, reporting whether it may be sent.
    ///
    /// `false` means a rule covers `collection` but the payload could not be
    /// rewritten; the caller must drop the push rather than deliver it.
    pub(super) fn redact(
        &mut self,
        store: &RedactionStore,
        collection: &str,
        payload: &mut Vec<u8>,
    ) -> bool {
        let needs_resolve =
            !matches!(&self.resolved, Some((resolved, _)) if resolved.as_str() == collection);
        if needs_resolve {
            self.resolved = Some((
                collection.to_string(),
                QueryRedaction::new(
                    self.tenant_id,
                    self.roles.clone(),
                    vec![(String::new(), collection.to_string())],
                ),
            ));
        }
        let Some((_, redaction)) = self.resolved.as_ref() else {
            return false;
        };
        redact_document_row_bytes(Some(redaction), store, payload)
    }
}

#[cfg(test)]
mod tests {
    use crate::control::security::redaction::{RedactionMode, RedactionPolicy, RedactionRule};

    use super::*;

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

    fn redaction_for(role: &str) -> RowPushRedaction {
        RowPushRedaction {
            tenant_id: TenantId::new(1),
            roles: vec![role.to_string()],
            resolved: None,
        }
    }

    fn row(email: &str) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({"email": email, "name": "Alice"}))
            .expect("encode row post-image")
    }

    fn email_of(payload: &[u8]) -> serde_json::Value {
        nodedb_types::json_from_msgpack(payload).expect("decode row post-image")["email"].clone()
    }

    /// The leak this type closes: a row post-image pushed to a device carried
    /// every column in the clear, and the device then persisted it.
    #[test]
    fn pushed_row_is_masked_for_a_ruled_role() {
        let store = store_with_mask("users", "support", "email");
        let mut redaction = redaction_for("support");
        let mut payload = row("a@b.c");

        assert!(redaction.redact(&store, "users", &mut payload));

        assert_eq!(email_of(&payload), "***");
    }

    /// The rules are keyed per collection, so the cached inputs must be
    /// rebuilt when consecutive deltas cross a collection boundary — a stale
    /// cache would either leak the ruled collection or over-redact the other.
    #[test]
    fn cached_inputs_are_rebuilt_when_the_collection_changes() {
        let store = store_with_mask("users", "support", "email");
        let mut redaction = redaction_for("support");

        let mut ruled = row("a@b.c");
        assert!(redaction.redact(&store, "users", &mut ruled));
        assert_eq!(email_of(&ruled), "***");

        let mut unruled = row("d@e.f");
        assert!(redaction.redact(&store, "audit", &mut unruled));
        assert_eq!(email_of(&unruled), "d@e.f");

        let mut ruled_again = row("g@h.i");
        assert!(redaction.redact(&store, "users", &mut ruled_again));
        assert_eq!(email_of(&ruled_again), "***");
    }

    /// A role no policy names reads the pushed row in the clear, byte for
    /// byte — the fix must not perturb an installation with no policy.
    #[test]
    fn pushed_row_is_byte_identical_for_an_unruled_role() {
        let store = store_with_mask("users", "support", "email");
        let mut redaction = redaction_for("analyst");
        let original = row("a@b.c");
        let mut payload = original.clone();

        assert!(redaction.redact(&store, "users", &mut payload));

        assert_eq!(payload, original);
    }

    /// A delete tombstone carries no post-image, so it is delivered as-is.
    #[test]
    fn delete_tombstone_is_delivered_unchanged() {
        let store = store_with_mask("users", "support", "email");
        let mut redaction = redaction_for("support");
        let mut payload = Vec::new();

        assert!(redaction.redact(&store, "users", &mut payload));
        assert!(payload.is_empty());
    }
}
