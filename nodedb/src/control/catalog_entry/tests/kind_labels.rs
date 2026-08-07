// SPDX-License-Identifier: BUSL-1.1

//! Stable `kind()` label coverage across variants.

use crate::control::catalog_entry::entry::CatalogEntry;
use crate::control::security::catalog::oidc_providers::StoredOidcProvider;
use crate::control::security::catalog::sequence_types::StoredSequence;
use crate::control::security::catalog::{StoredCollection, StoredScopeGrant};

#[test]
fn kind_label_is_stable() {
    assert_eq!(
        CatalogEntry::PutCollection(Box::new(StoredCollection::new(1, "a", "b"))).kind(),
        "put_collection"
    );
    assert_eq!(
        CatalogEntry::DeactivateCollection {
            database_id: 0,
            tenant_id: 1,
            name: "a".into()
        }
        .kind(),
        "deactivate_collection"
    );
    assert_eq!(
        CatalogEntry::PutSequence(Box::new(StoredSequence::new(1, "c".into(), "b".into()))).kind(),
        "put_sequence"
    );
    assert_eq!(
        CatalogEntry::DeleteSequence {
            tenant_id: 1,
            name: "c".into()
        }
        .kind(),
        "delete_sequence"
    );
    let provider = StoredOidcProvider {
        provider_name: "test".into(),
        issuer: "https://example.com".into(),
        jwks_uri: "https://example.com/.well-known/jwks.json".into(),
        audience: None,
        tenant_id: None,
        claim_mapping: vec![],
        created_at_lsn: 0,
    };
    assert_eq!(
        CatalogEntry::PutOidcProvider(Box::new(provider)).kind(),
        "put_oidc_provider"
    );
    assert_eq!(
        CatalogEntry::DeleteOidcProvider {
            name: "test".into()
        }
        .kind(),
        "delete_oidc_provider"
    );
    let scope_grant = StoredScopeGrant {
        scope_name: "pro:all".into(),
        grantee_type: "org".into(),
        grantee_id: "acme".into(),
        granted_by: "admin".into(),
        granted_at: 1_000,
        expires_at: 0,
        grace_period_secs: 0,
        on_expire_action: String::new(),
        conditions_json: String::new(),
    };
    assert_eq!(
        CatalogEntry::PutScopeGrant(Box::new(scope_grant)).kind(),
        "put_scope_grant"
    );
    assert_eq!(
        CatalogEntry::DeleteScopeGrant {
            scope_name: "pro:all".into(),
            grantee_type: "org".into(),
            grantee_id: "acme".into(),
        }
        .kind(),
        "delete_scope_grant"
    );
}
