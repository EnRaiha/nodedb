// SPDX-License-Identifier: BUSL-1.1

//! Synchronous catalog application for `PutOidcProvider` / `DeleteOidcProvider`.

use crate::control::security::catalog::{StoredOidcProvider, SystemCatalog, catalog_err};

pub fn put(provider: &StoredOidcProvider, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog.put_oidc_provider(provider).map_err(|e| {
        catalog_err(
            &format!("put_oidc_provider '{}'", provider.provider_name),
            e,
        )
    })
}

pub fn delete(name: &str, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .delete_oidc_provider(name)
        .map_err(|e| catalog_err(&format!("delete_oidc_provider '{name}'"), e))
}
