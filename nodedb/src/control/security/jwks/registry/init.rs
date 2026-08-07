// SPDX-License-Identifier: BUSL-1.1

//! Registry construction: fetch JWKS from every configured provider on
//! startup (falling back to the disk cache), then spawn the periodic
//! refresh task.

use std::sync::Arc;

use crate::config::auth::JwtAuthConfig;
use crate::control::security::jwks::cache::JwksCache;
use crate::control::security::jwks::fetch;

use super::cache_identity::static_cache_identity;
use super::state::JwksRegistry;

impl JwksRegistry {
    /// Create and initialize the registry.
    ///
    /// Fetches JWKS from all providers on startup, loads disk cache as fallback,
    /// and spawns the periodic refresh task.
    pub async fn init(config: JwtAuthConfig) -> crate::Result<Self> {
        // Registry construction is also a public entry point, so it must not
        // rely on the server-config loader to reject unsafe static providers.
        // Validate before creating cache state, fetching remote keys, or
        // spawning a refresh task.
        config.validate()?;
        let policy = Arc::new(config.jwks_policy().map_err(|e| crate::Error::Config {
            detail: format!("auth.jwt allow-list is invalid: {e}"),
        })?);
        let cache = Arc::new(JwksCache::new(config.jwks_cache_path.clone()));

        // Load disk cache first (offline fallback).
        cache.load_from_disk();

        // Fetch from all providers (best-effort — failures use disk cache).
        for provider in &config.providers {
            let cache_identity = static_cache_identity(&provider.name);
            fetch::fetch_and_cache(
                &cache_identity,
                &provider.name,
                &provider.jwks_url,
                &cache,
                &policy,
            )
            .await;
        }

        // Spawn periodic refresh.
        let refresh_handle = if !config.providers.is_empty() {
            let pairs: Vec<(String, String, String)> = config
                .providers
                .iter()
                .map(|p| {
                    (
                        static_cache_identity(&p.name),
                        p.name.clone(),
                        p.jwks_url.clone(),
                    )
                })
                .collect();
            Some(fetch::spawn_refresh_task(
                pairs,
                cache.clone(),
                config.jwks_refresh_secs,
                policy.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            providers: config.providers.clone(),
            cache,
            config,
            policy,
            _refresh_handle: refresh_handle,
        })
    }
}
