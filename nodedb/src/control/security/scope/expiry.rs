// SPDX-License-Identifier: BUSL-1.1

//! Periodic scope grant expiry processing.
//!
//! Checks for expired scope grants and executes their `ON EXPIRE` actions
//! (automatic downgrade or hard revoke). Runs on the Control Plane; the loop
//! that drives it lives in `bootstrap::background_loops`.

use std::sync::Arc;
use std::time::Duration;

use tracing::{info, warn};

use crate::control::security::audit::AuditEvent;
use crate::control::state::SharedState;

use super::grant::replication::{propose_grant, propose_revoke};
use super::grant::{ScopeGrantParams, ScopeStatus};

/// Spawn the periodic scope-grant expiry sweep.
///
/// The sweep must run exactly once cluster-wide — each pass proposes catalog
/// mutations, so every node running it duplicates them. The metadata
/// leader runs it ([`SharedState::is_singleton_worker`]). A one-node cluster
/// leads its own metadata group.
///
/// The pass awaits each replicated `ON EXPIRE` action on the loop's task.
pub fn spawn_expiry_task(shared: Arc<SharedState>, interval_secs: u64) {
    // Below ~10s the sweep costs more than the resolution it buys: expiry is
    // already enforced on every read by `ScopeGrant::is_effective`, and this
    // loop only makes the outcome durable. The startup config gate rejects
    // an interval below that floor, so no clamp runs here.
    let interval = Duration::from_secs(interval_secs);
    info!(interval_secs, "scope expiry sweep loop running");
    let loop_shared = Arc::clone(&shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "scope_expiry_sweep",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(interval);
            // Skip the first tick, which fires immediately.
            tick.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                if !loop_shared.is_singleton_worker() {
                    continue;
                }
                process_expired_grants(&loop_shared).await;
            }
        },
    );
}

/// A scope lifecycle event for CDC/webhook/audit.
#[derive(Debug, Clone)]
pub struct ScopeEvent {
    pub event_type: &'static str,
    pub scope_name: String,
    pub grantee_type: String,
    pub grantee_id: String,
    pub detail: String,
}

/// Process all expired and grace-period grants. Returns emitted events.
pub async fn process_expired_grants_with_events(state: &SharedState) -> Vec<ScopeEvent> {
    let mut events = Vec::new();
    process_expired_grants_inner(state, &mut events).await;
    events
}

/// Process all expired and grace-period grants, recording each lifecycle
/// event in the audit log.
///
/// Scope lifetime changes happen with no operator in the loop, so the audit
/// trail is the only record that a grant was downgraded or cut off; dropping
/// these events leaves the change invisible after the fact.
pub async fn process_expired_grants(state: &SharedState) {
    for event in process_expired_grants_with_events(state).await {
        state.audit_record(
            AuditEvent::AdminAction,
            None,
            "system:expiry",
            &format!(
                "{}: scope '{}' for {} '{}' ({})",
                event.event_type,
                event.scope_name,
                event.grantee_type,
                event.grantee_id,
                event.detail
            ),
        );
    }
}

async fn process_expired_grants_inner(state: &SharedState, events: &mut Vec<ScopeEvent>) {
    let all_grants = state.scope_grants.list(None);
    let mut expired_count = 0u32;
    let mut grace_count = 0u32;

    for grant in &all_grants {
        if grant.expires_at == 0 {
            continue; // Permanent — skip.
        }

        match grant.status() {
            ScopeStatus::Grace => {
                grace_count += 1;
                info!(
                    scope = %grant.scope_name,
                    grantee = %grant.grantee_id,
                    grantee_type = %grant.grantee_type,
                    "scope grant in grace period"
                );
                events.push(ScopeEvent {
                    event_type: "scope.grace_entered",
                    scope_name: grant.scope_name.clone(),
                    grantee_type: grant.grantee_type.clone(),
                    grantee_id: grant.grantee_id.clone(),
                    detail: format!("expires_at={}", grant.expires_at),
                });
            }
            ScopeStatus::Expired => {
                // One grant whose action cannot be replicated right now (a
                // lost leadership, a catalog write error) must not stall the
                // sweep for every other expired grant: report it and carry on.
                // The grant stays expired — so it authorizes nothing — and the
                // next sweep retries the action.
                if let Err(e) = execute_on_expire(state, grant).await {
                    warn!(
                        scope = %grant.scope_name,
                        grantee_type = %grant.grantee_type,
                        grantee = %grant.grantee_id,
                        error = %e,
                        "scope expiry: ON EXPIRE action failed; retrying next sweep"
                    );
                    continue;
                }
                expired_count += 1;
                events.push(ScopeEvent {
                    event_type: "scope.expired",
                    scope_name: grant.scope_name.clone(),
                    grantee_type: grant.grantee_type.clone(),
                    grantee_id: grant.grantee_id.clone(),
                    detail: grant.on_expire_action.clone(),
                });
            }
            ScopeStatus::Active | ScopeStatus::None => {}
        }
    }

    if expired_count > 0 || grace_count > 0 {
        info!(
            expired = expired_count,
            grace = grace_count,
            "scope expiry check completed"
        );
    }
}

/// Execute the `on_expire_action` for a fully expired grant.
///
/// Every mutation goes through the replicated propose path, so the outcome is
/// durable and reaches every node: a downgrade or cutoff that only touched the
/// sweeping node's memory is undone by the next restart and leaves
/// the rest of the cluster authorizing on the retired grant.
async fn execute_on_expire(
    state: &SharedState,
    grant: &super::grant::ScopeGrant,
) -> crate::Result<()> {
    let action = &grant.on_expire_action;

    if action.is_empty() {
        // No action configured — let it stay expired.
        // The grant is already filtered out of effective_scopes().
        return Ok(());
    }

    if action == "revoke_all" {
        // Hard cutoff: remove the grant entirely.
        propose_revoke(
            state,
            &grant.scope_name,
            &grant.grantee_type,
            &grant.grantee_id,
        )
        .await?;
        info!(
            scope = %grant.scope_name,
            grantee = %grant.grantee_id,
            "expired scope grant revoked (ON EXPIRE REVOKE ALL)"
        );
        return Ok(());
    }

    if let Some(downgrade_scope) = action.strip_prefix("grant:") {
        // Automatic downgrade: grant a replacement scope.
        let stored = state.scope_grants.prepare_grant(ScopeGrantParams {
            scope_name: downgrade_scope,
            grantee_type: &grant.grantee_type,
            grantee_id: &grant.grantee_id,
            granted_by: "system:expiry",
            expires_at: 0, // Permanent (no expiry on the downgrade).
            grace_period_secs: 0,
            on_expire_action: "",
            // The downgrade is a different, lesser scope; the expired
            // grant's conditions described access to the scope being
            // retired, so they are not carried over.
            conditions: Vec::new(),
        })?;
        // The replacement lands before the original is retired. Both are
        // idempotent upserts, so a failure here leaves the expired (and
        // therefore ineffective) original in place for the next sweep to
        // retry — rather than dropping the grantee to no scope at all.
        propose_grant(state, &stored).await?;
        propose_revoke(
            state,
            &grant.scope_name,
            &grant.grantee_type,
            &grant.grantee_id,
        )
        .await?;
        info!(
            old_scope = %grant.scope_name,
            new_scope = %downgrade_scope,
            grantee = %grant.grantee_id,
            "expired scope downgraded (ON EXPIRE GRANT)"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::cluster::test_one_node;

    fn now_secs() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Install a grant the way a `GRANT SCOPE` statement does — through the
    /// replicated propose path, so the catalog row exists too and the tests
    /// can assert on durable state rather than only the in-memory map.
    async fn install(state: &SharedState, params: ScopeGrantParams<'_>) {
        let stored = state
            .scope_grants
            .prepare_grant(params)
            .expect("prepare grant");
        propose_grant(state, &stored).await.expect("propose grant");
    }

    fn catalog_scopes(state: &SharedState) -> Vec<String> {
        state
            .credentials
            .catalog()
            .load_all_scope_grants()
            .expect("load scope grants")
            .into_iter()
            .map(|g| g.scope_name)
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_grant_with_revoke_all() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let past = now_secs() - 100;
        install(
            state,
            ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "org",
                grantee_id: "acme",
                granted_by: "admin",
                expires_at: past,
                grace_period_secs: 0,
                on_expire_action: "revoke_all",
                conditions: Vec::new(),
            },
        )
        .await;

        // Grant exists but is expired.
        assert!(
            !state
                .scope_grants
                .has_scope("u1", &["acme".into()], "pro:all")
        );

        // Process expiry — must revoke.
        process_expired_grants(state).await;

        // Grant is gone.
        assert_eq!(state.scope_grants.count(), 0);
        cluster.shutdown().await;
    }

    /// Why the action goes through the propose path: a
    /// revoke that only cleared the in-memory map comes back at the next
    /// restart, re-granting an expired scope.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expiry_removes_the_durable_grant() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let past = now_secs() - 100;
        install(
            state,
            ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "org",
                grantee_id: "acme",
                granted_by: "admin",
                expires_at: past,
                grace_period_secs: 0,
                on_expire_action: "revoke_all",
                conditions: Vec::new(),
            },
        )
        .await;
        assert_eq!(catalog_scopes(state), vec!["pro:all".to_string()]);

        process_expired_grants(state).await;

        assert!(
            catalog_scopes(state).is_empty(),
            "expired grant survived in the catalog"
        );
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn expired_grant_with_downgrade() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        let past = now_secs() - 100;
        install(
            state,
            ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "org",
                grantee_id: "acme",
                granted_by: "admin",
                expires_at: past,
                grace_period_secs: 0,
                on_expire_action: "grant:free:basic",
                conditions: Vec::new(),
            },
        )
        .await;

        process_expired_grants(state).await;

        // pro:all is gone, free:basic exists.
        assert!(
            !state
                .scope_grants
                .has_scope("u1", &["acme".into()], "pro:all")
        );
        assert!(
            state
                .scope_grants
                .has_scope("u1", &["acme".into()], "free:basic")
        );
        // …and the swap is durable, not only in memory.
        assert_eq!(catalog_scopes(state), vec!["free:basic".to_string()]);
        cluster.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn grace_period_still_effective() {
        let cluster = test_one_node::boot().await;
        let state = &cluster.state;
        // Expired 10s ago but grace is 60s.
        let past = now_secs() - 10;
        install(
            state,
            ScopeGrantParams {
                scope_name: "pro:all",
                grantee_type: "org",
                grantee_id: "acme",
                granted_by: "admin",
                expires_at: past,
                grace_period_secs: 60,
                on_expire_action: "revoke_all",
                conditions: Vec::new(),
            },
        )
        .await;

        // In grace period — still effective.
        assert!(
            state
                .scope_grants
                .has_scope("u1", &["acme".into()], "pro:all")
        );

        // Process expiry — does NOT revoke (still in grace).
        process_expired_grants(state).await;
        assert_eq!(state.scope_grants.count(), 1); // Still there.
        assert_eq!(catalog_scopes(state), vec!["pro:all".to_string()]);
        cluster.shutdown().await;
    }
}
