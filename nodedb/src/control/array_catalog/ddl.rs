// SPDX-License-Identifier: BUSL-1.1

//! Authorized Array DDL catalog transitions.
//!
//! SQL conversion is deliberately read-only. This module is called only by
//! the Control-Plane write funnel after a task owns an authorization capability
//! and immediately before the task is handed to the Data Plane.

use nodedb_physical::physical_plan::{ArrayOp, MetaOp};
use nodedb_types::config::retention::BitemporalRetention;

use crate::bridge::envelope::{PhysicalPlan, Response};
use crate::control::array_catalog::ArrayCatalogEntry;
use crate::engine::bitemporal::BitemporalEngineKind;
use crate::engine::bitemporal::registry::Entry as RetentionEntry;
use crate::types::TraceId;
use crate::types::{DatabaseId, TenantId};

/// The exact state replaced by an authorized Array DDL transition.
///
/// A token is created only after authorization and admission. The caller must
/// roll it back if dispatch cannot prove the Data Plane applied the task, and
/// finalize it after a successful response. DROP deliberately leaves its
/// durable row and surrogate bindings in place until finalization: recreating
/// bindings from a failed broadcast would otherwise be impossible.
pub(crate) struct AuthorizedDdlTransition {
    kind: TransitionKind,
    tenant_id: TenantId,
    database_id: DatabaseId,
    durable: Option<ArrayCatalogEntry>,
    in_memory: Option<ArrayCatalogEntry>,
    retention: Option<RetentionEntry>,
    /// The post-transition entry for CREATE/ALTER; needed to undo CREATE
    /// without deriving identity from any mutable mirror.
    transition_entry: Option<ArrayCatalogEntry>,
}

enum TransitionKind {
    None,
    Create,
    Alter,
    Drop {
        array_id: nodedb_array::types::ArrayId,
    },
}

impl AuthorizedDdlTransition {
    /// Restore all mirrors to their exact pre-transition state. This is
    /// idempotent so a caller can safely invoke it for any failed response.
    pub(crate) fn rollback(&self, state: &crate::control::state::SharedState) -> crate::Result<()> {
        match &self.durable {
            Some(entry) => super::persist::persist(state.credentials.catalog(), entry),
            None => match &self.kind {
                TransitionKind::Create => {
                    let entry =
                        self.transition_entry
                            .as_ref()
                            .ok_or_else(|| crate::Error::PlanError {
                                detail: "CREATE ARRAY rollback lost created entry".into(),
                            })?;
                    super::persist::remove(state.credentials.catalog(), &entry.array_id)
                }
                _ => Ok(()),
            },
        }
        .map_err(|e| crate::Error::PlanError {
            detail: format!("array DDL rollback catalog: {e}"),
        })?;

        let name = self
            .in_memory
            .as_ref()
            .or(self.durable.as_ref())
            .or(self.transition_entry.as_ref())
            .map(|entry| entry.name.as_str())
            .or(match &self.kind {
                TransitionKind::Drop { array_id } => Some(array_id.name.as_str()),
                _ => None,
            });
        if let Some(name) = name {
            let mut catalog = state
                .array_catalog
                .write()
                .map_err(|_| crate::Error::PlanError {
                    detail: "array catalog lock poisoned".into(),
                })?;
            catalog.unregister_in_database(self.tenant_id, self.database_id, name);
            if let Some(entry) = &self.in_memory {
                catalog
                    .register(entry.clone())
                    .map_err(|e| crate::Error::PlanError {
                        detail: format!("array DDL rollback catalog mirror: {e}"),
                    })?;
            }
        }

        if let Some(name) = name {
            state
                .bitemporal_retention_registry
                .unregister(self.database_id, self.tenant_id, name);
        }
        if let Some(retention) = &self.retention {
            state
                .bitemporal_retention_registry
                .register(
                    retention.database_id,
                    retention.tenant_id,
                    retention.collection.clone(),
                    retention.engine,
                    retention.retention,
                )
                .map_err(|e| crate::Error::PlanError {
                    detail: format!("array DDL rollback retention: {e}"),
                })?;
        }
        Ok(())
    }

    /// Whether an enqueued CREATE/ALTER must be preserved if its response is
    /// ambiguous. Their catalog transition may already match Data-Plane state,
    /// so rolling it back would leave an opened ghost engine.
    pub(crate) fn preserves_on_ambiguous_apply(&self) -> bool {
        matches!(self.kind, TransitionKind::Create | TransitionKind::Alter)
    }

    /// Complete irreversible work only after the Data Plane confirmed success.
    pub(crate) fn finalize(&self, state: &crate::control::state::SharedState) -> crate::Result<()> {
        if let TransitionKind::Drop { array_id } = &self.kind {
            super::persist::remove_with_surrogates(state.credentials.catalog(), array_id).map_err(
                |e| crate::Error::PlanError {
                    detail: format!(
                        "DROP ARRAY {}: catalog/surrogate delete: {e}",
                        array_id.name
                    ),
                },
            )?;
        }
        Ok(())
    }
}

/// Apply the reversible catalog transition required by an authorized Array DDL
/// task. In-memory mirrors change only after their durable update commits.
/// Execute the all-core reversible DROP protocol after authorization.
///
/// Catalog deletion is deferred until every core has staged its directory. On
/// any stage or finalize failure, compensation is broadcast to every core
/// before catalog mirrors are restored. Once deletion is durable, failed purge
/// is returned to the caller without recreating mirrors; tombstones then fence
/// later CREATE attempts until an operator/retry completes the purge.
pub(crate) async fn run_authorized_drop(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Response> {
    let array_id = match &plan {
        PhysicalPlan::Array(ArrayOp::DropArray { array_id }) => array_id.clone(),
        _ => {
            return Err(crate::Error::PlanError {
                detail: "array drop protocol received non-drop plan".into(),
            });
        }
    };
    let transition = apply_authorized_ddl(state, tenant_id, database_id, &plan)?;
    let restore = PhysicalPlan::Array(ArrayOp::RestoreArrayDrop {
        array_id: array_id.clone(),
    });
    let stage = crate::control::server::broadcast::broadcast_count_to_all_cores(
        state,
        tenant_id,
        database_id,
        plan,
        trace_id,
        "dropped",
    )
    .await;
    let response = match stage {
        Ok(response) => response,
        Err(error) => {
            crate::control::server::broadcast::broadcast_count_to_all_cores(
                state,
                tenant_id,
                database_id,
                restore,
                trace_id,
                "restored",
            )
            .await?;
            transition.rollback(state)?;
            return Err(error);
        }
    };
    if let Err(error) = transition.finalize(state) {
        crate::control::server::broadcast::broadcast_count_to_all_cores(
            state,
            tenant_id,
            database_id,
            restore,
            trace_id,
            "restored",
        )
        .await?;
        transition.rollback(state)?;
        return Err(error);
    }
    crate::control::server::broadcast::broadcast_count_to_all_cores(
        state,
        tenant_id,
        database_id,
        PhysicalPlan::Array(ArrayOp::PurgeArrayDrop { array_id }),
        trace_id,
        "purged",
    )
    .await?;
    Ok(response)
}

pub(crate) fn apply_authorized_ddl(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: &PhysicalPlan,
) -> crate::Result<AuthorizedDdlTransition> {
    let (kind, target) = match plan {
        PhysicalPlan::Array(ArrayOp::OpenArray {
            array_id,
            schema_msgpack,
            schema_hash,
            prefix_bits,
            audit_retain_ms,
            minimum_audit_retain_ms,
        }) => (
            TransitionKind::Create,
            Some(ArrayCatalogEntry {
                array_id: array_id.clone(),
                name: array_id.name.clone(),
                schema_msgpack: schema_msgpack.clone(),
                schema_hash: *schema_hash,
                created_at_ms: now_epoch_ms(),
                prefix_bits: *prefix_bits,
                audit_retain_ms: *audit_retain_ms,
                minimum_audit_retain_ms: *minimum_audit_retain_ms,
            }),
        ),
        PhysicalPlan::Array(ArrayOp::DropArray { array_id }) => (
            TransitionKind::Drop {
                array_id: array_id.clone(),
            },
            None,
        ),
        PhysicalPlan::Meta(MetaOp::AlterArray {
            array_id,
            audit_retain_ms,
            minimum_audit_retain_ms,
        }) => {
            let current = lookup_memory(state, tenant_id, database_id, array_id)?;
            let updated = ArrayCatalogEntry {
                audit_retain_ms: audit_retain_ms.unwrap_or(current.audit_retain_ms),
                minimum_audit_retain_ms: minimum_audit_retain_ms
                    .unwrap_or(current.minimum_audit_retain_ms),
                ..current
            };
            (TransitionKind::Alter, Some(updated))
        }
        _ => (TransitionKind::None, None),
    };
    if matches!(&kind, TransitionKind::None) {
        return Ok(AuthorizedDdlTransition {
            kind,
            tenant_id,
            database_id,
            durable: None,
            in_memory: None,
            retention: None,
            transition_entry: None,
        });
    }

    let identity = target
        .as_ref()
        .map(|entry| entry.array_id.clone())
        .or_else(|| match &kind {
            TransitionKind::Drop { array_id } => Some(array_id.clone()),
            _ => None,
        })
        .ok_or_else(|| crate::Error::PlanError {
            detail: "array DDL transition missing identity".into(),
        })?;
    let durable = state
        .credentials
        .catalog()
        .get_array_in_database(tenant_id, database_id, &identity.name)
        .map_err(|e| crate::Error::PlanError {
            detail: format!("array DDL catalog read: {e}"),
        })?;
    let in_memory = state
        .array_catalog
        .read()
        .map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?
        .lookup_by_id(&identity);
    let retention = state
        .bitemporal_retention_registry
        .snapshot()
        .into_iter()
        .find(|entry| {
            entry.database_id == database_id
                && entry.tenant_id == tenant_id
                && entry.collection == identity.name
        });
    let token = AuthorizedDdlTransition {
        kind,
        tenant_id,
        database_id,
        durable,
        in_memory,
        retention,
        transition_entry: target.clone(),
    };

    let apply = match &token.kind {
        TransitionKind::Create => {
            let entry = target.as_ref().ok_or_else(|| crate::Error::PlanError {
                detail: "CREATE ARRAY transition missing entry".into(),
            })?;
            // CREATE is authorized only from complete catalog absence. Checking
            // both copies prevents an incomplete DROP rollback (or a stale
            // mirror) from reaching OpenArray and purging its reversible
            // tombstone as if it were a finalized prior DROP.
            if token.durable.is_some() || token.in_memory.is_some() {
                return Err(crate::Error::PlanError {
                    detail: format!("CREATE ARRAY {}: already exists", entry.name),
                });
            }
            super::persist::persist(state.credentials.catalog(), entry).map_err(|e| {
                crate::Error::PlanError {
                    detail: format!("CREATE ARRAY {}: catalog persist: {e}", entry.name),
                }
            })?;
            install_memory(state, tenant_id, database_id, entry)
                .and_then(|_| register_retention(state, database_id, entry))
        }
        TransitionKind::Alter => {
            let entry = target.as_ref().ok_or_else(|| crate::Error::PlanError {
                detail: "ALTER ARRAY transition missing entry".into(),
            })?;
            if token.in_memory.is_none() {
                return Err(crate::Error::PlanError {
                    detail: format!("ALTER ARRAY {}: not found", entry.name),
                });
            }
            super::persist::persist(state.credentials.catalog(), entry).map_err(|e| {
                crate::Error::PlanError {
                    detail: format!("ALTER ARRAY {}: catalog persist: {e}", entry.name),
                }
            })?;
            install_memory(state, tenant_id, database_id, entry)
                .and_then(|_| register_retention(state, database_id, entry))
        }
        TransitionKind::Drop { array_id } => {
            if token.in_memory.is_none() {
                return Err(crate::Error::PlanError {
                    detail: format!("DROP ARRAY {}: not found", array_id.name),
                });
            }
            remove_memory(state, tenant_id, database_id, &array_id.name)?;
            state
                .bitemporal_retention_registry
                .unregister(database_id, tenant_id, &array_id.name);
            Ok(())
        }
        TransitionKind::None => Ok(()),
    };
    if let Err(error) = apply {
        let _ = token.rollback(state);
        return Err(error);
    }
    Ok(token)
}

fn lookup_memory(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: &str,
) -> crate::Result<ArrayCatalogEntry> {
    state
        .array_catalog
        .read()
        .map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?
        .lookup_by_name_in_database(tenant_id, database_id, name)
        .ok_or_else(|| crate::Error::PlanError {
            detail: format!("ALTER ARRAY {name}: not found"),
        })
}

fn install_memory(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    entry: &ArrayCatalogEntry,
) -> crate::Result<()> {
    let mut catalog = state
        .array_catalog
        .write()
        .map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
    catalog.unregister_in_database(tenant_id, database_id, &entry.name);
    catalog
        .register(entry.clone())
        .map_err(|e| crate::Error::PlanError {
            detail: format!("array catalog register: {e}"),
        })
}

fn remove_memory(
    state: &crate::control::state::SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    name: &str,
) -> crate::Result<()> {
    let mut catalog = state
        .array_catalog
        .write()
        .map_err(|_| crate::Error::PlanError {
            detail: "array catalog lock poisoned".into(),
        })?;
    catalog.unregister_in_database(tenant_id, database_id, name);
    Ok(())
}

fn register_retention(
    state: &crate::control::state::SharedState,
    database_id: DatabaseId,
    entry: &ArrayCatalogEntry,
) -> crate::Result<()> {
    match entry.audit_retain_ms {
        Some(audit_retain_ms) => state
            .bitemporal_retention_registry
            .register(
                database_id,
                entry.array_id.tenant_id,
                &entry.name,
                BitemporalEngineKind::Array,
                BitemporalRetention {
                    data_retain_ms: 0,
                    audit_retain_ms: audit_retain_ms as u64,
                    minimum_audit_retain_ms: entry.minimum_audit_retain_ms.unwrap_or(0),
                },
            )
            .map_err(|e| crate::Error::PlanError {
                detail: format!("array retention register: {e}"),
            }),
        None => {
            state.bitemporal_retention_registry.unregister(
                database_id,
                entry.array_id.tenant_id,
                &entry.name,
            );
            Ok(())
        }
    }
}

fn now_epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(kind: TransitionKind) -> AuthorizedDdlTransition {
        AuthorizedDdlTransition {
            kind,
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            durable: None,
            in_memory: None,
            retention: None,
            transition_entry: None,
        }
    }

    #[test]
    fn only_create_and_alter_preserve_catalog_on_ambiguous_apply() {
        assert!(token(TransitionKind::Create).preserves_on_ambiguous_apply());
        assert!(token(TransitionKind::Alter).preserves_on_ambiguous_apply());
        assert!(!token(TransitionKind::None).preserves_on_ambiguous_apply());
    }
}
