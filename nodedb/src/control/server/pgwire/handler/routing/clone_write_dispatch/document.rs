// SPDX-License-Identifier: BUSL-1.1

//! Document engine CoW write interception.
//!
//! On a `Shadowed`/`Materializing` clone: UPDATE copies the source row up then
//! applies; DELETE tombstones the source surrogate; INSERT/PUT/UPSERT tombstones
//! the same-key source row without a copy-up.

use std::sync::Arc;

use pgwire::error::PgWireResult;

use nodedb_types::{CloneStatus, DatabaseId, Lsn, Surrogate, TenantId};

use crate::control::clone::copyup::{CopyUpParams, perform_clone_copyup};
use crate::control::clone::tombstone::{TombstoneParams, perform_clone_tombstone};
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_collection;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
use nodedb_physical::physical_task::PhysicalTask;

use super::super::super::auth::pgwire_authorization_error;
use super::super::super::core::NodeDbPgHandler;
use super::entry::CloneWriteOutcome;
use super::probes::{fetch_source_row, probe_row_in_target};
use super::util::{strip_db_prefix, synthetic_affected_response, write_err};

/// The clone-relevant shape of one document write.
enum DocWriteKind<'a> {
    Update {
        document_id: &'a str,
        /// Target-side surrogate carried by the plan, used to probe the clone.
        surrogate: Surrogate,
    },
    Delete {
        document_id: &'a str,
        surrogate: Surrogate,
    },
    /// Insert / put / upsert. Carries one id per row the statement writes.
    Insert { document_ids: Vec<&'a str> },
}

/// One document write reduced to what the CoW protocol needs.
struct DocWrite<'a> {
    collection_qualified: &'a str,
    kind: DocWriteKind<'a>,
}

/// Classify a plan into the CoW shape it needs, or `None` when the clone write
/// path has nothing to do for it.
fn classify(plan: &PhysicalPlan) -> Option<DocWrite<'_>> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointUpdate {
            collection,
            document_id,
            surrogate,
            ..
        }) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Update {
                document_id: document_id.as_str(),
                surrogate: *surrogate,
            },
        }),
        PhysicalPlan::Document(DocumentOp::PointDelete {
            collection,
            document_id,
            surrogate,
            ..
        }) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Delete {
                document_id: document_id.as_str(),
                surrogate: *surrogate,
            },
        }),
        PhysicalPlan::Document(
            DocumentOp::PointInsert {
                collection,
                document_id,
                ..
            }
            | DocumentOp::PointPut {
                collection,
                document_id,
                ..
            }
            | DocumentOp::Upsert {
                collection,
                document_id,
                ..
            },
        ) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Insert {
                document_ids: vec![document_id.as_str()],
            },
        }),
        PhysicalPlan::Document(DocumentOp::BatchInsert {
            collection,
            documents,
            ..
        }) => Some(DocWrite {
            collection_qualified: collection.as_str(),
            kind: DocWriteKind::Insert {
                document_ids: documents.iter().map(|(id, _)| id.as_str()).collect(),
            },
        }),
        _ => None,
    }
}

/// Resolve the surrogate the source database bound to `document_id`.
///
/// `None` means the source never held that primary key.
fn source_surrogate(
    state: &SharedState,
    tenant_id: TenantId,
    source_db_id: DatabaseId,
    source_coll_qualified: &str,
    document_id: &str,
) -> PgWireResult<Option<Surrogate>> {
    state
        .surrogate_assigner
        .lookup(
            source_db_id,
            tenant_id,
            source_coll_qualified,
            document_id.as_bytes(),
        )
        .map_err(|e| write_err(&format!("clone write source surrogate lookup: {e}")))
}

impl NodeDbPgHandler {
    /// Handle Document CoW write interception.
    pub(super) async fn intercept_doc_clone_write(
        &self,
        task: &PhysicalTask,
        identity: &AuthenticatedIdentity,
        tenant_id: TenantId,
    ) -> PgWireResult<CloneWriteOutcome> {
        let Some(write) = classify(&task.plan) else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let catalog = self.state.credentials.catalog();

        let db_id = task.database_id;
        let coll_name = strip_db_prefix(db_id, write.collection_qualified);

        let desc = catalog
            .get_collection(db_id, tenant_id.as_u64(), coll_name)
            .map_err(|e| write_err(&format!("clone write: get_collection: {e}")))?;
        let Some(desc) = desc else {
            return Ok(CloneWriteOutcome::Passthrough);
        };

        let Some(ref origin) = desc.cloned_from else {
            return Ok(CloneWriteOutcome::Passthrough);
        };
        match desc.clone_status {
            CloneStatus::Materialized => return Ok(CloneWriteOutcome::Passthrough),
            CloneStatus::Shadowed | CloneStatus::Materializing { .. } => {}
        }

        let emitter = ArcAuditEmitter(Arc::clone(&self.state.audit));
        authorize_collection(
            identity,
            origin.source_database,
            &origin.source_collection,
            Permission::Read,
            &self.state.permissions,
            &self.state.roles,
            &emitter,
        )
        .map_err(pgwire_authorization_error)?;

        let source_db_id = origin.source_database;
        let source_coll_qualified =
            crate::control::planner::sql_plan_convert::convert::db_qualified(
                source_db_id,
                origin.source_collection.as_str(),
            );

        match write.kind {
            DocWriteKind::Insert { document_ids } => {
                for document_id in document_ids {
                    let Some(surrogate) = source_surrogate(
                        &self.state,
                        tenant_id,
                        source_db_id,
                        &source_coll_qualified,
                        document_id,
                    )?
                    else {
                        continue;
                    };
                    // A binding without a live row makes the tombstone a read no-op —
                    // not worth a probe round trip per inserted key.
                    perform_clone_tombstone(TombstoneParams {
                        state: &self.state,
                        target_db_id: db_id,
                        target_collection: coll_name,
                        source_surrogate: surrogate,
                    })
                    .map_err(|e| write_err(&format!("clone insert tombstone: {e}")))?;
                }
                Ok(CloneWriteOutcome::Passthrough)
            }

            DocWriteKind::Delete {
                document_id,
                surrogate,
            } => {
                let row_in_target = probe_row_in_target(
                    &self.state,
                    identity,
                    tenant_id,
                    db_id,
                    write.collection_qualified,
                    document_id,
                    surrogate,
                )
                .await
                .map_err(|e| write_err(&format!("clone write probe: {e}")))?;

                let src = source_surrogate(
                    &self.state,
                    tenant_id,
                    source_db_id,
                    &source_coll_qualified,
                    document_id,
                )?;

                // Tombstone regardless of target residency — after DELETE the clone
                // must never show the source copy again.
                if let Some(src) = src {
                    perform_clone_tombstone(TombstoneParams {
                        state: &self.state,
                        target_db_id: db_id,
                        target_collection: coll_name,
                        source_surrogate: src,
                    })
                    .map_err(|e| write_err(&format!("clone tombstone: {e}")))?;
                }

                if row_in_target {
                    return Ok(CloneWriteOutcome::Passthrough);
                }

                // The source read decides rows-affected (1 or 0) — a resolved surrogate
                // is not evidence the row exists, since a surrogate outlives its row.
                let source_row = match src {
                    Some(src) => fetch_source_row(
                        &self.state,
                        identity,
                        tenant_id,
                        source_db_id,
                        &source_coll_qualified,
                        document_id,
                        src,
                    )
                    .await
                    .map_err(|e| write_err(&format!("clone delete source probe: {e}")))?,
                    None => None,
                };

                Ok(CloneWriteOutcome::Handled(synthetic_affected_response(
                    self.next_request_id(),
                    Lsn::new(0),
                    u64::from(source_row.is_some()),
                )))
            }

            DocWriteKind::Update {
                document_id,
                surrogate,
            } => {
                let row_in_target = probe_row_in_target(
                    &self.state,
                    identity,
                    tenant_id,
                    db_id,
                    write.collection_qualified,
                    document_id,
                    surrogate,
                )
                .await
                .map_err(|e| write_err(&format!("clone write probe: {e}")))?;

                if row_in_target {
                    return Ok(CloneWriteOutcome::Passthrough);
                }

                let Some(src) = source_surrogate(
                    &self.state,
                    tenant_id,
                    source_db_id,
                    &source_coll_qualified,
                    document_id,
                )?
                else {
                    return Ok(CloneWriteOutcome::Passthrough);
                };

                let source_row_bytes = fetch_source_row(
                    &self.state,
                    identity,
                    tenant_id,
                    source_db_id,
                    &source_coll_qualified,
                    document_id,
                    src,
                )
                .await
                .map_err(|e| write_err(&format!("clone write fetch source: {e}")))?;

                let Some(source_row_bytes) = source_row_bytes else {
                    return Ok(CloneWriteOutcome::Passthrough);
                };

                perform_clone_copyup(CopyUpParams {
                    state: &Arc::clone(&self.state),
                    tenant_id,
                    target_db_id: db_id,
                    target_collection: coll_name,
                    source_surrogate: src,
                    source_doc_id: document_id.to_string(),
                    source_row_bytes,
                })
                .await
                .map_err(|e| write_err(&format!("clone copyup: {e}")))?;

                Ok(CloneWriteOutcome::Passthrough)
            }
        }
    }
}
