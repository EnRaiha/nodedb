// SPDX-License-Identifier: BUSL-1.1

//! Registration of the collection a remote-written metric lands in.
//!
//! Every metric is stored in a timeseries collection named after it. The
//! collection must exist in the catalog: the PromQL read endpoints discover
//! metrics there, SQL resolves the name there, and backup and DROP find the
//! data through it. Remote write therefore creates the collection the first
//! time a metric arrives, with the shape the read path expects:
//! `timestamp TIMESTAMP TIME_KEY` and `value FLOAT`. Labels become columns
//! as line-protocol ingest adds them.

use std::sync::Arc;

use crate::control::promql::PromqlError;
use crate::control::security::audit::ArcAuditEmitter;
use crate::control::security::identity::{AuthenticatedIdentity, Permission};
use crate::control::server::shared::authorization::authorize_database_permission;
use crate::control::server::shared::ddl::neutral::collection::{
    CreateCollectionRequest, create_collection, register_proposed_collection,
};
use crate::control::state::SharedState;
use crate::types::DatabaseId;

/// SQLSTATE of a CREATE that lost a race with another creator.
const DUPLICATE_OBJECT: &str = "42P07";

/// Make sure `metric` has an active collection in `database_id`.
///
/// An existing active collection is used as it is. A dropped one is
/// refused: re-creating it would purge the data an UNDROP can still
/// restore. Creating a collection needs the Create permission on the
/// database.
pub(crate) async fn ensure_metric_collection(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    metric: &str,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    match catalog.get_collection(database_id, identity.tenant_id.as_u64(), metric)? {
        Some(existing) if existing.is_active => return Ok(()),
        Some(_) => {
            return Err(PromqlError::UnwritableSeries {
                metric: metric.to_owned(),
                detail: "its collection is dropped; UNDROP or purge it first".to_owned(),
            }
            .into());
        }
        None => {}
    }

    let audit = ArcAuditEmitter(Arc::clone(&state.audit));
    authorize_database_permission(identity, database_id, Permission::Create, catalog, &audit)?;

    let columns = [
        ("timestamp".to_owned(), "TIMESTAMP TIME_KEY".to_owned()),
        ("value".to_owned(), "FLOAT".to_owned()),
    ];
    let request = CreateCollectionRequest {
        name: metric,
        engine: Some("timeseries"),
        columns: &columns,
        options: &[],
        flags: &[],
        balanced_raw: None,
    };
    match create_collection(state, identity, &request, database_id).await {
        Ok(created) => {
            register_proposed_collection(state, created.outcome, &created.collection).await
        }
        Err(error) if error.sqlstate == DUPLICATE_OBJECT => Ok(()),
        Err(error) => Err(PromqlError::UnwritableSeries {
            metric: metric.to_owned(),
            detail: format!(
                "its collection cannot be created: {} {}",
                error.sqlstate, error.message
            ),
        }
        .into()),
    }
}
