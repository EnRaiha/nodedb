// SPDX-License-Identifier: BUSL-1.1

//! `CREATE RESTORE POINT` and `SHOW RESTORE POINTS`: cluster restore points.
//! Superuser only.

use serde_json::{Map, Value as JsonValue};

use crate::control::pitr::restore_point::{
    RestorePointError, create_restore_point as create_point, list_restore_points,
};
use crate::control::security::catalog::restore_points::StoredRestorePoint;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::response_shape::types::{DdlColType, ShapedRows};
use crate::control::state::SharedState;

use super::super::super::result::{DdlError, DdlResult};
use super::support::ddl_err;

/// CREATE RESTORE POINT — take one consistent point across every Raft group.
pub async fn create_restore_point(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity, "create a restore point")?;
    let point = create_point(state).await.map_err(point_err)?;
    Ok(vec![rows(&[point])])
}

/// SHOW RESTORE POINTS — every cluster restore point, oldest first.
pub fn show_restore_points(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
) -> Result<Vec<DdlResult>, DdlError> {
    require_superuser(identity, "list restore points")?;
    let points = list_restore_points(state).map_err(point_err)?;
    Ok(vec![rows(&points)])
}

fn require_superuser(identity: &AuthenticatedIdentity, action: &str) -> Result<(), DdlError> {
    if identity.is_superuser {
        return Ok(());
    }
    Err(ddl_err(
        "42501",
        format!("permission denied: only superuser can {action}"),
    ))
}

fn point_err(e: RestorePointError) -> DdlError {
    match e {
        RestorePointError::Node(inner) => DdlError::internal(inner.to_string()),
    }
}

fn rows(points: &[StoredRestorePoint]) -> DdlResult {
    let columns = vec![
        "id".to_string(),
        "hlc".to_string(),
        "created_at_ms".to_string(),
    ];
    let column_types = vec![DdlColType::Int8, DdlColType::Int8, DdlColType::Int8];
    let rows = points
        .iter()
        .map(|point| {
            let mut row = Map::new();
            for (name, value) in [
                ("id", point.id),
                ("hlc", point.hlc),
                ("created_at_ms", point.created_at_ms),
            ] {
                row.insert(name.to_string(), JsonValue::String(value.to_string()));
            }
            row
        })
        .collect();
    DdlResult::Rows(ShapedRows::from_json_rows(columns, column_types, rows))
}
