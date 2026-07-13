// SPDX-License-Identifier: BUSL-1.1

//! Assign a fresh, catalog-registered surrogate for a row written into a
//! target collection on behalf of another operation.

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use super::pk::{TargetPk, extract_pk_value};
use crate::control::state::SharedState;

/// Assign a fresh, registered surrogate for one written row on the TARGET's
/// primary key.
pub(crate) fn assign_target_surrogate(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    target_collection: &str,
    target_pk: &TargetPk,
    body: &[u8],
) -> crate::Result<Surrogate> {
    match target_pk {
        TargetPk::AutoRowId => {
            state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection)
        }
        TargetPk::Field(field) => match extract_pk_value(body, field) {
            Some(pk) if !pk.is_empty() => state.surrogate_assigner.assign(
                database_id,
                tenant_id,
                target_collection,
                pk.as_bytes(),
            ),
            // No usable key value: mint a fresh unique surrogate rather than
            // collapsing every keyless inserted row onto one binding.
            _ => state
                .surrogate_assigner
                .assign_fresh(database_id, tenant_id, target_collection),
        },
    }
}
