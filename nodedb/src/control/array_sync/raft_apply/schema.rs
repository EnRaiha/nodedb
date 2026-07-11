// SPDX-License-Identifier: BUSL-1.1

//! Apply a committed `ArraySchema` entry on the local node.

use std::sync::Arc;

use tracing::warn;

use super::common::AppliedPosition;
use crate::control::distributed_applier::ProposeTracker;
use crate::control::state::SharedState;

/// Payload extracted from a `ReplicatedWrite::ArraySchema` entry.
pub(crate) struct ArraySchemaPayload<'a> {
    pub array: &'a str,
    pub snapshot_payload: &'a [u8],
    pub schema_hlc_bytes: [u8; 18],
}

/// Apply a committed `ArraySchema` entry on the local node.
///
/// 1. Imports the Loro snapshot into the local `OriginSchemaRegistry`.
/// 2. Decodes the `ArraySchema` and registers an `ArrayCatalogEntry` so the
///    Data Plane can open the array when a subsequent `ArrayOp` arrives.
///    This is the canonical DDL propagation path for followers: the Raft
///    `ArraySchema` entry is the single source of truth — no out-of-band
///    catalog registration is needed.
///
/// Returns `true` when the schema snapshot was durably imported, `false` when
/// the import failed. The caller uses this to gate Raft log compaction.
pub(crate) fn apply_array_schema(
    state: &Arc<SharedState>,
    tracker: &Arc<ProposeTracker>,
    pos: AppliedPosition,
    payload: ArraySchemaPayload<'_>,
) -> bool {
    let AppliedPosition {
        group_id,
        log_index,
        applied_key,
    } = pos;
    use nodedb_array::sync::hlc::Hlc;

    let ArraySchemaPayload {
        array,
        snapshot_payload,
        schema_hlc_bytes,
    } = payload;
    let remote_hlc = Hlc::from_bytes(&schema_hlc_bytes);

    // Use the replicated import path so every replica converges to the same
    // schema_hlc (the one committed in the Raft log entry) rather than each
    // bumping independently via their local HLC generator.
    if let Err(e) =
        state
            .array_sync_schemas
            .import_snapshot_replicated(array, snapshot_payload, remote_hlc)
    {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: import_snapshot_replicated failed"
        );
        tracker.complete(
            group_id,
            log_index,
            applied_key,
            Err(crate::Error::Internal {
                detail: format!("schema import: {e}"),
            }),
        );
        return false;
    }

    // Decode the ArraySchema from the just-imported Loro document and register
    // it in the array catalog so the Data Plane can open the array on this
    // node. Shared with the single-node direct-import path in `inbound.rs`
    // via `catalog_register::register_array_catalog_entry` so both codepaths
    // converge on the same catalog-visibility guarantee.
    //
    // Warn-and-continue: the schema snapshot import above already committed
    // durably and this apply loop has no fail-back path (the caller only
    // gates Raft log compaction on our `bool`, not correctness of catalog
    // state). A missing entry here is caught by the next `ensure_array_open`
    // lookup failure or by drift detection, not by re-running Raft apply.
    if let Err(e) =
        crate::control::array_sync::catalog_register::register_array_catalog_entry(state, array)
    {
        warn!(
            group_id, index = log_index, array = %array, error = %e,
            "apply_array_schema: register_array_catalog_entry failed (non-fatal)"
        );
    }

    tracker.complete(group_id, log_index, applied_key, Ok(vec![]));
    true
}
