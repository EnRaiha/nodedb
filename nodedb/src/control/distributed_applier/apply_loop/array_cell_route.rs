// SPDX-License-Identifier: BUSL-1.1

//! Route a committed array cell write to its array incarnation before it
//! applies.
//!
//! The write names the key its proposer planned against. This replica can
//! have applied a MOVE TENANT or a DROP of that array since. The write then
//! applies under the key its incarnation moved to, or concludes as superseded
//! with a final refusal: the array it named no longer exists, so it has
//! nothing to mutate. It never fails for a key its catalog no longer holds.
//!
//! Both the SQL cell writes (`ArrayCellPut` / `ArrayCellDelete`) and the Lite
//! sync ops (`ArrayOp`) route here.

use nodedb_raft::message::LogEntry;

use crate::bridge::envelope::ErrorCode;
use crate::control::array_catalog::cell_route::{CellRoute, gate_incarnation, route};
use crate::control::array_sync::raft_apply::AppliedPosition;
use crate::control::wal_replication::ReplicatedEntry;
use crate::control::write_gate::{self, GateKey, SharedGate};
use crate::types::{DatabaseId, TenantId};

use super::context::{ApplyContext, FinishedApply};
use super::proposal_gate::EntryOutcome;
use super::start::Prepared;
use super::write_dispatch::{EntryScope, prepare_generic_entry};

/// The array a committed cell write names, and the incarnation its proposer
/// stamped.
pub(super) struct CellWrite {
    pub tenant_id: TenantId,
    pub array: String,
    pub incarnation: nodedb_types::Hlc,
}

/// Prepare a committed SQL array cell write: route it, then apply it as an
/// exclusive generic entry while the incarnation's gate stays shared.
pub(super) fn prepare_array_cell_entry<'a>(
    ctx: ApplyContext<'a>,
    pos: AppliedPosition,
    entry: LogEntry,
    scope: EntryScope,
    cell: CellWrite,
) -> Prepared<'a> {
    Prepared::Exclusive(Box::pin(async move {
        let (_gate, database_id) = match route_cell_write(ctx, pos, scope.database_id, &cell).await
        {
            Ok(routed) => routed,
            Err(finished) => return *finished,
        };
        let (entry, scope) = if database_id == scope.database_id {
            (entry, scope)
        } else {
            match rekeyed(entry, database_id) {
                Ok(entry) => (
                    entry,
                    EntryScope {
                        database_id,
                        ..scope
                    },
                ),
                Err(error) => {
                    ctx.tracker
                        .complete(pos.group_id, pos.log_index, pos.applied_key, Err(error));
                    return concluded(pos, false);
                }
            }
        };
        match prepare_generic_entry(ctx, pos, entry, scope, true) {
            Prepared::Exclusive(apply) => apply.await,
            Prepared::Concluded(outcome) => FinishedApply {
                group_id: pos.group_id,
                log_index: pos.log_index,
                outcome,
            },
            Prepared::Enqueue(_) | Prepared::Barrier => {
                ctx.tracker.complete(
                    pos.group_id,
                    pos.log_index,
                    pos.applied_key,
                    Err(crate::Error::Internal {
                        detail: "an array cell write prepared as a non-exclusive entry".into(),
                    }),
                );
                concluded(pos, false)
            }
        }
    }))
}

/// Route a committed array cell write. Returns the incarnation's gate, held
/// shared until the write is on its core, with the database it applies
/// under; or the concluded apply of a superseded write, boxed because a
/// superseded write is rare and a concluded apply is large.
pub(super) async fn route_cell_write(
    ctx: ApplyContext<'_>,
    pos: AppliedPosition,
    database_id: DatabaseId,
    cell: &CellWrite,
) -> Result<(SharedGate, DatabaseId), Box<FinishedApply>> {
    let read_mirror = || match ctx.state.array_catalog.read() {
        Ok(mirror) => mirror,
        Err(poisoned) => poisoned.into_inner(),
    };
    let incarnation = gate_incarnation(
        &read_mirror(),
        cell.tenant_id,
        database_id,
        &cell.array,
        cell.incarnation,
    );
    let Some(incarnation) = incarnation else {
        return Err(Box::new(superseded(ctx, pos)));
    };
    let gate = write_gate::shared(GateKey::Array(incarnation)).await;
    let decision = route(
        &read_mirror(),
        cell.tenant_id,
        database_id,
        &cell.array,
        incarnation,
    );
    match decision {
        CellRoute::Here => Ok((gate, database_id)),
        CellRoute::Moved(to) => Ok((gate, to)),
        CellRoute::Superseded => Err(Box::new(superseded(ctx, pos))),
    }
}

/// Conclude a write whose array incarnation no longer exists: a final
/// refusal, durable because nothing is left to apply.
fn superseded(ctx: ApplyContext<'_>, pos: AppliedPosition) -> FinishedApply {
    ctx.tracker.complete(
        pos.group_id,
        pos.log_index,
        pos.applied_key,
        Err(crate::Error::DataPlane(ErrorCode::NotFound)),
    );
    concluded(pos, true)
}

fn concluded(pos: AppliedPosition, durable: bool) -> FinishedApply {
    FinishedApply {
        group_id: pos.group_id,
        log_index: pos.log_index,
        outcome: EntryOutcome::Applied {
            durable,
            result: None,
        },
    }
}

/// `entry` with its write moved to `database_id`.
fn rekeyed(entry: LogEntry, database_id: DatabaseId) -> crate::Result<LogEntry> {
    let mut replicated =
        ReplicatedEntry::from_bytes(&entry.data).ok_or_else(|| crate::Error::Internal {
            detail: format!(
                "array cell entry {} does not decode for its reroute",
                entry.index
            ),
        })?;
    replicated.database_id = database_id.as_u64();
    Ok(LogEntry {
        data: replicated.encode()?,
        ..entry
    })
}
