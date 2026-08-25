// SPDX-License-Identifier: BUSL-1.1

//! Resolve a governed columnar predicate `UPDATE`/`DELETE` to the concrete
//! rows it would write, on the Data Plane, ahead of proposing them through
//! Raft.
//!
//! Dispatches `ColumnarOp::ResolveDml` — a read-only op that scans the
//! target collection, applies the statement's `WHERE` filters (and, for an
//! UPDATE, its assignments), and decides the write policy against every
//! matched row's exact image — over the normal Control -> Data SPSC path,
//! the same [`dispatch_local`] helper the clone materializer and the other
//! statement-time orchestrators already use for an internal read.
//!
//! The response is decoded with `zerompk` directly into native
//! `nodedb_types::Value`, never through `response_codec::decode_payload`'s
//! JSON intermediate: that round-trip is documented-lossy for `Bytes`,
//! `Uuid`, `Ulid`, `Regex`, `DateTime`, `NaiveDateTime`, `Duration`, `Range`,
//! and `Record` columns, and a write policy naming one of those would then be
//! decided against a value the collection does not hold.

use nodedb_types::{DatabaseId, RlsWriteCheck, TenantId, Value};

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::dispatch_local;
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::ColumnarOp;

/// The concrete row set a governed predicate `UPDATE`/`DELETE` resolved to.
pub(super) enum ResolvedDml {
    /// `(primary key, post-image)` for every row the write policy admitted.
    Update(Vec<(Value, Vec<Value>)>),
    /// Primary key of every row the write policy admitted for removal.
    Delete(Vec<Value>),
}

/// Bundled arguments for [`resolve_dml`].
pub(super) struct ResolveDmlArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub collection: &'a str,
    pub filters: &'a [u8],
    pub updates: &'a [(String, Vec<u8>)],
    pub is_update: bool,
    pub rls_write_check: &'a RlsWriteCheck,
}

/// Dispatch `ColumnarOp::ResolveDml` and decode its response natively.
///
/// A row the Data Plane's write-policy gate refuses surfaces here as
/// `crate::Error::DataPlane(ErrorCode::RejectedAuthz { .. })` — the exact
/// error a direct predicate `UPDATE`/`DELETE` against this collection
/// already returns, unchanged, because it goes through the same
/// `rls_write_gate::admit_columnar_row` call.
pub(super) async fn resolve_dml(
    state: &SharedState,
    args: ResolveDmlArgs<'_>,
) -> crate::Result<ResolvedDml> {
    let ResolveDmlArgs {
        tenant_id,
        database_id,
        collection,
        filters,
        updates,
        is_update,
        rls_write_check,
    } = args;
    let plan = PhysicalPlan::Columnar(ColumnarOp::ResolveDml {
        collection: collection.to_string(),
        filters: filters.to_vec(),
        updates: updates.to_vec(),
        is_update,
        rls_write_check: rls_write_check.clone(),
    });
    let resp = dispatch_local(state, tenant_id, database_id, collection, plan, None).await?;
    if resp.status != Status::Ok {
        return Err(match resp.error_code {
            Some(code) => crate::Error::DataPlane(*code),
            None => crate::Error::Dispatch {
                detail: format!(
                    "columnar predicate DML: resolve on '{collection}' returned status {:?} with \
                     no error code",
                    resp.status
                ),
            },
        });
    }

    if is_update {
        let rows: Vec<(Value, Vec<Value>)> =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "columnar predicate DML: could not decode resolved rows for '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedDml::Update(rows))
    } else {
        let pks: Vec<Value> =
            zerompk::from_msgpack(&resp.payload).map_err(|e| crate::Error::Codec {
                detail: format!(
                    "columnar predicate DML: could not decode resolved pks for '{collection}': {e}"
                ),
            })?;
        Ok(ResolvedDml::Delete(pks))
    }
}
