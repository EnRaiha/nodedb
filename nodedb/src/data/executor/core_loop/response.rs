// SPDX-License-Identifier: BUSL-1.1

use nodedb_crdt::constraint::ConstraintSet;

use crate::bridge::envelope::{ErrorCode, Payload, Response, Status};
use crate::engine::crdt::tenant_state::TenantCrdtEngine;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

use super::super::task::ExecutionTask;
use super::CoreLoop;

impl CoreLoop {
    /// Hand a finished response back to the Control Plane, or report its loss.
    ///
    /// The response ring is bounded, so a Control Plane that has stopped
    /// draining it can refuse the push. Discarding the response quietly is what
    /// makes that unrecoverable: the caller blocks until its deadline and then
    /// reports a timeout, and when `write` says the batch had already committed
    /// that timeout names a write which IS durable. A client that retries on
    /// timeout then double-applies it; one that compensates erases a committed
    /// row. Neither can tell, so the drop is recorded where it is detected —
    /// this is the only place that still knows what happened to the write.
    pub(in crate::data::executor) fn send_response(
        &mut self,
        response: Response,
        write: crate::diag::LostResponseWrite,
    ) {
        if let Err(e) = self
            .response_tx
            .try_push(crate::bridge::dispatch::BridgeResponse { inner: response })
        {
            tracing::error!(
                core = self.core_id,
                error = %e,
                write = ?write,
                "failed to send response — caller can only learn a deadline"
            );
            crate::diag::data_plane_response_lost(self.core_id, write);
        }
    }

    pub(in crate::data::executor) fn response_ok(&self, task: &ExecutionTask) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    pub(in crate::data::executor) fn response_with_payload(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Build the response for a write that reports an affected-row count.
    ///
    /// Every handler whose plan renders a DML command tag (`INSERT n` /
    /// `UPDATE n` / `DELETE n`) MUST return through here, because the count is
    /// only knowable at the mutation: a point delete against an absent row and
    /// a point delete that removed a row are the same plan, and the primary key
    /// resolves to a surrogate either way (surrogate identity survives a delete
    /// so a later re-insert keeps it). A handler that returns [`response_ok`]
    /// instead leaves the Control Plane with no count to render, and a renderer
    /// that substitutes a default there reports a row that was never touched.
    ///
    /// [`response_ok`]: Self::response_ok
    pub(in crate::data::executor) fn response_affected(
        &self,
        task: &ExecutionTask,
        affected: u64,
    ) -> Response {
        let mut payload = Vec::with_capacity(16);
        nodedb_query::msgpack_scan::write_map_header(&mut payload, 1);
        nodedb_query::msgpack_scan::write_kv_i64(&mut payload, "affected", affected as i64);
        self.response_with_payload(task, payload)
    }

    pub(in crate::data::executor) fn response_partial(
        &self,
        task: &ExecutionTask,
        payload: Vec<u8>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Partial,
            attempt: 1,
            partial: true,
            payload: Payload::from_vec(payload),
            watermark_lsn: self.watermark,
            read_version_lsn: self.read_version_lsn(task),
            error_code: None,
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Per-collection read-version LSN for `task`'s plan: the scanned
    /// collection's `coll_write_lsn` at read time — a WAL LSN, the single domain
    /// the version index is fed in — and the sound comparand for cross-shard OCC
    /// read validation. `Lsn::ZERO` when the plan maps to no single collection or
    /// the collection has no recorded write on this core. Distinct from the
    /// core-global `watermark`.
    ///
    /// On a WRITE response this is the POST-write version: every write handler
    /// records its LSN into the index before building its response, so the value
    /// read back here already includes the write. That is what lets the apply
    /// path hand a committed write's own version back to its proposer.
    pub(in crate::data::executor) fn read_version_lsn(
        &self,
        task: &ExecutionTask,
    ) -> crate::types::Lsn {
        task.plan()
            .collection()
            .map(|c| {
                self.write_index
                    .collection_write_lsn(&super::write_index::CollKey {
                        db: task.request.database_id,
                        tenant: task.request.tenant_id,
                        collection: Box::from(c),
                    })
                    .unwrap_or(crate::types::Lsn::ZERO)
            })
            .unwrap_or(crate::types::Lsn::ZERO)
    }

    pub(in crate::data::executor) fn response_error(
        &self,
        task: &ExecutionTask,
        error_code: impl Into<ErrorCode>,
    ) -> Response {
        Response {
            request_id: task.request_id(),
            status: Status::Error,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: self.watermark,
            read_version_lsn: crate::types::Lsn::ZERO,
            error_code: Some(Box::new(error_code.into())),
            read_set_valid: None,
            write_set: Vec::new(),
        }
    }

    /// Build the map key for the four vector in-memory maps
    /// (`vector_collections`, `vector_params`, `index_configs`, `ivf_indexes`).
    ///
    /// Returns `(DatabaseId, TenantId, collection_key)` where `collection_key` is:
    /// - `collection` when `field_name` is empty, or
    /// - `"{collection}:{field_name}"` when a named field is specified.
    ///
    /// This replaces the old `format!("{tid}:{collection}")` string key with a
    /// structured tuple so database + tenant scoping is structural rather than
    /// lexical.
    pub(in crate::data::executor) fn vector_index_key(
        database_id: u64,
        tenant_id: u64,
        collection: &str,
        field_name: &str,
    ) -> (DatabaseId, TenantId, String) {
        let coll_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };
        (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            coll_key,
        )
    }

    /// Checkpoint filename for a vector collection key.
    ///
    /// Produces a `"{db}:{tid}:{coll}"` string. The `coll` component may itself
    /// contain `:` (it is `collection` or `collection:field`) — that is fine
    /// because parsing uses `splitn(3, ':')` and treats the remainder verbatim.
    pub(in crate::data::executor) fn vector_checkpoint_filename(
        key: &(DatabaseId, TenantId, String),
    ) -> String {
        format!("{}:{}:{}", key.0.as_u64(), key.1.as_u64(), key.2)
    }

    pub(in crate::data::executor) fn get_crdt_engine(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
    ) -> crate::Result<&mut TenantCrdtEngine> {
        let key = (database_id, tenant_id);
        if !self.crdt_engines.contains_key(&key) {
            tracing::debug!(
                core = self.core_id,
                %database_id,
                %tenant_id,
                "creating CRDT engine for database tenant"
            );
            let engine =
                TenantCrdtEngine::new(tenant_id, self.core_id as u64, ConstraintSet::new())?;
            self.crdt_engines.insert(key, engine);
        }
        Ok(self.crdt_engines.get_mut(&key).expect("just inserted"))
    }

    /// Release the per-collection validation candidates every CRDT engine on
    /// this core is holding.
    ///
    /// Called when a run of delta applies ends. The candidates make a run cost
    /// one collection copy instead of one per delta; keeping them past the run
    /// would just be a second document per collection sitting idle.
    pub(in crate::data::executor) fn release_crdt_apply_candidates(&mut self) {
        for engine in self.crdt_engines.values_mut() {
            engine.clear_apply_candidates();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::tests::{make_core_with_dir, make_default_task};
    use crate::diag::{LostResponseWrite, data_plane_responses_lost};

    /// The happy path this helper must not regress: a response the ring accepts
    /// reaches the Control Plane unchanged and records nothing.
    #[test]
    fn a_deliverable_response_is_handed_over_and_reports_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req_tx, mut resp_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let response = core.response_ok(&task);
        let before = data_plane_responses_lost();

        core.send_response(response, LostResponseWrite::Committed);

        let delivered = resp_rx.try_pop().expect("response delivered");
        assert_eq!(delivered.inner.request_id, task.request_id());
        assert_eq!(data_plane_responses_lost(), before);
    }

    /// The defect: a full response ring used to swallow the response with
    /// `let _ =`, so a committed batch write became a client-side deadline with
    /// no trace anywhere that the outcome was ambiguous. The drop is still
    /// unavoidable — the ring is bounded — but it must never be silent.
    #[test]
    fn a_refused_response_is_counted_rather_than_swallowed() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Never drained, so the ring saturates and then refuses.
        let (mut core, _req_tx, _resp_rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        // Comfortably past the ring's capacity, so the last push below is
        // guaranteed to be refused regardless of the configured depth.
        for _ in 0..256 {
            let filler = core.response_ok(&task);
            core.send_response(filler, LostResponseWrite::Committed);
        }
        let before = data_plane_responses_lost();

        let overflow = core.response_ok(&task);
        core.send_response(overflow, LostResponseWrite::Committed);

        assert_eq!(
            data_plane_responses_lost(),
            before + 1,
            "a response the ring refused must be reported, not discarded"
        );
    }
}
