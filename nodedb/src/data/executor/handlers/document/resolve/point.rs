// SPDX-License-Identifier: BUSL-1.1

//! Resolvers for the keyed document writes: `PointUpdate`, `PointDelete`.
//!
//! Each reads the row through the same current-state view its live handler
//! uses, computes the post-image with the same
//! [`CoreLoop::compute_point_update_body`] the live handler calls, and decides
//! the write policy with the same gate — then reports the mutation instead of
//! applying it. Reusing those functions is what stops the resolve and the apply
//! diverging on which row image the policy admitted.

use nodedb_physical::physical_plan::{
    DocumentResolveOutcome, ResolvedSumTarget, ReturningSpec, UpdateValue,
};
use nodedb_types::{RlsWriteCheck, Surrogate};

use super::context::{
    ResolveResult, ResolvedPut, affected_payload, delete_mutation, put_mutation,
    resolved_response_payload, row_key_of,
};
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::point::update::post_image::{
    PointUpdateImage, point_update_body_to_msgpack,
};
use crate::data::executor::handlers::rls_write_gate;
use crate::data::executor::task::ExecutionTask;

/// Borrowed arguments for [`CoreLoop::resolve_point_update`].
pub(super) struct ResolvePointUpdate<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub updates: &'a [(String, UpdateValue)],
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

/// Borrowed arguments for [`CoreLoop::resolve_point_delete`].
pub(super) struct ResolvePointDelete<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub document_id: &'a str,
    pub surrogate: Surrogate,
    pub returning: Option<&'a ReturningSpec>,
    pub rls_filters: &'a [u8],
    pub rls_write_check: &'a RlsWriteCheck,
    pub resolved_sum_targets: &'a [ResolvedSumTarget],
}

impl CoreLoop {
    /// Resolve a `PointUpdate` to the one row write it would apply.
    ///
    /// A row that is already gone resolves to no mutation and the
    /// `{"affected": 0}` reply the live handler returns for the same input.
    pub(super) fn resolve_point_update(
        &self,
        task: &ExecutionTask,
        args: ResolvePointUpdate<'_>,
    ) -> ResolveResult {
        let ResolvePointUpdate {
            tid,
            collection,
            document_id,
            surrogate,
            updates,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let row_key = row_key_of(surrogate);
        let row_key = row_key.as_str();

        let config_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        // The same two refusals `execute_point_update` makes before it reads
        // anything, in the same order. Raised HERE so a refused statement never
        // reaches Raft: a refusal discovered at apply time is discovered after
        // commit, on every replica.
        if let Some(config) = self.doc_configs.get(&config_key) {
            crate::data::executor::handlers::generated::check_generated_readonly(
                updates,
                &config.enforcement.generated_columns,
            )?;
            crate::data::executor::enforcement::append_only::check_point_update(
                collection,
                &config.enforcement,
            )?;
        }

        // A row that is gone reports `{"affected": 0}` even under a `RETURNING`
        // clause — verbatim what `execute_point_update` answers for the same
        // input.
        let Some(current_bytes) = self.doc_resolve_read(&ctx, collection, row_key)? else {
            return Ok(DocumentResolveOutcome {
                mutations: Vec::new(),
                response_payload: affected_payload(0),
            });
        };

        let is_strict = ctx.strict_schema.is_some();
        let has_expr = updates
            .iter()
            .any(|(_, v)| matches!(v, UpdateValue::Expr(_)));
        let has_generated = self.doc_configs.get(&config_key).is_some_and(|c| {
            !c.enforcement.generated_columns.is_empty()
                && crate::data::executor::handlers::generated::needs_recomputation(
                    updates,
                    &c.enforcement.generated_columns,
                )
        });
        // Stamped as the live handler stamps it. The stored image is what the
        // policy decides and `RETURNING` projects; the apply mints its own
        // version stamp when it writes.
        let sys_from_ms = if ctx.bitemporal {
            self.bitemporal_now_ms()
        } else {
            0
        };
        let image_params = PointUpdateImage {
            config_key: &config_key,
            current_bytes: &current_bytes,
            updates,
            is_strict,
            has_generated,
            has_expr,
            bitemporal: ctx.bitemporal,
            sys_from_ms,
        };
        let body = self.compute_point_update_body(image_params)?;
        // The STORED image the policy decides against and `RETURNING` projects,
        // and the pre-encode body the apply writes — both from ONE computation.
        let stored_image = self.encode_point_update_body(image_params, &body)?;
        let value = point_update_body_to_msgpack(&body);

        rls_write_gate::admit_stored_row(
            rls_write_check,
            &stored_image,
            document_id,
            ctx.strict_schema.as_ref(),
            tid,
            collection,
        )
        .map_err(ErrorCode::from)?;

        let response_payload = resolved_response_payload(
            returning,
            rls_filters,
            ctx.strict_schema.as_ref(),
            &[(document_id, stored_image.as_slice())],
        )?;
        Ok(DocumentResolveOutcome {
            mutations: vec![put_mutation(ResolvedPut {
                collection,
                document_id,
                surrogate,
                value,
                precondition: Some(current_bytes),
                resolved_sum_targets,
            })],
            response_payload,
        })
    }

    /// Resolve a `PointDelete` to the one row removal it would apply.
    ///
    /// The pre-image is the only image a delete has, so it is both what the
    /// policy decides and what `RETURNING` projects — the same pairing the live
    /// handler makes.
    pub(super) fn resolve_point_delete(
        &self,
        task: &ExecutionTask,
        args: ResolvePointDelete<'_>,
    ) -> ResolveResult {
        let ResolvePointDelete {
            tid,
            collection,
            document_id,
            surrogate,
            returning,
            rls_filters,
            rls_write_check,
            resolved_sum_targets,
        } = args;
        let ctx = self.doc_resolve_ctx(task, tid, collection);
        let row_key = row_key_of(surrogate);
        let row_key = row_key.as_str();

        // A row that is already absent removes nothing, so there is no image for
        // the policy to restrict — the same admission `gate_point_delete` makes.
        let Some(prior) = self.doc_resolve_read(&ctx, collection, row_key)? else {
            return Ok(DocumentResolveOutcome {
                mutations: Vec::new(),
                response_payload: resolved_response_payload(
                    returning,
                    rls_filters,
                    ctx.strict_schema.as_ref(),
                    &[],
                )?,
            });
        };

        rls_write_gate::admit_stored_row(
            rls_write_check,
            &prior,
            row_key,
            ctx.strict_schema.as_ref(),
            tid,
            collection,
        )
        .map_err(ErrorCode::from)?;

        let response_payload = resolved_response_payload(
            returning,
            rls_filters,
            ctx.strict_schema.as_ref(),
            &[(document_id, prior.as_slice())],
        )?;
        Ok(DocumentResolveOutcome {
            mutations: vec![delete_mutation(
                collection,
                document_id,
                surrogate,
                Some(prior),
                resolved_sum_targets,
            )],
            response_payload,
        })
    }
}
