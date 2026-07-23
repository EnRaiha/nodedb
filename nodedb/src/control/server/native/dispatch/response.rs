// SPDX-License-Identifier: BUSL-1.1

//! Native response conversion for direct physical operations.

use nodedb_types::protocol::{NativeResponse, ResponseStatus};

use crate::bridge::envelope::{PhysicalPlan, Response, Status};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::types::describe_plan;

use super::{DispatchCtx, shape_error_to_native, to_native_columns_rows};

pub(crate) fn data_plane_response_to_native(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    response: &Response,
) -> NativeResponse {
    if response.status == Status::Error {
        let message = if response.payload.is_empty() {
            response
                .error_code
                .as_ref()
                .map(|code| format!("{code:?}"))
                .unwrap_or_else(|| "unknown error".into())
        } else {
            String::from_utf8_lossy(&response.payload).into_owned()
        };
        return NativeResponse::error(seq, "XX000", message);
    }
    if response.payload.is_empty() {
        let mut native = NativeResponse::ok(seq);
        native.watermark_lsn = response.watermark_lsn.as_u64();
        return native;
    }
    match shape_response_materialized(
        &response.payload,
        plan,
        describe_plan(plan),
        None,
        ctx.state,
        ctx.database_id(),
        ctx.tenant_id(),
    ) {
        Ok(ShapeOutcome::Rows(shaped)) => {
            let (columns, rows) = to_native_columns_rows(&shaped);
            NativeResponse {
                seq,
                status: ResponseStatus::Ok,
                columns: Some(columns),
                rows: Some(rows),
                rows_affected: None,
                watermark_lsn: response.watermark_lsn.as_u64(),
                error: None,
                auth: None,
                warnings: shaped.notice.into_iter().collect(),
            }
        }
        Ok(ShapeOutcome::Passthrough) => {
            let mut native = NativeResponse::ok(seq);
            native.watermark_lsn = response.watermark_lsn.as_u64();
            native
        }
        Err(error) => shape_error_to_native(seq, &error),
    }
}
