// SPDX-License-Identifier: BUSL-1.1

//! Relational rows of a coordinator-gathered array read.
//!
//! An array read emits rows that are already relational: a slice or project
//! cell holds one field per dim and attr, keyed by name, an aggregate row is
//! `{group, result}`, an elementwise row is one cell. The storage flatten
//! that unwraps `{id, data}` document rows must not run over them: it would
//! read an attr named `data` as a row envelope.
//!
//! A local `ArrayOp::Slice` answers with one `ArraySliceResponse` envelope
//! per core it ran on, not with a bare row array, so its rows are read out
//! of the envelopes. A cluster slice reaches the coordinator as the bare
//! rows its array coordinator merged (`cluster_leaf`).

use nodedb_physical::physical_plan::{ArrayOp, ClusterArrayOp, PhysicalPlan};

use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::server::response_shape::compose::array_slice::TRUNCATED_BEFORE_HORIZON_NOTICE;
use crate::control::server::shared::session::statement_notice;
use crate::data::executor::response_codec::ArraySliceResponse;

/// How a gathered array read's payload holds its rows.
#[derive(Clone, Copy)]
pub(super) enum ArrayReadShape {
    /// One `ArraySliceResponse` envelope per core.
    SliceEnvelopes,
    /// A bare array of relational rows.
    Rows,
}

impl ArrayReadShape {
    /// The shape `plan`'s gathered payload takes, or `None` when `plan` is
    /// not an array read.
    pub(super) fn of(plan: &PhysicalPlan) -> Option<Self> {
        if matches!(plan, PhysicalPlan::Array(ArrayOp::Slice { .. })) {
            return Some(Self::SliceEnvelopes);
        }
        let bare_rows = matches!(
            plan,
            PhysicalPlan::Array(
                ArrayOp::Project { .. } | ArrayOp::Aggregate { .. } | ArrayOp::Elementwise { .. },
            ) | PhysicalPlan::ClusterArray(
                ClusterArrayOp::Slice { .. } | ClusterArrayOp::Agg { .. }
            )
        );
        bare_rows.then_some(Self::Rows)
    }

    /// The relational rows of `merged`, the gathered payload, as one msgpack
    /// array.
    pub(super) fn rows(self, merged: Vec<u8>) -> crate::Result<Vec<u8>> {
        match self {
            Self::SliceEnvelopes => local_slice_rows(&merged),
            Self::Rows => Ok(merged),
        }
    }
}

/// The rows of every slice envelope in `merged`, as one msgpack array.
///
/// `merged` holds one element per envelope. An envelope cut below its
/// retention horizon raises the same client notice a slice shaped on its
/// own reports, through the statement notice slot. An element that is not
/// an envelope is an error: it is not slice rows, and reading it as rows
/// would answer the query from the wrong data.
fn local_slice_rows(merged: &[u8]) -> crate::Result<Vec<u8>> {
    let mut rows = Vec::new();
    let mut truncated = false;
    for envelope in extract_msgpack_elements(merged) {
        let response: ArraySliceResponse =
            zerompk::from_msgpack(&envelope).map_err(|e| crate::Error::Codec {
                detail: format!("array slice response decode: {e}"),
            })?;
        truncated |= response.truncated_before_horizon;
        rows.extend(extract_msgpack_elements(&response.rows_msgpack));
    }
    if truncated {
        statement_notice::raise(TRUNCATED_BEFORE_HORIZON_NOTICE.to_string());
    }
    Ok(encode_msgpack_array(&rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One flat row, the plain msgpack map `{"k": value}`.
    fn row(value: u8) -> Vec<u8> {
        assert!(value < 0x80, "a positive fixint holds 0..=127");
        vec![0x81, 0xa1, b'k', value]
    }

    fn envelope(rows: &[Vec<u8>], truncated: bool) -> Vec<u8> {
        zerompk::to_msgpack_vec(&ArraySliceResponse {
            rows_msgpack: encode_msgpack_array(rows),
            truncated_before_horizon: truncated,
        })
        .expect("encode slice envelope")
    }

    #[test]
    fn rows_of_every_envelope_concatenate() {
        let merged = encode_msgpack_array(&[
            envelope(&[row(1), row(2)], false),
            envelope(&[row(3)], false),
        ]);
        let rows = ArrayReadShape::SliceEnvelopes
            .rows(merged)
            .expect("slice rows");
        assert_eq!(
            extract_msgpack_elements(&rows),
            vec![row(1), row(2), row(3)]
        );
    }

    #[test]
    fn a_bare_row_is_not_an_envelope() {
        let merged = encode_msgpack_array(&[row(1)]);
        assert!(ArrayReadShape::SliceEnvelopes.rows(merged).is_err());
    }

    #[test]
    fn bare_rows_pass_through() {
        let merged = encode_msgpack_array(&[row(1), row(2)]);
        let rows = ArrayReadShape::Rows.rows(merged.clone()).expect("rows");
        assert_eq!(rows, merged);
    }

    #[tokio::test]
    async fn a_truncated_envelope_raises_the_horizon_notice() {
        use crate::control::server::shared::session::conn_scope;
        conn_scope::scoped(async {
            let merged = encode_msgpack_array(&[envelope(&[row(1)], true)]);
            ArrayReadShape::SliceEnvelopes
                .rows(merged)
                .expect("slice rows");
            assert_eq!(
                statement_notice::take(),
                vec![TRUNCATED_BEFORE_HORIZON_NOTICE.to_string()]
            );
        })
        .await;
    }
}
