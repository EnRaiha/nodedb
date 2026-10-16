// SPDX-License-Identifier: BUSL-1.1

//! Helpers for `array_cluster_exec`: agg-partial finalisation, error mappers,
//! and response-opcode → `VShardMessageType` lookup for the local-dispatch
//! fast path.

use nodedb_cluster::distributed_array::ArrayAggGroupKey;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::distributed_array::scatter::{ArrayCoordError, ShardFailure};
use nodedb_cluster::error::ClusterError;
use nodedb_cluster::wire::VShardMessageType;

use crate::Error;

/// Raw scalar value for aggregate row map entries.
///
/// Writes as an untagged msgpack scalar — same wire shape as `AggCell` in
/// `data::executor::dispatch::array::aggregate` — so that
/// `decode_payload_to_json` produces clean JSON numbers and nulls.
pub(super) enum AggValue {
    Float(f64),
    Int(i64),
    Str(String),
    Bool(bool),
    Null,
}

impl zerompk::ToMessagePack for AggValue {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        match self {
            AggValue::Float(f) => writer.write_f64(*f),
            AggValue::Int(i) => writer.write_i64(*i),
            AggValue::Str(s) => writer.write_string(s),
            AggValue::Bool(b) => writer.write_boolean(*b),
            AggValue::Null => writer.write_nil(),
        }
    }
}

/// The `group` cell of a grouped row: the dim value, typed as the single-node
/// path writes it. A scalar partial carries no group, so it writes NULL.
fn group_cell(key: &ArrayAggGroupKey) -> AggValue {
    match key {
        ArrayAggGroupKey::Int64(v) | ArrayAggGroupKey::TimestampMs(v) => AggValue::Int(*v),
        ArrayAggGroupKey::Float64(v) => AggValue::Float(*v),
        ArrayAggGroupKey::String(s) => AggValue::Str(s.clone()),
        ArrayAggGroupKey::Scalar => AggValue::Null,
    }
}

/// Finalize merged `ArrayAggPartial`s into the same msgpack-map structure
/// the local `ArrayOp::Aggregate` path produces via `encode_agg_rows`.
///
/// For scalar (no group-by), returns a single `[{"result": f64}]`.
/// For group-by, returns one `{"group": <dim value>, "result": f64}` per
/// partial, the dim value typed as its dim.
/// When `emit_horizon` is set (temporal queries only), a trailing
/// `{"truncated_before_horizon": bool}` summary row is appended. This mirrors
/// the single-node path (`dispatch::array::aggregate`) exactly — including the
/// rule that the below-horizon signal is surfaced only for temporal reads — so
/// `decode_payload_to_json` produces byte-identical JSON on both topologies.
pub(super) fn finalize_agg_partials(
    partials: &[ArrayAggPartial],
    reducer: &nodedb_physical::physical_plan::ArrayReducer,
    group_by_dim: i32,
    truncated_before_horizon: bool,
    emit_horizon: bool,
) -> Vec<std::collections::BTreeMap<String, AggValue>> {
    use nodedb_physical::physical_plan::ArrayReducer;

    let finalize = |p: &ArrayAggPartial| -> AggValue {
        if p.count == 0 {
            return AggValue::Null;
        }
        let v = match reducer {
            ArrayReducer::Sum => p.sum,
            ArrayReducer::Count => p.count as f64,
            ArrayReducer::Min => p.min,
            ArrayReducer::Max => p.max,
            ArrayReducer::Mean => p.welford_mean,
        };
        AggValue::Float(v)
    };

    let is_grouped = group_by_dim >= 0;

    let mut rows: Vec<std::collections::BTreeMap<String, AggValue>> = partials
        .iter()
        .map(|p| {
            let mut row = std::collections::BTreeMap::new();
            if is_grouped {
                row.insert("group".to_string(), group_cell(&p.group_key));
            }
            row.insert("result".to_string(), finalize(p));
            row
        })
        .collect();

    // Trailing summary row carrying the below-horizon signal — only for
    // temporal queries, matching the single-node path so both topologies emit
    // the same row shape (non-temporal aggregates carry no summary row).
    if emit_horizon {
        let mut summary = std::collections::BTreeMap::new();
        summary.insert(
            "truncated_before_horizon".to_string(),
            AggValue::Bool(truncated_before_horizon),
        );
        rows.push(summary);
    }

    rows
}

/// One msgpack array holding the already-encoded `rows`, the shape a local
/// array read answers with.
pub(super) fn encode_row_array(rows: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + rows.iter().map(Vec::len).sum::<usize>());
    let n = rows.len();
    if n <= 15 {
        out.push(0x90 | (n as u8));
    } else if n <= u16::MAX as usize {
        out.push(0xdc);
        out.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        out.push(0xdd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for row in rows {
        out.extend_from_slice(row);
    }
    out
}

/// Map an array coordinator error to an [`Error`].
///
/// A request that never reached its shard ran nothing. It answers the
/// definite, retryable `NoLeader` for that vShard. `NoLeader` never moves
/// the gateway's routing hint. Every other error maps as [`cluster_err`].
pub(super) fn coord_err(e: ArrayCoordError) -> Error {
    match e {
        ArrayCoordError::Shard(ShardFailure { vshard_id, error }) if error.is_unsent() => {
            tracing::debug!(vshard_id, %error, "array shard request never reached its shard");
            Error::NoLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
            }
        }
        ArrayCoordError::Shard(ShardFailure { error, .. })
        | ArrayCoordError::Coordinator(error) => cluster_err(error),
    }
}

pub(super) fn cluster_err(e: ClusterError) -> Error {
    match e {
        // A shard did not answer within its timeout: surface as a deterministic
        // deadline rather than an opaque internal error, matching the
        // `TypedClusterError::DeadlineExceeded` mapping used elsewhere. A
        // sent request with no answer can have run, so it takes the same class.
        lost if lost.is_lost_answer() => Error::DeadlineExceeded {
            request_id: crate::types::RequestId::new(0),
        },
        // A shard's Data-Plane verdict keeps its code, so the statement
        // renders the SQLSTATE a single-node execution renders.
        ClusterError::DataPlane { code } => Error::DataPlane(code.into()),
        // A shard's typed execution error is rebuilt, so the statement
        // renders the SQLSTATE a single-node execution renders.
        ClusterError::ShardExecution { error, .. } | ClusterError::StreamTerminal { error, .. } => {
            Error::from(*error)
        }
        // The shard still refused after the fan-out's reroute retry. The
        // vShard's owner is moving, so the client retries the statement.
        ClusterError::WrongOwner {
            vshard_id,
            expected_owner_node,
        } => match expected_owner_node {
            // `WrongOwner` names the owner without a term.
            Some(leader_node) => Error::NotLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
                leader_node,
                leader_addr: String::new(),
                leader_term: 0,
            },
            None => Error::NoLeader {
                vshard_id: crate::types::VShardId::new(vshard_id),
            },
        },
        // The vShard is moving to another node. It has no serving owner
        // until the cut-over, so the client retries the statement.
        ClusterError::MigrationInProgress { vshard_id } => Error::NoLeader {
            vshard_id: crate::types::VShardId::new(vshard_id),
        },
        // Cluster machinery faults. The client can act on none of them.
        other @ (ClusterError::Raft(_)
        | ClusterError::VShardNotMapped { .. }
        | ClusterError::GroupNotFound { .. }
        | ClusterError::LearnerNotCaughtUp { .. }
        | ClusterError::MigrationPauseBudgetExceeded { .. }
        | ClusterError::NodeUnreachable { .. }
        | ClusterError::GhostNotFound { .. }
        | ClusterError::Transport { .. }
        | ClusterError::Storage { .. }
        | ClusterError::Codec { .. }
        | ClusterError::UnsupportedWireVersion { .. }
        | ClusterError::CircuitOpen { .. }
        | ClusterError::JoinGroupDisappeared { .. }
        | ClusterError::JoinCommitTimeout { .. }
        | ClusterError::ReadIndexNotLeader { .. }
        | ClusterError::ReadIndexTimeout { .. }
        | ClusterError::Config { .. }
        | ClusterError::MigrationCheckpoint(_)
        | ClusterError::MigrationRecovery(_)
        | ClusterError::Calvin(_)
        | ClusterError::SnapshotCrcMismatch { .. }
        | ClusterError::SnapshotOffsetRegression { .. }
        | ClusterError::PartialSnapshotCorrupt { .. }
        | ClusterError::PartialSnapshotCleanupFailed { .. }
        | ClusterError::SnapshotApplyFailed { .. }
        | ClusterError::Mirror(_)
        | ClusterError::BspBarrier(_)
        | ClusterError::VectorGather(_)
        | ClusterError::SpatialGather(_)
        | ClusterError::Bm25Gather(_)
        | ClusterError::TsGather(_)
        // The guards above take these. They stay named for exhaustiveness.
        | ClusterError::ShardTimeout { .. }
        | ClusterError::Unanswered { .. }
        | ClusterError::ShufflePush(_)
        | ClusterError::RemoteUntyped { .. }) => Error::Internal {
            detail: format!("array cluster: {other}"),
        },
    }
}

pub(super) fn encode_err(e: zerompk::Error) -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: format!("array cluster encode: {e}"),
    }
}

/// Map a numeric response opcode (81, 83, 85, 87, 89, 91, 93) to its
/// `VShardMessageType`.
///
/// Used by the local-dispatch fast path to build the response envelope without
/// going through the QUIC transport layer.
pub(super) fn array_resp_msg_type(opcode: u32) -> Option<VShardMessageType> {
    match opcode {
        81 => Some(VShardMessageType::ArrayShardSliceResp),
        83 => Some(VShardMessageType::ArrayShardAggResp),
        85 => Some(VShardMessageType::ArrayShardPutResp),
        87 => Some(VShardMessageType::ArrayShardDeleteResp),
        89 => Some(VShardMessageType::ArrayShardSurrogateBitmapResp),
        91 => Some(VShardMessageType::ArrayShardElementwiseResp),
        93 => Some(VShardMessageType::ArrayShardMaintResp),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::ErrorCode;

    /// A grouped row writes its dim value typed as the dim: a Float64 dim
    /// writes the float and a String dim the string, never a bit pattern or
    /// a hash.
    #[test]
    fn a_grouped_row_writes_the_typed_dim_value() {
        use nodedb_physical::physical_plan::ArrayReducer;
        let partials = vec![
            ArrayAggPartial::from_single(ArrayAggGroupKey::Float64(101.5), 2.0),
            ArrayAggPartial::from_single(ArrayAggGroupKey::String("chrX".into()), 3.0),
            ArrayAggPartial::from_single(ArrayAggGroupKey::Int64(7), 4.0),
        ];
        let rows = finalize_agg_partials(&partials, &ArrayReducer::Sum, 0, false, false);
        let bytes = zerompk::to_msgpack_vec(&rows).expect("encode rows");
        let json = nodedb_types::msgpack_to_json_string(&bytes).expect("rows to json");
        let rows: serde_json::Value = serde_json::from_str(&json).expect("parse json");
        assert_eq!(rows[0]["group"].as_f64(), Some(101.5));
        assert_eq!(rows[1]["group"].as_str(), Some("chrX"));
        assert_eq!(rows[2]["group"].as_i64(), Some(7));
        assert_eq!(rows[2]["result"].as_f64(), Some(4.0));
    }

    /// A shard request that never went out answers `NoLeader` for that
    /// shard: nothing ran, so the refusal is definite.
    #[test]
    fn an_unsent_shard_request_is_no_leader_for_its_vshard() {
        let unsent = ArrayCoordError::Shard(ShardFailure {
            vshard_id: 41,
            error: ClusterError::Transport {
                detail: "connect refused".into(),
            },
        });
        match coord_err(unsent) {
            Error::NoLeader { vshard_id } => assert_eq!(vshard_id.as_u32(), 41),
            other => panic!("expected NoLeader for vshard 41, got {other:?}"),
        }
    }

    /// Every other shard failure maps as `cluster_err` does.
    #[test]
    fn a_lost_shard_answer_stays_an_unknown_outcome() {
        let lost = ArrayCoordError::Shard(ShardFailure {
            vshard_id: 41,
            error: ClusterError::Unanswered {
                node_id: 2,
                detail: "stream reset".into(),
            },
        });
        assert!(matches!(coord_err(lost), Error::DeadlineExceeded { .. }));
    }

    /// A coordinator-side transport error names no shard, so it keeps its
    /// `cluster_err` mapping.
    #[test]
    fn a_coordinator_error_maps_as_cluster_err() {
        let local = ArrayCoordError::Coordinator(ClusterError::Codec {
            detail: "encode".into(),
        });
        assert!(matches!(coord_err(local), Error::Internal { .. }));
    }

    /// A shard verdict that crossed the cluster keeps its code at the
    /// coordinator, never `Internal`.
    #[test]
    fn a_shard_verdict_keeps_its_code() {
        let code = ErrorCode::Unsupported {
            detail: "not on this engine".into(),
        };
        let wire = nodedb_cluster::error::ClusterError::DataPlane {
            code: code.clone().into(),
        };
        match cluster_err(wire) {
            Error::DataPlane(rebuilt) => assert_eq!(rebuilt, code),
            other => panic!("expected the typed verdict, got {other:?}"),
        }
    }

    /// A shard that still refused after the reroute retry answers the
    /// retryable leader class, never `Internal`.
    #[test]
    fn a_persistent_wrong_owner_is_a_leader_error() {
        let known = nodedb_cluster::error::ClusterError::WrongOwner {
            vshard_id: 7,
            expected_owner_node: Some(3),
        };
        assert!(matches!(
            cluster_err(known),
            Error::NotLeader { leader_node: 3, .. }
        ));
        let unknown = nodedb_cluster::error::ClusterError::WrongOwner {
            vshard_id: 7,
            expected_owner_node: None,
        };
        assert!(matches!(cluster_err(unknown), Error::NoLeader { .. }));
    }

    /// A vShard mid-migration has no serving owner, so the statement answers
    /// the retryable no-leader class, never `Internal`.
    #[test]
    fn a_migrating_vshard_is_a_no_leader_error() {
        let wire = ClusterError::MigrationInProgress { vshard_id: 5 };
        match cluster_err(wire) {
            Error::NoLeader { vshard_id } => assert_eq!(vshard_id.as_u32(), 5),
            other => panic!("expected NoLeader, got {other:?}"),
        }
    }

    /// A typed terminal error is rebuilt, never flattened to `Internal`.
    #[test]
    fn a_typed_terminal_error_is_rebuilt() {
        let typed = nodedb_cluster::rpc_codec::TypedClusterError::DataPlane {
            code: ErrorCode::DivisionByZero.into(),
        };
        let wire = ClusterError::StreamTerminal {
            error: Box::new(typed),
            detail: "division by zero".into(),
        };
        match cluster_err(wire) {
            Error::DataPlane(code) => assert_eq!(code, ErrorCode::DivisionByZero),
            other => panic!("expected the typed verdict, got {other:?}"),
        }
    }
}
