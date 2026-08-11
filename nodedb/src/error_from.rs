// SPDX-License-Identifier: BUSL-1.1

//! Internal and public error conversions.

use nodedb_types::error::NodeDbError;

use crate::types::TenantId;

use super::Error;

impl From<nodedb_query::expr_parse::ExprParseError> for Error {
    fn from(e: nodedb_query::expr_parse::ExprParseError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

/// `EvalError` has exactly one variant today (`DivisionByZero`); the
/// `match` is exhaustive rather than a `_ =>` fallback so
/// a future evaluator error is forced to pick its own `crate::Error`
/// mapping instead of silently inheriting this one.
impl From<nodedb_query::EvalError> for Error {
    fn from(e: nodedb_query::EvalError) -> Self {
        match e {
            nodedb_query::EvalError::DivisionByZero => Self::DivisionByZero,
        }
    }
}

impl From<crate::engine::timeseries::ilp::IlpError> for Error {
    fn from(e: crate::engine::timeseries::ilp::IlpError) -> Self {
        Self::BadRequest {
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::columnar_segment::SegmentError> for Error {
    fn from(e: crate::engine::timeseries::columnar_segment::SegmentError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::engine::timeseries::query::QueryError> for Error {
    fn from(e: crate::engine::timeseries::query::QueryError) -> Self {
        Self::Storage {
            engine: "timeseries".into(),
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::crl::CrlError> for Error {
    fn from(e: crate::control::security::crl::CrlError) -> Self {
        Self::Config {
            detail: e.to_string(),
        }
    }
}

impl From<crate::control::security::jwt::JwtError> for Error {
    fn from(e: crate::control::security::jwt::JwtError) -> Self {
        Self::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: e.to_string(),
        }
    }
}

impl From<crate::storage::quarantine::engines::FtsOrQuarantine> for Error {
    fn from(e: crate::storage::quarantine::engines::FtsOrQuarantine) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

impl From<nodedb_vector::error::VectorError> for Error {
    /// Checkpoint failures fail-stop because replay history may be truncated.
    fn from(e: nodedb_vector::error::VectorError) -> Self {
        use nodedb_vector::error::VectorError as Ve;
        let detail = e.to_string();
        match e {
            Ve::BudgetExhausted(_) => Self::MemoryExhausted {
                engine: "vector".to_string(),
            },
            Ve::SegmentIo(_) => Self::Storage {
                engine: "vector".to_string(),
                detail,
            },
            Ve::DimensionMismatch { .. }
            | Ve::UnsupportedVersion { .. }
            | Ve::InvalidMagic
            | Ve::DeserializationFailed(_)
            | Ve::CheckpointEncryptedNoKey
            | Ve::CheckpointPlaintextKeyRequired
            | Ve::CheckpointEncryptionError { .. }
            | Ve::CheckpointSerializationError { .. }
            | Ve::CheckpointDeserializationError { .. } => Self::SegmentCorrupted { detail },
            // Unknown vector errors fail-stop.
            _ => Self::SegmentCorrupted { detail },
        }
    }
}

impl From<nodedb_spatial::RTreeCheckpointError> for Error {
    /// Spatial checkpoint failures fail-stop as segment corruption.
    fn from(e: nodedb_spatial::RTreeCheckpointError) -> Self {
        Self::SegmentCorrupted {
            detail: e.to_string(),
        }
    }
}

/// The mapping table itself lives in [`crate::error_classify`] and borrows,
/// so the native protocol's error frames — which only ever hold a `&Error` —
/// classify through the same table instead of a second one that would drift.
impl From<Error> for NodeDbError {
    fn from(e: Error) -> Self {
        crate::error_classify::classify(&e)
    }
}

/// Preserve wire-level cluster classifications for local retry handling.
impl From<nodedb_cluster::rpc_codec::TypedClusterError> for Error {
    fn from(e: nodedb_cluster::rpc_codec::TypedClusterError) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            TypedClusterError::NotLeader {
                group_id,
                leader_node_id,
                leader_addr,
                ..
            } => Error::NotLeader {
                // Clamp cluster-managed group IDs for vShard display.
                vshard_id: crate::types::VShardId::new(
                    (group_id as u32).min(crate::types::VShardId::COUNT - 1),
                ),
                leader_node: leader_node_id.unwrap_or(0),
                leader_addr: leader_addr.unwrap_or_default(),
            },
            TypedClusterError::DescriptorMismatch { collection, .. } => {
                Error::RetryableSchemaChanged {
                    descriptor: collection,
                }
            }
            TypedClusterError::DeadlineExceeded { .. } => Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(0),
            },
            TypedClusterError::Internal { code, message } => {
                // Legacy or unknown codes retain their message without panicking.
                match u16::try_from(code) {
                    Ok(code_u16) if code_u16 != 0 => Error::RemoteTyped {
                        code: nodedb_types::error::ErrorCode(code_u16),
                        message,
                    },
                    _ => Error::Internal { detail: message },
                }
            }
        }
    }
}

/// Build a `TypedClusterError::NotLeader` from an `Error::NotLeader`.
impl From<Error> for nodedb_cluster::rpc_codec::TypedClusterError {
    fn from(e: Error) -> Self {
        use nodedb_cluster::rpc_codec::TypedClusterError;
        match e {
            Error::NotLeader {
                vshard_id,
                leader_node,
                leader_addr,
            } => TypedClusterError::NotLeader {
                group_id: vshard_id.as_u32() as u64,
                leader_node_id: if leader_node == 0 {
                    None
                } else {
                    Some(leader_node)
                },
                leader_addr: if leader_addr.is_empty() {
                    None
                } else {
                    Some(leader_addr)
                },
                term: 0,
            },
            Error::DeadlineExceeded { .. } => TypedClusterError::DeadlineExceeded { elapsed_ms: 0 },
            Error::RemoteTyped { code, message } => TypedClusterError::Internal {
                code: u32::from(code.0),
                message,
            },
            other => {
                // Preserve classification across multi-hop forwarding.
                let message = other.to_string();
                let code = u32::from(NodeDbError::from(other).code().0);
                TypedClusterError::Internal { code, message }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::rpc_codec::TypedClusterError;
    use nodedb_types::error::ErrorCode;

    use super::Error;
    use crate::types::TenantId;

    /// A legacy peer (or one that never classified the failure) sends `code ==
    /// 0`. Decoding must degrade to the pre-existing generic `Error::Internal`
    /// rather than fabricating a bogus `ErrorCode(0)` classification.
    #[test]
    fn decode_zero_code_degrades_to_internal() {
        let wire = TypedClusterError::Internal {
            code: 0,
            message: "boom".to_owned(),
        };
        let err: Error = wire.into();
        match err {
            Error::Internal { detail } => assert_eq!(detail, "boom"),
            other => panic!("expected Error::Internal, got {other:?}"),
        }
    }

    /// A remote peer that populated a real `ErrorCode` must decode to
    /// `Error::RemoteTyped`, carrying that code forward instead of losing it.
    #[test]
    fn decode_real_code_becomes_remote_typed() {
        let wire = TypedClusterError::Internal {
            code: u32::from(ErrorCode::CONSTRAINT_VIOLATION.0),
            message: "duplicate key".to_owned(),
        };
        let err: Error = wire.into();
        match err {
            Error::RemoteTyped { code, message } => {
                assert_eq!(code, ErrorCode::CONSTRAINT_VIOLATION);
                assert_eq!(message, "duplicate key");
            }
            other => panic!("expected Error::RemoteTyped, got {other:?}"),
        }
    }

    /// Encoding a `RejectedConstraint` must derive its real code
    /// (`CONSTRAINT_VIOLATION`), never the old hardcoded 0 catch-all.
    #[test]
    fn encode_rejected_constraint_derives_nonzero_code() {
        let err = Error::RejectedConstraint {
            collection: "users".to_owned(),
            constraint: "unique_email".to_owned(),
            detail: "duplicate email".to_owned(),
        };
        let wire: TypedClusterError = err.into();
        match wire {
            TypedClusterError::Internal { code, .. } => {
                assert_ne!(code, 0);
                assert_eq!(code, u32::from(ErrorCode::CONSTRAINT_VIOLATION.0));
            }
            other => panic!("expected TypedClusterError::Internal, got {other:?}"),
        }
    }

    /// Encoding then decoding a typed error must preserve the numeric code
    /// end to end, which is the whole point of this fix.
    #[test]
    fn round_trip_preserves_code() {
        let original = Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: "secret_vault".to_owned(),
        };
        let wire: TypedClusterError = original.into();
        let decoded: Error = wire.into();
        match decoded {
            Error::RemoteTyped { code, message } => {
                assert_eq!(code, ErrorCode::AUTHORIZATION_DENIED);
                assert!(message.contains("secret_vault"));
            }
            other => panic!("expected Error::RemoteTyped, got {other:?}"),
        }
    }
}
