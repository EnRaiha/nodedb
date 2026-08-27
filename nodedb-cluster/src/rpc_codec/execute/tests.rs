// SPDX-License-Identifier: BUSL-1.1

//! Wire roundtrip coverage for the execute RPC family.

use crate::rpc_codec::data_plane_error::DataPlaneErrorCode;
use crate::rpc_codec::raft_rpc::RaftRpc;

use super::types::{
    DescriptorVersionEntry, ExecuteRequest, ExecuteResponse, ExecuteStreamChunk, ExecuteStreamEnd,
    PLAN_DECODE_FAILED, TypedClusterError,
};

fn roundtrip_req(req: ExecuteRequest) -> ExecuteRequest {
    let rpc = RaftRpc::ExecuteRequest(req);
    let encoded =
        crate::rpc_codec::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
            .unwrap();
    match crate::rpc_codec::decode(
        &encoded,
        &crate::cluster_epoch::ClusterEpochState::default(),
    )
    .unwrap()
    {
        RaftRpc::ExecuteRequest(r) => r,
        other => panic!("expected ExecuteRequest, got {other:?}"),
    }
}

fn roundtrip_resp(resp: ExecuteResponse) -> ExecuteResponse {
    let rpc = RaftRpc::ExecuteResponse(resp);
    let encoded =
        crate::rpc_codec::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
            .unwrap();
    match crate::rpc_codec::decode(
        &encoded,
        &crate::cluster_epoch::ClusterEpochState::default(),
    )
    .unwrap()
    {
        RaftRpc::ExecuteResponse(r) => r,
        other => panic!("expected ExecuteResponse, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_request_basic() {
    let req = ExecuteRequest {
        plan_bytes: b"msgpack-plan-bytes".to_vec(),
        tenant_id: 7,
        database_id: 0,
        deadline_remaining_ms: 5000,
        trace_id: [
            0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34, 0x56, 0x78, 0xDE, 0xAD, 0xBE, 0xEF, 0x12, 0x34,
            0x56, 0x78,
        ],
        descriptor_versions: vec![
            DescriptorVersionEntry {
                collection: "orders".into(),
                version: 42,
            },
            DescriptorVersionEntry {
                collection: "users".into(),
                version: 1,
            },
        ],
        txn_id: None,
    };
    let decoded = roundtrip_req(req.clone());
    assert_eq!(decoded.plan_bytes, req.plan_bytes);
    assert_eq!(decoded.tenant_id, 7);
    assert_eq!(decoded.deadline_remaining_ms, 5000);
    assert_eq!(
        decoded.trace_id, req.trace_id,
        "trace_id roundtrips correctly"
    );
    assert_eq!(decoded.descriptor_versions.len(), 2);
    assert_eq!(decoded.descriptor_versions[0].collection, "orders");
    assert_eq!(decoded.descriptor_versions[0].version, 42);
}

#[test]
fn roundtrip_execute_request_empty_descriptors() {
    let req = ExecuteRequest {
        plan_bytes: vec![0xAB, 0xCD],
        tenant_id: 0,
        database_id: 0,
        deadline_remaining_ms: 1000,
        trace_id: [0u8; 16],
        descriptor_versions: vec![],
        txn_id: None,
    };
    let decoded = roundtrip_req(req);
    assert!(decoded.descriptor_versions.is_empty());
}

#[test]
fn roundtrip_execute_response_success() {
    let resp = ExecuteResponse::ok(
        vec![b"row1".to_vec(), b"row2".to_vec()],
        0xCAFE_1234,
        0xBEEF_5678,
    );
    let decoded = roundtrip_resp(resp);
    assert!(decoded.success);
    assert_eq!(decoded.payloads.len(), 2);
    assert_eq!(decoded.payloads[0], b"row1");
    assert!(decoded.error.is_none());
    assert_eq!(
        decoded.watermark_lsn, 0xCAFE_1234,
        "read watermark roundtrips on the response body"
    );
    assert_eq!(
        decoded.read_version_lsn, 0xBEEF_5678,
        "per-collection read-version LSN roundtrips distinct from the watermark"
    );
}

#[test]
fn roundtrip_execute_response_not_leader() {
    let resp = ExecuteResponse::err(TypedClusterError::NotLeader {
        group_id: 3,
        leader_node_id: Some(1),
        leader_addr: Some("10.0.0.1:9400".into()),
        term: 7,
    });
    let decoded = roundtrip_resp(resp);
    assert!(!decoded.success);
    assert_eq!(
        decoded.watermark_lsn, 0,
        "error responses carry no watermark"
    );
    assert_eq!(
        decoded.read_version_lsn, 0,
        "error responses carry no read-version LSN"
    );
    match decoded.error {
        Some(TypedClusterError::NotLeader {
            group_id,
            leader_node_id,
            leader_addr,
            term,
        }) => {
            assert_eq!(group_id, 3);
            assert_eq!(leader_node_id, Some(1));
            assert_eq!(leader_addr.as_deref(), Some("10.0.0.1:9400"));
            assert_eq!(term, 7);
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_response_descriptor_mismatch() {
    let resp = ExecuteResponse::err(TypedClusterError::DescriptorMismatch {
        collection: "orders".into(),
        expected_version: 5,
        actual_version: 6,
    });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::DescriptorMismatch {
            collection,
            expected_version,
            actual_version,
        }) => {
            assert_eq!(collection, "orders");
            assert_eq!(expected_version, 5);
            assert_eq!(actual_version, 6);
        }
        other => panic!("expected DescriptorMismatch, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_response_deadline_exceeded() {
    let resp = ExecuteResponse::err(TypedClusterError::DeadlineExceeded { elapsed_ms: 3000 });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::DeadlineExceeded { elapsed_ms }) => {
            assert_eq!(elapsed_ms, 3000)
        }
        other => panic!("expected DeadlineExceeded, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_response_internal_error() {
    let resp = ExecuteResponse::err(TypedClusterError::Internal {
        code: PLAN_DECODE_FAILED,
        message: "failed to decode plan".into(),
    });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::Internal { code, message }) => {
            assert_eq!(code, PLAN_DECODE_FAILED);
            assert!(message.contains("plan"));
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_response_data_plane_verdict() {
    let resp = ExecuteResponse::err(TypedClusterError::DataPlane {
        code: DataPlaneErrorCode::DivisionByZero,
    });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::DataPlane { code }) => {
            assert_eq!(code, DataPlaneErrorCode::DivisionByZero);
        }
        other => panic!("expected DataPlane, got {other:?}"),
    }
}

/// Payload-bearing verdicts keep every field across the hop — the detail
/// string is what the client reads back.
#[test]
fn roundtrip_execute_response_data_plane_payload() {
    let resp = ExecuteResponse::err(TypedClusterError::DataPlane {
        code: DataPlaneErrorCode::RejectedConstraint {
            constraint: "unique".into(),
            detail: "key (id)=(7) already exists".into(),
        },
    });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::DataPlane {
            code: DataPlaneErrorCode::RejectedConstraint { constraint, detail },
        }) => {
            assert_eq!(constraint, "unique");
            assert!(detail.contains("(id)=(7)"));
        }
        other => panic!("expected DataPlane RejectedConstraint, got {other:?}"),
    }
}

fn roundtrip_stream_chunk(chunk: ExecuteStreamChunk) -> ExecuteStreamChunk {
    let rpc = RaftRpc::ExecuteStreamChunk(chunk);
    let encoded =
        crate::rpc_codec::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
            .unwrap();
    match crate::rpc_codec::decode(
        &encoded,
        &crate::cluster_epoch::ClusterEpochState::default(),
    )
    .unwrap()
    {
        RaftRpc::ExecuteStreamChunk(c) => c,
        other => panic!("expected ExecuteStreamChunk, got {other:?}"),
    }
}

fn roundtrip_stream_end(end: ExecuteStreamEnd) -> ExecuteStreamEnd {
    let rpc = RaftRpc::ExecuteStreamEnd(end);
    let encoded =
        crate::rpc_codec::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
            .unwrap();
    match crate::rpc_codec::decode(
        &encoded,
        &crate::cluster_epoch::ClusterEpochState::default(),
    )
    .unwrap()
    {
        RaftRpc::ExecuteStreamEnd(e) => e,
        other => panic!("expected ExecuteStreamEnd, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_stream_request_reuses_execute_request_body() {
    let req = ExecuteRequest {
        plan_bytes: b"streaming-plan".to_vec(),
        tenant_id: 11,
        database_id: 2,
        deadline_remaining_ms: 4242,
        trace_id: [9u8; 16],
        descriptor_versions: vec![DescriptorVersionEntry {
            collection: "wide".into(),
            version: 3,
        }],
        txn_id: None,
    };
    let rpc = RaftRpc::ExecuteStreamRequest(req.clone());
    let encoded =
        crate::rpc_codec::encode(&rpc, &crate::cluster_epoch::ClusterEpochState::default())
            .unwrap();
    match crate::rpc_codec::decode(
        &encoded,
        &crate::cluster_epoch::ClusterEpochState::default(),
    )
    .unwrap()
    {
        RaftRpc::ExecuteStreamRequest(r) => {
            assert_eq!(r.plan_bytes, req.plan_bytes);
            assert_eq!(r.tenant_id, 11);
            assert_eq!(r.database_id, 2);
            assert_eq!(r.deadline_remaining_ms, 4242);
            assert_eq!(r.trace_id, req.trace_id);
            assert_eq!(r.descriptor_versions.len(), 1);
            assert_eq!(r.descriptor_versions[0].collection, "wide");
            assert_eq!(r.descriptor_versions[0].version, 3);
        }
        other => panic!("expected ExecuteStreamRequest, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_stream_chunk_payload_and_lsn() {
    let chunk = ExecuteStreamChunk {
        payload: vec![0x91, 0x01, 0x02, 0x03],
        watermark_lsn: 0xDEAD_BEEF,
    };
    let decoded = roundtrip_stream_chunk(chunk.clone());
    assert_eq!(decoded.payload, chunk.payload);
    assert_eq!(decoded.watermark_lsn, 0xDEAD_BEEF);
}

#[test]
fn roundtrip_execute_stream_end_clean_eof() {
    let decoded = roundtrip_stream_end(ExecuteStreamEnd { error: None });
    assert!(decoded.error.is_none());
}

#[test]
fn roundtrip_execute_stream_end_terminal_error() {
    let decoded = roundtrip_stream_end(ExecuteStreamEnd {
        error: Some(TypedClusterError::Internal {
            code: PLAN_DECODE_FAILED,
            message: "stream failed mid-flight".into(),
        }),
    });
    match decoded.error {
        Some(TypedClusterError::Internal { code, message }) => {
            assert_eq!(code, PLAN_DECODE_FAILED);
            assert!(message.contains("stream failed"));
        }
        other => panic!("expected Internal, got {other:?}"),
    }
}

#[test]
fn roundtrip_execute_response_not_leader_no_hint() {
    let resp = ExecuteResponse::err(TypedClusterError::NotLeader {
        group_id: 0,
        leader_node_id: None,
        leader_addr: None,
        term: 0,
    });
    let decoded = roundtrip_resp(resp);
    match decoded.error {
        Some(TypedClusterError::NotLeader {
            leader_node_id,
            leader_addr,
            ..
        }) => {
            assert!(leader_node_id.is_none());
            assert!(leader_addr.is_none());
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }
}
