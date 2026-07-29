// SPDX-License-Identifier: BUSL-1.1

//! Envelope construction and field-access tests.

use super::*;
use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
use std::time::{Duration, Instant};

fn sample_request() -> Request {
    Request {
        request_id: RequestId::new(1),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Document(DocumentOp::PointGet {
            collection: "users".into(),
            document_id: "doc-1".into(),
            surrogate: nodedb_types::Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }),
        deadline: Instant::now() + Duration::from_secs(5),
        priority: Priority::Normal,
        trace_id: TraceId::generate(),
        consistency: ReadConsistency::Strong,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: Admission::Exempt(ExemptReason::Read),
    }
}

#[test]
fn request_fields_accessible() {
    let req = sample_request();
    assert_eq!(req.request_id, RequestId::new(1));
    assert_eq!(req.tenant_id, TenantId::new(1));
    assert_ne!(req.trace_id, TraceId::ZERO);
}

#[test]
fn response_ok() {
    let resp = Response {
        request_id: RequestId::new(1),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: Payload::from_vec(b"result".to_vec()),
        watermark_lsn: Lsn::new(42),
        error_code: None,
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    };
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(resp.watermark_lsn, Lsn::new(42));
    assert_eq!(&*resp.payload, b"result");
}

#[test]
fn response_error() {
    let resp = Response {
        request_id: RequestId::new(2),
        status: Status::Error,
        attempt: 1,
        partial: false,
        payload: Payload::empty(),
        watermark_lsn: Lsn::ZERO,
        error_code: Some(Box::new(ErrorCode::DeadlineExceeded)),
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    };
    assert_eq!(
        resp.error_code.as_deref(),
        Some(&ErrorCode::DeadlineExceeded)
    );
}

#[test]
fn priority_ordering() {
    assert!(Priority::Background < Priority::Normal);
    assert!(Priority::Normal < Priority::High);
    assert!(Priority::High < Priority::Critical);
}

#[test]
fn cancel_plan() {
    let req = Request {
        request_id: RequestId::new(99),
        tenant_id: TenantId::new(1),
        database_id: DatabaseId::DEFAULT,
        vshard_id: VShardId::new(0),
        plan: PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: RequestId::new(42),
        }),
        deadline: Instant::now() + Duration::from_secs(1),
        priority: Priority::Critical,
        trace_id: TraceId::ZERO,
        consistency: ReadConsistency::Eventual,
        idempotency_key: None,
        event_source: crate::event::EventSource::User,
        user_roles: Vec::new(),
        user_id: None,
        statement_digest: None,
        txn_id: None,
        wal_lsn: None,
        resolved_now_ms: None,
        admission: Admission::Exempt(ExemptReason::Read),
    };
    match req.plan {
        PhysicalPlan::Meta(MetaOp::Cancel { target_request_id }) => {
            assert_eq!(target_request_id, RequestId::new(42));
        }
        _ => panic!("expected Cancel plan"),
    }
}
