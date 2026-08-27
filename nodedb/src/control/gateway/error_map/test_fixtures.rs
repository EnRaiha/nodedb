// SPDX-License-Identifier: BUSL-1.1

//! Error fixtures shared by the per-surface mapping tests.

use crate::Error;
use crate::types::{RequestId, TenantId, VShardId};

pub(super) fn not_leader() -> Error {
    Error::NotLeader {
        vshard_id: VShardId::new(1),
        leader_node: 2,
        leader_addr: "10.0.0.1:9000".into(),
    }
}

pub(super) fn deadline() -> Error {
    Error::DeadlineExceeded {
        request_id: RequestId::new(1),
    }
}

pub(super) fn schema_changed() -> Error {
    Error::RetryableSchemaChanged {
        descriptor: "users".into(),
    }
}

pub(super) fn not_found() -> Error {
    Error::CollectionNotFound {
        tenant_id: TenantId::new(0),
        collection: "missing_col".into(),
    }
}

pub(super) fn authz() -> Error {
    Error::RejectedAuthz {
        tenant_id: TenantId::new(0),
        resource: "secret".into(),
    }
}

pub(super) fn internal() -> Error {
    Error::Internal {
        detail: "boom".into(),
    }
}

pub(super) fn serialization() -> Error {
    Error::Serialization {
        format: "msgpack".into(),
        detail: "bad encoding".into(),
    }
}
