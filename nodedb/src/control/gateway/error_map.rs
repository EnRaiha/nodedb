// SPDX-License-Identifier: BUSL-1.1

//! Translate gateway errors into listener-specific error shapes.
//!
//! Every listener calls `gateway.execute(plan)` and gets `Result<_, Error>`.
//! This module centralises the mapping from `crate::Error` into each
//! listener's error envelope so the translation is consistent and a change
//! to the SQLSTATE codes or HTTP status codes is a one-file edit.

use nodedb_types::error::sqlstate;

use crate::Error;

pub struct GatewayErrorMap;

impl GatewayErrorMap {
    /// Map a gateway error into `(sqlstate, message)` for pgwire.
    ///
    /// Returns a `'static` SQLSTATE string and an owned message string.
    /// The SQLSTATE codes match those in `pgwire::types::error_to_sqlstate`
    /// so migrated call-sites are wire-compatible with the old forwarding path.
    pub fn to_pgwire(err: &Error) -> (&'static str, String) {
        match err {
            Error::NotLeader { leader_addr, .. } => (
                sqlstate::DATABASE_DROPPED,
                format!("cluster in leader election; leader hint: {leader_addr}"),
            ),
            Error::DeadlineExceeded { .. } => (sqlstate::QUERY_CANCELED, err.to_string()),
            Error::RetryableSchemaChanged { descriptor } => (
                sqlstate::INTERNAL_ERROR,
                format!("schema changed during execution ({descriptor}); please retry"),
            ),
            Error::CollectionNotFound { collection, .. } => (
                sqlstate::UNDEFINED_TABLE,
                format!("collection \"{collection}\" does not exist"),
            ),
            Error::RejectedAuthz { .. } => (sqlstate::INSUFFICIENT_PRIVILEGE, err.to_string()),
            Error::BadRequest { detail } => (sqlstate::SYNTAX_ERROR, detail.clone()),
            Error::PlanError { detail } => (sqlstate::SYNTAX_ERROR, detail.clone()),
            Error::Serialization { .. } | Error::Codec { .. } => {
                (sqlstate::INTERNAL_ERROR, err.to_string())
            }
            Error::Internal { .. } => (sqlstate::INTERNAL_ERROR, err.to_string()),
            Error::NoLeader { .. } => (sqlstate::LOCK_NOT_AVAILABLE, err.to_string()),
            Error::CrossCollectionNotColocated { .. } => {
                (sqlstate::FEATURE_NOT_SUPPORTED, err.to_string())
            }
            Error::RemoteTyped { code, message } => (
                crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate(*code),
                message.clone(),
            ),
            _ => (sqlstate::INTERNAL_ERROR, err.to_string()),
        }
    }

    /// Map a gateway error into `(http_status_code, message)` for HTTP.
    ///
    /// Uses standard HTTP status semantics:
    /// - 400 Bad Request for client-side errors (bad SQL, not found)
    /// - 403 Forbidden for authz errors
    /// - 409 Conflict for write-conflict / constraint violations
    /// - 503 Service Unavailable for routing/leader errors
    /// - 504 Gateway Timeout for deadline exceeded
    /// - 500 Internal Server Error as the default fallback
    pub fn to_http(err: &Error) -> (u16, String) {
        match err {
            Error::NotLeader { leader_addr, .. } => (
                503,
                format!("cluster in leader election; leader hint: {leader_addr}"),
            ),
            Error::DeadlineExceeded { .. } => (504, err.to_string()),
            Error::RetryableSchemaChanged { descriptor } => (
                503,
                format!("schema changed during execution ({descriptor}); please retry"),
            ),
            Error::CollectionNotFound { collection, .. } => {
                (404, format!("collection \"{collection}\" does not exist"))
            }
            Error::RejectedAuthz { .. } => (403, err.to_string()),
            Error::BadRequest { detail } => (400, detail.clone()),
            Error::PlanError { detail } => (400, detail.clone()),
            Error::RejectedConstraint { detail, .. } => (409, detail.clone()),
            Error::NoLeader { .. } => (503, err.to_string()),
            Error::Serialization { .. } | Error::Codec { .. } => (500, err.to_string()),
            Error::Internal { .. } => (500, err.to_string()),
            // 501 Not Implemented: a valid op refused because cross-core
            // source-shipping is not yet supported (fail-closed safety floor).
            Error::CrossCollectionNotColocated { .. } => (501, err.to_string()),
            Error::RemoteTyped { code, message } => {
                (remote_code_to_http_status(*code), message.clone())
            }
            _ => (500, err.to_string()),
        }
    }

    /// Map a gateway error into a RESP simple-error string.
    ///
    /// Follows Redis error format: `ERR <message>` for generic errors, or
    /// a typed prefix (`WRONGTYPE`, `NOTFOUND`, etc.) where applicable.
    pub fn to_resp(err: &Error) -> String {
        match err {
            Error::NotLeader { leader_addr, .. } => {
                format!("MOVED 0 {leader_addr}")
            }
            Error::DeadlineExceeded { .. } => "TIMEOUT query deadline exceeded".into(),
            Error::CollectionNotFound { collection, .. } => {
                format!("NOTFOUND collection \"{collection}\" does not exist")
            }
            Error::RejectedAuthz { .. } => format!("NOPERM {}", err),
            Error::BadRequest { detail } | Error::PlanError { detail } => {
                format!("ERR {detail}")
            }
            Error::RejectedConstraint { detail, .. } => format!("CONSTRAINT {detail}"),
            Error::RetryableSchemaChanged { descriptor } => {
                format!("ERR schema changed ({descriptor}); please retry")
            }
            Error::RemoteTyped { code, message } => {
                format!("{} {message}", remote_code_to_resp_prefix(*code))
            }
            _ => format!("ERR {err}"),
        }
    }

    /// Map a gateway error into `(code, message)` for the native protocol.
    ///
    /// Error codes are aligned with `nodedb_types::error::ErrorCode` numeric
    /// values so native clients can switch on the code without string matching.
    pub fn to_native(err: &Error) -> (u32, String) {
        // Error code constants (subset matching nodedb_types numeric codes).
        const CODE_NOT_LEADER: u32 = 10;
        const CODE_DEADLINE: u32 = 20;
        const CODE_SCHEMA_CHANGED: u32 = 30;
        const CODE_NOT_FOUND: u32 = 40;
        const CODE_AUTHZ: u32 = 50;
        const CODE_BAD_REQUEST: u32 = 60;
        const CODE_CONSTRAINT: u32 = 70;
        const CODE_INTERNAL: u32 = 99;

        match err {
            Error::NotLeader { leader_addr, .. } => {
                (CODE_NOT_LEADER, format!("not leader; hint: {leader_addr}"))
            }
            Error::DeadlineExceeded { .. } => (CODE_DEADLINE, err.to_string()),
            Error::RetryableSchemaChanged { descriptor } => (
                CODE_SCHEMA_CHANGED,
                format!("schema changed ({descriptor})"),
            ),
            Error::CollectionNotFound { collection, .. } => (
                CODE_NOT_FOUND,
                format!("collection \"{collection}\" not found"),
            ),
            Error::RejectedAuthz { .. } => (CODE_AUTHZ, err.to_string()),
            Error::BadRequest { detail } | Error::PlanError { detail } => {
                (CODE_BAD_REQUEST, detail.clone())
            }
            Error::RejectedConstraint { detail, .. } => (CODE_CONSTRAINT, detail.clone()),
            Error::CrossCollectionNotColocated { .. } => (CODE_BAD_REQUEST, err.to_string()),
            Error::RemoteTyped { code, message } => {
                use nodedb_types::error::ErrorCode as Ec;
                let native_code = match *code {
                    Ec::DEADLINE_EXCEEDED => CODE_DEADLINE,
                    Ec::COLLECTION_NOT_FOUND => CODE_NOT_FOUND,
                    Ec::AUTHORIZATION_DENIED => CODE_AUTHZ,
                    Ec::BAD_REQUEST | Ec::PLAN_ERROR => CODE_BAD_REQUEST,
                    Ec::CONSTRAINT_VIOLATION => CODE_CONSTRAINT,
                    _ => CODE_INTERNAL,
                };
                (native_code, message.clone())
            }
            _ => (CODE_INTERNAL, err.to_string()),
        }
    }
}

/// Map a numeric `ErrorCode` from a `RemoteTyped` error to an HTTP status,
/// mirroring the local variant arms in `to_http` above for the same
/// condition (e.g. `CONSTRAINT_VIOLATION` mirrors `RejectedConstraint`'s 409).
fn remote_code_to_http_status(code: nodedb_types::error::ErrorCode) -> u16 {
    use nodedb_types::error::ErrorCode as Ec;
    match code {
        Ec::NOT_LEADER | Ec::NO_LEADER => 503,
        Ec::DEADLINE_EXCEEDED => 504,
        Ec::COLLECTION_NOT_FOUND => 404,
        Ec::AUTHORIZATION_DENIED => 403,
        Ec::BAD_REQUEST | Ec::PLAN_ERROR => 400,
        Ec::CONSTRAINT_VIOLATION | Ec::WRITE_CONFLICT => 409,
        _ => 500,
    }
}

/// Map a numeric `ErrorCode` from a `RemoteTyped` error to a RESP error
/// prefix, mirroring the local variant arms in `to_resp` above.
fn remote_code_to_resp_prefix(code: nodedb_types::error::ErrorCode) -> &'static str {
    use nodedb_types::error::ErrorCode as Ec;
    match code {
        Ec::DEADLINE_EXCEEDED => "TIMEOUT",
        Ec::COLLECTION_NOT_FOUND => "NOTFOUND",
        Ec::AUTHORIZATION_DENIED => "NOPERM",
        Ec::CONSTRAINT_VIOLATION => "CONSTRAINT",
        _ => "ERR",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{RequestId, TenantId, VShardId};

    fn not_leader() -> Error {
        Error::NotLeader {
            vshard_id: VShardId::new(1),
            leader_node: 2,
            leader_addr: "10.0.0.1:9000".into(),
        }
    }

    fn deadline() -> Error {
        Error::DeadlineExceeded {
            request_id: RequestId::new(1),
        }
    }

    fn schema_changed() -> Error {
        Error::RetryableSchemaChanged {
            descriptor: "users".into(),
        }
    }

    fn not_found() -> Error {
        Error::CollectionNotFound {
            tenant_id: TenantId::new(0),
            collection: "missing_col".into(),
        }
    }

    fn authz() -> Error {
        Error::RejectedAuthz {
            tenant_id: TenantId::new(0),
            resource: "secret".into(),
        }
    }

    fn internal() -> Error {
        Error::Internal {
            detail: "boom".into(),
        }
    }

    fn serialization() -> Error {
        Error::Serialization {
            format: "msgpack".into(),
            detail: "bad encoding".into(),
        }
    }

    // --- pgwire mapping ---

    #[test]
    fn pgwire_not_leader() {
        let (code, _msg) = GatewayErrorMap::to_pgwire(&not_leader());
        assert_eq!(code, sqlstate::DATABASE_DROPPED);
    }

    #[test]
    fn pgwire_deadline() {
        let (code, _) = GatewayErrorMap::to_pgwire(&deadline());
        assert_eq!(code, sqlstate::QUERY_CANCELED);
    }

    #[test]
    fn pgwire_schema_changed() {
        let (code, msg) = GatewayErrorMap::to_pgwire(&schema_changed());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
        assert!(msg.contains("users"));
    }

    #[test]
    fn pgwire_not_found() {
        let (code, msg) = GatewayErrorMap::to_pgwire(&not_found());
        assert_eq!(code, sqlstate::UNDEFINED_TABLE);
        assert!(msg.contains("missing_col"));
    }

    #[test]
    fn pgwire_authz() {
        let (code, _) = GatewayErrorMap::to_pgwire(&authz());
        assert_eq!(code, sqlstate::INSUFFICIENT_PRIVILEGE);
    }

    #[test]
    fn pgwire_internal() {
        let (code, _) = GatewayErrorMap::to_pgwire(&internal());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
    }

    #[test]
    fn pgwire_serialization() {
        let (code, _) = GatewayErrorMap::to_pgwire(&serialization());
        assert_eq!(code, sqlstate::INTERNAL_ERROR);
    }

    // --- HTTP mapping ---

    #[test]
    fn http_not_leader() {
        let (status, _) = GatewayErrorMap::to_http(&not_leader());
        assert_eq!(status, 503);
    }

    #[test]
    fn http_deadline() {
        let (status, _) = GatewayErrorMap::to_http(&deadline());
        assert_eq!(status, 504);
    }

    #[test]
    fn http_not_found() {
        let (status, _) = GatewayErrorMap::to_http(&not_found());
        assert_eq!(status, 404);
    }

    #[test]
    fn http_authz() {
        let (status, _) = GatewayErrorMap::to_http(&authz());
        assert_eq!(status, 403);
    }

    #[test]
    fn http_internal() {
        let (status, _) = GatewayErrorMap::to_http(&internal());
        assert_eq!(status, 500);
    }

    // --- RESP mapping ---

    #[test]
    fn resp_not_leader() {
        let msg = GatewayErrorMap::to_resp(&not_leader());
        assert!(msg.starts_with("MOVED"));
    }

    #[test]
    fn resp_deadline() {
        let msg = GatewayErrorMap::to_resp(&deadline());
        assert!(msg.starts_with("TIMEOUT"));
    }

    #[test]
    fn resp_not_found() {
        let msg = GatewayErrorMap::to_resp(&not_found());
        assert!(msg.starts_with("NOTFOUND"));
    }

    #[test]
    fn resp_authz() {
        let msg = GatewayErrorMap::to_resp(&authz());
        assert!(msg.starts_with("NOPERM"));
    }

    #[test]
    fn resp_internal() {
        let msg = GatewayErrorMap::to_resp(&internal());
        assert!(msg.starts_with("ERR"));
    }

    // --- Native mapping ---

    #[test]
    fn native_not_leader() {
        let (code, msg) = GatewayErrorMap::to_native(&not_leader());
        assert_eq!(code, 10);
        assert!(msg.contains("hint:"));
    }

    #[test]
    fn native_deadline() {
        let (code, _) = GatewayErrorMap::to_native(&deadline());
        assert_eq!(code, 20);
    }

    #[test]
    fn native_schema_changed() {
        let (code, _) = GatewayErrorMap::to_native(&schema_changed());
        assert_eq!(code, 30);
    }

    #[test]
    fn native_not_found() {
        let (code, _) = GatewayErrorMap::to_native(&not_found());
        assert_eq!(code, 40);
    }

    #[test]
    fn native_authz() {
        let (code, _) = GatewayErrorMap::to_native(&authz());
        assert_eq!(code, 50);
    }

    #[test]
    fn native_internal() {
        let (code, _) = GatewayErrorMap::to_native(&internal());
        assert_eq!(code, 99);
    }

    // --- RemoteTyped helpers ---
    //
    // `RemoteTyped` carries a numeric `ErrorCode` from a remote node across
    // cluster RPC. `remote_code_to_http_status` / `remote_code_to_resp_prefix`
    // translate that numeric code back into listener-specific shapes. The
    // unmapped-code case matters because remote peers may run newer code with
    // error codes this build doesn't recognize yet (rolling upgrades); the
    // fallback must degrade to a generic status/prefix rather than panicking
    // or misclassifying the failure.

    #[test]
    fn remote_http_status_maps_known_code() {
        use nodedb_types::error::ErrorCode;
        // Mirrors `RejectedConstraint`'s 409 in `to_http` above.
        assert_eq!(
            remote_code_to_http_status(ErrorCode::CONSTRAINT_VIOLATION),
            409
        );
        assert_eq!(
            remote_code_to_http_status(ErrorCode::AUTHORIZATION_DENIED),
            403
        );
    }

    #[test]
    fn remote_http_status_unmapped_code_falls_back_to_500() {
        use nodedb_types::error::ErrorCode;
        // A code with no explicit arm (e.g. one a newer remote node minted
        // that this build doesn't recognize) must degrade to the generic
        // 500 fallback, not silently misreport a specific status.
        assert_eq!(remote_code_to_http_status(ErrorCode(65000)), 500);
    }

    #[test]
    fn remote_resp_prefix_maps_known_code() {
        use nodedb_types::error::ErrorCode;
        assert_eq!(
            remote_code_to_resp_prefix(ErrorCode::AUTHORIZATION_DENIED),
            "NOPERM"
        );
        assert_eq!(
            remote_code_to_resp_prefix(ErrorCode::CONSTRAINT_VIOLATION),
            "CONSTRAINT"
        );
    }

    #[test]
    fn remote_resp_prefix_unmapped_code_falls_back_to_err() {
        use nodedb_types::error::ErrorCode;
        // Same degrade path as the HTTP fallback above: an unrecognized
        // remote code must still surface as the generic `ERR` prefix.
        assert_eq!(remote_code_to_resp_prefix(ErrorCode(65000)), "ERR");
    }

    #[test]
    fn to_http_remote_typed_is_wired_to_helper() {
        use nodedb_types::error::ErrorCode;
        let err = Error::RemoteTyped {
            code: ErrorCode::AUTHORIZATION_DENIED,
            message: "remote denied write".into(),
        };
        let (status, msg) = GatewayErrorMap::to_http(&err);
        assert_eq!(status, 403);
        assert_eq!(msg, "remote denied write");
    }

    #[test]
    fn to_resp_remote_typed_is_wired_to_helper() {
        use nodedb_types::error::ErrorCode;
        let err = Error::RemoteTyped {
            code: ErrorCode::CONSTRAINT_VIOLATION,
            message: "unique key clash".into(),
        };
        let msg = GatewayErrorMap::to_resp(&err);
        assert_eq!(msg, "CONSTRAINT unique key clash");
    }
}
