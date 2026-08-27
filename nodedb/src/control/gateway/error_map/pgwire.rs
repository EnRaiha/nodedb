// SPDX-License-Identifier: BUSL-1.1

//! pgwire error shape: `(SQLSTATE, message)`.

use nodedb_types::error::sqlstate;

use super::gateway_map::GatewayErrorMap;
use crate::Error;

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
            // A shard verdict that rode back as a typed code keeps the exact
            // SQLSTATE and message the direct dispatch path would have given.
            Error::DataPlane(code) => {
                let (_severity, state, message) =
                    crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(code);
                (state, message)
            }
            _ => (sqlstate::INTERNAL_ERROR, err.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_fixtures::{
        authz, deadline, internal, not_found, not_leader, schema_changed, serialization,
    };
    use super::*;

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
}
