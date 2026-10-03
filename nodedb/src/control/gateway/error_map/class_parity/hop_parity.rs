// SPDX-License-Identifier: BUSL-1.1

//! Transaction-state, dependency and DDL errors keep their exact SQLSTATE across a node hop.

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::ErrorCode;
use crate::control::server::native::dispatch::native_error_fields;
use crate::control::server::pgwire::types::error_to_sqlstate;

use super::support::HopEncoder;

/// An error builder, with the SQLSTATE and public code it must answer.
type ClassCase = (
    fn() -> crate::Error,
    &'static str,
    nodedb_types::error::ErrorCode,
);

/// The transaction-state and dependency variants render their exact
/// SQLSTATE after a node hop through both encoders, and carry the public
/// code of that class.
#[test]
fn transaction_and_dependency_variants_keep_their_sqlstate_across_a_hop() {
    use nodedb_cluster::rpc_codec::TypedClusterError;
    use nodedb_types::error::ErrorCode as Ec;

    use crate::control::cluster::data_plane_error_wire::execution_error_to_typed;

    let cases: [ClassCase; 6] = [
        (
            || crate::Error::CalvinParticipantError,
            sqlstate::TRANSACTION_ROLLBACK,
            Ec::TRANSACTION_ROLLBACK,
        ),
        (
            || crate::Error::NotInTransactionBlock {
                statement: "VACUUM".into(),
            },
            sqlstate::ACTIVE_SQL_TRANSACTION,
            Ec::ACTIVE_SQL_TRANSACTION,
        ),
        (
            || crate::Error::CrdtApplyForbiddenInTransaction,
            sqlstate::ACTIVE_SQL_TRANSACTION,
            Ec::ACTIVE_SQL_TRANSACTION,
        ),
        (
            || crate::Error::CrossShardInExplicitTransaction,
            sqlstate::ACTIVE_SQL_TRANSACTION,
            Ec::ACTIVE_SQL_TRANSACTION,
        ),
        (
            || crate::Error::DependentObjectsExist {
                tenant_id: 1,
                root_kind: "collection",
                root_name: "c".into(),
                dependent_count: 1,
                dependents: vec![("view".into(), "v".into())],
            },
            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
            Ec::DEPENDENT_OBJECTS_EXIST,
        ),
        (
            || crate::Error::RoleInUse {
                role: "analyst".into(),
                dependents: crate::control::security::role_assignment::RoleDependents::ChildRoles(
                    vec!["junior".into()],
                ),
            },
            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
            Ec::DEPENDENT_OBJECTS_EXIST,
        ),
    ];
    let encoders: [HopEncoder; 2] = [
        ("execution_error_to_typed", execution_error_to_typed),
        ("From<Error>", TypedClusterError::from),
    ];
    for (make, state, code) in cases {
        let err = make();
        assert_eq!(error_to_sqlstate(&err).1, state, "{err:?} locally");
        assert_eq!(native_error_fields(&err).code, code, "{err:?} native code");
        for (name, encode) in encoders {
            let rebuilt = crate::Error::from(encode(make()));
            assert_eq!(
                error_to_sqlstate(&rebuilt).1,
                state,
                "{name}: {err:?} after the hop as {rebuilt:?}"
            );
        }

        // Across the SPSC bridge: the Data-Plane code the variant becomes.
        let bridged = ErrorCode::from(make());
        let on_bridge = crate::Error::DataPlane(bridged.clone());
        assert_eq!(
            error_to_sqlstate(&on_bridge).1,
            state,
            "{err:?} across the bridge as {bridged:?}"
        );
        assert_eq!(
            native_error_fields(&on_bridge).code,
            code,
            "{err:?} native code across the bridge"
        );

        // Across the cluster Data-Plane wire: the code survives verbatim.
        let wire = nodedb_cluster::rpc_codec::DataPlaneErrorCode::from(bridged.clone());
        let back = ErrorCode::from(wire);
        assert_eq!(back, bridged, "{err:?} across the cluster wire");
        assert_eq!(
            error_to_sqlstate(&crate::Error::DataPlane(back)).1,
            state,
            "{err:?} after the cluster wire"
        );
    }
}

/// Each transaction-state and dependency Data-Plane code crosses the cluster
/// wire verbatim, and renders one SQLSTATE on both sides.
#[test]
fn transaction_and_dependency_codes_roundtrip_the_cluster_wire() {
    use nodedb_cluster::rpc_codec::DataPlaneErrorCode;

    let cases = [
        (
            ErrorCode::TransactionRollback {
                detail: "participant aborted".into(),
            },
            sqlstate::TRANSACTION_ROLLBACK,
        ),
        (
            ErrorCode::ActiveSqlTransaction {
                detail: "VACUUM cannot run inside a transaction block".into(),
            },
            sqlstate::ACTIVE_SQL_TRANSACTION,
        ),
        (
            ErrorCode::DependentObjectsExist {
                object: "collection 'c'".into(),
                detail: "cannot drop collection 'c': 1 dependent(s) exist (view:v)".into(),
            },
            sqlstate::DEPENDENT_OBJECTS_STILL_EXIST,
        ),
    ];
    for (code, state) in cases {
        let local = crate::Error::DataPlane(code.clone());
        assert_eq!(error_to_sqlstate(&local).1, state, "{code:?} locally");
        let back = ErrorCode::from(DataPlaneErrorCode::from(code.clone()));
        assert_eq!(back, code, "{code:?} across the cluster wire");
        let remote = crate::Error::DataPlane(back);
        assert_eq!(error_to_sqlstate(&remote).1, state, "{code:?} remotely");
        assert_eq!(
            native_error_fields(&remote).code,
            native_error_fields(&local).code,
            "{code:?} native code"
        );
    }
}

/// A DDL error keeps its exact SQLSTATE and code, including a SQLSTATE no
/// named constant covers.
#[test]
fn a_ddl_error_keeps_its_exact_sqlstate_and_code() {
    use crate::control::server::shared::ddl::DdlError;

    for state in ["42710", "42P07", sqlstate::INSUFFICIENT_PRIVILEGE, "57014"] {
        let ddl = if state == "57014" {
            DdlError::from_error(&crate::Error::DeadlineExceeded {
                request_id: crate::types::RequestId::new(1),
            })
        } else {
            DdlError::new(state, "refused")
        };
        let expected_code = ddl.code;
        let err = crate::Error::from(ddl);
        assert_eq!(error_to_sqlstate(&err).1, state, "{err:?}");
        let native = native_error_fields(&err);
        assert_eq!(native.sqlstate, state, "{err:?} native SQLSTATE");
        assert_eq!(native.code, expected_code, "{err:?} native code");
    }
}
