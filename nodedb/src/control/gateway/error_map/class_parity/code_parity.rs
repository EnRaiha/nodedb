// SPDX-License-Identifier: BUSL-1.1

//! Every Data-Plane `ErrorCode` answers one class on native, pgwire and HTTP.

use nodedb_types::error::sqlstate;

use crate::bridge::envelope::ErrorCode;
use crate::control::server::native::dispatch::{error_code_to_native, native_error_fields};
use crate::control::server::pgwire::types::error_map::numeric_code_to_sqlstate;
use crate::control::server::pgwire::types::error_to_sqlstate;

use super::super::GatewayErrorMap;
use super::code_samples::VARIANT_COUNT;
use super::code_samples::samples;
use super::code_samples::variant_index;
use super::support::class;

#[test]
fn every_variant_has_a_sample() {
    let mut seen = [false; VARIANT_COUNT];
    for code in samples() {
        seen[variant_index(&code)] = true;
    }
    let missing: Vec<usize> = (0..VARIANT_COUNT).filter(|i| !seen[*i]).collect();
    assert!(missing.is_empty(), "variants with no sample: {missing:?}");
}

/// The native frame carries pgwire's SQLSTATE, and its numeric code has the
/// same SQLSTATE class, on both native renderings: the typed `Err` and the
/// raw response frame.
#[test]
fn every_data_plane_code_has_one_class_on_native_and_pgwire() {
    for code in samples() {
        let err = crate::Error::DataPlane(code.clone());
        let (_, pg_state, _) = error_to_sqlstate(&err);

        let native = native_error_fields(&err);
        assert_eq!(native.sqlstate, pg_state, "native SQLSTATE for {code:?}");
        let native_state = numeric_code_to_sqlstate(native.code);
        assert_eq!(
            class(native_state),
            class(pg_state),
            "{code:?}: pgwire sends {pg_state}, native code {} renders {native_state}",
            native.code
        );

        let frame = error_code_to_native(1, Some(&code));
        let payload = frame.error.expect("error frames carry a payload");
        assert_eq!(
            payload.code, pg_state,
            "response-frame SQLSTATE for {code:?}"
        );
        assert_eq!(
            payload.ndb_code, native.code.0,
            "response-frame code for {code:?}"
        );
    }
}

/// A classified Data-Plane verdict never reads as a server fault over HTTP.
#[test]
fn classified_data_plane_codes_are_not_http_500() {
    for code in samples() {
        let err = crate::Error::DataPlane(code.clone());
        let (_, pg_state, _) = error_to_sqlstate(&err);
        let (status, _) = GatewayErrorMap::to_http(&err);
        if pg_state == sqlstate::INTERNAL_ERROR {
            assert_eq!(status, 500, "{code:?}");
        } else {
            assert_ne!(status, 500, "{code:?} is {pg_state} on pgwire");
        }
    }
}

/// The SQLSTATE status table agrees with the gateway status table for every
/// Data-Plane code. A DDL error and a query error of one class answer one
/// HTTP status.
#[test]
fn sqlstate_status_agrees_with_the_gateway_status() {
    for code in samples() {
        let err = crate::Error::DataPlane(code.clone());
        let (_, pg_state, _) = error_to_sqlstate(&err);
        let (status, _) = GatewayErrorMap::to_http(&err);
        assert_eq!(
            GatewayErrorMap::sqlstate_to_http(pg_state),
            status,
            "{code:?} is {pg_state} on pgwire"
        );
    }
}

/// `Unsupported` is feature-not-supported on every surface.
#[test]
fn unsupported_is_feature_not_supported_everywhere() {
    let err = crate::Error::DataPlane(ErrorCode::Unsupported {
        detail: "not on this engine".into(),
    });
    let (_, pg_state, _) = error_to_sqlstate(&err);
    assert_eq!(pg_state, sqlstate::FEATURE_NOT_SUPPORTED);
    let native = native_error_fields(&err);
    assert_eq!(native.code, nodedb_types::error::ErrorCode::SQL_NOT_ENABLED);
    assert_eq!(GatewayErrorMap::to_http(&err).0, 501);
}
