// SPDX-License-Identifier: BUSL-1.1

//! The bytes a participant's typed stage refusal crosses the sequencer log in.
//!
//! A participant whose stage refuses its slice votes abort with the refusal's
//! Data-Plane [`ErrorCode`]. The vote carries the code as the rkyv bytes of
//! its wire mirror [`DataPlaneErrorCode`], opaque to the sequencer. The
//! coordinator decodes the verdict's refusal back into the code, so the
//! client sees the SQLSTATE a direct write renders.

use nodedb_cluster::rpc_codec::DataPlaneErrorCode;

use crate::Error;
use crate::bridge::envelope::ErrorCode;

/// Encode `code` for an abort vote.
pub fn encode_refusal(code: &ErrorCode) -> Result<Vec<u8>, Error> {
    let wire = DataPlaneErrorCode::from(code.clone());
    rkyv::to_bytes::<rkyv::rancor::Error>(&wire)
        .map(|bytes| bytes.to_vec())
        .map_err(|e| Error::Internal {
            detail: format!("encode a Calvin stage refusal {code:?}: {e}"),
        })
}

/// Decode the refusal an abort verdict carries.
pub fn decode_refusal(bytes: &[u8]) -> Result<ErrorCode, Error> {
    let mut aligned = rkyv::util::AlignedVec::<16>::with_capacity(bytes.len());
    aligned.extend_from_slice(bytes);
    rkyv::from_bytes::<DataPlaneErrorCode, rkyv::rancor::Error>(&aligned)
        .map(ErrorCode::from)
        .map_err(|e| Error::Internal {
            detail: format!(
                "decode the stage refusal of a Calvin abort verdict ({} bytes): {e}",
                bytes.len()
            ),
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_refusal_survives_the_round_trip() {
        let codes = [
            ErrorCode::RejectedConstraint {
                constraint: "primary_key_immutable".into(),
                detail: "UPDATE cannot change primary key 'id'".into(),
            },
            ErrorCode::RejectedConstraint {
                constraint: "not_null".into(),
                detail: "column 'v' is NOT NULL".into(),
            },
            ErrorCode::BalanceViolation {
                collection: "ledger".into(),
                detail: "debits exceed credits".into(),
            },
        ];
        for code in codes {
            let bytes = encode_refusal(&code).expect("encode");
            assert_eq!(decode_refusal(&bytes).expect("decode"), code);
        }
    }

    #[test]
    fn undecodable_bytes_are_an_internal_error() {
        assert!(matches!(
            decode_refusal(&[0xde, 0xad]),
            Err(Error::Internal { .. })
        ));
    }
}
