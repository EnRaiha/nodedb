// SPDX-License-Identifier: BUSL-1.1

//! Versioned, self-describing WAL payloads for CRDT delta records.
//!
//! Writers encode V4 for a signed sync delta and V3 otherwise. Decode accepts
//! those two shapes only. A document delta always carries its row's bound
//! surrogate, and a snapshot import carries no row identity. Any other shape
//! is refused with a typed [`CrdtDeltaWalError`].

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;

const CRDT_DELTA_WAL_FORMAT_V3: u8 = 3;
const CRDT_DELTA_WAL_FORMAT_V4: u8 = 4;

/// Admission metadata of a signed sync delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CrdtDeltaSigning {
    pub auth_user_id: u64,
    pub auth_device_id: u64,
    pub auth_seq_no: u64,
    pub delta_signature: [u8; 32],
    pub required: bool,
}

/// What a CRDT delta record writes into.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CrdtDeltaTarget {
    /// One document's delta. `surrogate` is the row's bound identity and is
    /// never `Surrogate::ZERO`; [`CrdtDeltaTarget::document`] refuses it.
    Document {
        document_id: String,
        surrogate: Surrogate,
    },
    /// A per-collection snapshot import. It binds no row identity.
    Collection,
}

impl CrdtDeltaTarget {
    /// A document target. Refuses `Surrogate::ZERO`: a delta record without
    /// its row's identity cannot rebuild the row's projection on replay.
    pub(crate) fn document(
        document_id: String,
        surrogate: Surrogate,
    ) -> Result<Self, CrdtDeltaWalError> {
        if surrogate == Surrogate::ZERO {
            return Err(CrdtDeltaWalError::UnboundDocument { document_id });
        }
        Ok(Self::Document {
            document_id,
            surrogate,
        })
    }

    /// The wire pair `(document_id, surrogate)`: both set, or both absent.
    fn to_wire(&self) -> (Option<String>, Option<u32>) {
        match self {
            Self::Document {
                document_id,
                surrogate,
            } => (Some(document_id.clone()), Some(surrogate.as_u32())),
            Self::Collection => (None, None),
        }
    }

    fn from_wire(
        document_id: Option<String>,
        surrogate: Option<u32>,
    ) -> Result<Self, CrdtDeltaWalError> {
        match (document_id, surrogate) {
            (Some(document_id), Some(surrogate)) => {
                Self::document(document_id, Surrogate::new(surrogate))
            }
            (None, None) => Ok(Self::Collection),
            (document_id, surrogate) => Err(CrdtDeltaWalError::PartialTarget {
                has_document_id: document_id.is_some(),
                has_surrogate: surrogate.is_some(),
            }),
        }
    }
}

/// A CRDT delta record that cannot be written or replayed.
#[derive(Debug, thiserror::Error)]
pub(crate) enum CrdtDeltaWalError {
    /// A document delta carries `Surrogate::ZERO`.
    #[error("CRDT delta for document '{document_id}' carries no surrogate")]
    UnboundDocument { document_id: String },

    /// The record carries a document id without a surrogate, or the reverse.
    #[error(
        "CRDT delta target is partial (document id present: {has_document_id}, \
         surrogate present: {has_surrogate})"
    )]
    PartialTarget {
        has_document_id: bool,
        has_surrogate: bool,
    },

    /// The payload matches neither current format.
    #[error("CRDT delta payload matches neither the V3 nor the V4 shape")]
    UnknownShape,

    /// The payload did not encode.
    #[error("cannot encode CRDT delta payload: {source}")]
    Encode {
        #[source]
        source: zerompk::Error,
    },
}

/// Normalized CRDT WAL payload used by replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CrdtDeltaWalPayload {
    pub bytes: Vec<u8>,
    pub collection: String,
    pub provenance: Option<SyncProvenance>,
    pub expected_frontier_digest: Option<[u8; 32]>,
    pub target: CrdtDeltaTarget,
    pub signing: Option<CrdtDeltaSigning>,
    /// The peer that produced the delta. Replay validates under it, so a
    /// dead-letter entry replay stores names the same peer the live apply did.
    pub peer_id: u64,
}

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct CrdtDeltaWalPayloadV4 {
    format: u8,
    bytes: Vec<u8>,
    collection: String,
    provenance: Option<SyncProvenance>,
    expected_frontier_digest: Option<[u8; 32]>,
    document_id: Option<String>,
    surrogate: Option<u32>,
    peer_id: u64,
    auth_user_id: u64,
    auth_device_id: u64,
    auth_seq_no: u64,
    delta_signature: [u8; 32],
    signing_required: bool,
}

#[derive(zerompk::ToMessagePack, zerompk::FromMessagePack)]
struct CrdtDeltaWalPayloadV3 {
    format: u8,
    bytes: Vec<u8>,
    collection: String,
    provenance: Option<SyncProvenance>,
    expected_frontier_digest: Option<[u8; 32]>,
    document_id: Option<String>,
    surrogate: Option<u32>,
    peer_id: u64,
}

impl CrdtDeltaWalPayload {
    pub(crate) fn new(
        bytes: Vec<u8>,
        collection: String,
        provenance: Option<SyncProvenance>,
        expected_frontier_digest: Option<[u8; 32]>,
        target: CrdtDeltaTarget,
    ) -> Self {
        Self {
            bytes,
            collection,
            provenance,
            expected_frontier_digest,
            target,
            signing: None,
            peer_id: 0,
        }
    }

    pub(crate) fn with_signing(mut self, signing: CrdtDeltaSigning) -> Self {
        self.signing = Some(signing);
        self
    }

    pub(crate) fn with_peer_id(mut self, peer_id: u64) -> Self {
        self.peer_id = peer_id;
        self
    }

    /// Encode the current explicit wire format.
    pub(crate) fn encode(&self) -> Result<Vec<u8>, CrdtDeltaWalError> {
        let (document_id, surrogate) = self.target.to_wire();
        let encoded = match self.signing {
            Some(signing) => zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV4 {
                format: CRDT_DELTA_WAL_FORMAT_V4,
                bytes: self.bytes.clone(),
                collection: self.collection.clone(),
                provenance: self.provenance.clone(),
                expected_frontier_digest: self.expected_frontier_digest,
                document_id,
                surrogate,
                peer_id: self.peer_id,
                auth_user_id: signing.auth_user_id,
                auth_device_id: signing.auth_device_id,
                auth_seq_no: signing.auth_seq_no,
                delta_signature: signing.delta_signature,
                signing_required: signing.required,
            }),
            None => zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV3 {
                format: CRDT_DELTA_WAL_FORMAT_V3,
                bytes: self.bytes.clone(),
                collection: self.collection.clone(),
                provenance: self.provenance.clone(),
                expected_frontier_digest: self.expected_frontier_digest,
                document_id,
                surrogate,
                peer_id: self.peer_id,
            }),
        };
        encoded.map_err(|source| CrdtDeltaWalError::Encode { source })
    }

    /// Decode V4 or V3. No missing-field defaults or arity widening are used,
    /// and a document delta without its surrogate is refused.
    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, CrdtDeltaWalError> {
        if let Ok(v4) = zerompk::from_msgpack::<CrdtDeltaWalPayloadV4>(bytes)
            && v4.format == CRDT_DELTA_WAL_FORMAT_V4
        {
            return Ok(Self::new(
                v4.bytes,
                v4.collection,
                v4.provenance,
                v4.expected_frontier_digest,
                CrdtDeltaTarget::from_wire(v4.document_id, v4.surrogate)?,
            )
            .with_signing(CrdtDeltaSigning {
                auth_user_id: v4.auth_user_id,
                auth_device_id: v4.auth_device_id,
                auth_seq_no: v4.auth_seq_no,
                delta_signature: v4.delta_signature,
                required: v4.signing_required,
            })
            .with_peer_id(v4.peer_id));
        }
        if let Ok(v3) = zerompk::from_msgpack::<CrdtDeltaWalPayloadV3>(bytes)
            && v3.format == CRDT_DELTA_WAL_FORMAT_V3
        {
            return Ok(Self::new(
                v3.bytes,
                v3.collection,
                v3.provenance,
                v3.expected_frontier_digest,
                CrdtDeltaTarget::from_wire(v3.document_id, v3.surrogate)?,
            )
            .with_peer_id(v3.peer_id));
        }
        Err(CrdtDeltaWalError::UnknownShape)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn document(document_id: &str, surrogate: u32) -> CrdtDeltaTarget {
        CrdtDeltaTarget::document(document_id.into(), Surrogate::new(surrogate))
            .expect("bound document target")
    }

    #[test]
    fn v4_round_trips_authenticated_signing_admission() {
        let payload = CrdtDeltaWalPayload::new(
            vec![8, 9],
            "docs".into(),
            None,
            Some([0x11; 32]),
            document("doc-2", 43),
        )
        .with_signing(CrdtDeltaSigning {
            auth_user_id: 7,
            auth_device_id: 9,
            auth_seq_no: 11,
            delta_signature: [0x22; 32],
            required: true,
        });
        let decoded =
            CrdtDeltaWalPayload::decode(&payload.encode().expect("encode")).expect("decode v4");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn v3_round_trips_fenced_projection_identity() {
        let payload = CrdtDeltaWalPayload::new(
            vec![3, 4],
            "docs".into(),
            None,
            Some([0xa5; 32]),
            document("doc-1", 42),
        );
        let decoded =
            CrdtDeltaWalPayload::decode(&payload.encode().expect("encode")).expect("decode v3");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn a_snapshot_import_round_trips_without_row_identity() {
        let payload = CrdtDeltaWalPayload::new(
            vec![5],
            "docs".into(),
            None,
            None,
            CrdtDeltaTarget::Collection,
        );
        let decoded =
            CrdtDeltaWalPayload::decode(&payload.encode().expect("encode")).expect("decode v3");
        assert_eq!(decoded.target, CrdtDeltaTarget::Collection);
    }

    #[test]
    fn the_producing_peer_round_trips_in_both_current_formats() {
        let unsigned =
            CrdtDeltaWalPayload::new(vec![1], "docs".into(), None, None, document("d", 1))
                .with_peer_id(0xFEED);
        let decoded =
            CrdtDeltaWalPayload::decode(&unsigned.encode().expect("encode")).expect("decode v3");
        assert_eq!(decoded.peer_id, 0xFEED);

        let signed = unsigned.clone().with_signing(CrdtDeltaSigning {
            auth_user_id: 1,
            auth_device_id: 2,
            auth_seq_no: 3,
            delta_signature: [4; 32],
            required: true,
        });
        let decoded =
            CrdtDeltaWalPayload::decode(&signed.encode().expect("encode")).expect("decode v4");
        assert_eq!(decoded.peer_id, 0xFEED);
    }

    #[test]
    fn a_document_target_refuses_the_unbound_surrogate() {
        assert!(matches!(
            CrdtDeltaTarget::document("doc".into(), Surrogate::ZERO),
            Err(CrdtDeltaWalError::UnboundDocument { .. })
        ));
    }

    /// A V3 record carrying a zero surrogate, or only half of a document
    /// target, is refused on decode instead of replaying without identity.
    #[test]
    fn identity_less_document_records_are_refused_on_decode() {
        let wire = |document_id: Option<&str>, surrogate: Option<u32>| {
            zerompk::to_msgpack_vec(&CrdtDeltaWalPayloadV3 {
                format: CRDT_DELTA_WAL_FORMAT_V3,
                bytes: vec![1],
                collection: "docs".into(),
                provenance: None,
                expected_frontier_digest: None,
                document_id: document_id.map(str::to_owned),
                surrogate,
                peer_id: 0,
            })
            .expect("encode")
        };
        assert!(matches!(
            CrdtDeltaWalPayload::decode(&wire(Some("doc"), Some(0))),
            Err(CrdtDeltaWalError::UnboundDocument { .. })
        ));
        assert!(matches!(
            CrdtDeltaWalPayload::decode(&wire(Some("doc"), None)),
            Err(CrdtDeltaWalError::PartialTarget { .. })
        ));
        assert!(matches!(
            CrdtDeltaWalPayload::decode(&wire(None, Some(7))),
            Err(CrdtDeltaWalError::PartialTarget { .. })
        ));
    }

    /// The pre-surrogate three-field shape decodes to no current format.
    #[test]
    fn retired_shapes_are_refused() {
        #[derive(zerompk::ToMessagePack)]
        struct RetiredPayload {
            bytes: Vec<u8>,
            collection: Option<String>,
            provenance: Option<SyncProvenance>,
        }
        let bytes = zerompk::to_msgpack_vec(&RetiredPayload {
            bytes: vec![1, 2],
            collection: Some("docs".into()),
            provenance: None,
        })
        .expect("encode");
        assert!(matches!(
            CrdtDeltaWalPayload::decode(&bytes),
            Err(CrdtDeltaWalError::UnknownShape)
        ));
    }
}
