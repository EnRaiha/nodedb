// SPDX-License-Identifier: BUSL-1.1

//! Serialize / deserialize helpers for [`MetadataEntry`].
//!
//! Entries are wrapped in a [`crate::wire_version::Versioned`] envelope so
//! future variant additions can be detected and rejected cleanly on older
//! nodes rather than silently misinterpreted.

use crate::error::ClusterError;
use crate::metadata_group::entry::MetadataEntry;
use crate::wire_version::{decode_versioned, encode_versioned};

/// Encode a [`MetadataEntry`] into a v2 versioned wire envelope.
pub fn encode_entry(entry: &MetadataEntry) -> Result<Vec<u8>, ClusterError> {
    encode_versioned(entry).map_err(|e| ClusterError::Codec {
        detail: format!("metadata encode: {e}"),
    })
}

/// First byte of a stamped entry: `[STAMP_MARKER][hlc u64 BE][envelope]`.
const STAMP_MARKER: u8 = 0xC7;

/// Length of the stamp prefix.
const STAMP_LEN: usize = 1 + 8;

/// Prefix the encoded entry `bytes` with the HLC wall time, in nanoseconds,
/// the metadata leader stamped as it appended the entry. A restore keeps a
/// stamped entry only when the stamp is below its watermark.
pub fn stamp_entry(bytes: &[u8], hlc: u64) -> Vec<u8> {
    let mut stamped = Vec::with_capacity(STAMP_LEN + bytes.len());
    stamped.push(STAMP_MARKER);
    stamped.extend_from_slice(&hlc.to_be_bytes());
    stamped.extend_from_slice(bytes);
    stamped
}

/// The leader's HLC stamp of an encoded entry, `None` for an unstamped one.
pub fn entry_stamp(data: &[u8]) -> Option<u64> {
    if data.first() != Some(&STAMP_MARKER) {
        return None;
    }
    let raw: [u8; 8] = data.get(1..STAMP_LEN)?.try_into().ok()?;
    Some(u64::from_be_bytes(raw))
}

/// The versioned envelope of `data`, past any stamp.
fn envelope(data: &[u8]) -> &[u8] {
    match entry_stamp(data) {
        Some(_) => &data[STAMP_LEN..],
        None => data,
    }
}

/// Decode a [`MetadataEntry`] from bytes, stamped or not.
///
/// Requires a v2 versioned envelope. Rejects bytes without the envelope
/// marker and envelopes with unsupported future version numbers.
pub fn decode_entry(data: &[u8]) -> Result<MetadataEntry, ClusterError> {
    decode_versioned(envelope(data)).map_err(|e| ClusterError::Codec {
        detail: format!("metadata decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::entry::{
        MetadataEntry, PendingDdlObject, RoutingChange, TopologyChange,
    };
    use nodedb_types::Hlc;

    #[test]
    fn metadata_entry_versioned_roundtrip() {
        let entry = MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 42,
            addr: "127.0.0.1:7001".to_string(),
            swim_addr: Some("127.0.0.1:7002".to_string()),
        });
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn routing_change_set_placement_roundtrip() {
        let entry = MetadataEntry::RoutingChange(RoutingChange::SetPlacement {
            group_id: 3,
            placement: vec![10, 20, 30],
        });
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn ddl_pending_propose_roundtrip() {
        let entry = MetadataEntry::DdlPendingPropose {
            token: 7,
            objects: vec![
                PendingDdlObject::Create {
                    entry: Box::new(MetadataEntry::CatalogDdl {
                        payload: vec![1, 2, 3],
                    }),
                },
                PendingDdlObject::Alter {
                    entry: Box::new(MetadataEntry::CatalogDdl {
                        payload: vec![4, 5],
                    }),
                    before_image: vec![9, 9, 9],
                },
            ],
            proposed_at: Hlc::default(),
        };
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn ddl_pending_finalize_roundtrip() {
        let entry = MetadataEntry::DdlPendingFinalize { token: 11 };
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn a_stamped_entry_decodes_and_reports_its_stamp() {
        let entry = MetadataEntry::DdlPendingFinalize { token: 3 };
        let bytes = encode_entry(&entry).unwrap();
        assert_eq!(entry_stamp(&bytes), None);
        let stamped = stamp_entry(&bytes, 0x0102_0304_0506_0708);
        assert_eq!(entry_stamp(&stamped), Some(0x0102_0304_0506_0708));
        assert_eq!(decode_entry(&stamped).unwrap(), entry);
        assert_eq!(
            entry_stamp(&stamped[..4]),
            None,
            "a short stamp is no stamp"
        );
    }

    #[test]
    fn ddl_pending_cancel_roundtrip() {
        let entry = MetadataEntry::DdlPendingCancel { token: 12 };
        let bytes = encode_entry(&entry).unwrap();
        let decoded = decode_entry(&bytes).unwrap();
        assert_eq!(entry, decoded);
    }
}
