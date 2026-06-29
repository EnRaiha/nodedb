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

/// Decode a [`MetadataEntry`] from bytes.
///
/// Requires a v2 versioned envelope. Rejects bytes without the envelope
/// marker and envelopes with unsupported future version numbers.
pub fn decode_entry(data: &[u8]) -> Result<MetadataEntry, ClusterError> {
    decode_versioned(data).map_err(|e| ClusterError::Codec {
        detail: format!("metadata decode: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_group::entry::{MetadataEntry, RoutingChange, TopologyChange};

    #[test]
    fn metadata_entry_versioned_roundtrip() {
        let entry = MetadataEntry::TopologyChange(TopologyChange::Join {
            node_id: 42,
            addr: "127.0.0.1:7001".to_string(),
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
}
