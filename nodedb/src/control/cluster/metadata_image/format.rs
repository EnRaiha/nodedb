// SPDX-License-Identifier: BUSL-1.1

//! The metadata Raft group 0 snapshot image and its wire codec.
//!
//! One image is the replicated state of group 0 at one applied index:
//! - every `_system.*` table group 0 replicates, as raw rows (the list and
//!   the excluded tables are in
//!   [`crate::control::security::catalog::replicated_image`]);
//! - the committed consumer offsets (`event_plane/consumer_offsets.redb`);
//! - the overlap CA trust set (`tls/ca.d/*.crt`), as DER;
//! - the cluster catalog keys group 0 drives: the applied cluster epoch and
//!   the routing table.
//!
//! [`encode_metadata_image`] and [`decode_metadata_image`] are the only
//! codec. The Raft snapshot path and point-in-time restore both use them.

use crate::control::security::catalog::replicated_image::RawRows;

/// Group 0 state at `applied_index`.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub struct MetadataImage {
    /// The last group 0 log index the image includes.
    pub applied_index: u64,
    /// The term of the entry at `applied_index`.
    pub applied_term: u64,
    /// The cluster epoch applied through `applied_index`.
    pub cluster_epoch: u64,
    /// The routing table, encoded as `ClusterCatalog::save_routing` stores it.
    pub routing: Vec<u8>,
    /// `(label, rows)` for every replicated `_system.*` table.
    pub tables: Vec<(String, RawRows)>,
    /// `(key, encoded offset)` for every committed consumer offset.
    pub consumer_offsets: Vec<(String, Vec<u8>)>,
    /// DER of every overlap CA in the trust set.
    pub trusted_cas: Vec<Vec<u8>>,
}

impl MetadataImage {
    /// The routing table the image carries.
    pub fn routing_table(&self) -> crate::Result<nodedb_cluster::RoutingTable> {
        zerompk::from_msgpack(&self.routing).map_err(|e| crate::Error::Internal {
            detail: format!("metadata image: decode routing table: {e}"),
        })
    }
}

/// Encode `image` for the wire or for storage.
pub fn encode_metadata_image(image: &MetadataImage) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(image).map_err(|e| crate::Error::Internal {
        detail: format!(
            "encode metadata image at index {}: {e}",
            image.applied_index
        ),
    })
}

/// Decode an image [`encode_metadata_image`] produced.
pub fn decode_metadata_image(bytes: &[u8]) -> crate::Result<MetadataImage> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Internal {
        detail: format!("decode metadata image: {e}"),
    })
}

/// Encode `routing` the way the image carries it.
pub fn encode_routing(routing: &nodedb_cluster::RoutingTable) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(routing).map_err(|e| crate::Error::Internal {
        detail: format!("metadata image: encode routing table: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_image_round_trips() {
        let routing = nodedb_cluster::RoutingTable::uniform(2, &[1, 2], 2);
        let image = MetadataImage {
            applied_index: 42,
            applied_term: 3,
            cluster_epoch: 5,
            routing: encode_routing(&routing).unwrap(),
            tables: vec![("users".into(), vec![(vec![1], vec![2, 3])])],
            consumer_offsets: vec![("v2:0:1".into(), vec![9; 16])],
            trusted_cas: vec![vec![7; 8]],
        };
        let decoded = decode_metadata_image(&encode_metadata_image(&image).unwrap()).unwrap();
        assert_eq!(decoded, image);
        assert_eq!(
            decoded.routing_table().unwrap().num_groups(),
            routing.num_groups()
        );
    }
}
