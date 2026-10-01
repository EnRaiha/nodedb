// SPDX-License-Identifier: BUSL-1.1

//! Capture of a group 0 [`MetadataImage`].
//!
//! [`MetadataImageCapture::from_live`] runs on the Raft tick thread between
//! metadata apply batches. It opens read transactions and copies the small
//! in-memory cluster state, so the capture holds exactly the applied
//! entries; the rows are read later, off the tick. [`capture_metadata_image`]
//! reads catalogs that no Raft loop writes, for an offline capture.

use std::path::Path;

use nodedb_cluster::{ClusterCatalog, MetadataSnapshotCapture};

use crate::control::cluster::ca_trust::trusted_ca_ders;
use crate::control::cluster::tls::TLS_SUBDIR;
use crate::control::security::catalog::SystemCatalog;
use crate::control::security::catalog::replicated_image::ReplicatedCatalogRead;
use crate::control::state::SharedState;
use crate::event::cdc::OffsetStore;
use crate::event::cdc::consumer_group::state::OffsetImageRead;

use super::format::{MetadataImage, encode_metadata_image, encode_routing};

/// Group 0 state held at one applied index, not yet read out.
pub struct MetadataImageCapture {
    applied_index: u64,
    applied_term: u64,
    cluster_epoch: u64,
    routing: Vec<u8>,
    catalog: ReplicatedCatalogRead,
    offsets: OffsetImageRead,
    trusted_cas: Vec<Vec<u8>>,
}

impl MetadataImageCapture {
    /// Capture this node's live group 0 state at `applied_index`.
    ///
    /// Call only between metadata apply batches: the read transactions see
    /// every commit made so far, and the cluster epoch and routing are read
    /// from memory at the same point.
    pub fn from_live(
        shared: &SharedState,
        applied_index: u64,
        applied_term: u64,
    ) -> crate::Result<Self> {
        let routing = shared
            .cluster_routing
            .as_ref()
            .ok_or_else(|| crate::Error::Internal {
                detail: "group 0 capture: this node has no routing table".into(),
            })?;
        let routing = encode_routing(&routing.read().unwrap_or_else(|p| p.into_inner()))?;
        let cluster_epoch = shared
            .cluster_epoch
            .get()
            .map(|epoch| epoch.applied())
            .ok_or_else(|| crate::Error::Internal {
                detail: "group 0 capture: this node has no cluster epoch state".into(),
            })?;
        Ok(Self {
            applied_index,
            applied_term,
            cluster_epoch,
            routing,
            catalog: shared.credentials.catalog().begin_replicated_read()?,
            offsets: shared.offset_store.begin_image_read()?,
            trusted_cas: trusted_ca_ders(&shared.data_dir.join(TLS_SUBDIR))?,
        })
    }

    /// Read the captured state into an image.
    pub fn into_image(self) -> crate::Result<MetadataImage> {
        Ok(MetadataImage {
            applied_index: self.applied_index,
            applied_term: self.applied_term,
            cluster_epoch: self.cluster_epoch,
            routing: self.routing,
            tables: self.catalog.dump()?,
            consumer_offsets: self.offsets.dump()?,
            trusted_cas: self.trusted_cas,
        })
    }
}

impl MetadataSnapshotCapture for MetadataImageCapture {
    fn serialize(
        self: Box<Self>,
    ) -> std::result::Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let image = self.into_image()?;
        Ok(encode_metadata_image(&image)?)
    }
}

/// Capture a group 0 image from catalogs no Raft loop is writing.
///
/// `applied_index` and `applied_term` name the group 0 entry the catalogs
/// hold state through. The cluster epoch and routing come from `cluster`.
pub fn capture_metadata_image(
    system: &SystemCatalog,
    cluster: &ClusterCatalog,
    offsets: &OffsetStore,
    tls_dir: &Path,
    applied_index: u64,
    applied_term: u64,
) -> crate::Result<MetadataImage> {
    let cluster_err = |e: nodedb_cluster::ClusterError| crate::Error::Internal {
        detail: format!("group 0 capture: read cluster catalog: {e}"),
    };
    let routing = cluster
        .load_routing()
        .map_err(cluster_err)?
        .ok_or_else(|| crate::Error::Internal {
            detail: "group 0 capture: the cluster catalog holds no routing table".into(),
        })?;
    let cluster_epoch = cluster
        .load_cluster_epoch()
        .map_err(cluster_err)?
        .unwrap_or(0);
    Ok(MetadataImage {
        applied_index,
        applied_term,
        cluster_epoch,
        routing: encode_routing(&routing)?,
        tables: system.begin_replicated_read()?.dump()?,
        consumer_offsets: offsets.begin_image_read()?.dump()?,
        trusted_cas: trusted_ca_ders(tls_dir)?,
    })
}
