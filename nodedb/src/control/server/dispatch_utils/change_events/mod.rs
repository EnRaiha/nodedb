// SPDX-License-Identifier: BUSL-1.1

//! Write-metadata extraction and CDC change-event publishing for dispatched
//! writes.

mod extract;
mod publish;

pub(crate) use publish::{
    WriteChangeSet, extract_write_change_set, publish_change_set, publish_change_set_with_lsn,
    publish_cluster_array_change_events, publish_origin_change_events,
};
