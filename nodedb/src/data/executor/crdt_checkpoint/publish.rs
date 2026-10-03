// SPDX-License-Identifier: BUSL-1.1

//! Publishing a written CRDT checkpoint generation.
//!
//! Lives here rather than beside the writer so the manifest's encoding has
//! exactly one producer and one consumer, in the same module: the writer in
//! `handlers/control/checkpoint_crdt.rs` fills a generation directory, then
//! calls this to make it live.

use super::format::{CRDT_CKPT_FORMAT_VERSION, CrdtCheckpointManifest};
use super::manifest::storage_err;
use super::paths::CRDT_CKPT_MANIFEST;
use crate::types::Lsn;

/// Publish a written generation by atomically replacing the manifest.
///
/// This single write is the commit point of the whole checkpoint: before it
/// nothing changed; after it the entire generation is live at one LSN. It also
/// fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry, so that
/// entry cannot still be pending when the manifest naming it becomes visible.
pub(crate) fn publish_crdt_generation(
    ckpt_dir: &std::path::Path,
    generation: u64,
    durable_through: Lsn,
) -> crate::Result<()> {
    let manifest = CrdtCheckpointManifest {
        format_version: CRDT_CKPT_FORMAT_VERSION,
        generation,
        durable_through_lsn: durable_through.as_u64(),
    };
    let bytes = zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: format!("CRDT checkpoint manifest encode failed: {e}"),
    })?;
    let path = ckpt_dir.join(CRDT_CKPT_MANIFEST);
    nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, CRDT_CKPT_MANIFEST, &bytes)
        .map_err(|e| storage_err(&path, "publish manifest", &e))?;
    Ok(())
}
