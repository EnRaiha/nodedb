// SPDX-License-Identifier: BUSL-1.1

//! Publishing a written vector checkpoint generation.
//!
//! A free function rather than a `CoreLoop` method, so the manifest's
//! encoding has one producer beside the load path that reads it.

use super::format::{VECTOR_CKPT_FORMAT_VERSION, VectorCheckpointManifest};
use super::manifest::storage_err;
use super::paths::VECTOR_CKPT_MANIFEST;
use crate::types::replay_stamp::ReplayStamp;

/// Publish a written generation by atomically replacing the manifest.
///
/// This single write is the commit point of the whole checkpoint: before it
/// nothing changed; after it the entire generation is live, holding the
/// records `replay` names. It also
/// fsyncs `ckpt_dir`, the same directory holding the `gen-{n}/` entry, so that
/// entry cannot still be pending when the manifest naming it becomes visible.
pub(crate) fn publish_vector_generation(
    ckpt_dir: &std::path::Path,
    generation: u64,
    replay: ReplayStamp,
) -> crate::Result<()> {
    let manifest = VectorCheckpointManifest {
        format_version: VECTOR_CKPT_FORMAT_VERSION,
        generation,
        replay,
    };
    let bytes = zerompk::to_msgpack_vec(&manifest).map_err(|e| crate::Error::Serialization {
        format: "msgpack".to_string(),
        detail: format!("vector checkpoint manifest encode failed: {e}"),
    })?;
    let path = ckpt_dir.join(VECTOR_CKPT_MANIFEST);
    nodedb_wal::segment::write_checkpoint_framed(ckpt_dir, VECTOR_CKPT_MANIFEST, &bytes)
        .map_err(|e| storage_err(&path, "publish manifest", &e))?;
    Ok(())
}
