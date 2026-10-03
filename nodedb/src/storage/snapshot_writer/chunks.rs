// SPDX-License-Identifier: BUSL-1.1

//! Content-addressed snapshot chunks.
//!
//! Every chunk lives once at `chunks/{id}`, beside the snapshot prefixes of
//! one store. A base uploads only the chunks the store lacks and lists every
//! chunk it needs, so each manifest restores on its own.

use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use futures::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

use super::cdc::{CdcParams, cut_ranges};
use super::chunk_id::{ChunkKeyer, is_chunk_id};
use super::object_envelope::{
    CHUNK_HEADROOM, MAX_SNAPSHOT_OBJECT_BYTES, ObjectContext, SNAPSHOT_CHUNK_KIND,
    check_snapshot_object_size, decrypt_snapshot_object, encrypt_snapshot_object,
};

/// The largest chunk payload that always fits one snapshot object.
pub(super) const MAX_CHUNK_BYTES: usize = (MAX_SNAPSHOT_OBJECT_BYTES - CHUNK_HEADROOM) as usize;

/// The directory of every chunk in a snapshot store. It holds no manifest,
/// so snapshot discovery never lists it as a snapshot.
pub const CHUNK_DIR: &str = "chunks";

/// One chunk of a snapshot image, as the manifest lists it.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ChunkRef {
    /// Keyed hash of the plaintext, and the object name under [`CHUNK_DIR`].
    pub id: String,
    /// Plaintext length.
    pub len: u64,
}

/// The object key of chunk `id`.
pub fn chunk_path(id: &str) -> ObjectPath {
    ObjectPath::from(format!("{CHUNK_DIR}/{id}"))
}

/// One image cut into chunks: each chunk's ref and its byte range.
#[derive(Debug, Clone)]
pub(super) struct ImageChunks {
    pub refs: Vec<ChunkRef>,
    pub ranges: Vec<Range<usize>>,
}

/// Cut `bytes` at content-defined boundaries and id every chunk.
pub(super) fn cut_image(
    bytes: &[u8],
    params: CdcParams,
    keyer: &ChunkKeyer,
) -> crate::Result<ImageChunks> {
    let ranges = cut_ranges(bytes, params);
    let mut refs = Vec::with_capacity(ranges.len());
    for range in &ranges {
        let piece = bytes.get(range.clone()).unwrap_or_default();
        refs.push(ChunkRef {
            id: keyer.id(piece)?,
            len: piece.len() as u64,
        });
    }
    Ok(ImageChunks { refs, ranges })
}

/// What one base wrote to the chunk directory.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChunkUploads {
    /// Chunks put to the store.
    pub uploaded: u64,
    /// Plaintext bytes of the uploaded chunks.
    pub uploaded_bytes: u64,
    /// Distinct chunks the store already held before this base.
    pub reused: u64,
}

/// Where one base puts its chunks.
pub(super) struct ChunkWrite<'a> {
    pub store: &'a Arc<dyn ObjectStore>,
    pub node_name: &'a str,
    pub watermark: u64,
    pub key: &'a nodedb_wal::crypto::WalEncryptionKey,
}

/// Put every chunk of `image` the store lacks.
///
/// `present` holds the ids this base already found or wrote, so a chunk shared
/// by two images is checked once. The caller pins every id first, so no
/// garbage collection removes a chunk between its check and the manifest.
pub(super) async fn put_missing_chunks(
    w: &ChunkWrite<'_>,
    image: &[u8],
    chunks: &ImageChunks,
    present: &mut HashSet<String>,
    uploads: &mut ChunkUploads,
) -> crate::Result<()> {
    for (chunk, range) in chunks.refs.iter().zip(&chunks.ranges) {
        if present.contains(&chunk.id) {
            continue;
        }
        let path = chunk_path(&chunk.id);
        match w.store.head(&path).await {
            Ok(_) => uploads.reused += 1,
            Err(object_store::Error::NotFound { .. }) => {
                let piece = image.get(range.clone()).unwrap_or_default();
                let ctx = ObjectContext {
                    name: &chunk.id,
                    kind: SNAPSHOT_CHUNK_KIND,
                };
                let payload = encrypt_snapshot_object(piece, ctx, w.node_name, w.watermark, w.key)?;
                w.store
                    .put(&path, PutPayload::from(payload))
                    .await
                    .map_err(|e| storage(format!("put {path}: {e}")))?;
                uploads.uploaded += 1;
                uploads.uploaded_bytes += chunk.len;
            }
            Err(e) => return Err(storage(format!("head {path}: {e}"))),
        }
        present.insert(chunk.id.clone());
    }
    Ok(())
}

/// Fetch, authenticate, and check every chunk of one image, then join them.
///
/// A chunk opens only under its own id, and its plaintext must hash back to
/// that id, so no object can stand in for another.
pub(super) async fn fetch_chunked(
    store: &Arc<dyn ObjectStore>,
    chunks: &[ChunkRef],
    key: &nodedb_wal::crypto::WalEncryptionKey,
) -> crate::Result<Vec<u8>> {
    let keyer = ChunkKeyer::new(key)?;
    // Grown per checked chunk: the manifest lengths are not trusted for an
    // up-front allocation.
    let mut out = Vec::new();
    for chunk in chunks {
        let path = chunk_path(&chunk.id);
        let result = store
            .get(&path)
            .await
            .map_err(|e| storage(format!("get {path}: {e}")))?;
        check_snapshot_object_size(result.meta.size, &chunk.id)?;
        let raw = result
            .bytes()
            .await
            .map_err(|e| storage(format!("read {path}: {e}")))?;
        let ctx = ObjectContext {
            name: &chunk.id,
            kind: SNAPSHOT_CHUNK_KIND,
        };
        let payload = decrypt_snapshot_object(&raw, ctx, key)?;
        if payload.len() as u64 != chunk.len || keyer.id(&payload)? != chunk.id {
            return Err(crate::Error::SegmentCorrupted {
                detail: format!(
                    "snapshot chunk {path} does not match its manifest entry \
                     (len {}, expected len {})",
                    payload.len(),
                    chunk.len
                ),
            });
        }
        out.extend_from_slice(&payload);
    }
    Ok(out)
}

/// Every chunk id in the store, in no order. An object under [`CHUNK_DIR`]
/// whose name is not a chunk id is skipped: this module never wrote it.
pub async fn list_chunk_ids(store: &Arc<dyn ObjectStore>) -> crate::Result<Vec<String>> {
    let dir = ObjectPath::from(CHUNK_DIR);
    let objects: Vec<_> = store
        .list(Some(&dir))
        .try_collect()
        .await
        .map_err(|e| storage(format!("list {dir}: {e}")))?;
    Ok(objects
        .into_iter()
        .filter_map(|meta| meta.location.filename().map(str::to_owned))
        .filter(|name| is_chunk_id(name))
        .collect())
}

/// Delete chunk `id`. A chunk already gone is not an error.
pub async fn delete_chunk(store: &Arc<dyn ObjectStore>, id: &str) -> crate::Result<()> {
    let path = chunk_path(id);
    match store.delete(&path).await {
        Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
        Err(e) => Err(storage(format!("delete {path}: {e}"))),
    }
}

fn storage(detail: String) -> crate::Error {
    crate::Error::Storage {
        engine: "snapshot".into(),
        detail,
    }
}
