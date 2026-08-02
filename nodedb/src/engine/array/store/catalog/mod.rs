// SPDX-License-Identifier: BUSL-1.1

//! Per-array LSM store — manifest, memtable, open segment handles.
//!
//! Each [`ArrayStore`] manages one array's directory. The engine in
//! `engine.rs` keeps a `HashMap<ArrayId, ArrayStore>`. Stores are
//! Data-Plane only (`!Send`-compatible — no atomics, no shared mutability).

mod scan;
#[cfg(test)]
mod tests;
mod versions;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::cell_payload::CellPayload;
use nodedb_array::types::coord::value::CoordValue;

use nodedb_wal::crypto::WalEncryptionKey;

use super::manifest::{Manifest, ManifestError, SegmentRef, segment_path};
use super::segment_handle::{SegmentHandle, SegmentHandleError};
use crate::engine::array::memtable::Memtable;

/// One open array. Owns the directory layout below `root`:
///
/// ```text
/// <root>/manifest.ndam
/// <root>/<segment-id-1>.ndas
/// <root>/<segment-id-2>.ndas
/// ...
/// ```
/// One materialized cell version returned by an all-versions scan:
/// `(hilbert_prefix, coord, system_from_ms, payload)`.
pub type CellVersion = (u64, Vec<CoordValue>, i64, CellPayload);

pub struct ArrayStore {
    root: PathBuf,
    schema: Arc<ArraySchema>,
    schema_hash: u64,
    manifest: Manifest,
    pub(crate) memtable: Memtable,
    pub(crate) segments: HashMap<String, SegmentHandle>,
    next_segment_seq: u64,
    /// At-rest encryption key for SEGA segment envelopes. When `Some`,
    /// all segment opens use AES-256-GCM decryption.
    kek: Option<WalEncryptionKey>,
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayStoreError {
    #[error(transparent)]
    Manifest(#[from] ManifestError),
    #[error(transparent)]
    Segment(#[from] SegmentHandleError),
    #[error("array store io: {detail}")]
    Io { detail: String },
    #[error("schema_hash mismatch: store={store:x} new={new:x}")]
    SchemaHashMismatch { store: u64, new: u64 },
}

impl ArrayStore {
    /// Open or create the array store. Loads the manifest if present;
    /// opens every referenced segment and validates schema_hash.
    ///
    /// `kek` is a constructor input rather than a later `set_kek` call because
    /// the segments named by the manifest are opened right here: an at-rest
    /// encrypted (`SEGA`) segment opened without the key is a typed error, so
    /// installing the key afterwards would make every array that had ever
    /// flushed unopenable — and the WAL backing those cells is already gone,
    /// truncated by the checkpoint that the flush advanced.
    pub fn open(
        root: PathBuf,
        schema: Arc<ArraySchema>,
        schema_hash: u64,
        kek: Option<WalEncryptionKey>,
    ) -> Result<Self, ArrayStoreError> {
        std::fs::create_dir_all(&root).map_err(|e| ArrayStoreError::Io {
            detail: format!("mkdir {root:?}: {e}"),
        })?;
        let manifest = Manifest::load_or_new(&root, schema_hash)?;
        if manifest.schema_hash != schema_hash && !manifest.segments.is_empty() {
            return Err(ArrayStoreError::SchemaHashMismatch {
                store: manifest.schema_hash,
                new: schema_hash,
            });
        }
        let mut segments = HashMap::with_capacity(manifest.segments.len());
        let mut max_seq: u64 = 0;
        for seg in &manifest.segments {
            let h = SegmentHandle::open(
                &segment_path(&root, &seg.id),
                seg.id.clone(),
                schema_hash,
                kek.as_ref(),
            )?;
            if let Some(seq) = parse_segment_seq(&seg.id) {
                max_seq = max_seq.max(seq);
            }
            segments.insert(seg.id.clone(), h);
        }
        Ok(Self {
            root,
            schema,
            schema_hash,
            manifest,
            memtable: Memtable::new(),
            segments,
            next_segment_seq: max_seq + 1,
            kek,
        })
    }

    /// Install the at-rest encryption key on an already-open store.
    ///
    /// This covers key installation that happens *after* a store is open, so
    /// it applies to segments opened from here on — flushes, installs, and
    /// replacements. Handles opened before this call keep their existing
    /// backing, which is correct: those files were written without the key and
    /// re-opening them with one would (rightly) be rejected as plaintext.
    /// Segments named by the manifest at open time take the key through
    /// [`ArrayStore::open`] instead.
    pub fn set_kek(&mut self, kek: WalEncryptionKey) {
        self.kek = Some(kek);
    }

    pub fn kek(&self) -> Option<&WalEncryptionKey> {
        self.kek.as_ref()
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn schema(&self) -> &Arc<ArraySchema> {
        &self.schema
    }

    pub fn schema_hash(&self) -> u64 {
        self.schema_hash
    }

    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    pub fn manifest_mut(&mut self) -> &mut Manifest {
        &mut self.manifest
    }

    /// Allocate the next segment file name and bump the sequence.
    pub fn allocate_segment_id(&mut self) -> String {
        let seq = self.next_segment_seq;
        self.next_segment_seq += 1;
        format!("{seq:010}.ndas")
    }

    /// Register a freshly-flushed (or freshly-merged) segment. The file
    /// must already exist on disk. Updates the manifest in-memory only;
    /// callers must call [`ArrayStore::persist_manifest`] afterwards.
    pub fn install_segment(&mut self, seg: SegmentRef) -> Result<(), ArrayStoreError> {
        let h = SegmentHandle::open(
            &segment_path(&self.root, &seg.id),
            seg.id.clone(),
            self.schema_hash,
            self.kek.as_ref(),
        )?;
        self.segments.insert(seg.id.clone(), h);
        self.manifest.append(seg);
        Ok(())
    }

    /// Remove segments from the manifest and drop their handles. The
    /// underlying file is deleted only after the manifest is persisted
    /// (caller's responsibility — see [`ArrayStore::unlink_segment`]).
    pub fn replace_segments(
        &mut self,
        removed: &[String],
        added: Vec<SegmentRef>,
    ) -> Result<(), ArrayStoreError> {
        let mut new_handles = Vec::with_capacity(added.len());
        for seg in &added {
            let h = SegmentHandle::open(
                &segment_path(&self.root, &seg.id),
                seg.id.clone(),
                self.schema_hash,
                self.kek.as_ref(),
            )?;
            new_handles.push(h);
        }
        self.manifest.replace(removed, added);
        for id in removed {
            self.segments.remove(id);
        }
        for h in new_handles {
            self.segments.insert(h.id().to_string(), h);
        }
        Ok(())
    }

    pub fn persist_manifest(&self) -> Result<(), ArrayStoreError> {
        self.manifest.persist(&self.root)?;
        Ok(())
    }

    pub fn unlink_segment(&self, id: &str) -> Result<(), ArrayStoreError> {
        let path = segment_path(&self.root, id);
        match std::fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(ArrayStoreError::Io {
                detail: format!("unlink {path:?}: {e}"),
            }),
        }
    }
}

fn parse_segment_seq(id: &str) -> Option<u64> {
    id.split_once('.').and_then(|(stem, _)| stem.parse().ok())
}
