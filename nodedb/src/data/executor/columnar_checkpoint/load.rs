// SPDX-License-Identifier: BUSL-1.1

//! The columnar checkpoint load path: decode the published generation whole,
//! install every engine with its flushed segments and surrogate sidecar, and
//! record the replay floor it authorises.

use tracing::info;

use super::format::{COLUMNAR_CKPT_FORMAT_VERSION, ColumnarCheckpointFile};
use super::paths::{columnar_ckpt_dir, columnar_ckpt_gen_dir, parse_columnar_ckpt_stem};
use crate::data::executor::checkpoint_decode_error::CheckpointDecodeError;
use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, Lsn, TenantId};

/// One collection's restored engine plus the two lockstep halves of its flushed
/// state, as handed back by `MutationEngine::from_snapshot`.
///
/// Kept as one struct all the way to installation so the segment blobs and their
/// surrogate table are never in scope as independently-movable values — the
/// lockstep invariant is preserved by never offering the code a way to install
/// one without the other.
struct RestoredColumnar {
    engine: nodedb_columnar::MutationEngine,
    segments: Vec<Vec<u8>>,
    surrogates: nodedb_columnar::mutation::snapshot::FlushedSurrogateTable,
}

/// Every collection in a decoded generation, keyed by engine key.
type DecodedColumnarGeneration = Vec<((DatabaseId, TenantId, String), RestoredColumnar)>;

impl CoreLoop {
    /// Load the columnar checkpoint from disk on startup, BEFORE WAL replay and
    /// BEFORE `seed_columnar_schemas`.
    ///
    /// Reads this core's own checkpoint directory only
    /// (`{data_dir}/columnar-ckpt/core-{core_id}/`), so no core-ownership filter
    /// on the filename is needed — a core only ever sees its own collections.
    ///
    /// Ordering against the schema seed is what keeps the two from fighting:
    /// `seed_columnar_schemas` skips any collection that already has an engine,
    /// so running first means a restored engine keeps the exact schema it was
    /// exported with — including the bitemporal columns the seed would prepend —
    /// and the seed only creates engines for collections this restore did not
    /// cover. Running after the seed would instead leave the seed's empty engine
    /// in place and silently discard the restored rows.
    ///
    /// The manifest's LSN then becomes the replay floor: the columnar replay
    /// arms skip the records already folded in and apply everything above.
    ///
    /// # Fail-stop on corruption
    ///
    /// Columnar is memory-only on both halves — nothing writes the live
    /// memtables or flushed segment bytes to disk outside this checkpoint — so
    /// a published generation is the only non-WAL home of its rows once the
    /// WAL below its LSN has been truncated. A checkpoint that exists but
    /// cannot be read or decoded is therefore unrecoverable data loss: this
    /// returns `Err` in that case instead of skipping it, and the boot
    /// sequence refuses to bring the core up. An absent checkpoint directory
    /// is not an error — WAL replay reconstructs everything.
    pub fn load_columnar_checkpoints(&mut self) -> crate::Result<()> {
        let ckpt_dir = columnar_ckpt_dir(&self.data_dir, self.core_id);
        if !ckpt_dir.exists() {
            return Ok(());
        }
        let Some(manifest) = self.read_columnar_manifest(&ckpt_dir)? else {
            return Ok(());
        };
        let gen_dir = columnar_ckpt_gen_dir(&ckpt_dir, manifest.generation);

        // Decode the WHOLE generation before installing any of it. The manifest
        // promises a complete set at one LSN; installing a subset and claiming
        // that LSN would silently drop the collections that failed to decode,
        // and installing a subset WITHOUT the floor would re-apply every
        // non-idempotent Update record against the rows that did load. Either
        // way the failure must abort boot rather than restore nothing silently
        // — the WAL below this LSN may already be gone.
        let decoded = self.decode_columnar_generation(&gen_dir)?;

        let collections = decoded.len();
        let mut segments = 0usize;
        let mut geometry_rows = 0usize;
        for (key, restored) in decoded {
            let RestoredColumnar {
                engine,
                segments: blobs,
                surrogates,
            } = restored;

            // The R-tree entries for this collection's geometry columns are
            // rebuilt from the restored rows, not carried in the checkpoint —
            // see `geometry_restore.rs`. Done BEFORE the maps are populated so
            // it reads the restored engine and blobs directly and cannot see a
            // half-installed state.
            geometry_rows += self.restore_columnar_geometry_indexes(&key, &engine, &blobs);

            segments += blobs.len();
            // Both halves are installed from one destructured value, in the same
            // iteration, unconditionally. There is no branch on which one of the
            // two maps gets an entry, so the outer-index agreement that
            // `scan_flushed.rs` relies on cannot be broken here.
            self.columnar_flushed_segments.insert(key.clone(), blobs);
            self.columnar_flushed_surrogates
                .insert(key.clone(), surrogates);
            self.columnar_engines.insert(key, engine);
        }

        // Claimed only once every engine is in: the floor suppresses WAL
        // records, so claiming it over a half-restored generation would turn a
        // recoverable read failure into permanent data loss.
        self.floors
            .replay_floors
            .columnar
            .set(Lsn::new(manifest.durable_through_lsn));
        self.floors.columnar_durable_lsn = Lsn::new(manifest.durable_through_lsn);

        info!(
            core = self.core_id,
            generation = manifest.generation,
            collections,
            segments,
            geometry_rows,
            durable_through_lsn = manifest.durable_through_lsn,
            "columnar checkpoint restored"
        );
        Ok(())
    }

    /// Read and decode every collection file in a generation.
    ///
    /// `Err` if any file in the directory is unreadable, unparseable, or carries
    /// an unexpected format version — the caller then restores nothing.
    fn decode_columnar_generation(
        &self,
        gen_dir: &std::path::Path,
    ) -> Result<DecodedColumnarGeneration, CheckpointDecodeError> {
        let entries =
            std::fs::read_dir(gen_dir).map_err(|source| CheckpointDecodeError::ScanDir {
                dir: gen_dir.to_path_buf(),
                source,
            })?;

        let mut decoded = DecodedColumnarGeneration::new();
        for entry in entries {
            let entry = entry.map_err(|source| CheckpointDecodeError::DirEntry { source })?;
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("ckpt") {
                continue;
            }
            let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
            let (database_id, tenant_id, collection) =
                parse_columnar_ckpt_stem(stem).ok_or_else(|| {
                    CheckpointDecodeError::UnparseableFilename {
                        stem: stem.to_string(),
                    }
                })?;

            let bytes = nodedb_wal::segment::read_checkpoint_framed(&path).map_err(|source| {
                CheckpointDecodeError::ReadFile {
                    path: path.clone(),
                    source,
                }
            })?;
            let file =
                zerompk::from_msgpack::<ColumnarCheckpointFile>(&bytes).map_err(|source| {
                    CheckpointDecodeError::MsgpackDecode {
                        path: path.clone(),
                        source,
                    }
                })?;
            if file.format_version != COLUMNAR_CKPT_FORMAT_VERSION {
                return Err(CheckpointDecodeError::FormatVersion {
                    path: path.clone(),
                    found: file.format_version,
                    expected: COLUMNAR_CKPT_FORMAT_VERSION,
                });
            }

            // Rebuilding the engine here, not at install time, is what keeps the
            // generation all-or-nothing: this is the last fallible step, and
            // every one of them must be behind the caller's decision to install.
            let (engine, segments, surrogates) =
                nodedb_columnar::MutationEngine::from_snapshot(file.engine).map_err(|source| {
                    CheckpointDecodeError::EngineNotRebuildable {
                        path: path.clone(),
                        source: Box::new(source),
                    }
                })?;

            // A snapshot written before the surrogate sidecar existed decodes
            // with an empty table while carrying segments. Left as-is, every
            // flushed row would read as `None`-surrogate — which `scan_flushed`
            // already handles — but the two Vecs would disagree in length, and
            // the next export would persist that disagreement. Pad to the blob
            // count with all-`None` rows so the lockstep holds from the first
            // restore onwards, expressing exactly what such a snapshot knows:
            // these segments exist and their identities are unrecorded.
            let mut surrogates = surrogates;
            if surrogates.len() != segments.len() {
                if !surrogates.is_empty() {
                    return Err(CheckpointDecodeError::SurrogateLockstepMismatch {
                        path: path.clone(),
                        segments: segments.len(),
                        surrogates: surrogates.len(),
                    });
                }
                surrogates = segments.iter().map(|_| Vec::new()).collect();
            }

            decoded.push((
                (
                    DatabaseId::new(database_id),
                    TenantId::new(tenant_id),
                    collection,
                ),
                RestoredColumnar {
                    engine,
                    segments,
                    surrogates,
                },
            ));
        }
        Ok(decoded)
    }
}
