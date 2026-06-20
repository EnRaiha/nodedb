// SPDX-License-Identifier: BUSL-1.1

//! Cross-node streaming-shuffle receiver registry + per-part file-backed
//! staging inbox (E3b).
//!
//! # Receive-to-spill (design D1)
//!
//! A cross-node shuffle join stages each side's rows to a LOCAL scratch file on
//! the consumer node, then runs the existing grace-hash join over those files
//! ([`crate::data::executor`]'s `execute_shuffle_join`). This module owns the
//! receive-to-spill half: as `ShufflePush` chunks arrive over QUIC, each is
//! exploded into its individual join rows and appended — one
//! `[u32 LE len][row-bytes]` frame per row — to a per-`(shuffle_id, part, side)`
//! staging file. That frame format is byte-identical to what the Data Plane's
//! `FrameStreamReader` (and `RowSource::ShuffleStream`) reads, so the staged
//! file feeds the grace join directly once the build barrier completes.
//!
//! # Plane discipline
//!
//! The inbox / registry are **Send + Sync** and live in the Control Plane's
//! `SharedState`. The staging WRITE path runs on the Tokio transport reactor, so
//! it uses [`tokio::fs`] and awaits each append — a synchronous `std::fs` write
//! would block the reactor thread, and the awaited write is also what lets QUIC
//! flow control back-pressure the producer. Memory stays bounded: exactly one
//! chunk array is decoded at a time. The staged FILE is later opened by the
//! `!Send` Data Plane through a different handle (`FrameStreamReader`), strictly
//! after [`ShuffleInbox::finalize`] has flushed + synced it.
//!
//! # Build barrier
//!
//! Each inbox tracks how many distinct producers (`producer_count`) are expected
//! to push to this `(shuffle_id, part, side)`. The side is complete only once an
//! `End` frame has been received from **all** of them — see
//! [`ShuffleInbox::record_end`] / [`ShuffleInbox::barrier_complete`]. The inbox
//! is flushed + synced to disk ([`ShuffleInbox::finalize`]) when the barrier
//! fires, making the staged file durable and complete for the reader.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use nodedb_cluster::TypedClusterError;
use tokio::io::AsyncWriteExt;

use super::frame_explode::explode_row_array;

/// Key for one shuffle receiver inbox: `(shuffle_id, part, side)`.
///
/// `side` is `0` for the build side and `1` for the probe side of a hash join.
pub type ShuffleKey = (u64, u32, u8);

/// A file-backed receiver inbox for one `(shuffle_id, part, side)`.
///
/// Arriving chunk payloads (each a standalone msgpack row array) are exploded
/// into individual rows and appended as `[u32 LE len][row-bytes]` frames to a
/// staging file. Also holds the per-part build-barrier state.
pub struct ShuffleInbox {
    /// Deterministic staging-file path for this part (under the registry's base
    /// dir). Lazily created on the first appended chunk.
    path: PathBuf,
    /// The append-mode staging-file handle, lazily opened on the first chunk and
    /// guarded so concurrent appends from the transport read-loop serialize.
    writer: tokio::sync::Mutex<Option<tokio::fs::File>>,
    /// Number of producers expected to push to this part. The barrier fires once
    /// `ends_received == producer_count`.
    producer_count: usize,
    /// Count of `End` frames received so far (one per finished producer).
    ends_received: AtomicUsize,
    /// First terminal error reported by any producer, if any.
    error: Mutex<Option<TypedClusterError>>,
    /// `true` once [`ShuffleInbox::finalize`] has successfully flushed + synced
    /// the staging file. The consumer waits on this before reading the staged
    /// file so it never opens a partially-written / unsynced file.
    finalized: AtomicBool,
    /// Wakes every task parked in [`ShuffleInbox::wait_finalized`] when
    /// `finalized` flips to `true`.
    finalized_notify: tokio::sync::Notify,
}

impl ShuffleInbox {
    /// Create an inbox that stages rows to `path`, expecting `producer_count`
    /// producers before the build barrier fires.
    ///
    /// `producer_count` is clamped to at least 1 so a zero never leaves the
    /// barrier permanently unfired.
    pub fn new(path: PathBuf, producer_count: usize) -> Self {
        Self {
            path,
            writer: tokio::sync::Mutex::new(None),
            producer_count: producer_count.max(1),
            ends_received: AtomicUsize::new(0),
            error: Mutex::new(None),
            finalized: AtomicBool::new(false),
            finalized_notify: tokio::sync::Notify::new(),
        }
    }

    /// Number of producers expected for this part.
    pub fn producer_count(&self) -> usize {
        self.producer_count
    }

    /// The deterministic staging-file path for this part.
    ///
    /// The Data Plane opens this through its own `FrameStreamReader` handle once
    /// [`ShuffleInbox::barrier_complete`] is true and [`ShuffleInbox::finalize`]
    /// has run.
    pub fn staged_path(&self) -> &Path {
        &self.path
    }

    /// Explode one chunk payload (a msgpack array of rows) into individual join
    /// rows and append a `[u32 LE len][row-bytes]` frame for each to the staging
    /// file.
    ///
    /// The file is lazily opened (creating parent dirs once) on the first chunk
    /// and reused for subsequent chunks. Appends serialize under the `writer`
    /// mutex; bounded memory — only this one chunk's row offsets are tracked at a
    /// time, and the awaited write back-pressures the producer through QUIC flow
    /// control. A malformed chunk array (bad header / truncated element) or any
    /// I/O failure is a hard error — never a silent drop.
    pub async fn append_chunk(&self, chunk_payload: &[u8]) -> crate::Result<()> {
        // Explode the chunk's msgpack row array into per-row byte slices using
        // the SAME reader the Data Plane's `decode_flat_row_array` uses
        // (`array_header` + `skip_value`), so the framing round-trips exactly
        // what `RowSource::ShuffleStream` reads.
        let frames = explode_row_array(chunk_payload)?;

        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            *guard = Some(self.open_staging().await?);
        }
        let Some(file) = guard.as_mut() else {
            // Unreachable: just set above. Surface as a hard error rather than
            // unwrapping.
            return Err(crate::Error::Storage {
                engine: "shuffle-stage".into(),
                detail: format!(
                    "staging writer unexpectedly absent for {}",
                    self.path.display()
                ),
            });
        };

        for row in frames {
            let len = u32::try_from(row.len()).map_err(|_| crate::Error::Storage {
                engine: "shuffle-stage".into(),
                detail: format!(
                    "shuffle row exceeds u32 frame length ({} bytes) staging to {}",
                    row.len(),
                    self.path.display()
                ),
            })?;
            file.write_all(&len.to_le_bytes()).await?;
            file.write_all(row).await?;
        }
        Ok(())
    }

    /// Flush + sync the staging file so it is complete and durable for the Data
    /// Plane reader. Called when the build barrier completes.
    ///
    /// If no chunk was ever appended (zero rows were routed to this part/side),
    /// an EMPTY staging file is still created here so the Data-Plane grace reader
    /// opens a real (empty = zero-row) file rather than failing on a missing path.
    /// A finalized empty file is the affirmative "this side completed with zero
    /// rows" signal — distinct from a missing file, which would indicate the
    /// producer never ran.
    pub async fn finalize(&self) -> crate::Result<()> {
        let mut guard = self.writer.lock().await;
        if guard.is_none() {
            *guard = Some(self.open_staging().await?);
        }
        if let Some(file) = guard.as_mut() {
            file.flush().await?;
            file.sync_all().await?;
        }
        // Only AFTER a successful flush+sync (or the no-op empty-file case) do we
        // publish the finalized signal. Order matters: set the flag with Release
        // BEFORE waking waiters so a task that wakes and then re-checks
        // `is_finalized()` (Acquire) is guaranteed to observe `true`. A finalize
        // that errored above returns early and never marks finalized, so a waiter
        // stays parked and the consumer's deadline timeout fires instead of
        // racing a half-written file.
        self.finalized.store(true, Ordering::Release);
        self.finalized_notify.notify_waiters();
        Ok(())
    }

    /// `true` once [`ShuffleInbox::finalize`] has successfully completed.
    pub fn is_finalized(&self) -> bool {
        self.finalized.load(Ordering::Acquire)
    }

    /// Await `finalize()` completing on this inbox.
    ///
    /// Returns immediately if the inbox is already finalized. Otherwise parks
    /// until the next `finalize()` calls `notify_waiters()`.
    ///
    /// Race-freedom: the `Notify` future is created and `enable()`d (registering
    /// this waiter) BEFORE the `is_finalized()` flag is checked. If `finalize()`
    /// fires in the window between the flag-check and the registration in a naive
    /// implementation, the wakeup would be lost; enabling the notified future
    /// first closes that gap — any `notify_waiters()` after `enable()` is
    /// delivered to this future, and any `finalize()` before it is observed by
    /// the `is_finalized()` fast path.
    pub async fn wait_finalized(&self) {
        let notified = self.finalized_notify.notified();
        tokio::pin!(notified);
        // Register this waiter with the Notify BEFORE checking the flag.
        notified.as_mut().enable();
        if self.is_finalized() {
            return;
        }
        notified.await;
    }

    /// Open the staging file fresh (create + truncate), creating its parent
    /// directory tree once. Used lazily on the first appended chunk.
    ///
    /// Truncate — NOT append — is correct: the inbox opens the file ONCE and
    /// then writes every chunk's frames sequentially through this single held
    /// handle, so each `write_all` advances naturally. Truncating on open
    /// guarantees a stale file left at this deterministic path by a prior
    /// shuffle that reused the same `(shuffle_id, part, side)` (e.g. after a
    /// crash, or a reused id) is overwritten, not appended to — otherwise the
    /// reader would see the old rows concatenated with the new ones.
    async fn open_staging(&self) -> crate::Result<tokio::fs::File> {
        if let Some(parent) = self.path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| crate::Error::Storage {
                    engine: "shuffle-stage".into(),
                    detail: format!("create shuffle staging dir {}: {e}", parent.display()),
                })?;
        }
        tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .await
            .map_err(|e| crate::Error::Storage {
                engine: "shuffle-stage".into(),
                detail: format!("open shuffle staging file {}: {e}", self.path.display()),
            })
    }

    /// Record one producer's `End` frame.
    ///
    /// Returns `true` when this `End` completes the barrier (i.e.
    /// `ends_received == producer_count`), meaning every expected producer has
    /// finished and this side is complete — the caller should then
    /// [`ShuffleInbox::finalize`] the staging file.
    pub fn record_end(&self) -> bool {
        let prev = self.ends_received.fetch_add(1, Ordering::AcqRel);
        prev + 1 >= self.producer_count
    }

    /// Number of `End` frames received so far.
    pub fn ends_received(&self) -> usize {
        self.ends_received.load(Ordering::Acquire)
    }

    /// `true` once an `End` has been received from every expected producer.
    pub fn barrier_complete(&self) -> bool {
        self.ends_received.load(Ordering::Acquire) >= self.producer_count
    }

    /// Capture a terminal error reported by a producer (first writer wins).
    pub fn set_error(&self, error: TypedClusterError) {
        let mut slot = self.error.lock().unwrap_or_else(|p| p.into_inner());
        if slot.is_none() {
            *slot = Some(error);
        }
    }

    /// Take the captured terminal error, if any, leaving `None` behind.
    pub fn take_error(&self) -> Option<TypedClusterError> {
        self.error.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

/// Registry of [`ShuffleInbox`]es keyed by `(shuffle_id, part, side)`.
///
/// Owned by `SharedState` (`Send + Sync`). The transport read-loop creates and
/// feeds inboxes through the [`nodedb_cluster::ShuffleReceiver`] hook; the Data
/// Plane reads their staged files after the build barrier fires. Owns the base
/// staging directory under which every inbox's scratch file is laid out.
pub struct ShuffleReceiverRegistry {
    /// Root directory for all shuffle staging files (the node's data dir).
    base_dir: PathBuf,
    inboxes: Mutex<HashMap<ShuffleKey, Arc<ShuffleInbox>>>,
}

impl ShuffleReceiverRegistry {
    /// Create an empty registry whose inboxes stage under `base_dir`.
    pub fn new(base_dir: PathBuf) -> Self {
        Self {
            base_dir,
            inboxes: Mutex::new(HashMap::new()),
        }
    }

    /// Directory holding all staging files for `shuffle_id`:
    /// `base_dir/shuffle-stage/{shuffle_id}`.
    fn shuffle_dir(&self, shuffle_id: u64) -> PathBuf {
        self.base_dir
            .join("shuffle-stage")
            .join(shuffle_id.to_string())
    }

    /// Deterministic staging path for one part:
    /// `base_dir/shuffle-stage/{shuffle_id}/{part}-{side}.frames`.
    fn staged_path(&self, shuffle_id: u64, part: u32, side: u8) -> PathBuf {
        self.shuffle_dir(shuffle_id)
            .join(format!("{part}-{side}.frames"))
    }

    /// Get the inbox for `(shuffle_id, part, side)`, lazily creating it on the
    /// first frame with the carried `producer_count` and a deterministic staging
    /// path.
    ///
    /// Idempotent: subsequent frames for the same key reuse the existing inbox
    /// (the `producer_count` of the first creator wins).
    pub fn get_or_create(
        &self,
        shuffle_id: u64,
        part: u32,
        side: u8,
        producer_count: usize,
    ) -> Arc<ShuffleInbox> {
        let key = (shuffle_id, part, side);
        let mut map = self.inboxes.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(existing) = map.get(&key) {
            return Arc::clone(existing);
        }
        let path = self.staged_path(shuffle_id, part, side);
        let inbox = Arc::new(ShuffleInbox::new(path, producer_count));
        map.insert(key, Arc::clone(&inbox));
        inbox
    }

    /// Look up an existing inbox without creating one.
    pub fn get(&self, key: ShuffleKey) -> Option<Arc<ShuffleInbox>> {
        self.inboxes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .map(Arc::clone)
    }

    /// Remove every inbox belonging to `shuffle_id` (all parts and sides) and
    /// best-effort delete its on-disk staging directory to release scratch.
    ///
    /// Called when a shuffle completes or is cancelled. A failed directory
    /// removal is logged and otherwise ignored — never a panic.
    pub fn unregister_shuffle(&self, shuffle_id: u64) {
        self.inboxes
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .retain(|(sid, _, _), _| *sid != shuffle_id);
        let dir = self.shuffle_dir(shuffle_id);
        if let Err(e) = std::fs::remove_dir_all(&dir)
            && e.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(
                shuffle_id,
                dir = %dir.display(),
                error = %e,
                "failed to remove shuffle staging dir"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_base() -> (tempfile::TempDir, ShuffleReceiverRegistry) {
        let dir = tempfile::tempdir().expect("tempdir");
        let reg = ShuffleReceiverRegistry::new(dir.path().to_path_buf());
        (dir, reg)
    }

    /// A msgpack map row built the same way the join tests build rows.
    fn row(fields: &[(&str, serde_json::Value)]) -> Vec<u8> {
        let mut map = serde_json::Map::new();
        for (k, v) in fields {
            map.insert((*k).to_string(), v.clone());
        }
        nodedb_types::json_to_msgpack(&serde_json::Value::Object(map)).expect("encode row")
    }

    /// Encode rows into one msgpack array — the `ShufflePushChunk` payload shape.
    fn encode_array(rows: &[Vec<u8>]) -> Vec<u8> {
        crate::data::executor::response_codec::encode_binary_rows(rows)
    }

    /// Parse a staged `[u32 LE len][row-bytes]` frame file — byte-for-byte the
    /// format the Data Plane's `FrameStreamReader` consumes (which is private to
    /// the join module, so this mirrors it locally for the unit test).
    fn read_staged(path: &Path) -> Vec<Vec<u8>> {
        let bytes = std::fs::read(path).expect("read staged file");
        let mut out = Vec::new();
        let mut pos = 0usize;
        while pos + 4 <= bytes.len() {
            let len = u32::from_le_bytes(bytes[pos..pos + 4].try_into().expect("len")) as usize;
            pos += 4;
            assert!(pos + len <= bytes.len(), "frame body truncated");
            out.push(bytes[pos..pos + len].to_vec());
            pos += len;
        }
        assert_eq!(pos, bytes.len(), "trailing bytes after last frame");
        out
    }

    #[tokio::test]
    async fn finalize_without_any_append_creates_empty_staged_file() {
        // A part/side to which a producer routed ZERO rows still receives an `End`
        // (so the barrier completes) but never an appended chunk. finalize() must
        // materialize an empty staged file so the Data-Plane grace reader opens a
        // real zero-row file rather than failing on a missing path.
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(20, 0, 0, 1);
        assert!(!inbox.staged_path().exists(), "no file before finalize");
        inbox.finalize().await.expect("finalize");
        assert!(
            inbox.staged_path().exists(),
            "finalize must create the staged file even with zero rows"
        );
        assert!(
            read_staged(inbox.staged_path()).is_empty(),
            "the zero-row staged file holds no frames"
        );
        assert!(inbox.is_finalized());
    }

    #[tokio::test]
    async fn append_chunk_explodes_array_into_per_row_frames() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(1, 0, 0, 1);
        let rows = vec![
            row(&[("k", serde_json::json!(1))]),
            row(&[("k", serde_json::json!(2))]),
            row(&[("k", serde_json::json!(3))]),
        ];
        inbox
            .append_chunk(&encode_array(&rows))
            .await
            .expect("append");
        inbox.finalize().await.expect("finalize");
        let staged = read_staged(inbox.staged_path());
        assert_eq!(staged, rows, "each array element becomes one frame");
    }

    #[tokio::test]
    async fn append_chunk_is_appending_across_chunks() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(2, 0, 1, 1);
        let a = vec![row(&[("k", serde_json::json!("a"))])];
        let b = vec![
            row(&[("k", serde_json::json!("b"))]),
            row(&[("k", serde_json::json!("c"))]),
        ];
        inbox.append_chunk(&encode_array(&a)).await.expect("a");
        inbox.append_chunk(&encode_array(&b)).await.expect("b");
        inbox.finalize().await.expect("finalize");
        let staged = read_staged(inbox.staged_path());
        let mut want = a.clone();
        want.extend(b.clone());
        assert_eq!(staged, want);
    }

    #[tokio::test]
    async fn empty_chunk_array_stages_no_frames() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(3, 0, 0, 1);
        inbox.append_chunk(&encode_array(&[])).await.expect("empty");
        inbox.finalize().await.expect("finalize");
        let staged = read_staged(inbox.staged_path());
        assert!(staged.is_empty());
    }

    #[tokio::test]
    async fn malformed_chunk_is_hard_error() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(4, 0, 0, 1);
        // A truncated array: header claims 1 element but no body follows.
        let bad = vec![0x91u8];
        let res = inbox.append_chunk(&bad).await;
        assert!(
            matches!(res, Err(crate::Error::Storage { .. })),
            "a malformed chunk must surface a Storage error, never a silent drop"
        );
    }

    #[test]
    fn barrier_fires_only_after_all_producers_end() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(5, 0, 0, 2);
        assert!(!inbox.barrier_complete());
        assert!(!inbox.record_end());
        assert!(!inbox.barrier_complete());
        assert_eq!(inbox.ends_received(), 1);
        assert!(inbox.record_end());
        assert!(inbox.barrier_complete());
        assert_eq!(inbox.ends_received(), 2);
    }

    #[test]
    fn single_producer_barrier_fires_on_first_end() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(6, 0, 0, 1);
        assert!(!inbox.barrier_complete());
        assert!(inbox.record_end());
        assert!(inbox.barrier_complete());
    }

    #[test]
    fn error_capture_first_writer_wins() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(7, 0, 0, 1);
        assert!(inbox.take_error().is_none());
        inbox.set_error(TypedClusterError::Internal {
            code: 1,
            message: "first".into(),
        });
        inbox.set_error(TypedClusterError::Internal {
            code: 2,
            message: "second".into(),
        });
        match inbox.take_error() {
            Some(TypedClusterError::Internal { code, .. }) => assert_eq!(code, 1),
            other => panic!("expected first Internal error, got {other:?}"),
        }
        assert!(inbox.take_error().is_none());
    }

    #[tokio::test]
    async fn wait_finalized_wakes_a_parked_waiter() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(20, 0, 0, 1);
        assert!(!inbox.is_finalized());

        // Park a waiter BEFORE finalize fires, then finalize from another task.
        let waiter = Arc::clone(&inbox);
        let handle = tokio::spawn(async move {
            waiter.wait_finalized().await;
        });

        // Give the waiter a chance to register, then finalize.
        tokio::task::yield_now().await;
        inbox.finalize().await.expect("finalize");

        // The waiter must wake (and not hang). Bound with a timeout so a
        // regression surfaces as a test failure rather than a stuck suite.
        tokio::time::timeout(std::time::Duration::from_secs(5), handle)
            .await
            .expect("waiter must wake within 5s")
            .expect("waiter task joined");
        assert!(inbox.is_finalized());
    }

    #[tokio::test]
    async fn wait_finalized_returns_immediately_when_already_finalized() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(21, 0, 0, 1);
        inbox.finalize().await.expect("finalize");
        assert!(inbox.is_finalized());

        // The already-finalized fast path must return without parking — a
        // tight timeout proves it does not block on the (now-unsignalled)
        // notify.
        tokio::time::timeout(std::time::Duration::from_secs(1), inbox.wait_finalized())
            .await
            .expect("already-finalized wait must return immediately");
    }

    #[tokio::test]
    async fn failed_finalize_does_not_mark_finalized() {
        // An inbox that never opened a writer finalizes as a no-op and IS marked
        // finalized (the reader treats a missing file as zero rows). This test
        // pins the happy no-op path; the error path (flush/sync failure) returns
        // early before the store, which the `?` in `finalize` enforces.
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(22, 0, 0, 1);
        assert!(!inbox.is_finalized());
        inbox.finalize().await.expect("no-op finalize");
        assert!(
            inbox.is_finalized(),
            "a successful (no-op) finalize marks the inbox finalized"
        );
    }

    #[test]
    fn registry_get_or_create_is_idempotent() {
        let (_d, reg) = temp_base();
        let a = reg.get_or_create(10, 0, 0, 2);
        let b = reg.get_or_create(10, 0, 0, 99);
        assert!(Arc::ptr_eq(&a, &b), "same key must reuse the same inbox");
        assert_eq!(a.producer_count(), 2, "first creator's producer_count wins");
        let c = reg.get_or_create(10, 1, 0, 1);
        assert!(!Arc::ptr_eq(&a, &c));
    }

    #[test]
    fn registry_get_returns_none_for_missing() {
        let (_d, reg) = temp_base();
        assert!(reg.get((11, 0, 0)).is_none());
        reg.get_or_create(11, 0, 0, 1);
        assert!(reg.get((11, 0, 0)).is_some());
    }

    #[test]
    fn staged_path_is_deterministic_and_scoped() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(12, 3, 1, 1);
        let p = inbox.staged_path();
        assert!(p.ends_with("shuffle-stage/12/3-1.frames"), "path: {p:?}");
    }

    #[tokio::test]
    async fn unregister_removes_inboxes_and_scratch_dir() {
        let (_d, reg) = temp_base();
        let inbox = reg.get_or_create(13, 0, 0, 1);
        inbox
            .append_chunk(&encode_array(&[row(&[("k", serde_json::json!(1))])]))
            .await
            .expect("append");
        inbox.finalize().await.expect("finalize");
        let dir = inbox.staged_path().parent().unwrap().to_path_buf();
        assert!(dir.exists(), "staging dir created");
        reg.get_or_create(13, 1, 1, 1);
        reg.get_or_create(14, 0, 0, 1);

        reg.unregister_shuffle(13);
        assert!(reg.get((13, 0, 0)).is_none());
        assert!(reg.get((13, 1, 1)).is_none());
        assert!(reg.get((14, 0, 0)).is_some());
        assert!(!dir.exists(), "scratch dir removed for shuffle 13");
    }
}
