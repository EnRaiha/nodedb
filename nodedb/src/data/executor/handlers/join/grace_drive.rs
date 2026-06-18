// SPDX-License-Identifier: BUSL-1.1

//! Memory-bounded build-side driver for the grace-hash join.
//!
//! This module owns the streaming build/probe completion that
//! `execute_hash_join` uses ONLY when both join sides are plain local scans
//! (no Exchange sub-plan, no bitmap prefilter) and the join is NOT a cross /
//! keyless join. It streams the build (right) side row-at-a-time, tracking byte
//! total against the SAME budget the materializing path uses
//! (`scan_bytes_exceeded`: id + value bytes, strict `>`). Two outcomes:
//!
//! - **Under-budget build** — the build side finishes at or below budget. The
//!   driver KEEPS the fully-buffered build rows, builds an in-memory
//!   `HashIndex` over them, and STREAMS the probe (left) side against that index
//!   in bounded ≤budget batches (one batch resident at a time — the probe is
//!   never fully materialized). The shared `results` / `index_matched` are
//!   accumulated across batches via the reusable `probe_rows_into` /
//!   `emit_unmatched_right_into` pieces, so the output is byte-identical to the
//!   old in-memory path that materialized the whole probe side and called
//!   `probe_hash_index` once. Byte-identity holds because
//!   `scan_collection_for_each` yields rows in the SAME ORDER as
//!   `scan_collection` (proven by the order-contract tests in
//!   `scan_normalize.rs`), the build buffer is the same row set/order the
//!   in-memory path would have built its index from, and batching only changes
//!   WHEN a probe row is processed, never the order or the emission rule.
//! - **Over-budget build** — the build side crosses budget mid-stream. The
//!   driver switches to a [`PartitionedSpiller`], pushes the already-buffered
//!   build rows plus the rest of the build stream, then streams the probe (left)
//!   side straight into the spiller (never materialized). `finish_and_probe()`
//!   completes the join; the result is the already-encoded join rows. This path
//!   COMPLETES — it never returns `ResourcesExhausted` for being over input
//!   budget.
//!
//! Both arms return the completed, encoded-ready join rows (pre-`filter_and_project`)
//! as a `Vec<Vec<u8>>`; `try_grace_hash_join` then applies the SAME
//! output-budget guard + `filter_and_project` + `encode_binary_rows` to both.
//!
//! Cross / keyless joins are NOT handled here — `try_grace_hash_join` returns
//! `None` for them so the caller falls through to the unchanged in-memory path
//! (which handles the cartesian product). Streaming the cross-join probe is a
//! declared, separate deferral.
//!
//! Why a fixed `P = 64`: 64 partitions keeps each partition's working set small
//! enough that a per-partition `HashIndex` + probe stays well inside one core's
//! arena even for a build side that is many multiples of the budget, while the
//! per-partition file/handle overhead (64 build + 64 probe spill files per join)
//! remains modest. `per_partition_budget = max_scan_result_bytes / P` so the
//! aggregate in-memory residency across all partitions stays bounded by the same
//! `max_scan_result_bytes` budget the materializing path enforces.

use super::grace_partitioner::GraceSpec;
use super::grace_spill::PartitionedSpiller;
use super::hash::{HashIndex, ProbeParams, emit_unmatched_right_into, probe_rows_into};
use super::params::JoinParams;
use super::row_source::RowSource;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::scan_budget::budget_exceeded;

/// Fixed partition count for the grace-hash spill path. See module docs.
const GRACE_PARTITIONS: usize = 64;

/// Per-side streaming accumulation state. Starts `Buffering`; transitions to
/// `Spilling` exactly once, when the running byte total crosses `budget`.
enum BuildState {
    Buffering {
        docs: Vec<(String, Vec<u8>)>,
        bytes: usize,
    },
    // Boxed: `PartitionedSpiller` is large (P partition buffers + writers);
    // box it so `BuildState` stays compact next to the small `Buffering` variant.
    Spilling(Box<PartitionedSpiller>),
}

impl CoreLoop {
    /// Memory-bounded completion entry point for a both-sides-local hash join.
    ///
    /// Returns:
    /// - `None` — the join is a cross / keyless join (declared deferral: cross
    ///   probe streaming is separate). The caller MUST fall through to the
    ///   unchanged in-memory hash-join path, which handles the cartesian product.
    /// - `Some(response)` — the memory-bounded path completed the join (either
    ///   the under-budget-build streamed-probe path or the over-budget-build
    ///   grace-spill path) and produced the final encoded response, OR an error
    ///   response (scan failure, or the no-LIMIT output exceeding the per-query
    ///   byte budget) is being surfaced.
    ///
    /// For every both-local, non-cross join this returns `Some`. The path
    /// COMPLETES the join — it never returns `ResourcesExhausted` for being over
    /// the *input* budget. The output-budget enforcement below is the SAME guard
    /// the in-memory path applies: a no-LIMIT join whose output exceeds the byte
    /// budget surfaces a deterministic `ResourcesExhausted` rather than silently
    /// truncating.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn try_grace_hash_join(
        &self,
        join: &JoinParams<'_>,
        tid: u64,
        left_collection: &str,
        right_collection: &str,
        left_alias: Option<&str>,
        right_alias: Option<&str>,
        budget: usize,
    ) -> Option<Response> {
        let probe_collection = left_alias.unwrap_or(left_collection);
        let index_collection = right_alias.unwrap_or(right_collection);

        // Probe-side (left) and build-side (right) join-key field names.
        let probe_keys: Vec<&str> = join.on.iter().map(|(l, _)| l.as_str()).collect();
        let build_keys: Vec<&str> = join.on.iter().map(|(_, r)| r.as_str()).collect();

        // Cross / keyless join: NOT streamed here. A cartesian product cannot be
        // hash-partitioned by key, and the streamed-probe path needs join keys to
        // build an index. Declared deferral — fall through to the unchanged
        // in-memory path (which handles cross). (Streaming the cross-join probe is
        // a separate unit.)
        if join.join_type == "cross" || build_keys.is_empty() || probe_keys.is_empty() {
            return None;
        }

        // Identical output-bound derivation to the in-memory path: an explicit
        // user LIMIT is honored exactly (no budget check); a no-LIMIT join is
        // bounded by the per-query byte budget (or truly unbounded when 0).
        let (probe_limit, enforce_output_budget) = if join.limit != usize::MAX {
            (join.limit, false)
        } else if budget == 0 {
            (usize::MAX, false)
        } else {
            (
                crate::data::executor::handlers::scan_budget::fetch_limit_for(
                    usize::MAX,
                    0,
                    budget,
                ),
                true,
            )
        };

        let spec = GraceSpec {
            build_keys: &build_keys,
            probe_keys: &probe_keys,
            join_type: join.join_type,
            limit: probe_limit,
            probe_collection,
            index_collection,
            // Matches the in-memory call: local-scan joins always emit unmatched
            // build-side rows for RIGHT/FULL (no broadcast de-duplication here).
            emit_unmatched_right: true,
        };

        let unique_join_id = join.task.request_id().as_u64();
        let did = join.task.request.database_id.as_u64();
        let mut results = match self.drive_grace_build(
            did,
            tid,
            left_collection,
            right_collection,
            &spec,
            budget,
            unique_join_id,
        ) {
            Ok(rows) => rows,
            // A depth-cap skew error is "over budget" semantics — identical to
            // what the in-memory path surfaces — so it must map to
            // `ResourcesExhausted`, NOT `Internal`. (The envelope maps
            // `Error::MemoryExhausted` → `ErrorCode::ResourcesExhausted`; we match
            // the variant here so the wire code is correct regardless of where
            // the error was minted.) Any other error stays `Internal`.
            Err(crate::Error::MemoryExhausted { .. }) => {
                return Some(self.response_error(join.task, ErrorCode::ResourcesExhausted));
            }
            Err(e) => {
                return Some(self.response_error(
                    join.task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ));
            }
        };

        // Same output-budget enforcement as the in-memory path: a no-LIMIT join
        // whose output fills the budget ceiling surfaces a deterministic error
        // rather than dropping rows.
        if enforce_output_budget && results.len() >= probe_limit {
            return Some(self.response_error(join.task, ErrorCode::ResourcesExhausted));
        }

        if let Err(e) = join.filter_and_project(&mut results) {
            return Some(self.response_error(
                join.task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ));
        }

        let payload = crate::data::executor::response_codec::encode_binary_rows(&results);
        Some(self.response_with_payload(join.task, payload))
    }

    /// Drive the memory-bounded build + probe for a both-sides-local,
    /// non-cross hash join. Always runs the join to completion and returns
    /// the encoded-ready join rows.
    ///
    /// Streams the build (right) collection, buffering until the per-query byte
    /// budget is crossed.
    ///
    /// - **Not crossed** (build fits budget, or `budget == 0` = unlimited): keep
    ///   the buffered build rows, build an in-memory [`HashIndex`], and stream
    ///   the probe (left) collection against it in bounded ≤budget batches via
    ///   [`probe_rows_into`], sharing one `results` Vec and one `index_matched`
    ///   Vec across batches; a final [`emit_unmatched_right_into`] sweep handles
    ///   RIGHT/FULL unmatched build rows. The probe is never fully materialized.
    /// - **Crossed**: switch to a [`PartitionedSpiller`], stream the probe into
    ///   it, complete the join, and remove the per-join spill directory.
    #[allow(clippy::too_many_arguments)]
    pub(super) fn drive_grace_build(
        &self,
        did: u64,
        tid: u64,
        left_collection: &str,
        right_collection: &str,
        spec: &GraceSpec<'_>,
        budget: usize,
        unique_join_id: u64,
    ) -> crate::Result<Vec<Vec<u8>>> {
        let spill_dir = self
            .data_dir
            .join("join-spill")
            .join(format!("core-{}", self.core_id()))
            .join(format!("{unique_join_id}"));

        // Per-partition residency bound: the aggregate across P partitions stays
        // within the same `max_scan_result_bytes` budget the in-memory path uses.
        // Floored at 1 so a tiny-but-nonzero budget still spills (a 0
        // per-partition budget would make `PartitionedSpiller` never spill —
        // i.e. stay fully in memory — which would defeat the bound). We only
        // reach this path when `budget != 0`, so the floor never fabricates a
        // bound for the unlimited case.
        let per_partition_budget = (budget / GRACE_PARTITIONS).max(1);

        let mut state = BuildState::Buffering {
            docs: Vec::new(),
            bytes: 0,
        };

        // Stream the BUILD (right) side. The closure transitions the state from
        // Buffering to Spilling exactly once, the first time the running byte
        // total crosses `budget` (matching `scan_bytes_exceeded`: id + value
        // bytes, strict `>`).
        let build_source = RowSource::LocalScan {
            database_id: did,
            tenant_id: tid,
            collection: right_collection.to_string(),
        };
        build_source.for_each(self, |id, bytes| {
            // Append this row to the active side. When a buffering side crosses
            // budget, `mem::take` the buffered rows out (leaving the buffer empty)
            // and return them so we transition Buffering → Spilling exactly once.
            // Returning the drained rows from the match arm itself means there is
            // no separate (and unreachable) re-match of the post-transition state.
            let drained: Option<Vec<(String, Vec<u8>)>> = match &mut state {
                BuildState::Buffering { docs, bytes: total } => {
                    *total = total.saturating_add(bytes.len()).saturating_add(id.len());
                    // Only the value bytes are later fed to push_build; the id
                    // is never used, so avoid the allocation.
                    docs.push((String::new(), bytes.to_vec()));
                    // budget == 0 → unlimited → never spill.
                    if budget_exceeded(*total, budget) {
                        Some(std::mem::take(docs))
                    } else {
                        None
                    }
                }
                BuildState::Spilling(spiller) => {
                    spiller.push_build(bytes)?;
                    None
                }
            };

            if let Some(drained) = drained {
                // Transition Buffering → Spilling: create the spill dir, drain the
                // buffered build rows into the spiller, then continue streaming to it.
                std::fs::create_dir_all(&spill_dir).map_err(|e| crate::Error::Storage {
                    engine: "join-spill".into(),
                    detail: format!(
                        "failed to create grace-join spill dir {}: {e}",
                        spill_dir.display()
                    ),
                })?;
                let mut spiller = PartitionedSpiller::new(
                    spec,
                    GRACE_PARTITIONS,
                    per_partition_budget,
                    // FINISH-TIME re-partition trigger: the FULL per-query budget.
                    // `finish_and_probe` materializes ONE partition at a time, so
                    // a partition is only too big to materialize when it exceeds
                    // the whole-query budget — NOT `per_partition_budget` (which is
                    // `budget / 64` and would force every partition to look
                    // oversized). When this path runs `budget != 0` always (the
                    // build side only crosses into spilling when `budget != 0`), so
                    // `materialize_cap` is a real positive bound here.
                    budget,
                    spill_dir.clone(),
                );
                for (_, row) in &drained {
                    spiller.push_build(row)?;
                }
                state = BuildState::Spilling(Box::new(spiller));
            }
            Ok(())
        })?;

        match state {
            BuildState::Buffering { docs, .. } => {
                // Build side stayed within budget — keep the buffered build rows,
                // build the in-memory index, and STREAM the probe (left) side
                // against it in bounded ≤budget batches. Byte-identical to the old
                // in-memory path: same build row set/order, same `probe_rows_into`
                // emission, same global limit / index_matched accumulation; only
                // WHEN each probe row is processed differs, never the order.
                self.stream_probe_against_index(did, tid, left_collection, &docs, spec, budget)
            }
            BuildState::Spilling(mut spiller) => {
                // Stream the PROBE (left) side directly into the spiller — never
                // materialized in RAM. Errors propagate (no silent drop); on any
                // error we still remove the spill dir below by routing through the
                // shared cleanup tail.
                let probe_result = (|| -> crate::Result<Vec<Vec<u8>>> {
                    let spill_probe_source = RowSource::LocalScan {
                        database_id: did,
                        tenant_id: tid,
                        collection: left_collection.to_string(),
                    };
                    spill_probe_source.for_each(self, |_id, bytes| spiller.push_probe(bytes))?;
                    spiller.finish_and_probe()
                })();

                // Remove the per-join spill directory regardless of outcome.
                // Best-effort: the rows have already been read back into RAM by
                // `finish_and_probe`, so a failed cleanup cannot corrupt results;
                // it only leaves temp files. Surface it loudly via tracing.
                if let Err(e) = std::fs::remove_dir_all(&spill_dir)
                    && spill_dir.exists()
                {
                    tracing::warn!(
                        error = %e,
                        dir = %spill_dir.display(),
                        "failed to remove grace-join spill dir"
                    );
                }

                probe_result
            }
        }
    }

    /// Stream the probe (left) side against an in-memory `HashIndex` built over
    /// the buffered, under-budget build rows, in bounded ≤budget batches.
    ///
    /// Only ONE probe batch is resident at a time: rows accumulate into `batch`
    /// (tracking the same `id + value` byte total as the build side, `budget != 0`
    /// gated, strict `>`) until the running total crosses `budget`, at which
    /// point the batch is fed through [`probe_rows_into`] and cleared. The final
    /// partial batch is flushed after the stream ends. `results` and
    /// `index_matched` are shared across all batches so the global output limit
    /// (`spec.limit`) and RIGHT/FULL match tracking accumulate correctly; a final
    /// [`emit_unmatched_right_into`] sweep (gated on `emit_unmatched_right`)
    /// emits unmatched build rows.
    ///
    /// A `budget` of 0 (unlimited) never crosses, so the whole probe side flushes
    /// as a single final batch — still bounded only by the in-memory build index,
    /// matching the unlimited in-memory path.
    fn stream_probe_against_index(
        &self,
        did: u64,
        tid: u64,
        left_collection: &str,
        build_docs: &[(String, Vec<u8>)],
        spec: &GraceSpec<'_>,
        budget: usize,
    ) -> crate::Result<Vec<Vec<u8>>> {
        let index = HashIndex::build(build_docs, spec.build_keys);

        let is_right = spec.join_type == "right" || spec.join_type == "full";
        let mut index_matched: Vec<bool> = if is_right {
            vec![false; build_docs.len()]
        } else {
            Vec::new()
        };
        let mut results: Vec<Vec<u8>> = Vec::new();

        // One ≤budget probe batch resident at a time.
        let mut batch: Vec<(String, Vec<u8>)> = Vec::new();
        let mut batch_bytes: usize = 0;

        // Process the accumulated batch through the shared emission loop, then
        // clear it for reuse. Honors `spec.limit` against the SHARED results.
        let flush = |batch: &mut Vec<(String, Vec<u8>)>,
                     batch_bytes: &mut usize,
                     results: &mut Vec<Vec<u8>>,
                     index_matched: &mut [bool]| {
            if batch.is_empty() {
                return;
            }
            probe_rows_into(
                &ProbeParams {
                    probe_docs: batch,
                    index: &index,
                    index_docs: build_docs,
                    probe_keys: spec.probe_keys,
                    join_type: spec.join_type,
                    limit: spec.limit,
                    probe_collection: spec.probe_collection,
                    index_collection: spec.index_collection,
                    emit_unmatched_right: spec.emit_unmatched_right,
                },
                results,
                index_matched,
            );
            batch.clear();
            *batch_bytes = 0;
        };

        let stream_probe_source = RowSource::LocalScan {
            database_id: did,
            tenant_id: tid,
            collection: left_collection.to_string(),
        };
        stream_probe_source.for_each(self, |id, bytes| {
            batch_bytes = batch_bytes
                .saturating_add(bytes.len())
                .saturating_add(id.len());
            // Only the value bytes are fed to probe_rows_into; the id is never
            // used for matching, so avoid the allocation.
            batch.push((String::new(), bytes.to_vec()));
            // budget == 0 → unlimited → never flush mid-stream (matches the
            // build-side accounting and the unlimited in-memory path).
            if budget_exceeded(batch_bytes, budget) {
                flush(
                    &mut batch,
                    &mut batch_bytes,
                    &mut results,
                    &mut index_matched,
                );
            }
            Ok(())
        })?;

        // Flush the final partial batch.
        flush(
            &mut batch,
            &mut batch_bytes,
            &mut results,
            &mut index_matched,
        );

        // RIGHT/FULL: emit unmatched index-side rows ONCE, after all probe
        // batches. The in-memory path runs this same sweep via probe_hash_index.
        if is_right && spec.emit_unmatched_right {
            emit_unmatched_right_into(
                &ProbeParams {
                    probe_docs: &[],
                    index: &index,
                    index_docs: build_docs,
                    probe_keys: spec.probe_keys,
                    join_type: spec.join_type,
                    limit: spec.limit,
                    probe_collection: spec.probe_collection,
                    index_collection: spec.index_collection,
                    emit_unmatched_right: spec.emit_unmatched_right,
                },
                &mut results,
                &index_matched,
            );
        }

        Ok(results)
    }
}
