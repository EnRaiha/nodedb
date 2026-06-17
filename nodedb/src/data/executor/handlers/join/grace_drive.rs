// SPDX-License-Identifier: BUSL-1.1

//! Memory-bounded build-side driver for the grace-hash join.
//!
//! This module owns the streaming build-side accumulation that
//! `execute_hash_join` uses ONLY when both join sides are plain local scans
//! (no Exchange sub-plan, no bitmap prefilter). It streams the build (right)
//! side row-at-a-time, tracking byte total against the SAME budget the
//! materializing path uses (`scan_bytes_exceeded`). Two outcomes:
//!
//! - **Under budget** — the build side finishes at or below budget. The driver
//!   returns the fully-buffered `right_docs` Vec and the caller falls through to
//!   the UNCHANGED in-memory hash-join path. Output ordering is byte-identical
//!   to today for this case: nothing is routed through the spiller.
//! - **Over budget** — the build side crosses budget mid-stream. The driver
//!   switches to a [`PartitionedSpiller`], pushes the already-buffered build
//!   rows plus the rest of the build stream, then streams the probe (left) side
//!   straight into the spiller (never materialized). `finish_and_probe()`
//!   completes the join; the result is the already-encoded join rows. This path
//!   COMPLETES — it never returns `ResourcesExhausted` for being over input
//!   budget.
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
use super::params::JoinParams;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

/// Fixed partition count for the grace-hash spill path. See module docs.
const GRACE_PARTITIONS: usize = 64;

/// Outcome of streaming the build side under a memory budget.
pub(super) enum BuildOutcome {
    /// Build side finished at or under budget. The caller MUST fall through to
    /// the UNCHANGED in-memory hash-join path (which re-scans both sides via the
    /// materializing `scan_collection`). The streamed build buffer is NOT reused
    /// here: `scan_collection_for_each` and `scan_collection` only guarantee the
    /// same row SET, not the same row ORDER, and the under-budget case must be
    /// byte-identical to today's output — so we deliberately discard the buffer
    /// and let the unchanged path produce today's exact ordering.
    UnderBudget,
    /// Build side crossed budget — the grace spill path ran to completion and
    /// produced these already-encoded binary join rows (pre-`filter_and_project`).
    Spilled(Vec<Vec<u8>>),
}

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
    /// - `None` — the build side stayed within budget; the caller MUST fall
    ///   through to the unchanged in-memory hash-join path (output ordering is
    ///   byte-identical to today for this case).
    /// - `Some(response)` — either the grace-spill path completed and produced
    ///   the final encoded response, or an error response (scan failure, or the
    ///   no-LIMIT output exceeding the per-query byte budget) is being surfaced.
    ///
    /// The grace-spill path COMPLETES the join — it never returns
    /// `ResourcesExhausted` for being over the *input* budget. The output-budget
    /// enforcement below is the SAME guard the in-memory path applies: a no-LIMIT
    /// join whose output exceeds the byte budget surfaces a deterministic
    /// `ResourcesExhausted` rather than silently truncating.
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
        let outcome = match self.drive_grace_build(
            tid,
            left_collection,
            right_collection,
            &spec,
            budget,
            unique_join_id,
        ) {
            Ok(o) => o,
            Err(e) => {
                return Some(self.response_error(
                    join.task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ));
            }
        };

        let mut results = match outcome {
            // Under budget — caller falls through to the unchanged in-memory path.
            BuildOutcome::UnderBudget => return None,
            BuildOutcome::Spilled(rows) => rows,
        };

        // Same output-budget enforcement as the in-memory path: a no-LIMIT join
        // whose output fills the budget ceiling surfaces a deterministic error
        // rather than dropping rows.
        if enforce_output_budget && results.len() >= probe_limit {
            return Some(self.response_error(join.task, ErrorCode::ResourcesExhausted));
        }

        join.filter_and_project(&mut results);

        let payload = crate::data::executor::response_codec::encode_binary_rows(&results);
        Some(self.response_with_payload(join.task, payload))
    }

    /// Drive the memory-bounded build side for a both-sides-local hash join.
    ///
    /// Streams the build (right) collection, buffering until the per-query byte
    /// budget is crossed. If it is never crossed, returns
    /// [`BuildOutcome::Buffered`] so the caller runs the unchanged in-memory
    /// path. If it is crossed, switches to a [`PartitionedSpiller`], streams the
    /// probe (left) collection into it, completes the join, removes the per-join
    /// spill directory, and returns [`BuildOutcome::Spilled`].
    ///
    /// `budget` of 0 means "unlimited": the build side never crosses budget, so
    /// this always returns `Buffered` (identical to today's behavior).
    #[allow(clippy::too_many_arguments)]
    pub(super) fn drive_grace_build(
        &self,
        tid: u64,
        left_collection: &str,
        right_collection: &str,
        spec: &GraceSpec<'_>,
        budget: usize,
        unique_join_id: u64,
    ) -> crate::Result<BuildOutcome> {
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
        self.scan_collection_for_each(tid, right_collection, |id, bytes| {
            // Decide whether this row crosses the budget threshold WITHOUT
            // holding a `&mut state` borrow across the state reassignment below.
            let crossed = match &mut state {
                BuildState::Buffering { docs, bytes: total } => {
                    *total = total.saturating_add(bytes.len()).saturating_add(id.len());
                    // Only the value bytes are later fed to push_build; the id
                    // is never used, so avoid the allocation.
                    docs.push((String::new(), bytes.to_vec()));
                    // budget == 0 → unlimited → never spill.
                    budget != 0 && *total > budget
                }
                BuildState::Spilling(spiller) => {
                    spiller.push_build(bytes)?;
                    false
                }
            };

            if crossed {
                // Take the buffered rows out and transition Buffering → Spilling
                // exactly once. Create the spill dir, drain the buffered build
                // rows into the spiller, then continue streaming to the spiller.
                let drained = match std::mem::replace(
                    &mut state,
                    BuildState::Buffering {
                        docs: Vec::new(),
                        bytes: 0,
                    },
                ) {
                    BuildState::Buffering { docs, .. } => docs,
                    // Unreachable: `crossed` is only ever true from the Buffering
                    // arm. Treat as no rows rather than panicking.
                    BuildState::Spilling(_) => Vec::new(),
                };
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
            BuildState::Buffering { .. } => {
                // Build side stayed within budget — fall through to the in-memory
                // hash-join path unchanged. The buffered rows are intentionally
                // discarded (see `BuildOutcome::UnderBudget`).
                Ok(BuildOutcome::UnderBudget)
            }
            BuildState::Spilling(mut spiller) => {
                // Stream the PROBE (left) side directly into the spiller — never
                // materialized in RAM. Errors propagate (no silent drop); on any
                // error we still remove the spill dir below by routing through the
                // shared cleanup tail.
                let probe_result = (|| -> crate::Result<Vec<Vec<u8>>> {
                    self.scan_collection_for_each(tid, left_collection, |_id, bytes| {
                        spiller.push_probe(bytes)
                    })?;
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

                Ok(BuildOutcome::Spilled(probe_result?))
            }
        }
    }
}
