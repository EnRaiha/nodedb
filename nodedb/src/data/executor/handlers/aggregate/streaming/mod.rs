// SPDX-License-Identifier: BUSL-1.1

//! Streaming, spill-backed GROUP BY accumulation shared by the per-shard scan
//! path, the input-sourced (catalog) path, and the distributed-shuffle
//! partial-state producer / consumer.
//!
//! Split by phase so each concern is independently reusable and each file
//! stays under the project's hard size limit:
//! - `accumulate` — the spill-feed loop that turns `(doc_id, bytes)` rows into
//!   a `HashMap<group_key, GroupState>` (plus optional sub-group map).
//! - `finalize` — the tail that turns per-group states into the encoded
//!   Response payload: build rows, HAVING, alias-rename, ORDER BY, LIMIT.
//! - `over_docs` — `aggregate_over_docs`, which orchestrates both phases and
//!   layers the per-shard result cache on top.

pub(in crate::data::executor) mod accumulate;
pub(in crate::data::executor) mod finalize;
pub(in crate::data::executor) mod over_docs;
