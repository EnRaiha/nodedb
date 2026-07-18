// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral post-dispatch read-set recording.
//!
//! Every transport that dispatches a read makes the same shuffle-aware-or-plain
//! decision once the response returns: a distributed shuffle JOIN records ONE
//! read-set entry per side capture — each from its own single-collection scan
//! plan and REAL observed read-version, so the commit-time OCC validator re-homes
//! and revalidates each side's vshard independently (probe = left, build =
//! right) — while every other read records a single collection-scoped entry from
//! the executed plan and the responding shards' watermarks. When shuffle
//! captures are present the default single-collection entry is SKIPPED, because a
//! `HashJoin` plan collapses to the left collection via `extract_collection` and
//! would miss the build side entirely. This module hosts that decision so all
//! transports funnel through one implementation instead of divergent copies.

use std::net::SocketAddr;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::exchange::ShuffleReadCapture;
use crate::control::state::SharedState;
use crate::types::{Lsn, TenantId, VShardId};

use super::read_set::{ReadCapture, record_read_set};
use super::store::SessionStore;

/// The observed reads produced by one dispatched response.
///
/// `plan` / `watermarks` / `read_version_lsn` / `found` describe the plain
/// single-collection observation (used when `shuffle_reads` is empty).
/// `shuffle_reads`, when non-empty, carries the per-side captures of a
/// distributed shuffle JOIN and takes precedence over the plain fields.
///
/// `shuffle_read_lsn_vshard` is the vshard stamped into each per-capture entry's
/// single-shard SI `read_lsn` slot (paired with [`Lsn::ZERO`], since the sound
/// cross-shard comparand is the capture's own `read_version_lsn`). It is
/// consulted only on the shuffle branch.
pub struct ResponseReads<'a> {
    pub plan: &'a PhysicalPlan,
    pub watermarks: &'a [(VShardId, Lsn)],
    pub read_version_lsn: Lsn,
    pub found: bool,
    pub shuffle_reads: &'a [ShuffleReadCapture],
    pub shuffle_read_lsn_vshard: VShardId,
}

/// Record a dispatched response's reads into the session transaction read-set.
///
/// Protocol-neutral: pgwire and native direct-ops both call this after a read
/// returns. With shuffle captures present, records one entry per capture from
/// its own scan plan and read-version; otherwise records the single plain entry.
/// Delegates every entry to [`record_read_set`], which still applies the
/// session's own-write floor and drops the entry outside a transaction block.
pub async fn record_reads_for_response(
    state: &SharedState,
    sessions: &SessionStore,
    addr: &SocketAddr,
    tenant_id: TenantId,
    reads: ResponseReads<'_>,
) {
    if !reads.shuffle_reads.is_empty() {
        for cap in reads.shuffle_reads {
            record_read_set(
                state,
                sessions,
                addr,
                tenant_id,
                ReadCapture {
                    plan: &cap.scan_plan,
                    watermarks: &[(reads.shuffle_read_lsn_vshard, Lsn::ZERO)],
                    read_version_lsn: cap.read_version_lsn,
                    found: false,
                },
            )
            .await;
        }
    } else {
        record_read_set(
            state,
            sessions,
            addr,
            tenant_id,
            ReadCapture {
                plan: reads.plan,
                watermarks: reads.watermarks,
                read_version_lsn: reads.read_version_lsn,
                found: reads.found,
            },
        )
        .await;
    }
}
