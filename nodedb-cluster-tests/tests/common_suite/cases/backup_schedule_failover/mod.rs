// SPDX-License-Identifier: BUSL-1.1

//! A scheduled backup survives a leader change of vShard 0.
//!
//! Only the leader of vShard 0 fires scheduled backups. It picks the due
//! minute from the schedule mark the metadata group replicates, read after
//! it applied the group through a confirmed read index. The tests drive each
//! node's scheduler tick with a chosen clock.
//!
//! - The leader arms the schedule one minute before a due minute `M`, then
//!   dies before its tick fires `M`. The new leader runs `M` once, the other
//!   survivor runs nothing, and later ticks run nothing more.
//! - The leader runs `M`, the survivors' catalogs are held back from
//!   applying that mark, and the leader dies. The new leader's catalog still
//!   shows `M` as due, but its read cannot confirm the mark, so it skips the
//!   tick. Once the survivors apply the mark, nothing is due. `M` is never
//!   run twice. This one needs `--features failpoints`.
//! - The leader is cut off from both peers when `M` comes due. Its leader
//!   lease lapses, so it fires nothing, while the peers elect a new leader
//!   that runs `M` once, before the partition heals.
//!
//! A local "last fired" marker fails the first. A read of the local catalog
//! without the read-index barrier fails the second. A coordinator check on
//! the Raft role alone fails the third: the cut-off leader keeps its role.
//!
//! All three run on the multi-thread runtime: the cluster harness serves DDL
//! and metadata proposals on its nodes' async tasks with `block_in_place`.

mod fixture;
mod isolated_leader;
#[cfg(feature = "failpoints")]
mod lagging_catalog;
mod new_leader;
