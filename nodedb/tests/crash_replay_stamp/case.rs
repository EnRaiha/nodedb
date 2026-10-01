// SPDX-License-Identifier: BUSL-1.1

//! The per-engine description of one in-flight run, and the collection names
//! it uses.

use crate::crash_harness::vshards::names_in_distinct_data_groups;

/// Boot 1 writes no checkpoint, so a seeded write stays in memory until the
/// kill.
pub(super) const QUIET_CHECKPOINT_INTERVAL_SECS: &str = "3600";

/// The name prefix of the collection the filler goes to. The test never reads
/// it back.
const FILLER_PREFIX: &str = "stamp_trunc_fill";

/// One engine's run of the in-flight sequence.
///
/// Every statement is a template: `{held}` stands for the held collection's
/// name and `{applied}` for the B collection's name. The names come from the
/// prefixes, each applied in its own data Raft group (see [`Names`]).
pub(super) struct Case {
    /// The name prefix of the collection write A goes to.
    pub(super) held: &'static str,
    /// The name prefix of the collection the B writes go to.
    pub(super) applied: &'static str,
    pub(super) create_held: &'static str,
    pub(super) create_applied: &'static str,
    /// A write to the held collection in boot 1, still in memory at the kill.
    /// Boot 2 replays it, so the held collection has state for boot 2's
    /// checkpoint to write while A is parked.
    pub(super) seed_held: Option<&'static str>,
    /// Write A.
    pub(super) insert_held: &'static str,
    /// Reads A's row back as one column.
    pub(super) read_held: &'static str,
    /// The value `read_held` returns once A applied. Two numbers compare by
    /// value, so `8` equals `8.0`.
    pub(super) held_value: &'static str,
    /// Write B number `n` into the named collection.
    pub(super) insert_applied: fn(&str, usize) -> String,
    /// Reads every B row as one column.
    pub(super) read_applied: &'static str,
    /// How the test learns the B rows while A is parked.
    pub(super) live_applied: LiveApplied,
    /// Boot 2's checkpoint interval. It must pass after A is minted when the
    /// engine writes a collection only while it holds unwritten state.
    pub(super) checkpoint_interval_secs: &'static str,
    /// `RUST_LOG` directives that enable the `published` and `restored` lines.
    pub(super) log_directives: &'static str,
    /// The log message of a published checkpoint. Its `applied_ranges` field
    /// counts the ranges its stamp names above the prefix.
    pub(super) published: &'static str,
    /// The boot-3 log message that proves this run reproduced the in-flight
    /// write, and the numeric field that must be above zero on it.
    pub(super) restored: (&'static str, &'static str),
    /// Seal A's segment and wait for truncation runs while A is parked.
    pub(super) wal_truncation: bool,
}

/// How the test learns the B rows while A is parked. A linearizable read in
/// A's data group waits behind A's parked entry and fails, so no read
/// reaches that group before A is released.
pub(super) enum LiveApplied {
    /// Run `read_applied`. The read routes to the B collection's data group
    /// only.
    Read,
    /// Compute the rows `read_applied` returns from the number of B writes.
    /// An array read fans out to every shard, A's group among them.
    Computed(fn(usize) -> Vec<String>),
}

/// The collection names of one run. A parks inside its apply entry's
/// enqueue, and every later entry of its data Raft group waits for it. So the
/// B collection and the filler collection each apply in a data group no other
/// name applies in. An array cell routes by its coordinate instead, so the
/// array case separates its cells by coordinate (see `applied_array_coord`).
pub(super) struct Names {
    pub(super) held: String,
    pub(super) applied: String,
    pub(super) filler: String,
}

impl Names {
    pub(super) fn of(case: &Case) -> Self {
        let [held, applied, filler] =
            names_in_distinct_data_groups([case.held, case.applied, FILLER_PREFIX]);
        Self {
            held,
            applied,
            filler,
        }
    }

    /// `template` with `{held}` and `{applied}` replaced by the names.
    pub(super) fn fill(&self, template: &str) -> String {
        template
            .replace("{held}", &self.held)
            .replace("{applied}", &self.applied)
    }
}
