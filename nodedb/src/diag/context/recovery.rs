// SPDX-License-Identifier: BUSL-1.1

//! Forensic payloads for startup-replay capture sites.

use faultbox::DomainContext;
use faultbox::serde_json::{Value, json};

/// A committed, CRC-valid WAL record could not be applied during startup
/// replay, so the replayed suffix would have had a hole in it.
pub(in crate::diag) struct ReplayRecordUnapplied<'a> {
    /// Which engine's replay arm detected it (`kv`, `fts`, `spatial`, ...).
    pub engine: &'a str,
    /// Which step inside that arm failed (`decode`, `handler`, `open`, ...).
    pub stage: &'a str,
    pub core_id: usize,
    pub record_lsn: u64,
    /// Why the step failed, as the detecting site described it.
    pub detail: &'a str,
}

impl DomainContext for ReplayRecordUnapplied<'_> {
    fn domain_kind(&self) -> &'static str {
        "nodedb.replay_record_unapplied"
    }

    fn grouping_key(&self) -> String {
        // Engine + failing step name the bug; LSN/core are the occurrence,
        // collapsing every record of one malformed class into one group.
        format!("engine={};stage={}", self.engine, self.stage)
    }

    fn to_json(&self) -> Value {
        json!({
            "engine": self.engine,
            "stage": self.stage,
            "core_id": self.core_id,
            "record_lsn": self.record_lsn,
            "detail": self.detail,
            "why_fatal": "the record's CRC verified, so its bytes are intact — it is a \
                          transaction that was acknowledged as committed and cannot be \
                          applied. Skipping it would open the database with committed \
                          writes silently missing from the replayed suffix, which no \
                          later read can distinguish from data that was never written",
            "operator_action": "the WAL tail at this LSN is intact but unreadable by this \
                                 build — check for a downgrade past the record shape that \
                                 wrote it, then preserve the WAL directory before any \
                                 further start attempt",
        })
    }
}
