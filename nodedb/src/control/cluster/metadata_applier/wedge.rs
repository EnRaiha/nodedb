// SPDX-License-Identifier: BUSL-1.1

//! Classification of a failed metadata host-side apply, and the durable
//! marker a permanent failure leaves behind.
//!
//! The apply loop must never advance its watermark past an entry it could not
//! apply — skipping a committed metadata entry is silent divergence from the
//! quorum. So both a transient and a permanent failure stop the batch. What
//! they must NOT share is the *story told to operators*:
//!
//! * A transient failure (a full disk, a redb lock contention, a subsystem
//!   handle not installed yet) clears by itself; Raft re-delivers the entry
//!   and the applier resumes. Halt-and-retry is the whole treatment.
//! * A permanent failure is a pure function of the entry and the local state,
//!   so every re-delivery reproduces it exactly. Retrying forever is a lie:
//!   the node is wedged, and it must stop advertising itself as ready or the
//!   only symptom operators ever see is an unrelated-looking lease timeout on
//!   every subsequent query.

use std::sync::OnceLock;

/// Whether a failed host-side apply can plausibly succeed on re-delivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApplyFailureClass {
    /// May clear on its own; halt-and-retry is sufficient.
    Transient,
    /// Deterministic in the entry and local state — re-delivery re-fails.
    Permanent,
}

impl ApplyFailureClass {
    pub fn is_permanent(self) -> bool {
        matches!(self, Self::Permanent)
    }
}

/// Classify a host-side apply failure.
///
/// Deliberately an allowlist of the variants that are *provably* a pure
/// function of the entry plus local persisted state. Everything else is
/// treated as transient, because the cost of the two mistakes is asymmetric:
/// calling a transient failure permanent takes a node that would have healed
/// itself out of rotation, while calling a permanent failure transient only
/// costs the loud health signal — the watermark halts either way.
pub fn classify(error: &crate::Error) -> ApplyFailureClass {
    match error {
        // The carried version is compared against the persisted prior. Neither
        // side changes while the applier is stopped, so the comparison yields
        // the same verdict on every re-delivery, forever.
        crate::Error::DescriptorVersionAnomaly { .. } => ApplyFailureClass::Permanent,
        // The bytes being encoded/decoded are fixed by the committed entry, so
        // a codec rejection is reproducible.
        crate::Error::Serialization { .. } | crate::Error::Codec { .. } => {
            ApplyFailureClass::Permanent
        }
        // A committed entry that the host rejects as malformed will be just as
        // malformed next time.
        crate::Error::BadRequest { .. } | crate::Error::TypeMismatch { .. } => {
            ApplyFailureClass::Permanent
        }
        _ => ApplyFailureClass::Transient,
    }
}

/// What the applier recorded when it stopped on a permanent failure.
#[derive(Debug, Clone)]
pub struct WedgeReport {
    /// Raft index of the entry that could not be applied.
    pub raft_index: u64,
    /// Highest index whose state is guaranteed visible — one below the stall.
    pub last_applied_watermark: u64,
    /// Variant name of the undeliverable entry.
    pub entry_kind: String,
    /// Rendered error, so the readiness probe can name the real cause.
    pub error: String,
}

/// Node-wide marker set once when the metadata applier stops on a permanent
/// failure. Read by the readiness probe so a wedged node stops reporting
/// itself healthy.
///
/// First writer wins: the applier retries the same entry on every re-delivery
/// and would otherwise overwrite the original cause with an identical copy on
/// every tick. There is no clear path — the applier only resumes if the entry
/// applies, and if it applies the process has already made progress past the
/// point this marker describes, so operator intervention is required either
/// way.
#[derive(Debug, Default)]
pub struct MetadataApplyWedge {
    report: OnceLock<WedgeReport>,
}

impl MetadataApplyWedge {
    /// Record the first permanent failure. Later calls are ignored.
    pub fn record(&self, report: WedgeReport) {
        let _ = self.report.set(report);
    }

    /// The recorded failure, if this node's metadata applier is wedged.
    pub fn report(&self) -> Option<&WedgeReport> {
        self.report.get()
    }

    pub fn is_wedged(&self) -> bool {
        self.report.get().is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_anomaly_is_permanent() {
        let error = crate::Error::DescriptorVersionAnomaly {
            descriptor: "orders".into(),
            carried: 1,
            prior: 1,
        };
        assert_eq!(classify(&error), ApplyFailureClass::Permanent);
    }

    #[test]
    fn storage_failure_is_transient() {
        let error = crate::Error::Storage {
            engine: "catalog".into(),
            detail: "no space left on device".into(),
        };
        assert_eq!(classify(&error), ApplyFailureClass::Transient);
    }

    #[test]
    fn unrecognized_failure_defaults_to_transient() {
        let error = crate::Error::Internal {
            detail: "metadata enrollment apply has no cluster transport".into(),
        };
        assert_eq!(classify(&error), ApplyFailureClass::Transient);
    }

    #[test]
    fn wedge_keeps_the_first_recorded_cause() {
        let wedge = MetadataApplyWedge::default();
        assert!(!wedge.is_wedged());
        wedge.record(WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "first".into(),
        });
        wedge.record(WedgeReport {
            raft_index: 3,
            last_applied_watermark: 2,
            entry_kind: "DdlPrepared".into(),
            error: "second".into(),
        });
        assert!(wedge.is_wedged());
        assert_eq!(
            wedge.report().map(|report| report.error.as_str()),
            Some("first")
        );
    }
}
