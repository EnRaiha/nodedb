// SPDX-License-Identifier: BUSL-1.1

//! Mapping of a Calvin cross-shard ABORT verdict to the error the client sees.

use nodedb_cluster::calvin::AbortReason;

use crate::Error;

/// Pick the error for an ABORT verdict from the reason the verdict recorded.
///
/// A participant error never validated a read-set, so it must not be reported
/// as a serialization conflict. `None` — a pre-existing durable verdict with no
/// recorded reason — keeps the historical serialization-conflict reading.
pub fn calvin_abort_error(reason: Option<AbortReason>) -> Error {
    match reason {
        Some(AbortReason::ParticipantError) => Error::CalvinParticipantError,
        Some(AbortReason::SerializationConflict) | None => Error::CalvinSerializationConflict,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn participant_error_is_not_reported_as_a_serialization_conflict() {
        assert!(matches!(
            calvin_abort_error(Some(AbortReason::ParticipantError)),
            Error::CalvinParticipantError
        ));
    }

    #[test]
    fn a_stale_read_set_and_a_reasonless_legacy_verdict_stay_serialization_conflicts() {
        assert!(matches!(
            calvin_abort_error(Some(AbortReason::SerializationConflict)),
            Error::CalvinSerializationConflict
        ));
        assert!(matches!(
            calvin_abort_error(None),
            Error::CalvinSerializationConflict
        ));
    }
}
