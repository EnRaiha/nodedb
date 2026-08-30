// SPDX-License-Identifier: BUSL-1.1

//! This participant's local commit vote on a staged static Calvin transaction,
//! derived from the staged executor response.

use nodedb_cluster::calvin::AbortReason;

use crate::bridge::envelope::{Response, Status};

/// A participant's local verdict on its staged slice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StagedVote {
    /// The read-set was still current, or the path carries none.
    Commit,
    /// Validated and stale: a genuine serialization conflict.
    SerializationConflict,
    /// The participant never staged, so no read-set was ever validated.
    ParticipantError,
}

impl StagedVote {
    /// `None` for a commit, the abort cause otherwise.
    pub(super) fn abort_reason(self) -> Option<AbortReason> {
        match self {
            Self::Commit => None,
            Self::SerializationConflict => Some(AbortReason::SerializationConflict),
            Self::ParticipantError => Some(AbortReason::ParticipantError),
        }
    }
}

/// Derive a local staged vote without ever treating an executor error as a
/// commit, and keep the two abort causes apart. A `None` read-set stays
/// affirmative only for a successful dependent-read or active staged response,
/// which has no versioned read-set.
pub(super) fn staged_commit_vote(response: &Response) -> StagedVote {
    if response.status != Status::Ok {
        return StagedVote::ParticipantError;
    }
    if response.read_set_valid == Some(false) {
        return StagedVote::SerializationConflict;
    }
    StagedVote::Commit
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Payload;
    use crate::types::{Lsn, RequestId};

    fn staged_response(status: Status, read_set_valid: Option<bool>) -> Response {
        Response {
            request_id: RequestId::new(1),
            status,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    #[test]
    fn executor_error_votes_participant_error_whatever_the_read_set_field_says() {
        // The participant never staged, so no read-set was ever validated —
        // reporting a serialization conflict here would be a lie.
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Error, None)),
            StagedVote::ParticipantError
        );
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Error, Some(true))),
            StagedVote::ParticipantError
        );
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Error, Some(false))),
            StagedVote::ParticipantError
        );
    }

    #[test]
    fn stale_read_set_on_a_successful_response_votes_serialization_conflict() {
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Ok, Some(false))),
            StagedVote::SerializationConflict
        );
    }

    #[test]
    fn successful_response_with_a_current_or_absent_read_set_votes_commit() {
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Ok, Some(true))),
            StagedVote::Commit
        );
        assert_eq!(
            staged_commit_vote(&staged_response(Status::Ok, None)),
            StagedVote::Commit
        );
    }

    #[test]
    fn only_an_abort_carries_a_reason() {
        assert_eq!(StagedVote::Commit.abort_reason(), None);
        assert_eq!(
            StagedVote::SerializationConflict.abort_reason(),
            Some(AbortReason::SerializationConflict)
        );
        assert_eq!(
            StagedVote::ParticipantError.abort_reason(),
            Some(AbortReason::ParticipantError)
        );
    }
}
