// SPDX-License-Identifier: BUSL-1.1

//! Error helpers for the inverted index module.

/// Map an `FtsIndexError<crate::Error>` to `crate::Error`.
///
/// `InvalidQuery` variants produce `crate::Error::BadRequest` so callers
/// receive a meaningful error code instead of a generic storage error.
pub(super) fn fts_index_err(e: nodedb_fts::FtsIndexError<crate::Error>) -> crate::Error {
    use nodedb_fts::FtsIndexError;
    match e {
        FtsIndexError::InvalidQuery(q) => crate::Error::BadRequest {
            detail: q.to_string(),
        },
        FtsIndexError::Backend(inner) => inner,
        // A document term past the segment format's cap is the caller's
        // input, not a storage fault.
        FtsIndexError::TermTooLong { len, max } => crate::Error::LimitExceeded {
            limit_name: "fts_term_length",
            value: len as u64,
            max: max as u64,
        },
        // The engine's memory budget, the same class a vector budget refusal has.
        FtsIndexError::BudgetExhausted(_) => crate::Error::MemoryExhausted {
            engine: "fts".into(),
        },
        // On-disk index state that is wrong: a segment that fails
        // validation, a listed segment that is gone, or a state blob that
        // does not decode.
        other @ (FtsIndexError::CorruptSegment { .. }
        | FtsIndexError::MissingSegment { .. }
        | FtsIndexError::CorruptState { .. }) => crate::Error::SegmentCorrupted {
            detail: other.to_string(),
        },
        other @ (FtsIndexError::SurrogateOutOfRange { .. }
        | FtsIndexError::Segment(_)
        | FtsIndexError::StateEncode { .. }) => crate::Error::Storage {
            engine: "inverted".into(),
            detail: other.to_string(),
        },
        // `FtsIndexError` is `#[non_exhaustive]` and lives in another crate,
        // so the compiler requires this arm. A variant this build cannot name
        // is a storage fault.
        other => crate::Error::Storage {
            engine: "inverted".into(),
            detail: other.to_string(),
        },
    }
}

/// Wrap a redb-side failure as `crate::Error::Storage` with engine context.
pub(super) fn inverted_err(ctx: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "inverted".into(),
        detail: format!("{ctx}: {e}"),
    }
}

/// Identity adapter so callers can `.map_err(into_result_err)` without
/// importing the type — kept as a function so future error widening (e.g.
/// adding chained context) is a single edit.
pub(super) fn into_result_err(e: crate::Error) -> crate::Error {
    e
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An over-long term is the caller's input, not a storage fault.
    #[test]
    fn an_over_long_term_is_a_limit_refusal() {
        let term: nodedb_fts::FtsIndexError<crate::Error> =
            nodedb_fts::FtsIndexError::TermTooLong {
                len: 70_000,
                max: 65_535,
            };
        assert!(matches!(
            fts_index_err(term),
            crate::Error::LimitExceeded {
                limit_name: "fts_term_length",
                value: 70_000,
                max: 65_535,
            }
        ));
    }

    /// A corrupt or missing segment is on-disk corruption, not a generic
    /// storage fault.
    #[test]
    fn a_corrupt_segment_is_segment_corruption() {
        let corrupt: nodedb_fts::FtsIndexError<crate::Error> =
            nodedb_fts::FtsIndexError::CorruptSegment {
                segment_id: "L0:0000000000000001".into(),
                source: nodedb_fts::lsm::segment::error::SegmentError::Truncated,
            };
        assert!(matches!(
            fts_index_err(corrupt),
            crate::Error::SegmentCorrupted { ref detail } if detail.contains("L0:0000000000000001")
        ));
        let missing: nodedb_fts::FtsIndexError<crate::Error> =
            nodedb_fts::FtsIndexError::MissingSegment {
                segment_id: "L0:0000000000000002".into(),
            };
        assert!(matches!(
            fts_index_err(missing),
            crate::Error::SegmentCorrupted { .. }
        ));
    }
}
