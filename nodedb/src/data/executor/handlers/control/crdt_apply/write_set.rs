// SPDX-License-Identifier: BUSL-1.1

//! The one-document-per-delta contract.
//!
//! Cross-engine identity assigns exactly one Control-Plane surrogate per delta,
//! so the Data Plane can materialize only the single frame-declared row. A
//! delta whose write-set names any other or additional row must be refused
//! loudly rather than materialized partially — silently dropping the extra rows
//! is the data-loss bug this guard closes.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Enforce the one-document-per-delta contract: every row a validated delta
    /// wrote must be exactly the frame-declared `(collection, document_id)`.
    ///
    /// A client that coalesced N document upserts into one delta, or tagged the
    /// frame with a synthetic id matching no written row, has no surrogate for
    /// the extra rows; materializing just one would silently drop the rest.
    /// Returns a human-readable detail naming the offending rows so the caller
    /// surfaces the violation instead of losing data.
    pub(crate) fn single_document_write_set(
        collection: &str,
        document_id: &str,
        write_set: &[(String, String)],
    ) -> Result<(), String> {
        let foreign: Vec<String> = write_set
            .iter()
            .filter(|(coll, row)| coll != collection || row != document_id)
            .map(|(coll, row)| format!("{coll}/{row}"))
            .collect();
        if foreign.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "delta for {collection}/{document_id} wrote {} row(s) outside its frame \
                 target: [{}]; a delta must carry exactly one document (cross-engine \
                 identity binds one surrogate per delta)",
                foreign.len(),
                foreign.join(", ")
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(c, r)| (c.to_string(), r.to_string()))
            .collect()
    }

    #[test]
    fn single_matching_row_is_accepted() {
        assert!(CoreLoop::single_document_write_set("users", "a", &ws(&[("users", "a")])).is_ok());
    }

    #[test]
    fn empty_write_set_is_accepted() {
        // A delete / no-op delta wrote no rows — nothing to materialize, no
        // contract violation.
        assert!(CoreLoop::single_document_write_set("users", "a", &ws(&[])).is_ok());
    }

    #[test]
    fn additional_row_is_rejected() {
        // Frame targets "a" but the delta also wrote "b": the extra row has no
        // surrogate and would be silently dropped.
        let err = CoreLoop::single_document_write_set(
            "users",
            "a",
            &ws(&[("users", "a"), ("users", "b")]),
        )
        .expect_err("multi-row delta must be rejected");
        assert!(
            err.contains("users/b"),
            "detail names the offending row: {err}"
        );
    }

    #[test]
    fn synthetic_frame_id_matching_no_written_row_is_rejected() {
        // The batch-coalesced bug: frame id "5_ops" matches no real written
        // row, so every written row is "foreign" and the delta is rejected
        // instead of materializing zero rows.
        let err = CoreLoop::single_document_write_set(
            "entries",
            "5_ops",
            &ws(&[("entries", "u1"), ("entries", "u2")]),
        )
        .expect_err("synthetic frame id must be rejected");
        assert!(err.contains("entries/u1") && err.contains("entries/u2"));
    }

    #[test]
    fn foreign_collection_is_rejected() {
        let err = CoreLoop::single_document_write_set("users", "a", &ws(&[("orders", "a")]))
            .expect_err("row in a different collection must be rejected");
        assert!(err.contains("orders/a"));
    }
}
