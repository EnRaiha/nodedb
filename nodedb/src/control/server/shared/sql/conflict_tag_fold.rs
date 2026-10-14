// SPDX-License-Identifier: BUSL-1.1

//! The tag of a statement whose rows staged as separate `ON CONFLICT DO
//! UPDATE` point writes, each deciding its own verb.

use super::staging_predicates::StagedTagKind;

/// Folds the per-row verbs of one statement. The statement reports `UPDATE`
/// when every row it counted updated, and `INSERT` otherwise, as the
/// autocommit page reports.
#[derive(Debug, Default)]
pub struct ConflictTagFold {
    counted: usize,
    updated: usize,
}

impl ConflictTagFold {
    /// Note the tag one counted row staged under.
    pub fn note(&mut self, kind: StagedTagKind) {
        self.counted += 1;
        if kind == (StagedTagKind::KvUpsert { updated: true }) {
            self.updated += 1;
        }
    }

    /// The statement's tag: `kind` itself, unless `kind` is a conflict
    /// upsert, whose verb the rows decide.
    pub fn fold(&self, kind: StagedTagKind) -> StagedTagKind {
        match kind {
            StagedTagKind::KvUpsert { .. } => StagedTagKind::KvUpsert {
                updated: self.counted > 0 && self.updated == self.counted,
            },
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_row_updated_reports_update() {
        let mut fold = ConflictTagFold::default();
        fold.note(StagedTagKind::KvUpsert { updated: true });
        fold.note(StagedTagKind::KvUpsert { updated: true });
        assert_eq!(
            fold.fold(StagedTagKind::KvUpsert { updated: false }),
            StagedTagKind::KvUpsert { updated: true }
        );
    }

    #[test]
    fn one_inserted_row_reports_insert() {
        let mut fold = ConflictTagFold::default();
        fold.note(StagedTagKind::KvUpsert { updated: true });
        fold.note(StagedTagKind::KvUpsert { updated: false });
        assert_eq!(
            fold.fold(StagedTagKind::KvUpsert { updated: true }),
            StagedTagKind::KvUpsert { updated: false }
        );
    }

    #[test]
    fn other_tags_pass_through() {
        let mut fold = ConflictTagFold::default();
        fold.note(StagedTagKind::Insert);
        assert_eq!(fold.fold(StagedTagKind::Merge), StagedTagKind::Merge);
    }
}
