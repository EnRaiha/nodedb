// SPDX-License-Identifier: BUSL-1.1

//! Apply backup schedule marks to `SystemCatalog` redb.

use crate::control::security::catalog::backup_schedule_marks::StoredBackupScheduleMark;
use crate::control::security::catalog::{SystemCatalog, catalog_err};

/// Apply a `PutBackupScheduleMark` entry. A mark below the stored one of the
/// same incarnation writes nothing, so a replay or a late proposal from a
/// former leader never moves the mark back.
pub fn raise_mark(mark: &StoredBackupScheduleMark, catalog: &SystemCatalog) -> crate::Result<()> {
    catalog
        .raise_backup_schedule_mark(mark)
        .map(drop)
        .map_err(|e| catalog_err(&format!("put_backup_schedule_mark '{}'", mark.job), e))
}
