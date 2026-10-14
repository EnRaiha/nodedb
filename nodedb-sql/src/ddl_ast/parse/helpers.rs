// SPDX-License-Identifier: Apache-2.0

//! Shared name readers for the DDL parsers, built on the clause cursor.

use super::clause::ClauseCursor;
use crate::error::SqlError;

/// Read `[IF EXISTS] <name>` at the cursor. Returns `(if_exists, name)`.
pub(super) fn if_exists_name(
    c: &mut ClauseCursor<'_, '_>,
    what: &str,
) -> Result<(bool, String), SqlError> {
    let if_exists = c.eat_keywords(&["IF", "EXISTS"]);
    let name = c.ident(what)?;
    Ok((if_exists, name))
}

/// Read `[IF NOT EXISTS] <name>` at the cursor. Returns `(if_not_exists, name)`.
pub(super) fn if_not_exists_name(
    c: &mut ClauseCursor<'_, '_>,
    what: &str,
) -> Result<(bool, String), SqlError> {
    let if_not_exists = c.eat_keywords(&["IF", "NOT", "EXISTS"]);
    let name = c.ident(what)?;
    Ok((if_not_exists, name))
}
