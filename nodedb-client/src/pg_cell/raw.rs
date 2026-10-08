// SPDX-License-Identifier: Apache-2.0

//! The raw bytes of one result cell, read from a `tokio_postgres::Row`.

use std::error::Error;

use tokio_postgres::types::{FromSql, Type};

/// The undecoded bytes of a non-NULL cell. It accepts every column type, so
/// `Row::try_get::<_, Option<RawCell>>` hands the bytes to [`super::decode_cell`],
/// which decodes them by the column type.
pub(crate) struct RawCell<'a>(pub(crate) &'a [u8]);

impl<'a> FromSql<'a> for RawCell<'a> {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(RawCell(raw))
    }

    fn accepts(_ty: &Type) -> bool {
        true
    }
}
