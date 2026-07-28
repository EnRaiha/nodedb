// SPDX-License-Identifier: Apache-2.0

//! The declared shape of a timeseries collection, propagated from the
//! catalog to the Data Plane on collection registration.

/// A timeseries collection's DDL-declared column list plus its designated
/// `TIME_KEY` column.
///
/// This is the authority for the collection's storage layout. Without it the
/// Data Plane would have to derive a schema from whatever the first ingested
/// batch happened to contain, which cannot recover the declared column order,
/// the declared types, or — critically — which column is the time key. Every
/// path that needs to know "which column carries this collection's time"
/// reads [`Self::time_key`]; none may guess it from a column name.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct TimeseriesSchema {
    /// Name of the designated time-key column, exactly as declared.
    pub time_key: String,
    /// Declared `(column_name, type_str)` pairs in declaration order.
    pub columns: Vec<(String, String)>,
}

impl TimeseriesSchema {
    /// Position of the time key in `columns`, or `None` when the declared
    /// column list does not contain it (an inconsistent catalog record).
    pub fn time_key_index(&self) -> Option<usize> {
        self.columns.iter().position(|(n, _)| *n == self.time_key)
    }
}
