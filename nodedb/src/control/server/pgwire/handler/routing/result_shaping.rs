// SPDX-License-Identifier: BUSL-1.1

//! Result-shaping inputs for simple and prepared pgwire execution.

use pgwire::api::portal::Format;

use crate::control::server::response_shape::schema::OutputSchema;

/// `formats` is the client's result-format request. Each column's format is
/// resolved from it and the column's type where the response is encoded.
#[derive(Clone, Copy)]
pub(crate) struct ResultShaping<'a> {
    pub projection: Option<&'a OutputSchema>,
    pub formats: &'a Format,
}
