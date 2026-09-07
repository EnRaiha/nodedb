// SPDX-License-Identifier: Apache-2.0

//! Catalog wrapper that resolves CTE and derived-alias names as relations.

use nodedb_types::DatabaseId;

use crate::resolver::derived::open_subquery_relation;
use crate::types::{CollectionInfo, SqlCatalog, SqlCatalogError};

/// Catalog wrapper that answers for a synthesized relation before delegating
/// to the stored catalog.
pub(crate) struct CteCatalog<'a> {
    pub(crate) inner: &'a dyn SqlCatalog,
    /// Each synthesized relation name paired with the shape its body exposes.
    pub(crate) relations: Vec<(String, CollectionInfo)>,
}

impl<'a> CteCatalog<'a> {
    /// A catalog exposing one relation whose shape is not inferable.
    ///
    /// The recursive arm of a `WITH RECURSIVE` names the working table while
    /// planning its own body, so that arm's shape is not known yet.
    pub(crate) fn open(inner: &'a dyn SqlCatalog, name: &str) -> Self {
        Self {
            inner,
            relations: vec![(name.to_string(), open_subquery_relation(name))],
        }
    }
}

impl SqlCatalog for CteCatalog<'_> {
    fn get_collection(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        if let Some((_, info)) = self.relations.iter().find(|(key, _)| key == name) {
            return Ok(Some(info.clone()));
        }
        self.inner.get_collection(database_id, name)
    }
}
