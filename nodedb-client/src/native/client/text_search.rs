// SPDX-License-Identifier: Apache-2.0

//! Native full-text search.
//!
//! The statement comes from the shared builder in `search_sql`, the one the
//! pgwire client sends, and the hits decode through the same row decoder.
//! Both clients return the same ids and scores for one search.

use std::collections::HashSet;

use nodedb_types::error::NodeDbResult;
use nodedb_types::result::SearchResult;
use nodedb_types::text_search::TextSearchParams;

use crate::row_decode::decode_search_hits;
use crate::search_sql::{TextSearchRequest, text_hit_source, text_search_sql};

use super::core::NativeClient;

impl NativeClient {
    pub(super) async fn text_search_impl(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        if allowed_ids.is_some_and(HashSet::is_empty) || top_k == 0 {
            return Ok(Vec::new());
        }
        let key = match allowed_ids {
            Some(_) => Some(self.identity_column(collection).await?),
            None => None,
        };
        let sql = text_search_sql(&TextSearchRequest {
            collection,
            field,
            query,
            top_k,
            params: &params,
            allowed: key.as_deref().zip(allowed_ids),
        });
        let result = self.query(&sql).await?;
        decode_search_hits(&text_hit_source(collection), &result.columns, &result.rows)
    }
}
