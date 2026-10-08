// SPDX-License-Identifier: Apache-2.0

//! Vector operation implementations for `NodeDbRemote`.

use std::collections::HashSet;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;

use crate::remote_parse::format_vector_array;
use crate::row_decode::search_hit::{DISTANCE_COLUMN, ID_COLUMN};
use crate::row_decode::{HitSource, decode_search_hits};
use crate::sql_escape::quote_identifier;

use super::super::sql::{build_vector_search_sql, render_metadata_filter_public};
use super::core::NodeDbRemote;

impl NodeDbRemote {
    pub(super) async fn vector_search_impl(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // An empty allowed set admits no candidate.
        if allowed_ids.is_some_and(HashSet::is_empty) {
            return Ok(Vec::new());
        }
        // The allowed ids restrict the key column, so the server lowers them
        // to the candidate set the index search ranks within.
        let key = match allowed_ids {
            Some(_) => Some(self.identity_column(collection).await?),
            None => None,
        };
        let allowed = key.as_deref().zip(allowed_ids);
        let sql = build_vector_search_sql(collection, query, k, filter, allowed)?;

        let (columns, rows) = self.query_raw(&sql, &[]).await?;
        decode_search_hits(
            &HitSource {
                op: "vector_search",
                collection,
                score_column: DISTANCE_COLUMN,
            },
            &columns,
            &rows,
        )
    }

    pub(super) async fn vector_insert_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // The vector lands on the column the caller names, not on whichever
        // vector column the planner picks when the column name is omitted.
        let key = self.identity_column(collection).await?;
        let metadata_param = metadata.as_ref().map(|_| "$2");
        let sql = vector_insert_sql(collection, &key, field_name, embedding, metadata_param);

        if let Some(d) = metadata {
            let meta_json = sonic_rs::to_string(&d)
                .map_err(|e| NodeDbError::storage(format!("metadata serialization: {e}")))?;
            self.execute_raw(&sql, &[&id, &meta_json]).await?;
        } else {
            self.execute_raw(&sql, &[&id]).await?;
        }
        Ok(())
    }

    pub(super) async fn vector_search_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Field-aware path: use the 2-arg form of `vector_distance` so
        // the planner scopes the HNSW lookup to the named column. The
        // single-arg form `vector_distance(ARRAY[...])` only works on
        // collections that have exactly one vector column.
        let coll = quote_identifier(collection);
        let field = quote_identifier(field_name);
        let vec_lit = format_vector_array(query);
        let where_clause = match filter {
            Some(f) => {
                let rendered = render_metadata_filter_public(f)?;
                format!(" WHERE {rendered}")
            }
            None => String::new(),
        };
        let sql = format!(
            "SELECT {ID_COLUMN}, vector_distance({field}, {vec_lit}) AS {DISTANCE_COLUMN} \
             FROM {coll}{where_clause} \
             ORDER BY vector_distance({field}, {vec_lit}) \
             LIMIT {k}"
        );

        let (columns, rows) = self.query_raw(&sql, &[]).await?;
        decode_search_hits(
            &HitSource {
                op: "vector_search_field",
                collection,
                score_column: DISTANCE_COLUMN,
            },
            &columns,
            &rows,
        )
    }

    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let key = self.identity_column(collection).await?;
        let meta_json = match metadata {
            Some(d) => sonic_rs::to_string(&d)
                .map_err(|e| NodeDbError::storage(format!("metadata serialization: {e}")))?,
            None => "{}".into(),
        };
        let sql = vector_insert_sql(
            collection,
            &key,
            DEFAULT_VECTOR_COLUMN,
            embedding,
            Some("$2::jsonb"),
        );
        self.execute_raw(&sql, &[&id, &meta_json]).await?;
        Ok(())
    }

    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let key = self.identity_column(collection).await?;
        let sql = format!(
            "DELETE FROM {} WHERE {} = $1",
            quote_identifier(collection),
            quote_identifier(&key)
        );
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}

/// The vector column a field-less `vector_insert` writes.
const DEFAULT_VECTOR_COLUMN: &str = "embedding";

/// `INSERT INTO <collection> (<key>, <field>[, metadata]) VALUES ($1,
/// ARRAY[...][, <metadata>])`.
///
/// `key` is the collection's identity column, which holds the id bound as
/// `$1`. `metadata` is the SQL expression of the metadata parameter, or
/// `None` for no metadata column.
fn vector_insert_sql(
    collection: &str,
    key: &str,
    field: &str,
    embedding: &[f32],
    metadata: Option<&str>,
) -> String {
    let collection = quote_identifier(collection);
    let key = quote_identifier(key);
    let field = quote_identifier(field);
    let vector = format_vector_array(embedding);
    match metadata {
        Some(param) => format!(
            "INSERT INTO {collection} ({key}, {field}, metadata) VALUES ($1, {vector}, {param})"
        ),
        None => format!("INSERT INTO {collection} ({key}, {field}) VALUES ($1, {vector})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_vector_insert_writes_the_id_under_the_identity_column() {
        assert_eq!(
            vector_insert_sql("vecs", "sku", "embedding", &[1.0, 0.5], Some("$2::jsonb")),
            "INSERT INTO \"vecs\" (\"sku\", \"embedding\", metadata) \
             VALUES ($1, ARRAY[1,0.5], $2::jsonb)"
        );
        assert_eq!(
            vector_insert_sql("vecs", "id", "img", &[2.0], None),
            "INSERT INTO \"vecs\" (\"id\", \"img\") VALUES ($1, ARRAY[2])"
        );
    }
}
