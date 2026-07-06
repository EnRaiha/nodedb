// SPDX-License-Identifier: BUSL-1.1

//! `CREATE VECTOR INDEX` DSL handler.
//!
//! Parses the full quantization-parameter surface advertised in `docs/vectors.md`:
//! INDEX_TYPE (hnsw | hnsw_pq | ivf_pq), PQ_M, IVF_CELLS, IVF_NPROBE. Unknown
//! INDEX_TYPE values are rejected at the DDL layer; invalid combinations
//! (e.g. PQ_M that does not divide DIM) are rejected before reaching the engine.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::DatabaseId;
use crate::types::TraceId;
use nodedb_physical::physical_plan::VectorOp;

use super::super::super::result::{DdlError, DdlResult};
use super::helpers::{find_param_str, find_param_usize};
use super::support::ddl_err;

/// Supported INDEX_TYPE keywords — kept in sync with
/// `nodedb_vector::index_config::IndexType`.
const KNOWN_INDEX_TYPES: &[&str] = &["hnsw", "hnsw_pq", "ivf_pq"];

/// CREATE VECTOR INDEX <name> ON <collection> [(<column>)]
///   [METRIC cosine|l2|inner_product|...] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>]
///   [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M <m>] [IVF_CELLS <n>] [IVF_NPROBE <n>]
///
/// The optional `(<column>)` names the embedding column the index covers, so
/// one collection can carry several vector indexes (e.g. a text-embedding and
/// an image-embedding column). Omitted → the collection's default vector field.
pub async fn create_vector_index(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    parts: &[&str],
) -> Result<Vec<DdlResult>, DdlError> {
    if parts.len() < 6 {
        return Err(ddl_err(
            "42601",
            "syntax: CREATE VECTOR INDEX <name> ON <collection> [(<column>)] \
             [METRIC cosine|l2] [M <m>] [EF_CONSTRUCTION <ef>] [DIM <dim>] \
             [INDEX_TYPE hnsw|hnsw_pq|ivf_pq] [PQ_M <m>] [IVF_CELLS <n>] [IVF_NPROBE <n>]",
        ));
    }

    let index_name = parts[3];
    if !parts[4].eq_ignore_ascii_case("ON") {
        return Err(ddl_err("42601", "expected ON after index name"));
    }
    let collection = parts[5];
    let tenant_id = identity.tenant_id;

    // Optional `(<column>)` right after the collection name. Everything after
    // it (or after the collection name, if absent) is option keywords.
    let (field_name, opts_start) = parse_optional_column(parts, 6);

    let upper_parts: Vec<String> = parts[opts_start..]
        .iter()
        .map(|p| p.to_uppercase())
        .collect();

    let metric = find_param_str(&upper_parts, "METRIC").unwrap_or_else(|| "COSINE".into());
    let m = find_param_usize(&upper_parts, "M").unwrap_or(16);
    let ef_construction = find_param_usize(&upper_parts, "EF_CONSTRUCTION").unwrap_or(200);
    let dim = find_param_usize(&upper_parts, "DIM").unwrap_or(0);

    // Quantization parameters (advertised in docs/vectors.md).
    let index_type = find_param_str(&upper_parts, "INDEX_TYPE")
        .map(|s| s.to_lowercase())
        .unwrap_or_default();
    let pq_m = find_param_usize(&upper_parts, "PQ_M").unwrap_or(0);
    let ivf_cells = find_param_usize(&upper_parts, "IVF_CELLS").unwrap_or(0);
    let ivf_nprobe = find_param_usize(&upper_parts, "IVF_NPROBE").unwrap_or(0);

    validate_quantization(&index_type, dim, pq_m, ivf_cells, ivf_nprobe)?;

    crate::control::server::shared::ddl::owner::propose_owner(
        state,
        "vector_index",
        tenant_id,
        index_name,
        &identity.username,
    )?;

    let vshard =
        crate::types::VShardId::from_collection_in_database(DatabaseId::DEFAULT, collection);
    let set_params_plan = PhysicalPlan::Vector(VectorOp::SetParams {
        collection: collection.to_string(),
        field_name: field_name.clone(),
        m,
        ef_construction,
        metric: metric.to_lowercase(),
        index_type: index_type.clone(),
        pq_m,
        ivf_cells,
        ivf_nprobe,
    });

    // Persist the index parameters to the WAL before dispatching to the Data
    // Plane. Without this the params live only in Data Plane memory and are
    // lost on restart: `replay_vector_wal` re-registers them from this record
    // so that secondary vector indexes over document collections can be
    // rebuilt (with their real per-row surrogates) from the journalled
    // document writes on the next startup.
    crate::control::server::wal_dispatch::wal_append_if_write(
        &state.wal,
        tenant_id,
        vshard,
        DatabaseId::DEFAULT,
        &set_params_plan,
    )
    .map_err(|e| ddl_err("XX000", format!("persist vector index params to WAL: {e}")))?;

    let _ = crate::control::server::dispatch_utils::dispatch_to_data_plane(
        state,
        tenant_id,
        crate::types::DatabaseId::DEFAULT,
        vshard,
        set_params_plan,
        TraceId::ZERO,
    )
    .await;

    state.audit_record(
        crate::control::security::audit::AuditEvent::AdminAction,
        Some(tenant_id),
        &identity.username,
        &format!(
            "created vector index '{index_name}' on '{collection}'{} \
             (metric={metric}, m={m}, ef_construction={ef_construction}, dim={dim}, \
             index_type={}, pq_m={pq_m}, ivf_cells={ivf_cells}, ivf_nprobe={ivf_nprobe})",
            if field_name.is_empty() {
                String::new()
            } else {
                format!(" column '{field_name}'")
            },
            if index_type.is_empty() {
                "hnsw"
            } else {
                &index_type
            }
        ),
    );

    Ok(vec![DdlResult::Status {
        command: "CREATE VECTOR INDEX".to_string(),
        rows_affected: None,
    }])
}

/// If the token(s) right after the collection name form a `(<column>)` group,
/// return the unquoted column name and the index where the option keywords
/// begin. Otherwise return `("", start)`.
///
/// Handles `(col)` as one token and `( col )` split across three; an
/// unterminated `(` is treated as "no column" rather than an error.
fn parse_optional_column(parts: &[&str], start: usize) -> (String, usize) {
    let Some(first) = parts.get(start) else {
        return (String::new(), start);
    };
    if !first.starts_with('(') {
        return (String::new(), start);
    }
    let mut end = start;
    while end < parts.len() && !parts[end].ends_with(')') {
        end += 1;
    }
    if end >= parts.len() {
        return (String::new(), start);
    }
    let joined: String = parts[start..=end].concat();
    let col = joined
        .trim_start_matches('(')
        .trim_end_matches(')')
        .trim()
        .trim_matches('"')
        .to_string();
    if col.is_empty() {
        return (String::new(), start);
    }
    (col, end + 1)
}

fn validate_quantization(
    index_type: &str,
    dim: usize,
    pq_m: usize,
    ivf_cells: usize,
    ivf_nprobe: usize,
) -> Result<(), DdlError> {
    if !index_type.is_empty() && !KNOWN_INDEX_TYPES.contains(&index_type) {
        return Err(ddl_err(
            "42601",
            format!(
                "unknown index_type '{index_type}'; supported: {}",
                KNOWN_INDEX_TYPES.join(", ")
            ),
        ));
    }

    let uses_pq = matches!(index_type, "hnsw_pq" | "ivf_pq");
    if uses_pq && pq_m > 0 && dim > 0 && !dim.is_multiple_of(pq_m) {
        return Err(ddl_err(
            "42601",
            format!("pq_m ({pq_m}) must divide dim ({dim}) evenly"),
        ));
    }

    if !uses_pq && (pq_m > 0 || ivf_cells > 0 || ivf_nprobe > 0) {
        return Err(ddl_err(
            "42601",
            "pq_m / ivf_cells / ivf_nprobe require INDEX_TYPE hnsw_pq or ivf_pq",
        ));
    }

    if index_type == "ivf_pq" && ivf_nprobe > 0 && ivf_cells > 0 && ivf_nprobe > ivf_cells {
        return Err(ddl_err(
            "42601",
            format!("ivf_nprobe ({ivf_nprobe}) must not exceed ivf_cells ({ivf_cells})"),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::parse_optional_column;

    #[test]
    fn no_column_clause() {
        let parts = [
            "CREATE", "VECTOR", "INDEX", "idx", "ON", "coll", "METRIC", "cosine",
        ];
        assert_eq!(parse_optional_column(&parts, 6), (String::new(), 6));
    }

    #[test]
    fn column_one_token() {
        let parts = [
            "CREATE",
            "VECTOR",
            "INDEX",
            "idx",
            "ON",
            "coll",
            "(text_emb)",
            "METRIC",
            "cosine",
        ];
        assert_eq!(
            parse_optional_column(&parts, 6),
            ("text_emb".to_string(), 7)
        );
    }

    #[test]
    fn column_split_tokens() {
        let parts = [
            "CREATE", "VECTOR", "INDEX", "idx", "ON", "coll", "(", "text_emb", ")", "DIM", "512",
        ];
        assert_eq!(
            parse_optional_column(&parts, 6),
            ("text_emb".to_string(), 9)
        );
    }

    #[test]
    fn column_at_end_of_statement() {
        let parts = [
            "CREATE",
            "VECTOR",
            "INDEX",
            "idx",
            "ON",
            "coll",
            "(image_emb)",
        ];
        assert_eq!(
            parse_optional_column(&parts, 6),
            ("image_emb".to_string(), 7)
        );
    }

    #[test]
    fn unterminated_paren_is_ignored() {
        let parts = [
            "CREATE", "VECTOR", "INDEX", "idx", "ON", "coll", "(oops", "METRIC",
        ];
        assert_eq!(parse_optional_column(&parts, 6), (String::new(), 6));
    }
}
